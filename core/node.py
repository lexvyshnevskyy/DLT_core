from __future__ import annotations

import json
from typing import Any, Dict, Optional

import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from message_filters import ApproximateTimeSynchronizer, Subscriber

from core.srv import Query
from database.srv import Query as DatabaseQuery
from msgs.msg import Ads, E720, Measurement

from .classes.core_controller import CoreController
from .classes.temperature_control import LegacyPidController, RampHoldProgram


class CoreNode(Node):
    def __init__(self) -> None:
        super().__init__('core')

        self.declare_parameter('ads_topic', '/ads1256')
        self.declare_parameter('measure_topic', '/measure_device')
        self.declare_parameter('ltm_measurement_topic', '/ltm2985/measurement')
        self.declare_parameter('hmi_commands_topic', '/hmi/commands')
        self.declare_parameter('database_service', '/database/query')
        self.declare_parameter('sync_queue_size', 10)
        self.declare_parameter('sync_slop', 0.5)
        self.declare_parameter('enable_database_client', True)
        self.declare_parameter('enable_pwm_controller', False)
        self.declare_parameter('enable_temperature_control', False)
        self.declare_parameter('control_period_sec', 1.0)
        self.declare_parameter('control_channel', 9)
        self.declare_parameter('monitor_channel', 3)
        self.declare_parameter('target_temperature_c', 100.0)
        self.declare_parameter('pid_k', 100.0)
        self.declare_parameter('pid_ti', 3000.0)
        self.declare_parameter('pid_td', 5.0)
        self.declare_parameter('pid_sample_time_model', 100.0)
        self.declare_parameter('profile_ramp_seconds_per_degree', 6.0)
        self.declare_parameter('profile_hold_seconds', 86400.0)
        self.declare_parameter('pwm_pin', 18)
        self.declare_parameter('pwm_frequency', 100)
        self.declare_parameter('pwm_range', 1000)
        self.declare_parameter('status_topic', '/core/temperature_control_status')

        self.ads_topic = str(self.get_parameter('ads_topic').value)
        self.measure_topic = str(self.get_parameter('measure_topic').value)
        self.ltm_measurement_topic = str(self.get_parameter('ltm_measurement_topic').value)
        self.hmi_commands_topic = str(self.get_parameter('hmi_commands_topic').value)
        self.database_service = str(self.get_parameter('database_service').value)
        self.sync_queue_size = int(self.get_parameter('sync_queue_size').value)
        self.sync_slop = float(self.get_parameter('sync_slop').value)
        self.enable_database_client = bool(self.get_parameter('enable_database_client').value)
        self.enable_pwm_controller = bool(self.get_parameter('enable_pwm_controller').value)
        self.enable_temperature_control = bool(self.get_parameter('enable_temperature_control').value)
        self.control_period_sec = float(self.get_parameter('control_period_sec').value)
        self.control_channel = int(self.get_parameter('control_channel').value)
        self.monitor_channel = int(self.get_parameter('monitor_channel').value)
        self.target_temperature_c = float(self.get_parameter('target_temperature_c').value)
        self.pwm_pin = int(self.get_parameter('pwm_pin').value)
        self.pwm_frequency = int(self.get_parameter('pwm_frequency').value)
        self.pwm_range = int(self.get_parameter('pwm_range').value)

        self.latest_ads: Optional[Ads] = None
        self.latest_measurement: Optional[E720] = None
        self.latest_ltm_by_channel: Dict[int, Measurement] = {}
        self.latest_temperature_c_by_channel: Dict[int, float] = {}
        self.db_client = None
        self.controller: Optional[CoreController] = None

        self.status_publisher = self.create_publisher(String, str(self.get_parameter('status_topic').value), 10)
        self.hmi_publisher = self.create_publisher(String, self.hmi_commands_topic, 10)
        self.service = self.create_service(Query, 'query', self.handle_query)

        self.ads_subscriber = Subscriber(self, Ads, self.ads_topic)
        self.measure_subscriber = Subscriber(self, E720, self.measure_topic)
        self.sync = ApproximateTimeSynchronizer(
            [self.ads_subscriber, self.measure_subscriber],
            queue_size=self.sync_queue_size,
            slop=self.sync_slop,
        )
        self.sync.registerCallback(self.sensor_callback)
        self.ltm_subscription = self.create_subscription(
            Measurement,
            self.ltm_measurement_topic,
            self.ltm_callback,
            50,
        )

        if self.enable_database_client:
            self.db_client = self.create_client(DatabaseQuery, self.database_service)
            if self.db_client.wait_for_service(timeout_sec=2.0):
                self.get_logger().info(f'Database service available: {self.database_service}')
            else:
                self.get_logger().warn(
                    f'Database service not available at startup: {self.database_service}. '
                    'The node will continue without a live DB link.'
                )

        if self.enable_pwm_controller:
            try:
                self.controller = CoreController(
                    pwm_ch1_pin=self.pwm_pin,
                    pwm_ch2_pin=None,
                    pwm_frequency=self.pwm_frequency,
                    pwm_range=self.pwm_range,
                )
                self.get_logger().info(
                    f'PWM controller initialized on GPIO {self.pwm_pin} '
                    f'(freq={self.pwm_frequency}, range={self.pwm_range}).'
                )
            except Exception as exc:
                self.controller = None
                self.get_logger().error(f'Failed to initialize PWM controller: {exc}')

        self.temperature_program = RampHoldProgram(
            ramp_seconds_per_degree=float(self.get_parameter('profile_ramp_seconds_per_degree').value),
            hold_seconds=float(self.get_parameter('profile_hold_seconds').value),
        )
        self.pid = LegacyPidController(
            k=float(self.get_parameter('pid_k').value),
            ti=float(self.get_parameter('pid_ti').value),
            td=float(self.get_parameter('pid_td').value),
            sample_time_model=float(self.get_parameter('pid_sample_time_model').value),
            output_min=0,
            output_max=self.pwm_range,
        )
        self.control_active = self.enable_temperature_control
        self.last_pwm_command = 0
        self.current_setpoint_c: Optional[float] = None

        self.control_timer = self.create_timer(self.control_period_sec, self._control_loop)

        self.get_logger().info(
            'Core node ready. '
            f'ads_topic={self.ads_topic}, measure_topic={self.measure_topic}, '
            f'ltm_measurement_topic={self.ltm_measurement_topic}, '
            f'control_channel={self.control_channel}, monitor_channel={self.monitor_channel}'
        )

    def sensor_callback(self, ads_msg: Ads, meas_msg: E720) -> None:
        self.latest_ads = ads_msg
        self.latest_measurement = meas_msg
        self.get_logger().debug('Received synchronized ADS1256 and measure_device frames.')

    def ltm_callback(self, msg: Measurement) -> None:
        self.latest_ltm_by_channel[int(msg.channel)] = msg

        temp_c = self._measurement_to_celsius(msg)
        if temp_c is not None:
            self.latest_temperature_c_by_channel[int(msg.channel)] = temp_c

            if (
                self.control_active
                and not self.temperature_program.is_active()
                and int(msg.channel) == self.control_channel
            ):
                self._start_hold_profile(temp_c)

    def handle_query(self, request: Query.Request, response: Query.Response) -> Query.Response:
        self.get_logger().info(f'Received core query: {request.query}')
        try:
            payload = json.loads(request.query) if request.query else {}
            result = self.process_query(payload)
        except Exception as exc:
            result = {'result': 'False', 'error': str(exc)}
        response.response = json.dumps(result)
        return response

    def process_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            'result': 'Ok',
            'sensors': self._sensor_snapshot(),
            'temperature_control': self._temperature_control_snapshot(),
            'database_service_ready': bool(self.db_client and self.db_client.service_is_ready()),
        }

        hmi_message = query.get('hmi_message')
        if hmi_message is not None:
            msg = String()
            msg.data = str(hmi_message)
            self.hmi_publisher.publish(msg)
            result['hmi_published'] = True

        pwm = query.get('pwm')
        if pwm is not None:
            result['pwm'] = self._apply_pwm(pwm)

        temperature_control = query.get('temperature_control')
        if temperature_control is not None:
            result['temperature_control'] = self._apply_temperature_control_command(temperature_control)

        result['echo'] = query
        return result

    def _apply_pwm(self, pwm: Dict[str, Any]) -> Dict[str, Any]:
        if self.controller is None:
            raise RuntimeError(
                'PWM controller is not enabled. Set parameter enable_pwm_controller:=true and ensure pigpio is available.'
            )

        ch1 = pwm.get('ch1')
        ch2 = pwm.get('ch2')
        if ch1 is not None:
            self.controller.set_channel_1(int(ch1))
            self.last_pwm_command = int(ch1)
        if ch2 is not None:
            self.controller.set_channel_2(int(ch2))
        return self.controller.snapshot()

    def _apply_temperature_control_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        enabled = command.get('enabled')
        if enabled is not None:
            self.control_active = bool(enabled)
            if not self.control_active:
                self.temperature_program.reset()
                self.current_setpoint_c = None
                self.pid.reset(output=0)
                self._set_pwm_output(0)

        target_c = command.get('target_c')
        if target_c is not None:
            self.target_temperature_c = float(target_c)

        restart_profile = command.get('restart_profile')
        if bool(restart_profile):
            control_temp = self.latest_temperature_c_by_channel.get(self.control_channel)
            if control_temp is None:
                raise RuntimeError(
                    f'No valid temperature yet on control channel {self.control_channel}'
                )
            self._start_hold_profile(control_temp)

        pwm_override = command.get('pwm_override')
        if pwm_override is not None:
            self.pid.reset(output=int(pwm_override))
            self._set_pwm_output(int(pwm_override))

        return self._temperature_control_snapshot()

    def _start_hold_profile(self, current_temp_c: float) -> None:
        self.temperature_program.start(current_temp_c=current_temp_c, target_temp_c=self.target_temperature_c)
        self.pid.reset(output=self.last_pwm_command)
        self.current_setpoint_c = current_temp_c
        self.get_logger().info(
            'Temperature control profile started: '
            f'current={current_temp_c:.2f} C, target={self.target_temperature_c:.2f} C'
        )

    def _control_loop(self) -> None:
        if not self.control_active:
            return

        control_temp_c = self.latest_temperature_c_by_channel.get(self.control_channel)
        if control_temp_c is None:
            return

        if not self.temperature_program.is_active():
            self._start_hold_profile(control_temp_c)

        setpoint_c = self.temperature_program.step(self.control_period_sec)
        if setpoint_c is None:
            return

        self.current_setpoint_c = setpoint_c
        pwm_command = self.pid.update(setpoint_c=setpoint_c, measured_c=control_temp_c)
        self._set_pwm_output(pwm_command)
        self._publish_control_status()

    def _set_pwm_output(self, value: int) -> None:
        value = max(0, min(int(value), self.pwm_range))
        self.last_pwm_command = value
        if self.controller is not None:
            self.controller.set_channel_1(value)

    def _publish_control_status(self) -> None:
        msg = String()
        msg.data = json.dumps(self._temperature_control_snapshot())
        self.status_publisher.publish(msg)

    def _temperature_control_snapshot(self) -> Dict[str, Any]:
        control_temp_c = self.latest_temperature_c_by_channel.get(self.control_channel)
        monitor_temp_c = self.latest_temperature_c_by_channel.get(self.monitor_channel)
        return {
            'enabled': self.control_active,
            'target_temperature_c': self.target_temperature_c,
            'current_setpoint_c': self.current_setpoint_c,
            'control_channel': self.control_channel,
            'control_temperature_c': control_temp_c,
            'monitor_channel': self.monitor_channel,
            'monitor_temperature_c': monitor_temp_c,
            'pwm_pin': self.pwm_pin,
            'pwm_command': self.last_pwm_command,
            'pwm_enabled': self.controller is not None,
            'program': self.temperature_program.snapshot(),
            'pid': self.pid.snapshot(),
        }

    def _sensor_snapshot(self) -> Dict[str, Any]:
        return {
            'ads': self._message_to_dict(self.latest_ads),
            'measure_device': self._message_to_dict(self.latest_measurement),
            'ltm_measurements': {
                str(channel): self._message_to_dict(msg)
                for channel, msg in sorted(self.latest_ltm_by_channel.items())
            },
            'ltm_temperature_c': {
                str(channel): value
                for channel, value in sorted(self.latest_temperature_c_by_channel.items())
            },
        }

    def _measurement_to_celsius(self, msg: Measurement) -> Optional[float]:
        if not bool(msg.valid):
            return None

        msg_type = str(msg.type).lower()
        value = float(msg.value)
        if msg_type == 'temperature_k':
            return value - 273.15
        if msg_type.startswith('temperature'):
            return value
        return None

    @staticmethod
    def _message_to_dict(msg: Optional[Any]) -> Optional[Dict[str, Any]]:
        if msg is None:
            return None

        result: Dict[str, Any] = {}
        for field in getattr(msg, 'get_fields_and_field_types', lambda: {})().keys():
            value = getattr(msg, field)
            if hasattr(value, 'data'):
                result[field] = value.data
            elif hasattr(value, 'sec') and hasattr(value, 'nanosec'):
                result[field] = {'sec': value.sec, 'nanosec': value.nanosec}
            elif hasattr(value, 'frame_id') and hasattr(value, 'stamp'):
                result[field] = {
                    'frame_id': value.frame_id,
                    'stamp': {'sec': value.stamp.sec, 'nanosec': value.stamp.nanosec},
                }
            else:
                result[field] = value
        return result

    def shutdown(self) -> None:
        self._set_pwm_output(0)
        if self.controller is not None:
            self.controller.stop()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = CoreNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.shutdown()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
