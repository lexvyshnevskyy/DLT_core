from __future__ import annotations

import json
from typing import Any, Dict, Optional

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

from database.srv import Query
from msgs.msg import Measurement

from .classes.core_controller import CoreController
from .classes.temperature_control import TemperatureControlWorker


class CoreNode(Node):
    def __init__(self) -> None:
        super().__init__('core')

        self.declare_parameter('measurement_topic', 'ltm2985/measurement')
        self.declare_parameter('hmi_commands_topic', '/hmi/commands')
        self.declare_parameter('database_service', '/database/query')
        self.declare_parameter('enable_database_client', True)
        self.declare_parameter('enable_pwm_controller', False)
        self.declare_parameter('pwm_pin', 18)
        self.declare_parameter('pwm_frequency_hz', 10)
        self.declare_parameter('pwm_range', 1000)
        self.declare_parameter('control_channel', 9)
        self.declare_parameter('monitor_channel', 3)
        self.declare_parameter('target_k', 373.15)
        self.declare_parameter('control_period_sec', 1.0)
        self.declare_parameter('measurement_timeout_sec', 5.0)
        self.declare_parameter('kp', 25.0)
        self.declare_parameter('ki', 0.08)
        self.declare_parameter('deadband_k', 0.3)
        self.declare_parameter('max_output_step', 60)

        self.measurement_topic = str(self.get_parameter('measurement_topic').value)
        self.hmi_commands_topic = str(self.get_parameter('hmi_commands_topic').value)
        self.database_service = str(self.get_parameter('database_service').value)
        self.enable_database_client = bool(self.get_parameter('enable_database_client').value)
        self.enable_pwm_controller = bool(self.get_parameter('enable_pwm_controller').value)

        self.latest_measurements: Dict[int, Measurement] = {}
        self.db_client = None
        self.controller: Optional[CoreController] = None
        self.temperature_control: Optional[TemperatureControlWorker] = None

        self.hmi_publisher = self.create_publisher(String, self.hmi_commands_topic, 10)
        self.service = self.create_service(Query, 'query', self.handle_query)
        self.measurement_subscription = self.create_subscription(
            Measurement,
            self.measurement_topic,
            self.measurement_callback,
            50,
        )

        if self.enable_database_client:
            self.db_client = self.create_client(Query, self.database_service)
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
                    pwm_pin=int(self.get_parameter('pwm_pin').value),
                    pwm_frequency_hz=int(self.get_parameter('pwm_frequency_hz').value),
                    pwm_range=int(self.get_parameter('pwm_range').value),
                )
                self.temperature_control = TemperatureControlWorker(
                    set_output_callback=self.controller.set_heater_output,
                    control_channel=int(self.get_parameter('control_channel').value),
                    monitor_channel=int(self.get_parameter('monitor_channel').value),
                    target_k=float(self.get_parameter('target_k').value),
                    control_period_sec=float(self.get_parameter('control_period_sec').value),
                    measurement_timeout_sec=float(self.get_parameter('measurement_timeout_sec').value),
                    kp=float(self.get_parameter('kp').value),
                    ki=float(self.get_parameter('ki').value),
                    deadband_k=float(self.get_parameter('deadband_k').value),
                    max_output_step=int(self.get_parameter('max_output_step').value),
                    output_min=0,
                    output_max=int(self.get_parameter('pwm_range').value),
                )
                self.temperature_control.start()
                self.get_logger().info('PWM controller and threaded temperature control initialized.')
            except Exception as exc:
                self.controller = None
                self.temperature_control = None
                self.get_logger().error(f'Failed to initialize PWM controller: {exc}')

        self.get_logger().info(
            'Core node ready. '
            f'measurement_topic={self.measurement_topic}, '
            f'hmi_commands_topic={self.hmi_commands_topic}, '
            f'control_channel={int(self.get_parameter("control_channel").value)}, '
            f'monitor_channel={int(self.get_parameter("monitor_channel").value)}'
        )

    def measurement_callback(self, msg: Measurement) -> None:
        self.latest_measurements[int(msg.channel)] = msg
        if self.temperature_control is not None and msg.type in ('temperature_K', 'temperature_C'):
            try:
                self.temperature_control.update_measurement(
                    channel=int(msg.channel),
                    raw_value=float(msg.value),
                    raw_type=str(msg.type),
                    valid=bool(msg.valid),
                )
            except ValueError as exc:
                self.get_logger().warning(str(exc))

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
            'measurements': self._measurement_snapshot(),
            'database_service_ready': bool(self.db_client and self.db_client.service_is_ready()),
            'temperature_control': self.temperature_control.get_snapshot() if self.temperature_control else None,
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
            result['temperature_control'] = self._apply_temperature_control(temperature_control)

        result['echo'] = query
        return result

    def _apply_pwm(self, pwm: Dict[str, Any]) -> Dict[str, Any]:
        if self.controller is None:
            raise RuntimeError(
                'PWM controller is not enabled. Set parameter enable_pwm_controller:=true and ensure pigpio is available.'
            )

        heater = pwm.get('heater')
        if heater is None:
            heater = pwm.get('duty_cycle')
        if heater is None:
            raise RuntimeError('Manual PWM request must contain heater or duty_cycle.')
        self.controller.set_heater_output(int(heater))
        return self.controller.snapshot()

    def _apply_temperature_control(self, config: Dict[str, Any]) -> Dict[str, Any]:
        if self.temperature_control is None:
            raise RuntimeError(
                'Temperature control is not enabled. Set enable_pwm_controller:=true and ensure pigpio is available.'
            )

        return self.temperature_control.configure(
            enabled=self._opt_bool(config, 'enabled'),
            target_k=self._opt_float(config, 'target_k'),
            control_channel=self._opt_int(config, 'control_channel'),
            monitor_channel=self._opt_int(config, 'monitor_channel'),
            kp=self._opt_float(config, 'kp'),
            ki=self._opt_float(config, 'ki'),
            deadband_k=self._opt_float(config, 'deadband_k'),
            max_output_step=self._opt_int(config, 'max_output_step'),
            control_period_sec=self._opt_float(config, 'control_period_sec'),
            measurement_timeout_sec=self._opt_float(config, 'measurement_timeout_sec'),
            reset_integral=bool(config.get('reset_integral', False)),
        )

    def _measurement_snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {
            str(channel): self._message_to_dict(msg)
            for channel, msg in sorted(self.latest_measurements.items())
        }

    @staticmethod
    def _opt_bool(data: Dict[str, Any], key: str) -> Optional[bool]:
        return bool(data[key]) if key in data else None

    @staticmethod
    def _opt_int(data: Dict[str, Any], key: str) -> Optional[int]:
        return int(data[key]) if key in data else None

    @staticmethod
    def _opt_float(data: Dict[str, Any], key: str) -> Optional[float]:
        return float(data[key]) if key in data else None

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
        if self.temperature_control is not None:
            self.temperature_control.stop()
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
