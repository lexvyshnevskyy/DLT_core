from __future__ import annotations

import json
import queue
import threading
import time
from typing import Any, Dict, Optional

import rclpy
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from std_msgs.msg import String

from database.srv import Query
from msgs.msg import E720, Measurement

from .classes.core_controller import CoreController
from .classes.temperature_control import TemperatureControlWorker
from .e720_util import e720_from_msg
from .measurement_log import build_measurement_row, insert_measurement_immediate
from .program_manager import ProgramExperimentManager


class CoreNode(Node):
    def __init__(self) -> None:
        super().__init__('core')

        self.declare_parameter('measurement_topic', 'ltm2985/measurement')
        self.declare_parameter('hmi_commands_topic', '/hmi/commands')
        self.declare_parameter('database_service', '/database/query')
        self.declare_parameter('experiment_status_topic', 'core/experiment/status')
        self.declare_parameter('enable_database_client', True)
        self.declare_parameter('enable_program_scheduler', True)
        self.declare_parameter('enable_pwm_controller', False)
        self.declare_parameter('pwm_pin', 18)
        self.declare_parameter('pwm_pin_ch2', 19)
        self.declare_parameter('pwm_frequency_hz', 10)
        self.declare_parameter('pwm_range', 1000)
        self.declare_parameter('control_channel', 9)
        self.declare_parameter('monitor_channel', 3)
        self.declare_parameter('target_k', 373.15)
        self.declare_parameter('measure_topic', '/measure_device')
        self.declare_parameter('control_period_sec', 1.0)
        self.declare_parameter('control_watchdog_period_sec', 0.25)
        self.declare_parameter('experiment_status_publish_period_sec', 0.25)
        self.declare_parameter('enable_measurement_logging', True)
        self.declare_parameter('measurement_log_e720_max_age_sec', 1.0)
        self.declare_parameter('measurement_timeout_sec', 5.0)
        self.declare_parameter('database_query_timeout_sec', 15.0)
        self.declare_parameter('kp', 25.0)
        self.declare_parameter('ki', 0.08)
        self.declare_parameter('deadband_k', 0.3)
        self.declare_parameter('max_output_step', 60)

        self.measurement_topic = str(self.get_parameter('measurement_topic').value)
        self.measure_topic = str(self.get_parameter('measure_topic').value)
        self.hmi_commands_topic = str(self.get_parameter('hmi_commands_topic').value)
        self.database_service = str(self.get_parameter('database_service').value)
        self.experiment_status_topic = str(self.get_parameter('experiment_status_topic').value)
        self.enable_database_client = bool(self.get_parameter('enable_database_client').value)
        self.enable_program_scheduler = bool(self.get_parameter('enable_program_scheduler').value)
        self.enable_pwm_controller = bool(self.get_parameter('enable_pwm_controller').value)
        self.database_query_timeout_sec = float(self.get_parameter('database_query_timeout_sec').value)
        self.control_period_sec = float(self.get_parameter('control_period_sec').value)
        self.control_watchdog_period_sec = float(self.get_parameter('control_watchdog_period_sec').value)
        self.experiment_status_publish_period_sec = float(
            self.get_parameter('experiment_status_publish_period_sec').value
        )
        self.enable_measurement_logging = bool(self.get_parameter('enable_measurement_logging').value)
        self.measurement_log_e720_max_age_sec = float(self.get_parameter('measurement_log_e720_max_age_sec').value)
        self.control_channel = int(self.get_parameter('control_channel').value)
        self.monitor_channel = int(self.get_parameter('monitor_channel').value)

        self.latest_measurements: Dict[int, Measurement] = {}
        self._latest_e720: Optional[E720] = None
        self._latest_e720_monotonic: float = 0.0
        self.db_client = None
        self.controller: Optional[CoreController] = None
        self.temperature_control: Optional[TemperatureControlWorker] = None
        self.program_manager: Optional[ProgramExperimentManager] = None
        self._last_status_publish_monotonic: float = 0.0
        self._measurement_db_queue: queue.Queue = queue.Queue(maxsize=500)
        self._measurement_db_stop = threading.Event()
        self._measurement_db_thread: Optional[threading.Thread] = None
        # Core service callbacks call the database client while handling /core/query.
        # Use a reentrant group so the database client response can be processed
        # while the program-start callback waits for it.
        self._service_cb_group = ReentrantCallbackGroup()

        self.hmi_publisher = self.create_publisher(String, self.hmi_commands_topic, 10)
        self.experiment_status_publisher = self.create_publisher(
            String,
            self.experiment_status_topic,
            10,
        )
        self.service = self.create_service(
            Query,
            'query',
            self.handle_query,
            callback_group=self._service_cb_group,
        )
        self.measurement_subscription = self.create_subscription(
            Measurement,
            self.measurement_topic,
            self.measurement_callback,
            50,
        )
        self.e720_subscription = self.create_subscription(
            E720,
            self.measure_topic,
            self._e720_callback,
            10,
        )

        if self.enable_database_client:
            self.db_client = self.create_client(
                Query,
                self.database_service,
                callback_group=self._service_cb_group,
            )
            if self.db_client.wait_for_service(timeout_sec=2.0):
                self.get_logger().info(f'Database service available: {self.database_service}')
            else:
                self.get_logger().warn(
                    f'Database service not available at startup: {self.database_service}. '
                    'Program scheduler needs database.'
                )

        if self.enable_pwm_controller:
            try:
                self.controller = CoreController(
                    pwm_pin=int(self.get_parameter('pwm_pin').value),
                    pwm_pin_ch2=int(self.get_parameter('pwm_pin_ch2').value),
                    pwm_frequency_hz=int(self.get_parameter('pwm_frequency_hz').value),
                    pwm_range=int(self.get_parameter('pwm_range').value),
                )
                self.temperature_control = TemperatureControlWorker(
                    set_output_callback=self.controller.set_heater_output,
                    control_channel=self.control_channel,
                    monitor_channel=self.monitor_channel,
                    target_k=float(self.get_parameter('target_k').value),
                    control_period_sec=self.control_period_sec,
                    control_watchdog_period_sec=self.control_watchdog_period_sec,
                    measurement_timeout_sec=float(self.get_parameter('measurement_timeout_sec').value),
                    event_driven=True,
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

        if self.enable_program_scheduler and self.enable_pwm_controller:
            self.program_manager = ProgramExperimentManager(
                db_query=self._db_query,
                configure_temperature=self._apply_temperature_control,
                log=lambda msg: self.get_logger().info(msg),
                database_ready=self._database_service_ready,
                database_error=self._database_unavailable_reason,
                temperature_enabled=lambda: self.temperature_control is not None,
            )
            self.get_logger().info(
                'Program experiment scheduler enabled (measurement-driven, no fixed 1 Hz timer).'
            )
        if self.enable_measurement_logging and self.enable_database_client:
            self._measurement_db_thread = threading.Thread(
                target=self._measurement_db_worker,
                name='core-measurement-db',
                daemon=True,
            )
            self._measurement_db_thread.start()
        if self.enable_program_scheduler and not self.enable_pwm_controller:
            self.get_logger().warn(
                'enable_program_scheduler is true but PWM/temperature control is disabled.'
            )

        self.get_logger().info(
            'Core node ready. '
            f'measurement_topic={self.measurement_topic}, '
            f'experiment_status_topic={self.experiment_status_topic}, '
            f'control_channel={self.control_channel}, event_driven_control=True'
        )

    def _e720_callback(self, msg: E720) -> None:
        self._latest_e720 = msg
        self._latest_e720_monotonic = time.monotonic()

    @staticmethod
    def _is_temperature_msg(msg: Measurement) -> bool:
        return str(msg.type) in ('temperature_K', 'temperature_C')

    def measurement_callback(self, msg: Measurement) -> None:
        self.latest_measurements[int(msg.channel)] = msg
        if not self._is_temperature_msg(msg):
            return

        channel = int(msg.channel)
        is_control = channel == self.control_channel

        if is_control and self.program_manager is not None:
            try:
                was_running = self.program_manager.is_running()
                if was_running:
                    self.program_manager.tick()
                if was_running and not self.program_manager.is_running():
                    self._publish_experiment_status(force=True)
            except Exception as exc:
                self.get_logger().error(f'Program scheduler tick failed: {exc}')

        if self.temperature_control is not None:
            try:
                self.temperature_control.update_measurement(
                    channel=channel,
                    raw_value=float(msg.value),
                    raw_type=str(msg.type),
                    valid=bool(msg.valid),
                )
            except ValueError as exc:
                self.get_logger().warning(str(exc))

        if is_control and bool(msg.valid):
            self._log_measurement_sample_if_running()
            self._publish_experiment_status(force=False)

    def _log_measurement_sample_if_running(self) -> None:
        if not self.enable_measurement_logging or self.program_manager is None:
            return
        if not self.program_manager.is_running():
            return
        if self.db_client is None or not self.db_client.service_is_ready():
            return

        status = self.program_manager.status()
        program_id = status.get('program_id')
        run_id = status.get('run_id')
        if program_id is None or run_id is None:
            return

        control_msg = self.latest_measurements.get(self.control_channel)
        monitor_msg = self.latest_measurements.get(self.monitor_channel)
        if control_msg is None or not bool(control_msg.valid):
            return

        target_k = status.get('last_target_k')
        if self.temperature_control is not None:
            tc = self.temperature_control.get_snapshot()
            if tc.get('target_k') is not None:
                target_k = tc.get('target_k')

        e720 = e720_from_msg(self._latest_e720)
        row = build_measurement_row(
            int(program_id),
            e720,
            float(control_msg.value),
            float(monitor_msg.value) if monitor_msg is not None else 0.0,
            target_k,
            run_id=int(run_id),
            e720_updated_monotonic=self._latest_e720_monotonic,
            e720_max_age_sec=self.measurement_log_e720_max_age_sec,
        )
        try:
            self._measurement_db_queue.put_nowait(row)
        except queue.Full:
            self.get_logger().warning('measurement DB queue full — sample dropped')

    def _measurement_db_worker(self) -> None:
        while not self._measurement_db_stop.is_set():
            try:
                row = self._measurement_db_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if not insert_measurement_immediate(self._db_query, row):
                    self.get_logger().warning('measurement_insert returned failure')
            except Exception as exc:
                self.get_logger().error(f'measurement_insert failed: {exc}')

    def _publish_experiment_status(self, *, force: bool = False) -> None:
        if self.program_manager is None and self.temperature_control is None:
            return
        now = time.monotonic()
        if (
            not force
            and (now - self._last_status_publish_monotonic)
            < max(0.05, self.experiment_status_publish_period_sec)
        ):
            return
        self._last_status_publish_monotonic = now
        payload = {
            'result': 'Ok',
            'program': self.program_manager.status() if self.program_manager else None,
            'temperature_control': (
                self.temperature_control.get_snapshot() if self.temperature_control else None
            ),
            'timestamp': time.time(),
        }
        msg = String()
        msg.data = json.dumps(payload)
        self.experiment_status_publisher.publish(msg)

    def handle_query(self, request: Query.Request, response: Query.Response) -> Query.Response:
        try:
            payload = json.loads(request.query) if request.query else {}
            if 'program' not in payload:
                self.get_logger().info(f'Received core query: {request.query}')
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

        program_cmd = query.get('program')
        if program_cmd is not None:
            result.update(self._handle_program_command(program_cmd))

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
            if self.program_manager is not None and self.program_manager.is_running():
                result['temperature_control'] = self.temperature_control.get_snapshot()
                result['program_scheduler_note'] = (
                    'Manual temperature_control ignored while program is running in core.'
                )
            else:
                result['temperature_control'] = self._apply_temperature_control(temperature_control)

        if self.program_manager is not None and 'program' not in query:
            result['program'] = self.program_manager.status()

        return result

    def _handle_program_command(self, program_cmd: Dict[str, Any]) -> Dict[str, Any]:
        if self.program_manager is None:
            return {
                'result': 'False',
                'error': 'Program scheduler not available (enable_pwm_controller and enable_program_scheduler)',
            }
        cmd = str(program_cmd.get('cmd', '')).strip().lower()
        if cmd == 'start':
            program_id = int(program_cmd.get('program_id', 0))
            out = self.program_manager.start(program_id)
            self._publish_experiment_status(force=True)
            return out
        if cmd == 'stop':
            program_id = program_cmd.get('program_id')
            pid = int(program_id) if program_id is not None else None
            out = self.program_manager.stop(program_id=pid, final_status='Stopped')
            self._publish_experiment_status(force=True)
            return out
        if cmd in ('stop_all', 'stopall'):
            out = self.program_manager.stop_all()
            self._publish_experiment_status(force=True)
            return out
        if cmd == 'status':
            return {'result': 'Ok', 'program': self.program_manager.status()}
        return {'result': 'False', 'error': f'Unknown program command: {cmd}'}

    def _database_service_ready(self) -> bool:
        if not self.enable_database_client or self.db_client is None:
            return False
        try:
            if self.db_client.service_is_ready():
                return True
            return bool(self.db_client.wait_for_service(timeout_sec=5.0))
        except Exception:
            return False

    def _database_unavailable_reason(self) -> str:
        if not self.enable_database_client or self.db_client is None:
            return (
                'Core database client is disabled. Set DELATOMETRY_CORE_ENABLE_DATABASE_CLIENT=true '
                'in /etc/default/delatometry and restart delatometry-core.service.'
            )
        return (
            f'Database service not available at {self.database_service}. '
            'Ensure delatometry-database.service is running.'
        )

    def _db_query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.db_client is None:
            raise RuntimeError('Database client is disabled')
        if not self.db_client.service_is_ready():
            if not self.db_client.wait_for_service(timeout_sec=2.0):
                raise RuntimeError(f'Database service not available: {self.database_service}')
        request = Query.Request()
        request.query = json.dumps(payload)
        future = self.db_client.call_async(request)

        # rclpy Future.result() must not be used with timeout=.  Polling also
        # avoids deadlock when this method is called from a service callback: the
        # MultiThreadedExecutor can complete the database client response on a
        # different worker in the reentrant callback group.
        deadline = time.monotonic() + max(0.1, float(self.database_query_timeout_sec))
        while not future.done() and time.monotonic() < deadline:
            time.sleep(0.005)
        if not future.done():
            try:
                future.cancel()
            except Exception:
                pass
            raise TimeoutError(f'Timeout waiting for {self.database_service}')
        try:
            response = future.result()
        except Exception as exc:
            raise RuntimeError(f'Database query failed: {exc}') from exc
        if response is None:
            raise RuntimeError('Database query failed')
        return json.loads(response.response or '{}')

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
        self._measurement_db_stop.set()
        if self._measurement_db_thread is not None:
            self._measurement_db_thread.join(timeout=2.0)
        if self.program_manager is not None and self.program_manager.is_running():
            try:
                self.program_manager.stop_all()
            except Exception:
                pass
        if self.temperature_control is not None:
            self.temperature_control.stop()
        if self.controller is not None:
            self.controller.stop()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = CoreNode()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        node.shutdown()
        executor.shutdown()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
