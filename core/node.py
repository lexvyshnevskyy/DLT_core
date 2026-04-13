from __future__ import annotations

import json
from typing import Any, Dict, Optional

import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from message_filters import ApproximateTimeSynchronizer, Subscriber

from core.srv import Query
from database.srv import Query as DatabaseQuery
from msgs.msg import Ads, E720

from .classes.core_controller import CoreController


class CoreNode(Node):
    def __init__(self) -> None:
        super().__init__('core')

        self.declare_parameter('ads_topic', '/ads1256')
        self.declare_parameter('measure_topic', '/measure_device')
        self.declare_parameter('hmi_commands_topic', '/hmi/commands')
        self.declare_parameter('database_service', '/database/query')
        self.declare_parameter('sync_queue_size', 10)
        self.declare_parameter('sync_slop', 0.5)
        self.declare_parameter('enable_database_client', True)
        self.declare_parameter('enable_pwm_controller', False)

        self.ads_topic = str(self.get_parameter('ads_topic').value)
        self.measure_topic = str(self.get_parameter('measure_topic').value)
        self.hmi_commands_topic = str(self.get_parameter('hmi_commands_topic').value)
        self.database_service = str(self.get_parameter('database_service').value)
        self.sync_queue_size = int(self.get_parameter('sync_queue_size').value)
        self.sync_slop = float(self.get_parameter('sync_slop').value)
        self.enable_database_client = bool(self.get_parameter('enable_database_client').value)
        self.enable_pwm_controller = bool(self.get_parameter('enable_pwm_controller').value)

        self.latest_ads: Optional[Ads] = None
        self.latest_measurement: Optional[E720] = None
        self.db_client = None
        self.controller: Optional[CoreController] = None

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
                self.controller = CoreController()
                self.get_logger().info('PWM controller initialized.')
            except Exception as exc:
                self.controller = None
                self.get_logger().error(f'Failed to initialize PWM controller: {exc}')

        self.get_logger().info(
            'Core node ready. '
            f'ads_topic={self.ads_topic}, measure_topic={self.measure_topic}, '
            f'hmi_commands_topic={self.hmi_commands_topic}'
        )

    def sensor_callback(self, ads_msg: Ads, meas_msg: E720) -> None:
        self.latest_ads = ads_msg
        self.latest_measurement = meas_msg
        self.get_logger().debug('Received synchronized ADS1256 and measure_device frames.')

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

        # Preserve the old package behavior: accept JSON input even when no command is implemented yet.
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
        if ch2 is not None:
            self.controller.set_channel_2(int(ch2))
        return self.controller.snapshot()

    def _sensor_snapshot(self) -> Dict[str, Any]:
        return {
            'ads': self._message_to_dict(self.latest_ads),
            'measure_device': self._message_to_dict(self.latest_measurement),
        }

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
