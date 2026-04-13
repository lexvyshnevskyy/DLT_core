from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    return LaunchDescription([
        DeclareLaunchArgument('namespace', default_value='core'),
        DeclareLaunchArgument('ads_topic', default_value='/ads1256'),
        DeclareLaunchArgument('measure_topic', default_value='/measure_device'),
        DeclareLaunchArgument('hmi_commands_topic', default_value='/hmi/commands'),
        DeclareLaunchArgument('database_service', default_value='/database/query'),
        DeclareLaunchArgument('sync_queue_size', default_value='10'),
        DeclareLaunchArgument('sync_slop', default_value='0.5'),
        DeclareLaunchArgument('enable_database_client', default_value='true'),
        DeclareLaunchArgument('enable_pwm_controller', default_value='false'),
        Node(
            package='core',
            executable='run.py',
            name='core',
            namespace=LaunchConfiguration('namespace'),
            output='screen',
            parameters=[{
                'ads_topic': LaunchConfiguration('ads_topic'),
                'measure_topic': LaunchConfiguration('measure_topic'),
                'hmi_commands_topic': LaunchConfiguration('hmi_commands_topic'),
                'database_service': LaunchConfiguration('database_service'),
                'sync_queue_size': LaunchConfiguration('sync_queue_size'),
                'sync_slop': LaunchConfiguration('sync_slop'),
                'enable_database_client': LaunchConfiguration('enable_database_client'),
                'enable_pwm_controller': LaunchConfiguration('enable_pwm_controller'),
            }],
        ),
    ])
