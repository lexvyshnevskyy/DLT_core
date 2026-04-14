from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description() -> LaunchDescription:
    default_params = PathJoinSubstitution([
        FindPackageShare('core'),
        'config',
        'core.params.yaml',
    ])

    return LaunchDescription([
        DeclareLaunchArgument('namespace', default_value='core'),
        DeclareLaunchArgument('params_file', default_value=default_params),
        Node(
            package='core',
            executable='run.py',
            name='core',
            namespace=LaunchConfiguration('namespace'),
            output='screen',
            parameters=[LaunchConfiguration('params_file')],
        ),
    ])
