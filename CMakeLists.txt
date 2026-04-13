cmake_minimum_required(VERSION 3.8)
project(core)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclpy REQUIRED)
find_package(std_msgs REQUIRED)
find_package(message_filters REQUIRED)
find_package(msgs REQUIRED)
find_package(database REQUIRED)
find_package(rosidl_default_generators REQUIRED)

set(service_files
  "srv/Query.srv"
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${service_files}
)

ament_export_dependencies(rosidl_default_runtime)

install(PROGRAMS
        core/run.py
  DESTINATION lib/${PROJECT_NAME}
)

install(DIRECTORY
        core
  DESTINATION lib/${PROJECT_NAME}
  FILES_MATCHING PATTERN "*.py"
)

install(DIRECTORY
        launch
  DESTINATION share/${PROJECT_NAME}
)

ament_package()
