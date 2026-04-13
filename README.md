# core (ROS 2)

ROS 2 port of the legacy ROS 1 `core` package.

## Notes
- Keeps the original `core/srv/Query` service name.
- Expects external ROS 2 packages `msgs` and `database` in the same workspace.
- Preserves the sensor synchronizer, HMI publisher, and JSON-based query handling.
- PWM/pigpio support is optional and only activated if explicitly requested by a query.
