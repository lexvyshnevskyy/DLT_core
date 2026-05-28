# core (ROS 2)

ROS 2 port of the legacy ROS 1 `core` package.

## Notes
- Keeps the original `core/srv/Query` service name.
- Expects external ROS 2 packages `msgs` and `database` in the same workspace.
- Preserves the sensor synchronizer, HMI publisher, and JSON-based query handling.
- PWM/pigpio support is optional and only activated if explicitly requested by a query.

## Temperature programs (experiment control)

All program ramp logic and PI setpoints run inside **core** (`ProgramExperimentManager` + threaded `TemperatureControlWorker`). WebUI and HMI only send commands; if they restart, the experiment continues.

JSON on `/core/query`:

```json
{"program": {"cmd": "start", "program_id": 4}}
{"program": {"cmd": "stop", "program_id": 4}}
{"program": {"cmd": "stop_all"}}
{"program": {"cmd": "status"}}
```

Live status is published on `core/experiment/status` (`std_msgs/String` JSON with `program` and `temperature_control`).

**Control loop:** PI heating and program ramp run on each **control-channel temperature sample** (LTM today; same `Measurement` topic for ADS later). There is no fixed 1 Hz experiment timer.

**Database:** During a run, each logged sample uses `measurement_insert` with an immediate `commit` (no webui bulk queue).

Requires `enable_pwm_controller:=true`, `enable_database_client:=true`, and `enable_program_scheduler:=true` (default on when PWM is enabled).
