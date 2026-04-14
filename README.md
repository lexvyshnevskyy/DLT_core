# core (ROS 2)

ROS 2 port of the legacy ROS 1 `core` package.

## What is implemented now
- Keeps the original `core/srv/Query` service name.
- Preserves the older JSON query pattern.
- Adds LTM2985-based temperature control using `msgs/Measurement` as the source.
- Uses one Raspberry Pi PWM output for the heater.
- Ignores ADS1256 for temperature control.
- Ports the Delphi-style hold mode logic:
  - build a ramp from the current PT temperature to the target
  - then hold that target for a long period
  - apply an incremental PID similar to the old `reg()` function

## Current assumptions
- PT channel is the control channel (default: channel 9).
- T thermocouple is the monitor channel (default: channel 3).
- Control loop period is 1 second to stay close to the old Delphi timing.
- PWM output range is `0..1000`.
- GPIO pin is configurable and defaults to `18`.

## Runtime control through the existing service
Example JSON payload for `/core/query`:

```json
{
  "temperature_control": {
    "enabled": true,
    "target_c": 120.0,
    "restart_profile": true
  }
}
```

Disable control and force PWM to zero:

```json
{
  "temperature_control": {
    "enabled": false
  }
}
```

## Notes
- `enable_pwm_controller` must be true to drive a real GPIO pin.
- `pigpiod` must be running on the Raspberry Pi.
- The LTM firmware currently sends `temperature_K` for valid temperature frames, so the node normalizes those values back to Celsius internally for control.
