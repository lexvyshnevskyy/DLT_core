# core

ROS 2 experiment control: programs, PI/PWM, measurement logging, `/core/query`, `/core/experiment/status`.

**Experiment modes:** `default` (heating + LTM ticks), `measure_only`, `measure_ltm` (timed impedance logging). **Measure source:** `e720` or `im3536` via `DELATOMETRY_MEASURE_SOURCE`.

**Full documentation:** [docs/en/core.md](../../docs/en/core.md) · [docs/uk/core.md](../../docs/uk/core.md) · Web UI **Documentation** → Core node.

Build: `colcon build --packages-select core` · Service: `delatometry-core.service`

**Документація українською:** [docs/uk/core.md](../../docs/uk/core.md)
