"""Threaded absolute-temperature heater control in Kelvin."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, asdict
from typing import Callable, Dict, Optional


@dataclass
class MeasurementState:
    channel: int
    value_k: float
    valid: bool
    stamp_monotonic: float
    raw_type: str
    raw_value: float


@dataclass
class ControlSnapshot:
    enabled: bool = False
    target_k: float = 373.15
    control_channel: int = 9
    monitor_channel: int = 3
    latest_control_temp_k: Optional[float] = None
    latest_monitor_temp_k: Optional[float] = None
    heater_output: int = 0
    measurement_fresh: bool = False
    integral_term: float = 0.0
    error_k: Optional[float] = None
    controller_mode: str = 'pi'
    reason: str = 'idle'

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


class PIHeaterController:
    """PI controller with anti-windup, deadband, and output slew limiting.

    This is a better default than the legacy incremental PID for a slow heater:
    - derivative is omitted because temperature signals are slow and noisy
    - absolute setpoint is used directly in Kelvin
    - integral term is protected from windup at output limits
    """

    def __init__(
        self,
        kp: float,
        ki: float,
        output_min: int,
        output_max: int,
        deadband_k: float,
        max_output_step: int,
        integral_min: float,
        integral_max: float,
    ) -> None:
        self.kp = float(kp)
        self.ki = float(ki)
        self.output_min = int(output_min)
        self.output_max = int(output_max)
        self.deadband_k = float(deadband_k)
        self.max_output_step = int(max_output_step)
        self.integral_min = float(integral_min)
        self.integral_max = float(integral_max)
        self.integral = 0.0
        self.output = 0

    def reset(self) -> None:
        self.integral = 0.0
        self.output = 0

    def update(self, target_k: float, measured_k: float, dt: float) -> tuple[int, float]:
        dt = max(float(dt), 1e-6)
        error_k = float(target_k) - float(measured_k)

        effective_error = 0.0 if abs(error_k) <= self.deadband_k else error_k

        proposed_integral = self.integral + effective_error * dt
        proposed_integral = max(self.integral_min, min(self.integral_max, proposed_integral))

        unclamped = self.kp * effective_error + self.ki * proposed_integral
        clamped = max(self.output_min, min(self.output_max, round(unclamped)))

        saturated_high = clamped >= self.output_max and effective_error > 0.0
        saturated_low = clamped <= self.output_min and effective_error < 0.0
        if not (saturated_high or saturated_low):
            self.integral = proposed_integral

        requested = max(self.output_min, min(self.output_max, round(self.kp * effective_error + self.ki * self.integral)))
        if requested > self.output:
            self.output = min(requested, self.output + self.max_output_step)
        else:
            self.output = max(requested, self.output - self.max_output_step)

        self.output = max(self.output_min, min(self.output_max, self.output))
        return self.output, error_k


class TemperatureControlWorker:
    def __init__(
        self,
        set_output_callback: Callable[[int], None],
        *,
        control_channel: int = 9,
        monitor_channel: int = 3,
        target_k: float = 373.15,
        control_period_sec: float = 1.0,
        measurement_timeout_sec: float = 5.0,
        kp: float = 25.0,
        ki: float = 0.08,
        deadband_k: float = 0.3,
        max_output_step: int = 60,
        output_min: int = 0,
        output_max: int = 1000,
    ) -> None:
        self._set_output = set_output_callback
        self._control_period_sec = float(control_period_sec)
        self._measurement_timeout_sec = float(measurement_timeout_sec)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._measurements: Dict[int, MeasurementState] = {}
        self._enabled = False
        self._last_update_monotonic = time.monotonic()

        self.controller = PIHeaterController(
            kp=kp,
            ki=ki,
            output_min=output_min,
            output_max=output_max,
            deadband_k=deadband_k,
            max_output_step=max_output_step,
            integral_min=-5000.0,
            integral_max=5000.0,
        )
        self.snapshot = ControlSnapshot(
            enabled=False,
            target_k=float(target_k),
            control_channel=int(control_channel),
            monitor_channel=int(monitor_channel),
            heater_output=0,
            controller_mode='pi',
            reason='idle',
        )

    @staticmethod
    def to_kelvin(raw_value: float, raw_type: str) -> float:
        if raw_type == 'temperature_K':
            return float(raw_value)
        if raw_type == 'temperature_C':
            return float(raw_value) + 273.15
        raise ValueError(f'Unsupported temperature type: {raw_type}')

    def update_measurement(self, channel: int, raw_value: float, raw_type: str, valid: bool) -> None:
        value_k = self.to_kelvin(raw_value, raw_type)
        state = MeasurementState(
            channel=int(channel),
            value_k=value_k,
            valid=bool(valid),
            stamp_monotonic=time.monotonic(),
            raw_type=str(raw_type),
            raw_value=float(raw_value),
        )
        with self._lock:
            self._measurements[state.channel] = state
            if state.channel == self.snapshot.control_channel:
                self.snapshot.latest_control_temp_k = state.value_k
            if state.channel == self.snapshot.monitor_channel:
                self.snapshot.latest_monitor_temp_k = state.value_k

    def configure(
        self,
        *,
        enabled: Optional[bool] = None,
        target_k: Optional[float] = None,
        control_channel: Optional[int] = None,
        monitor_channel: Optional[int] = None,
        kp: Optional[float] = None,
        ki: Optional[float] = None,
        deadband_k: Optional[float] = None,
        max_output_step: Optional[int] = None,
        control_period_sec: Optional[float] = None,
        measurement_timeout_sec: Optional[float] = None,
        reset_integral: bool = False,
    ) -> Dict[str, object]:
        with self._lock:
            if target_k is not None:
                self.snapshot.target_k = float(target_k)
            if control_channel is not None:
                self.snapshot.control_channel = int(control_channel)
            if monitor_channel is not None:
                self.snapshot.monitor_channel = int(monitor_channel)
            if kp is not None:
                self.controller.kp = float(kp)
            if ki is not None:
                self.controller.ki = float(ki)
            if deadband_k is not None:
                self.controller.deadband_k = float(deadband_k)
            if max_output_step is not None:
                self.controller.max_output_step = int(max_output_step)
            if control_period_sec is not None:
                self._control_period_sec = max(0.05, float(control_period_sec))
            if measurement_timeout_sec is not None:
                self._measurement_timeout_sec = max(0.1, float(measurement_timeout_sec))
            if reset_integral:
                self.controller.reset()
                self.snapshot.integral_term = 0.0
            if enabled is not None:
                self._enabled = bool(enabled)
                self.snapshot.enabled = self._enabled
                if not self._enabled:
                    self.controller.reset()
                    self._set_output(0)
                    self.snapshot.heater_output = 0
                    self.snapshot.reason = 'disabled'
        return self.snapshot.to_dict()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name='temperature-control', daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._set_output(0)
        with self._lock:
            self.snapshot.heater_output = 0
            self.snapshot.enabled = False
            self.snapshot.reason = 'stopped'
            self._enabled = False
            self.controller.reset()

    def get_snapshot(self) -> Dict[str, object]:
        with self._lock:
            return self.snapshot.to_dict()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            with self._lock:
                enabled = self._enabled
                control_channel = self.snapshot.control_channel
                monitor_channel = self.snapshot.monitor_channel
                target_k = self.snapshot.target_k
                control_state = self._measurements.get(control_channel)
                monitor_state = self._measurements.get(monitor_channel)

            if not enabled:
                self._sleep_until_next_cycle(cycle_start)
                continue

            now = time.monotonic()
            dt = max(1e-3, now - self._last_update_monotonic)
            self._last_update_monotonic = now

            measurement_fresh = (
                control_state is not None
                and control_state.valid
                and (now - control_state.stamp_monotonic) <= self._measurement_timeout_sec
            )

            if monitor_state is not None:
                with self._lock:
                    self.snapshot.latest_monitor_temp_k = monitor_state.value_k

            if not measurement_fresh:
                self.controller.reset()
                self._set_output(0)
                with self._lock:
                    self.snapshot.measurement_fresh = False
                    self.snapshot.heater_output = 0
                    self.snapshot.error_k = None
                    self.snapshot.integral_term = self.controller.integral
                    self.snapshot.reason = 'waiting_for_fresh_measurement'
                self._sleep_until_next_cycle(cycle_start)
                continue

            assert control_state is not None
            heater_output, error_k = self.controller.update(target_k=target_k, measured_k=control_state.value_k, dt=dt)
            self._set_output(heater_output)
            with self._lock:
                self.snapshot.latest_control_temp_k = control_state.value_k
                self.snapshot.measurement_fresh = True
                self.snapshot.heater_output = heater_output
                self.snapshot.error_k = error_k
                self.snapshot.integral_term = self.controller.integral
                self.snapshot.reason = 'controlling'
            self._sleep_until_next_cycle(cycle_start)

    def _sleep_until_next_cycle(self, cycle_start: float) -> None:
        elapsed = time.monotonic() - cycle_start
        remaining = self._control_period_sec - elapsed
        if remaining > 0.0:
            self._stop_event.wait(timeout=remaining)
