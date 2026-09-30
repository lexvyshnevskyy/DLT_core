"""Threaded absolute-temperature control: CH1 heater + CH2 cooler (bipolar PI)."""

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
    monitor_channel: int = 9
    latest_control_temp_k: Optional[float] = None
    latest_monitor_temp_k: Optional[float] = None
    # Split-range outputs: never heat and cool at the same time.
    heater_output: int = 0  # CH1 duty 0..pwm_range
    cooler_output: int = 0  # CH2 duty 0..pwm_range
    signed_output: int = 0  # +heat / -cool
    measurement_fresh: bool = False
    integral_term: float = 0.0
    error_k: Optional[float] = None
    # Predictive lead: T_pred = T + tau * dT/dt (used for early correction).
    predicted_k: Optional[float] = None
    dT_dt: float = 0.0
    controller_mode: str = 'pi_bipolar'
    # bipolar | heat_only | cool_only — program ramps clamp actuator direction
    actuator_mode: str = 'bipolar'
    reason: str = 'idle'

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def normalize_actuator_mode(value: Optional[str]) -> str:
    mode = str(value or 'bipolar').strip().lower()
    if mode in ('heat_only', 'heat', 'heating'):
        return 'heat_only'
    if mode in ('cool_only', 'cool', 'cooling'):
        return 'cool_only'
    return 'bipolar'


def clamp_signed_output(signed: int, actuator_mode: str) -> int:
    """Restrict PI output to the allowed actuator direction for the current step."""
    mode = normalize_actuator_mode(actuator_mode)
    value = int(signed)
    if mode == 'heat_only':
        return max(0, value)
    if mode == 'cool_only':
        return min(0, value)
    return value


def apply_cooler_min_duty(signed: int, cooler_min_duty: int) -> int:
    """When cooler is commanded, never run below cooler_min_duty (PWM counts)."""
    floor = max(0, int(cooler_min_duty))
    if floor <= 0:
        return int(signed)
    value = int(signed)
    if value < 0 and abs(value) < floor:
        return -floor
    return value


class PIThermalController:
    """Predictive bipolar PI: positive heats, negative cools.

    Controls on a lead-predicted temperature so actuation starts *before* the
    plant crosses the agenda (thermal lag). That is what keeps CH9 inside
    ±deadband of theory instead of overshooting then freefalling.

        T_pred = T_meas + predict_tau_s * dT/dt
        error  = target - T_pred

    When the *actual* error is already inside the deadband and the rate is
    small, the actuator is frozen (true hold) so idle gas losses are fought
    by the last good bias rather than by slewing toward zero.
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
        predict_tau_s: float = 20.0,
        settle_rate_k_s: float = 0.05,
    ) -> None:
        self.kp = float(kp)
        self.ki = float(ki)
        self.output_min = int(output_min)
        self.output_max = int(output_max)
        self.deadband_k = float(deadband_k)
        self.max_output_step = int(max_output_step)
        self.integral_min = float(integral_min)
        self.integral_max = float(integral_max)
        self.predict_tau_s = max(0.0, float(predict_tau_s))
        self.settle_rate_k_s = max(0.0, float(settle_rate_k_s))
        self.integral = 0.0
        self.output = 0
        self._prev_meas: Optional[float] = None
        self.dT_dt: float = 0.0
        self.predicted_k: Optional[float] = None

    def reset(self) -> None:
        self.integral = 0.0
        self.output = 0
        self._prev_meas = None
        self.dT_dt = 0.0
        self.predicted_k = None

    def note_measurement(self, measured_k: float, dt: float) -> float:
        """Update dT/dt / T_pred without running PI (e.g. during preheat)."""
        dt = max(float(dt), 1e-6)
        measured = float(measured_k)
        if self._prev_meas is None:
            self.dT_dt = 0.0
        else:
            # Light EMA so a single noisy sample does not yank the prediction.
            instant = (measured - self._prev_meas) / dt
            self.dT_dt = 0.6 * self.dT_dt + 0.4 * instant
        self._prev_meas = measured
        self.predicted_k = measured + self.predict_tau_s * self.dT_dt
        return self.dT_dt

    def is_settled(self, target_k: float, measured_k: float) -> bool:
        """True when actual T is inside ±deadband and rate is nearly flat."""
        err = float(target_k) - float(measured_k)
        return abs(err) <= self.deadband_k and abs(self.dT_dt) <= self.settle_rate_k_s

    def update(self, target_k: float, measured_k: float, dt: float) -> tuple[int, float]:
        dt = max(float(dt), 1e-6)
        measured = float(measured_k)
        target = float(target_k)

        self.note_measurement(measured, dt)
        assert self.predicted_k is not None
        error_actual = target - measured
        error_pred = target - float(self.predicted_k)

        # True hold only when BOTH measured and predicted are in-band and flat.
        # Freezing while T_pred is still climbing (or after a full-heat handoff)
        # leaves the heater stuck high and temperature runs away.
        pred_in_band = abs(error_pred) <= self.deadband_k
        meas_in_band = abs(error_actual) <= self.deadband_k
        flat = abs(self.dT_dt) <= self.settle_rate_k_s
        if meas_in_band and pred_in_band and flat:
            self.output = max(self.output_min, min(self.output_max, int(self.output)))
            return self.output, error_actual

        # Overheating (measured above agenda): allow cooler, but do not slam
        # from a small overshoot (that freefalls below a descending ramp).
        if error_actual < -self.deadband_k:
            effective_error = min(error_pred, error_actual)
            if self.integral > 0.0:
                if error_actual < -1.0:
                    # Clearly hot vs agenda — drop heat bias.
                    self.integral = 0.0
                else:
                    # Mild overshoot — bleed I gradually.
                    self.integral = max(0.0, self.integral + effective_error * dt * 3.0)
        else:
            # Still at/below agenda. If clearly cold, keep heating urgency.
            if error_actual > self.deadband_k:
                effective_error = max(error_pred, error_actual)
            else:
                effective_error = error_pred
            if error_actual > self.deadband_k and self.integral < 0.0:
                # Below agenda with cool bias — clear it so heater can catch up.
                if error_actual > 1.0:
                    self.integral = 0.0
                else:
                    self.integral = min(0.0, self.integral + effective_error * dt * 4.0)

        proposed_integral = self.integral + effective_error * dt
        proposed_integral = max(self.integral_min, min(self.integral_max, proposed_integral))

        unclamped = self.kp * effective_error + self.ki * proposed_integral
        clamped = max(self.output_min, min(self.output_max, round(unclamped)))

        saturated_high = clamped >= self.output_max and effective_error > 0.0
        saturated_low = clamped <= self.output_min and effective_error < 0.0
        if not (saturated_high or saturated_low):
            self.integral = proposed_integral

        requested = max(
            self.output_min,
            min(self.output_max, round(self.kp * effective_error + self.ki * self.integral)),
        )
        # Only force a strong cool command when clearly above agenda (>1 K).
        if error_actual < -1.0 and requested > 0:
            cool_req = round(self.kp * effective_error + self.ki * self.integral)
            cool_req = max(self.output_min, min(0, cool_req))
            min_cool = -max(100, int(abs(error_actual) * self.kp))
            requested = min(cool_req, min_cool)
            requested = max(self.output_min, requested)

        step = self.max_output_step
        if error_actual < -1.0 and requested < self.output:
            step = max(step, min(200, abs(self.output - requested)))
        elif error_actual > self.deadband_k and requested < self.output:
            # Below agenda: only slowly decrease heater.
            step = max(1, min(step, max(10, self.max_output_step // 4)))
        # Below agenda and still cooling: slew toward heat faster.
        if error_actual > 1.0 and requested > self.output:
            step = max(step, min(200, abs(requested - self.output)))
        if requested > self.output:
            self.output = min(requested, self.output + step)
        else:
            self.output = max(requested, self.output - step)

        self.output = max(self.output_min, min(self.output_max, self.output))
        return self.output, error_actual


# Backward-compatible alias
PIHeaterController = PIThermalController


class TemperatureControlWorker:
    def __init__(
        self,
        set_output_callback: Callable[..., None],
        *,
        control_channel: int = 9,
        monitor_channel: int = 9,
        target_k: float = 373.15,
        control_period_sec: float = 1.0,
        control_watchdog_period_sec: float = 0.25,
        measurement_timeout_sec: float = 5.0,
        event_driven: bool = True,
        kp: float = 25.0,
        ki: float = 0.08,
        deadband_k: float = 0.2,
        max_output_step: int = 60,
        output_min: int = -1000,
        output_max: int = 1000,
        cooler_min_duty: int = 200,
        cooler_max_duty: Optional[int] = None,
        agenda_tol_k: float = 0.2,
        predict_tau_s: float = 20.0,
        settle_rate_k_s: float = 0.05,
    ) -> None:
        self._set_output = set_output_callback
        self._control_period_sec = max(0.01, float(control_period_sec))
        self._control_watchdog_period_sec = max(0.05, float(control_watchdog_period_sec))
        self._measurement_timeout_sec = float(measurement_timeout_sec)
        self._event_driven = bool(event_driven)
        self._cooler_min_duty = max(0, int(cooler_min_duty))
        self._cooler_max_duty = int(cooler_max_duty) if cooler_max_duty is not None else int(output_max)
        self._agenda_tol_k = max(0.0, float(agenda_tol_k))
        # Pre-experiment stabilize soft-hold (side from actuator_mode):
        #   heat_only after preheat — heater floor, never freefall via cooler
        #   cool_only after precool — cooler floor, never runaway via heater
        self._stabilize_mode: bool = False
        self._stabilize_heater_floor: int = 280
        self._stabilize_cooler_floor: int = 280
        # Pre-experiment preheat: when set, actuator holds this signed output and
        # the PI is paused (used to drive heat until predicted T crosses t_exp).
        self._hold_output: Optional[int] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._measurements: Dict[int, MeasurementState] = {}
        self._enabled = False
        self._last_update_monotonic = time.monotonic()

        self.controller = PIThermalController(
            kp=kp,
            ki=ki,
            output_min=output_min,
            output_max=output_max,
            deadband_k=deadband_k,
            max_output_step=max_output_step,
            # Wide I-limits so a holding bias against idle gas flow can build.
            integral_min=-20000.0,
            integral_max=20000.0,
            predict_tau_s=predict_tau_s,
            settle_rate_k_s=settle_rate_k_s,
        )
        self.snapshot = ControlSnapshot(
            enabled=False,
            target_k=float(target_k),
            control_channel=int(control_channel),
            monitor_channel=int(monitor_channel),
            heater_output=0,
            cooler_output=0,
            signed_output=0,
            controller_mode='pi_bipolar',
            actuator_mode='bipolar',
            reason='idle',
        )

    @staticmethod
    def to_kelvin(raw_value: float, raw_type: str) -> float:
        if raw_type == 'temperature_K':
            return float(raw_value)
        if raw_type == 'temperature_C':
            return float(raw_value) + 273.15
        raise ValueError(f'Unsupported temperature type: {raw_type}')

    @staticmethod
    def split_thermal_output(signed: int, cooler_min_duty: int = 0) -> tuple[int, int]:
        """Map signed PI output to heater/cooler duties (cooler idle = min PWM)."""
        value = int(signed)
        idle = max(0, int(cooler_min_duty))
        if value > 0:
            return value, idle
        if value < 0:
            return 0, max(idle, abs(value))
        return 0, idle

    def _apply_signed_output(self, signed: int, *, cooler_off: bool = False) -> tuple[int, int]:
        heater, cooler = self.split_thermal_output(signed, self._cooler_min_duty)
        if cooler_off:
            cooler = 0
        try:
            self._set_output(signed, cooler_off=cooler_off)
        except TypeError:
            try:
                self._set_output(signed)
            except TypeError:
                # Legacy callback that only accepts a single non-negative duty.
                self._set_output(heater if heater else cooler)
        return heater, cooler

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
        run_control = False
        with self._lock:
            self._measurements[state.channel] = state
            if state.channel == self.snapshot.control_channel:
                self.snapshot.latest_control_temp_k = state.value_k
            if state.channel == self.snapshot.monitor_channel:
                self.snapshot.latest_monitor_temp_k = state.value_k
            if (
                self._event_driven
                and self._enabled
                and state.valid
                and state.channel == self.snapshot.control_channel
            ):
                run_control = True
        if run_control:
            self.run_control_step(time.monotonic())

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
        actuator_mode: Optional[str] = None,
        reset_integral: bool = False,
        seed_cooler_min: bool = False,
        preheat: bool = False,
        precool: bool = False,
        clear_hold: bool = False,
        handoff_hold: bool = False,
        stabilize_mode: Optional[bool] = None,
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
            if actuator_mode is not None:
                self.snapshot.actuator_mode = normalize_actuator_mode(actuator_mode)
                self.snapshot.controller_mode = f'pi_{self.snapshot.actuator_mode}'
            if stabilize_mode is not None:
                self._stabilize_mode = bool(stabilize_mode)
                if self._stabilize_mode:
                    # Side comes from actuator_mode (heat_only / cool_only).
                    # Never leave bipolar — that freefalls after preheat overshoot
                    # and runaways after precool soft-land.
                    mode = normalize_actuator_mode(self.snapshot.actuator_mode)
                    if mode == 'bipolar':
                        mode = 'heat_only'
                    self.snapshot.actuator_mode = mode
                    self.snapshot.controller_mode = f'pi_{mode}'
            if reset_integral:
                self.controller.reset()
                self.snapshot.integral_term = 0.0
            # Soft handoff: KEEP the prior side duty; seed I to sustain it.
            if handoff_hold:
                hold = self._hold_output
                self._hold_output = None
                mode = normalize_actuator_mode(self.snapshot.actuator_mode)
                if mode == 'cool_only':
                    # After precool: keep cooler; never snap to heater.
                    if hold is not None:
                        self.controller.output = min(int(hold), 0)
                    else:
                        self.controller.output = min(int(self.controller.output), 0)
                    floor_mag = max(
                        1,
                        self._cooler_min_duty,
                        self._stabilize_cooler_floor if self._stabilize_mode else 0,
                    )
                    if abs(int(self.controller.output)) < floor_mag:
                        self.controller.output = -floor_mag
                else:
                    # After preheat: keep heater; never snap to cooler.
                    if hold is not None:
                        self.controller.output = max(int(hold), 0)
                    floor = self._stabilize_heater_floor if self._stabilize_mode else 0
                    self.controller.output = max(int(self.controller.output), int(floor))
                ki = float(self.controller.ki) or 1e-6
                seeded = float(self.controller.output) / ki
                self.controller.integral = max(
                    self.controller.integral_min,
                    min(self.controller.integral_max, seeded),
                )
                self.snapshot.integral_term = self.controller.integral
                heater, cooler = self._apply_signed_output(self.controller.output)
                self.snapshot.heater_output = heater
                self.snapshot.cooler_output = cooler
                self.snapshot.signed_output = int(self.controller.output)
                self.snapshot.reason = 'stabilizing' if self._stabilize_mode else 'controlling'
            # Any explicit reset / clear hands control back to the PI.
            if clear_hold or reset_integral:
                self._hold_output = None
            if preheat:
                # Full-power heat until released (pre-experiment). Positive =
                # heater only; PI paused while hold is active.
                self._hold_output = int(self.controller.output_max)
                self.controller.output = self._hold_output
                heater, cooler = self._apply_signed_output(self._hold_output)
                self.snapshot.heater_output = heater
                self.snapshot.cooler_output = cooler
                self.snapshot.signed_output = self._hold_output
                self.snapshot.reason = 'preheating'
            if precool:
                # Full-power cool until released (pre-experiment). Negative =
                # cooler only; gas floor applied by CoreController.
                self._hold_output = -int(abs(self.controller.output_min))
                self.controller.output = self._hold_output
                heater, cooler = self._apply_signed_output(self._hold_output)
                self.snapshot.heater_output = heater
                self.snapshot.cooler_output = cooler
                self.snapshot.signed_output = self._hold_output
                self.snapshot.reason = 'precooling'
            if seed_cooler_min:
                # Cool-down start: cooler never below min duty (even in bipolar).
                floor = -max(1, self._cooler_min_duty)
                if self.controller.output > floor:
                    self.controller.output = floor
            if enabled is not None:
                self._enabled = bool(enabled)
                self.snapshot.enabled = self._enabled
                if not self._enabled:
                    self._hold_output = None
                    self._stabilize_mode = False
                    self.controller.reset()
                    heater, cooler = self._apply_signed_output(0, cooler_off=True)
                    self.snapshot.heater_output = heater
                    self.snapshot.cooler_output = cooler
                    self.snapshot.signed_output = 0
                    self.snapshot.actuator_mode = 'bipolar'
                    self.snapshot.controller_mode = 'pi_bipolar'
                    self.snapshot.reason = 'disabled'
                elif seed_cooler_min:
                    signed = apply_cooler_min_duty(self.controller.output, self._cooler_min_duty)
                    if signed >= 0:
                        signed = -max(1, self._cooler_min_duty)
                    self.controller.output = signed
                    heater, cooler = self._apply_signed_output(signed)
                    self.snapshot.heater_output = heater
                    self.snapshot.cooler_output = cooler
                    self.snapshot.signed_output = signed
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
        heater, cooler = self._apply_signed_output(0, cooler_off=True)
        with self._lock:
            self.snapshot.heater_output = heater
            self.snapshot.cooler_output = cooler
            self.snapshot.signed_output = 0
            self.snapshot.enabled = False
            self.snapshot.reason = 'stopped'
            self._enabled = False
            self.controller.reset()

    def get_snapshot(self) -> Dict[str, object]:
        with self._lock:
            return self.snapshot.to_dict()

    def run_control_step(self, now: Optional[float] = None) -> None:
        """Run PI once (called on each fresh control-channel temperature sample)."""
        if now is None:
            now = time.monotonic()
        with self._lock:
            if not self._enabled:
                return
            control_channel = self.snapshot.control_channel
            target_k = self.snapshot.target_k
            control_state = self._measurements.get(control_channel)
            monitor_state = self._measurements.get(self.snapshot.monitor_channel)
            hold_output = self._hold_output

        if monitor_state is not None:
            with self._lock:
                self.snapshot.latest_monitor_temp_k = monitor_state.value_k

        measurement_fresh = (
            control_state is not None
            and control_state.valid
            and (now - control_state.stamp_monotonic) <= self._measurement_timeout_sec
        )
        if not measurement_fresh:
            self._handle_stale_measurement()
            return

        # Pre-experiment preheat: hold a fixed output, PI paused. Still refresh
        # temperature + prediction so the scheduler can soft-land on T_pred.
        if hold_output is not None:
            dt = max(1e-3, now - self._last_update_monotonic)
            self._last_update_monotonic = now
            self.controller.note_measurement(control_state.value_k, dt)
            applied = int(hold_output)
            if applied < 0:
                applied = -min(
                    max(abs(applied), max(1, self._cooler_min_duty)),
                    max(1, self._cooler_max_duty),
                )
            self.controller.output = applied
            heater_output, cooler_output = self._apply_signed_output(applied)
            with self._lock:
                self.snapshot.latest_control_temp_k = control_state.value_k
                self.snapshot.measurement_fresh = True
                self.snapshot.heater_output = heater_output
                self.snapshot.cooler_output = cooler_output
                self.snapshot.signed_output = applied
                self.snapshot.error_k = float(target_k) - float(control_state.value_k)
                self.snapshot.predicted_k = self.controller.predicted_k
                self.snapshot.dT_dt = self.controller.dT_dt
                self.snapshot.reason = (
                    'precooling' if applied < 0 else 'preheating'
                )
            return

        dt = max(1e-3, now - self._last_update_monotonic)
        self._last_update_monotonic = now
        assert control_state is not None
        with self._lock:
            stabilize = self._stabilize_mode
            heater_floor = self._stabilize_heater_floor
            cooler_floor = self._stabilize_cooler_floor
            cooler_min = self._cooler_min_duty
            cooler_max = self._cooler_max_duty
            agenda_tol = self._agenda_tol_k
            actuator_mode = self.snapshot.actuator_mode
            prev_output = int(self.controller.output)

        signed_output, error_k = self.controller.update(
            target_k=target_k,
            measured_k=control_state.value_k,
            dt=dt,
        )
        # Keep the raw PI output as controller state. Actuator-mode clamp and
        # gas-cooler idle/[min,max] band apply ONLY to the hardware command —
        # never write those back into controller.output (traps / freefall).
        # Exception: stabilize soft-hold must rewrite controller state so the
        # next PI tick cannot slam the wrong actuator and runaway / freefall.
        in_band = abs(error_k) <= agenda_tol
        if stabilize and normalize_actuator_mode(actuator_mode) == 'cool_only':
            # After precool: cool-side hold. Forcing heater floor here ran away
            # 181→230 K. Keep cooler; bleed gently; heat only if undershoot.
            if error_k > 1.5:
                # Cold vs target — pull back with heater (exit pure cool hold).
                applied_output = max(0, int(signed_output))
                max_up = 40
                if applied_output > max(0, prev_output):
                    applied_output = min(applied_output, max(0, prev_output) + max_up)
                boost = min(
                    int(self.controller.output_max),
                    int(error_k * max(40.0, self.controller.kp)),
                )
                applied_output = max(applied_output, boost)
            else:
                applied_output = min(0, int(signed_output))
                max_toward_zero = 25  # gentle bleed after full-cool handoff
                if applied_output > prev_output:
                    applied_output = min(applied_output, prev_output + max_toward_zero)
                floor_mag = max(1, cooler_min, cooler_floor)
                if error_k < -0.4:
                    # Still hot — never drop below cooler floor.
                    applied_output = min(applied_output, -floor_mag)
                    if error_k < -1.0:
                        boost = -min(
                            int(cooler_max),
                            floor_mag + int(abs(error_k) * max(40.0, self.controller.kp)),
                        )
                        applied_output = min(applied_output, boost)
                else:
                    # Near / slightly cold: allow easing toward idle cooler min.
                    if applied_output < 0:
                        applied_output = min(
                            -max(1, cooler_min),
                            max(applied_output, -int(cooler_max)),
                        )
            self.controller.output = int(applied_output)
            ki = float(self.controller.ki) or 1e-6
            seeded = float(applied_output) / ki
            if abs(self.controller.integral) < abs(seeded) * 0.5 or (
                applied_output < 0 and self.controller.integral > 0.0
            ) or (applied_output > 0 and self.controller.integral < 0.0):
                self.controller.integral = max(
                    self.controller.integral_min,
                    min(self.controller.integral_max, seeded),
                )
            reason = 'stabilizing' if not in_band else 'stabilize_ok'
        elif stabilize:
            # After preheat: heat-only + floor. Dumping heater→0 freefalls ~10 K.
            applied_output = max(0, int(signed_output))
            max_down = 25  # ~25 PWM/s — gentle bleed after preheat handoff
            if applied_output < prev_output:
                applied_output = max(applied_output, prev_output - max_down)
            applied_output = max(heater_floor, applied_output)
            if error_k > 1.0:
                boost = min(
                    int(self.controller.output_max),
                    heater_floor + int(error_k * max(40.0, self.controller.kp)),
                )
                applied_output = max(applied_output, boost)
            elif error_k > 0.4:
                applied_output = max(applied_output, heater_floor + 80)
            self.controller.output = int(applied_output)
            ki = float(self.controller.ki) or 1e-6
            seeded = float(applied_output) / ki
            if self.controller.integral < seeded * 0.5 or self.controller.integral < 0.0:
                self.controller.integral = max(
                    0.0,
                    min(self.controller.integral_max, seeded),
                )
            reason = 'stabilizing' if not in_band else 'stabilize_ok'
        else:
            applied_output = clamp_signed_output(int(signed_output), actuator_mode)
            if applied_output < 0:
                # Cooling increases gas flow above idle floor up to cooler_max.
                magnitude = min(
                    max(abs(applied_output), max(1, cooler_min)),
                    max(1, cooler_max),
                )
                applied_output = -magnitude
            reason = 'agenda_ok' if in_band else 'controlling'

        heater_output, cooler_output = self._apply_signed_output(applied_output)
        with self._lock:
            self.snapshot.latest_control_temp_k = control_state.value_k
            self.snapshot.measurement_fresh = True
            self.snapshot.heater_output = heater_output
            self.snapshot.cooler_output = cooler_output
            self.snapshot.signed_output = applied_output
            self.snapshot.error_k = error_k
            self.snapshot.predicted_k = self.controller.predicted_k
            self.snapshot.dT_dt = self.controller.dT_dt
            self.snapshot.integral_term = self.controller.integral
            self.snapshot.reason = reason

    def _handle_stale_measurement(self) -> None:
        self.controller.reset()
        heater, cooler = self._apply_signed_output(0, cooler_off=True)
        with self._lock:
            self.snapshot.measurement_fresh = False
            self.snapshot.heater_output = heater
            self.snapshot.cooler_output = cooler
            self.snapshot.signed_output = 0
            self.snapshot.error_k = None
            self.snapshot.integral_term = self.controller.integral
            self.snapshot.reason = 'waiting_for_fresh_measurement'

    def _run(self) -> None:
        """Watchdog: disable actuators if control samples stop (event-driven mode)."""
        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            with self._lock:
                enabled = self._enabled
                control_channel = self.snapshot.control_channel
                control_state = self._measurements.get(control_channel)

            if enabled and control_state is not None:
                now = time.monotonic()
                fresh = (
                    control_state.valid
                    and (now - control_state.stamp_monotonic) <= self._measurement_timeout_sec
                )
                if not fresh:
                    self._handle_stale_measurement()

            if not self._event_driven and enabled:
                self.run_control_step(time.monotonic())

            elapsed = time.monotonic() - cycle_start
            wait = (
                self._control_watchdog_period_sec
                if self._event_driven
                else self._control_period_sec
            ) - elapsed
            if wait > 0.0:
                self._stop_event.wait(timeout=wait)
