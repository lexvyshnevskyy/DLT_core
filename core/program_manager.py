from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .measure_source import measure_device_label, normalize_measure_source
from .program_experiment import (
    ExperimentState,
    ProgramScheduler,
    ProgramStep,
    actuator_mode_for_step,
    is_cooldown_step,
    normalize_experiment_mode,
    program_elapsed_s,
    state_to_public_dict,
    total_program_duration_s,
    uses_temperature_control,
)

if TYPE_CHECKING:
    from .node import CoreNode


DbQueryFn = Callable[[Dict[str, Any]], Dict[str, Any]]
TcConfigureFn = Callable[[Dict[str, Any]], Dict[str, Any]]
LogFn = Callable[[str], None]
ZeroHeatersFn = Callable[[], None]


class ProgramExperimentManager:
    """Owns program ramp + temperature setpoints; survives webui/hmi restarts."""

    def __init__(
        self,
        *,
        db_query: DbQueryFn,
        configure_temperature: TcConfigureFn,
        log: LogFn,
        database_ready: Callable[[], bool],
        database_error: Callable[[], str],
        temperature_enabled: Callable[[], bool],
        zero_heaters: Optional[ZeroHeatersFn] = None,
        measure_source: str = 'e720',
        control_temperature: Optional[Callable[[], Optional[float]]] = None,
        temperature_snapshot: Optional[Callable[[], Dict[str, Any]]] = None,
        fuse_enter_k: float = 5.0,
        fuse_release_k: float = 1.0,
        precondition_stabilize_sec: float = 120.0,
        precondition_band_k: float = 1.0,
        agenda_tol_k: float = 0.2,
        settle_hold_sec: float = 5.0,
    ) -> None:
        self._db_query = db_query
        self._configure_temperature = configure_temperature
        self._log = log
        self._database_ready = database_ready
        self._database_error = database_error
        self._temperature_enabled = temperature_enabled
        self._zero_heaters = zero_heaters or (lambda: None)
        self._measure_source = normalize_measure_source(measure_source)
        self._control_temperature = control_temperature or (lambda: None)
        self._temperature_snapshot = temperature_snapshot or (lambda: {})
        self._fuse_enter_k = max(0.0, float(fuse_enter_k))
        self._fuse_release_k = max(0.0, float(fuse_release_k))
        # Fast preheat/precool until |T − t_exp| ≤ band, then stabilize this long.
        self._precondition_stabilize_sec = max(0.0, float(precondition_stabilize_sec))
        self._precondition_band_k = max(0.05, float(precondition_band_k))
        self._agenda_tol_k = max(0.0, float(agenda_tol_k))
        self._settle_hold_sec = max(0.0, float(settle_hold_sec))
        self._settle_since_monotonic: Optional[float] = None
        self._lock = threading.RLock()
        self._state = ExperimentState()
        self._scheduler = ProgramScheduler()

    def is_running(self) -> bool:
        with self._lock:
            return self._state.program_id is not None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return state_to_public_dict(self._state)

    def elapsed_s(self) -> float:
        """Scheduler elapsed time — same clock as UI timing (steps, not precondition)."""
        with self._lock:
            return program_elapsed_s(self._state)

    def run_elapsed_s(self) -> float:
        """Wall time since run start — used for measurement rows / charts."""
        with self._lock:
            started = self._state.started_monotonic
            if started is None:
                return 0.0
            return max(0.0, time.monotonic() - float(started))

    def start(self, program_id: int) -> Dict[str, Any]:
        with self._lock:
            return self._start_locked(int(program_id))

    def _db_cmd_ok(self, response: Dict[str, Any], context: str) -> bool:
        if response.get('result') == 'Ok':
            return True
        err = str(response.get('error', '') or 'failed')
        self._log(f'{context}: {err}')
        return False

    def _update_program_status(self, program_id: int, status: str) -> bool:
        try:
            return self._db_cmd_ok(
                self._db_query({
                    'cmd': 'program_update_status',
                    'id': int(program_id),
                    'status': status,
                }),
                f'program_update_status({program_id}→{status})',
            )
        except Exception as exc:
            self._log(f'program_update_status({program_id}→{status}): {exc}')
            return False

    def _finish_program_run(self, run_id: int, status: str) -> bool:
        if run_id <= 0:
            return True
        try:
            return self._db_cmd_ok(
                self._db_query({
                    'cmd': 'program_run_finish',
                    'run_id': int(run_id),
                    'status': status,
                }),
                f'program_run_finish(run={run_id}→{status})',
            )
        except Exception as exc:
            self._log(f'program_run_finish(run={run_id}→{status}): {exc}')
            return False

    def _abort_failed_start(self, program_id: int, run_id: int, reason: str) -> None:
        """Roll back DB + core state when start fails after program_run_start."""
        self._zero_heaters()
        self._halt_temperature_control()
        if self._database_ready():
            try:
                self._finish_program_run(run_id, 'Failed')
                self._update_program_status(program_id, 'Stopped')
                self._finish_active_runs(program_id, 'Failed')
            except Exception as exc:
                self._log(f'Failed to roll back aborted start for program {program_id}: {exc}')
        self._state = ExperimentState()
        self._log(f'Program {program_id} start aborted (run {run_id or "—"}): {reason}')

    def _load_program_detail(self, program_id: int) -> Dict[str, Any]:
        response = self._db_query({'cmd': 'get_program_detail', 'id': program_id})
        if response.get('result') != 'Ok':
            return {}
        row = response.get('row') or {}
        return row if isinstance(row, dict) else {}

    def _load_experiment_mode(self, program_id: int) -> str:
        row = self._load_program_detail(program_id)
        meta = row.get('meta') if isinstance(row.get('meta'), dict) else {}
        return normalize_experiment_mode(str(meta.get('experiment_mode', 'default')))

    def _persist_run_setup_meta(self, run_id: int, program_id: int) -> None:
        row = self._load_program_detail(program_id)
        meta = row.get('meta') if isinstance(row.get('meta'), dict) else {}
        experiment_mode = normalize_experiment_mode(str(meta.get('experiment_mode', 'default')))
        description = str(row.get('description', '') or '')

        measure_source = self._measure_source
        measure_device = measure_device_label(measure_source)

        e720 = row.get('e720') if isinstance(row.get('e720'), dict) else {}
        sweep_mode = int(e720.get('param', 0) or 0)
        sweep_device = measure_source
        config = e720.get('config')
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                config = {}
        if isinstance(config, dict) and config.get('device'):
            sweep_device = normalize_measure_source(str(config['device']))

        fields = {
            'measure_source': measure_source,
            'measure_device': measure_device,
            'experiment_mode': experiment_mode,
            'program_description': description,
            'sweep_device': sweep_device,
            'sweep_mode': str(sweep_mode),
        }
        for key, value in fields.items():
            resp = self._db_query({
                'cmd': 'program_run_meta_set',
                'run_id': int(run_id),
                'key': key,
                'value': str(value),
            })
            if resp.get('result') != 'Ok':
                self._log(f'program_run_meta_set({key}): {resp.get("error", "failed")}')

    def _start_locked(self, program_id_int: int) -> Dict[str, Any]:
        if not self._database_ready():
            return {'result': 'False', 'error': self._database_error()}
        experiment_mode = self._load_experiment_mode(program_id_int)
        if uses_temperature_control(experiment_mode) and not self._temperature_enabled():
            return {
                'result': 'False',
                'error': 'Temperature control not enabled (enable_pwm_controller:=true)',
            }
        steps, steps_err = self._load_steps(program_id_int)
        if steps_err:
            return {'result': 'False', 'error': steps_err}
        if not steps:
            return {'result': 'False', 'error': f'Program {program_id_int} has no steps'}

        with self._lock:
            active_id = self._state.program_id
            if active_id is not None:
                if int(active_id) == program_id_int:
                    return {
                        'result': 'False',
                        'error': f'Program {program_id_int} is already running in core',
                    }
                self._stop_locked(program_id=None, final_status='Stopped')

        run_id = 0
        try:
            run_resp = self._db_query({'cmd': 'program_run_start', 'program_id': program_id_int})
            if run_resp.get('result') != 'Ok':
                raise RuntimeError(run_resp.get('error', 'program_run_start failed'))
            run_row = run_resp.get('row') or {}
            run_id = int(run_row.get('run_id', 0) or 0)
            run_index = int(run_row.get('run_index', 0) or 0)
            if run_id <= 0:
                raise RuntimeError('program_run_start returned no run_id')

            if not self._update_program_status(program_id_int, 'Running'):
                raise RuntimeError('program_update_status failed (program not marked Running in DB)')

            self._mark_other_programs_stopped(program_id_int)

            now = time.monotonic()
            first_target_k = float(steps[0].t_start)
            self._state = ExperimentState(
                program_id=program_id_int,
                run_id=run_id,
                run_index=run_index,
                experiment_mode=experiment_mode,
                steps=steps,
                step_index=0,
                step_started_monotonic=None,
                started_monotonic=now,
                status='Running',
                last_target_k=first_target_k,
            )

            if uses_temperature_control(experiment_mode):
                current_t = self._current_control_temp()
                band = self._precondition_band_k
                # Fast pre-experiment: full heat/cool until |CH9 − t_exp| ≤ band,
                # then stabilize for precondition_stabilize_sec before timing.
                if current_t is not None:
                    delta = float(current_t) - first_target_k
                    if abs(delta) > band:
                        self._state.preconditioning = True
                        self._state.precondition_target_k = first_target_k
                        if delta < 0.0:
                            self._state.precondition_phase = 'preheat'
                            self._state.status = (
                                f'Preheating to {first_target_k:.1f} K ±{band:.1f} K'
                            )
                            self._preheat()
                            self._log(
                                f'Pre-experiment: T={current_t:.1f} K < start '
                                f'{first_target_k:.1f} K — fast preheat until '
                                f'|CH9−t_exp|≤{band:.1f} K, then '
                                f'{self._precondition_stabilize_sec:.0f}s stabilize'
                            )
                        else:
                            self._state.precondition_phase = 'precool'
                            self._state.status = (
                                f'Precooling to {first_target_k:.1f} K ±{band:.1f} K'
                            )
                            self._precool()
                            self._log(
                                f'Pre-experiment: T={current_t:.1f} K > start '
                                f'{first_target_k:.1f} K — fast precool until '
                                f'|CH9−t_exp|≤{band:.1f} K, then '
                                f'{self._precondition_stabilize_sec:.0f}s stabilize'
                            )
                    else:
                        # Already inside ±band → go straight to stabilize.
                        self._state.preconditioning = True
                        self._state.precondition_target_k = first_target_k
                        self._enter_stabilize_locked(first_target_k, current_t, 'in-band')
                else:
                    self._apply_target(
                        first_target_k,
                        reset_integral=True,
                        raise_on_error=True,
                        actuator_mode=actuator_mode_for_step(steps[0]),
                        seed_cooler_min=is_cooldown_step(steps[0]),
                    )

            self._persist_run_setup_meta(run_id, program_id_int)
        except Exception as exc:
            self._abort_failed_start(program_id_int, run_id, str(exc))
            return {'result': 'False', 'error': str(exc)}

        total_min = total_program_duration_s(steps) / 60.0
        self._log(
            f'Core started program {program_id_int} run {run_id} '
            f'({len(steps)} steps, ~{total_min:.1f} min, mode={experiment_mode})'
        )
        return {'result': 'Ok', 'program': state_to_public_dict(self._state)}

    def stop(self, program_id: Optional[int] = None, final_status: str = 'Stopped') -> Dict[str, Any]:
        with self._lock:
            return self._stop_locked(program_id, final_status)

    def _stop_locked(
        self,
        program_id: Optional[int],
        final_status: str,
    ) -> Dict[str, Any]:
        active_id = self._state.program_id
        run_id = self._state.run_id

        if active_id is None:
            if program_id is not None and self._database_ready():
                pid = int(program_id)
                updated = self._update_program_status(pid, final_status)
                self._finish_active_runs(pid, final_status)
                if not updated:
                    self._update_program_status(pid, final_status)
            elif self._database_ready():
                reconciled = self._reconcile_db_stale_running(final_status)
                if reconciled:
                    self._log(
                        f'Reconciled {reconciled} stale Running program(s) in DB → {final_status}'
                    )
            self._zero_heaters()
            self._halt_temperature_control()
            return {'result': 'Ok', 'program': state_to_public_dict(self._state)}

        if program_id is not None and int(program_id) != int(active_id):
            return {
                'result': 'False',
                'error': f'Program {program_id} is not the active run (active={active_id})',
            }

        self._finish_program(run_id, int(active_id), final_status)
        return {'result': 'Ok', 'program': state_to_public_dict(self._state)}

    def stop_all(self) -> Dict[str, Any]:
        with self._lock:
            return self._stop_locked(program_id=None, final_status='Stopped')

    def _current_control_temp(self) -> Optional[float]:
        try:
            value = self._control_temperature()
        except Exception:
            return None
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _preheat(self) -> None:
        """Command full-power heat (PI paused) during pre-experiment."""
        if not self._temperature_enabled():
            return
        target = self._state.precondition_target_k
        try:
            payload: Dict[str, Any] = {'enabled': True, 'preheat': True}
            if target is not None:
                payload['target_k'] = float(target)
            self._configure_temperature(payload)
        except Exception as exc:
            self._log(f'Preheat command failed: {exc}')

    def _precool(self) -> None:
        """Command full-power cool (PI paused) during pre-experiment."""
        if not self._temperature_enabled():
            return
        target = self._state.precondition_target_k
        try:
            payload: Dict[str, Any] = {'enabled': True, 'precool': True}
            if target is not None:
                payload['target_k'] = float(target)
            self._configure_temperature(payload)
        except Exception as exc:
            self._log(f'Precool command failed: {exc}')

    def _thermal_snapshot(self) -> Dict[str, Any]:
        try:
            snap = self._temperature_snapshot()
        except Exception:
            return {}
        return snap if isinstance(snap, dict) else {}

    def _is_settled_at(self, target_k: float) -> bool:
        """CH9 inside ±agenda_tol with low predicted rate (from thermal worker)."""
        snap = self._thermal_snapshot()
        temp = snap.get('latest_control_temp_k')
        if temp is None:
            temp = self._current_control_temp()
        if temp is None:
            return False
        err = abs(float(target_k) - float(temp))
        if err > self._agenda_tol_k:
            return False
        dT = float(snap.get('dT_dt') or 0.0)
        return abs(dT) <= 0.05

    def _handoff_from_preheat(
        self,
        target_k: float,
        actuator_mode: str,
        *,
        stabilize: bool = False,
    ) -> None:
        """Hand control from preheat/precool hold to predictive PI."""
        if not self._temperature_enabled():
            return
        try:
            self._configure_temperature({
                'enabled': True,
                'target_k': float(target_k),
                'actuator_mode': str(actuator_mode),
                'handoff_hold': True,
                # Stabilize soft-hold side matches actuator_mode (heat/cool).
                'stabilize_mode': bool(stabilize),
            })
        except Exception as exc:
            self._log(f'Preheat handoff failed: {exc}')

    def _enter_stabilize_locked(
        self,
        target_k: float,
        current_t: float,
        why: str,
    ) -> None:
        """Switch into the fixed-duration stabilize window at t_exp."""
        # Side follows how we approached t_exp:
        #   preheat → heat_only (+ heater floor) — bipolar cool freefalls
        #   precool → cool_only (+ cooler floor) — heat floor runaway to +50 K
        prev = self._state.precondition_phase or 'preheat'
        mode = 'cool_only' if prev == 'precool' else 'heat_only'
        self._handoff_from_preheat(float(target_k), mode, stabilize=True)
        self._state.precondition_phase = 'stabilize'
        self._state.stabilize_until_monotonic = (
            time.monotonic() + self._precondition_stabilize_sec
        )
        self._settle_since_monotonic = None
        self._log(
            f'Pre-experiment soft-land ({why}): CH9={float(current_t):.2f} K → '
            f'stabilize at {target_k:.1f} K for {self._precondition_stabilize_sec:.0f}s '
            f'({mode} hold, then ±{self._agenda_tol_k:.1f} K settle)'
        )
        self._state.status = (
            f'Stabilizing at {target_k:.1f} K '
            f'({self._precondition_stabilize_sec:.0f}s, ±{self._agenda_tol_k:.1f} K)'
        )

    def _precondition_tick_locked(self) -> bool:
        """Pre-experiment: fast heat/cool → ±band → stabilize → start."""
        target = self._state.precondition_target_k
        if target is None:
            self._state.preconditioning = False
            self._state.precondition_phase = ''
            self._settle_since_monotonic = None
            return False

        phase = self._state.precondition_phase or 'preheat'
        band = self._precondition_band_k
        snap = self._thermal_snapshot()
        current_t = snap.get('latest_control_temp_k')
        if current_t is None:
            current_t = self._current_control_temp()
        predicted = snap.get('predicted_k')

        if phase in ('preheat', 'precool'):
            if current_t is None:
                return True
            err = abs(float(current_t) - float(target))
            # Full power until inside ±band (1 K), then 2‑minute stabilize.
            if err <= band:
                self._enter_stabilize_locked(float(target), float(current_t), f'±{band:.1f} K')
                return True
            verb = 'Preheating' if phase == 'preheat' else 'Precooling'
            pred_txt = (
                f', pred={float(predicted):.1f} K' if predicted is not None else ''
            )
            self._state.status = (
                f'{verb} to {target:.1f} K ±{band:.1f} K '
                f'(CH9={float(current_t):.1f} K, err={err:.1f} K{pred_txt})'
            )
            return True

        # phase == 'stabilize'
        until = self._state.stabilize_until_monotonic or 0.0
        min_remaining = until - time.monotonic()
        settled = self._is_settled_at(float(target))
        now = time.monotonic()
        if settled:
            if self._settle_since_monotonic is None:
                self._settle_since_monotonic = now
        else:
            self._settle_since_monotonic = None

        settle_held = (
            self._settle_since_monotonic is not None
            and (now - self._settle_since_monotonic) >= self._settle_hold_sec
        )
        ready = min_remaining <= 0.0 and settle_held

        if ready:
            self._state.preconditioning = False
            self._state.precondition_phase = ''
            self._state.stabilize_until_monotonic = None
            self._state.precondition_target_k = None
            self._settle_since_monotonic = None
            self._state.status = 'Running'
            t_txt = f'{float(current_t):.2f}' if current_t is not None else '?'
            self._log(
                f'Stabilization settled: CH9={t_txt} K within ±{self._agenda_tol_k:.1f} K '
                f'of {target:.1f} K — starting program timing'
            )
            return False

        t_txt = f', CH9={float(current_t):.2f} K' if current_t is not None else ''
        pred_txt = (
            f', pred={float(predicted):.2f} K' if predicted is not None else ''
        )
        if min_remaining > 0.0:
            self._state.status = (
                f'Stabilizing at {target:.1f} K '
                f'({min_remaining:.0f}s left{t_txt}{pred_txt})'
            )
        elif settled:
            held = now - float(self._settle_since_monotonic or now)
            need = self._settle_hold_sec
            self._state.status = (
                f'Settling ±{self._agenda_tol_k:.1f} K '
                f'({held:.0f}/{need:.0f}s{t_txt})'
            )
        else:
            err = (
                abs(float(target) - float(current_t))
                if current_t is not None
                else float('nan')
            )
            self._state.status = (
                f'Waiting ±{self._agenda_tol_k:.1f} K '
                f'(err={err:.2f} K{t_txt}{pred_txt})'
            )
        return True

    def tick(self) -> None:
        with self._lock:
            if self._state.program_id is None:
                return
            just_released = False
            if self._state.preconditioning:
                if self._precondition_tick_locked():
                    return
                # Released this tick → fall through so the scheduler starts
                # step 1 timing immediately, using a smooth handoff (below).
                just_released = True
            action = self._scheduler.tick(self._state)

            if not action.get('active'):
                if action.get('finished'):
                    pid = int(action.get('program_id', self._state.program_id or 0))
                    rid = int(self._state.run_id or 0)
                    hold_k = action.get('target_k')
                    if hold_k is None:
                        hold_k = self._state.last_target_k
                    self._finish_program(
                        rid,
                        pid,
                        'Finished',
                        hold_target_k=hold_k,
                    )
                return

            target_k = action.get('target_k')
            if target_k is None:
                return
            reset = bool(action.get('step_started') or action.get('reset_integral'))
            # Never bump PI on a continuous step handoff (cool→hold at same T).
            if action.get('continuous_transition'):
                reset = False
            if uses_temperature_control(self._state.experiment_mode):
                step_idx = int(self._state.step_index)
                mode = 'bipolar'
                if 0 <= step_idx < len(self._state.steps):
                    mode = actuator_mode_for_step(self._state.steps[step_idx])
                if just_released:
                    # Leave stabilize soft-hold; bipolar for program steps.
                    # Do NOT seed cooler — that freefalls below a descending agenda.
                    self._apply_target(
                        float(target_k),
                        reset_integral=False,
                        actuator_mode=mode,
                        seed_cooler_min=False,
                        stabilize_mode=False,
                    )
                else:
                    seed_cooler = bool(action.get('step_started')) and is_cooldown_step(
                        self._state.steps[step_idx]
                    ) if 0 <= step_idx < len(self._state.steps) else False
                    self._apply_target(
                        float(target_k),
                        reset_integral=reset,
                        actuator_mode=mode,
                        seed_cooler_min=seed_cooler,
                    )
            if action.get('step_started') or action.get('advanced_step'):
                step = int(action.get('step_index', 0)) + 1
                total = int(action.get('step_count', 0))
                cont = ' continuous' if action.get('continuous_transition') else ''
                self._log(
                    f'Program {self._state.program_id}: step {step}/{total}, '
                    f'target {float(target_k):.2f} K{cont}'
                )

    def _persist_program_finish(self, run_id: int, program_id: int, final_status: str) -> bool:
        """Write run + program final status to DB; return True if persistence succeeded."""
        if not self._database_ready():
            return False

        run_ok = self._finish_program_run(run_id, final_status)
        prog_ok = self._update_program_status(program_id, final_status)
        if run_ok and prog_ok:
            return True

        try:
            self._finish_active_runs(program_id, final_status)
            prog_ok = self._update_program_status(program_id, final_status)
            run_ok = self._finish_program_run(run_id, final_status)
            return prog_ok and run_ok
        except Exception as exc:
            self._log(f'Program {program_id} finish fallback failed: {exc}')
            return False

    def _finish_program(
        self,
        run_id: int,
        program_id: int,
        final_status: str,
        *,
        hold_target_k: Optional[float] = None,
    ) -> None:
        # A completed run keeps the last agenda temperature. Stop / FAIL still
        # drops the outputs. Stabilize or a new program is what moves T next.
        held_k: Optional[float] = None
        if str(final_status) == 'Finished' and uses_temperature_control(self._state.experiment_mode):
            if hold_target_k is None:
                hold_target_k = self._state.last_target_k
            if hold_target_k is None and self._state.steps:
                hold_target_k = float(self._state.steps[-1].t_stop)
            if hold_target_k is not None and self._hold_finished_temperature(float(hold_target_k)):
                held_k = float(hold_target_k)
        if held_k is None:
            self._zero_heaters()
            self._halt_temperature_control()

        db_ok = self._persist_program_finish(run_id, program_id, final_status)
        if not db_ok:
            self._log(
                f'CRITICAL: Program {program_id} run {run_id} ended in core but DB finish failed '
                f'(target status {final_status}) — forcing DB reconcile'
            )
            try:
                self._finish_active_runs(program_id, final_status)
                if not self._update_program_status(program_id, final_status):
                    self._log(
                        f'CRITICAL: program_update_status still failed for program {program_id} '
                        f'after finish_active_runs'
                    )
            except Exception as exc:
                self._log(f'CRITICAL: DB reconcile after finish failed for program {program_id}: {exc}')

        self._state = ExperimentState()
        hold_note = f' — holding {held_k:.2f} K' if held_k is not None else ''
        self._log(
            f'Program {program_id} ended: {final_status}{hold_note}'
            + ('' if db_ok else ' (DB sync required — check program_runs)')
        )

    def _hold_finished_temperature(self, target_k: float) -> bool:
        """Keep bipolar PI on the last program temperature after a normal finish."""
        if not self._temperature_enabled():
            return False
        try:
            self._configure_temperature({
                'enabled': True,
                'target_k': float(target_k),
                'actuator_mode': 'bipolar',
                'stabilize_mode': False,
                'clear_hold': True,
            })
        except Exception as exc:
            self._log(f'Hold after finish failed: {exc}')
            return False
        return True

    def _apply_target(
        self,
        target_k: float,
        *,
        reset_integral: bool = False,
        raise_on_error: bool = False,
        actuator_mode: Optional[str] = None,
        seed_cooler_min: bool = False,
        stabilize_mode: Optional[bool] = None,
    ) -> None:
        if not self._temperature_enabled():
            if raise_on_error:
                raise RuntimeError('Temperature control is not enabled')
            return
        payload: Dict[str, Any] = {'enabled': True, 'target_k': float(target_k)}
        if reset_integral:
            payload['reset_integral'] = True
        if actuator_mode is not None:
            payload['actuator_mode'] = str(actuator_mode)
        if seed_cooler_min:
            payload['seed_cooler_min'] = True
        if stabilize_mode is not None:
            payload['stabilize_mode'] = bool(stabilize_mode)
        try:
            self._configure_temperature(payload)
        except Exception as exc:
            self._log(f'Temperature control update failed: {exc}')
            if raise_on_error:
                raise RuntimeError(f'Temperature control update failed: {exc}') from exc

    def _halt_temperature_control(self) -> None:
        if not self._temperature_enabled():
            return
        try:
            self._configure_temperature({'enabled': False})
        except Exception as exc:
            self._log(f'Failed to disable temperature control: {exc}')

    def _load_steps(self, program_id: int) -> tuple[List[ProgramStep], str]:
        response = self._db_query({'cmd': 'program_step_list', 'id': program_id})
        if response.get('result') != 'Ok':
            err = str(response.get('error', '') or 'Failed to load program steps')
            return [], err
        steps = ProgramScheduler.parse_steps(response.get('row', []))
        return steps, ''

    def _reconcile_db_stale_running(
        self,
        final_status: str,
        *,
        except_program_id: Optional[int] = None,
    ) -> int:
        """Finish DB programs/runs still marked Running while core is idle."""
        reconciled = 0
        try:
            response = self._db_query({'cmd': 'program_all_list'})
            if response.get('result') != 'Ok':
                return 0
            for raw_row in response.get('row', []):
                parts = str(raw_row).split('^')
                if len(parts) < 3:
                    continue
                pid = int(parts[0])
                if except_program_id is not None and pid == int(except_program_id):
                    continue
                status = str(parts[2] or '').strip().lower()
                if status != 'running':
                    continue
                if self._update_program_status(pid, final_status):
                    self._finish_active_runs(pid, final_status)
                    reconciled += 1
        except Exception as exc:
            self._log(f'Failed to reconcile stale Running programs in DB: {exc}')
        return reconciled

    def _mark_other_programs_stopped(self, except_program_id: int) -> None:
        self._reconcile_db_stale_running('Stopped', except_program_id=except_program_id)

    def _finish_active_runs(self, program_id: int, status: str) -> None:
        try:
            self._db_query({
                'cmd': 'program_run_finish_active',
                'program_id': program_id,
                'status': status,
            })
        except Exception as exc:
            self._log(f'finish_active_runs: {exc}')
