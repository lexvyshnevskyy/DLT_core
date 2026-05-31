from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .program_experiment import (
    ExperimentState,
    ProgramScheduler,
    ProgramStep,
    state_to_public_dict,
    total_program_duration_s,
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
    ) -> None:
        self._db_query = db_query
        self._configure_temperature = configure_temperature
        self._log = log
        self._database_ready = database_ready
        self._database_error = database_error
        self._temperature_enabled = temperature_enabled
        self._zero_heaters = zero_heaters or (lambda: None)
        self._lock = threading.RLock()
        self._state = ExperimentState()
        self._scheduler = ProgramScheduler()

    def is_running(self) -> bool:
        with self._lock:
            return self._state.program_id is not None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return state_to_public_dict(self._state)

    def start(self, program_id: int) -> Dict[str, Any]:
        with self._lock:
            return self._start_locked(int(program_id))

    def _abort_failed_start(self, program_id: int, run_id: int, reason: str) -> None:
        """Roll back DB + core state when start fails after program_run_start."""
        self._zero_heaters()
        self._halt_temperature_control()
        if self._database_ready():
            try:
                if run_id > 0:
                    self._db_query({
                        'cmd': 'program_run_finish',
                        'run_id': run_id,
                        'status': 'Failed',
                    })
                self._db_query({'cmd': 'program_update_status', 'id': program_id, 'status': 'Stopped'})
                self._finish_active_runs(program_id, 'Failed')
            except Exception as exc:
                self._log(f'Failed to roll back aborted start for program {program_id}: {exc}')
        self._state = ExperimentState()
        self._log(f'Program {program_id} start aborted (run {run_id or "—"}): {reason}')

    def _start_locked(self, program_id_int: int) -> Dict[str, Any]:
        if not self._database_ready():
            return {'result': 'False', 'error': self._database_error()}
        if not self._temperature_enabled():
            return {
                'result': 'False',
                'error': 'Temperature control not enabled (enable_pwm_controller:=true)',
            }
        steps, steps_err = self._load_steps(program_id_int)
        if steps_err:
            return {'result': 'False', 'error': steps_err}
        if not steps:
            return {'result': 'False', 'error': f'Program {program_id_int} has no steps'}

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

            status_resp = self._db_query({
                'cmd': 'program_update_status',
                'id': program_id_int,
                'status': 'Running',
            })
            if status_resp.get('result') != 'Ok':
                raise RuntimeError(status_resp.get('error', 'program_update_status failed'))

            self._mark_other_programs_stopped(program_id_int)

            now = time.monotonic()
            first_target_k = float(steps[0].t_start)
            self._state = ExperimentState(
                program_id=program_id_int,
                run_id=run_id,
                run_index=run_index,
                steps=steps,
                step_index=0,
                step_started_monotonic=None,
                started_monotonic=now,
                status='Running',
                last_target_k=first_target_k,
            )

            self._apply_target(first_target_k, reset_integral=True, raise_on_error=True)
        except Exception as exc:
            self._abort_failed_start(program_id_int, run_id, str(exc))
            return {'result': 'False', 'error': str(exc)}

        total_min = total_program_duration_s(steps) / 60.0
        self._log(
            f'Core started program {program_id_int} run {run_id} '
            f'({len(steps)} steps, ~{total_min:.1f} min)'
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
                self._db_query({'cmd': 'program_update_status', 'id': pid, 'status': final_status})
                self._finish_active_runs(pid, final_status)
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

    def tick(self) -> None:
        with self._lock:
            if self._state.program_id is None:
                return
            action = self._scheduler.tick(self._state)

            if not action.get('active'):
                if action.get('finished'):
                    pid = int(action.get('program_id', self._state.program_id or 0))
                    rid = int(self._state.run_id or 0)
                    self._finish_program(rid, pid, 'Finished')
                return

            target_k = action.get('target_k')
            if target_k is None:
                return
            reset = bool(action.get('step_started') or action.get('reset_integral'))
            self._apply_target(float(target_k), reset_integral=reset)
            if action.get('step_started') or action.get('advanced_step'):
                step = int(action.get('step_index', 0)) + 1
                total = int(action.get('step_count', 0))
                self._log(
                    f'Program {self._state.program_id}: step {step}/{total}, '
                    f'target {float(target_k):.2f} K'
                )

    def _finish_program(self, run_id: int, program_id: int, final_status: str) -> None:
        self._zero_heaters()
        self._halt_temperature_control()
        if self._database_ready() and run_id > 0:
            try:
                self._db_query({
                    'cmd': 'program_run_finish',
                    'run_id': run_id,
                    'status': final_status,
                })
                self._db_query({'cmd': 'program_update_status', 'id': program_id, 'status': final_status})
            except Exception as exc:
                self._log(f'Failed to update program status: {exc}')
        with self._lock:
            self._state = ExperimentState()
        self._log(f'Program {program_id} ended: {final_status}')

    def _apply_target(
        self,
        target_k: float,
        *,
        reset_integral: bool = False,
        raise_on_error: bool = False,
    ) -> None:
        if not self._temperature_enabled():
            if raise_on_error:
                raise RuntimeError('Temperature control is not enabled')
            return
        payload: Dict[str, Any] = {'enabled': True, 'target_k': float(target_k)}
        if reset_integral:
            payload['reset_integral'] = True
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
                self._db_query({'cmd': 'program_update_status', 'id': pid, 'status': final_status})
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
