"""Temperature program scheduler — runs inside core (not webui/hmi)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ProgramStep:
    step_id: int
    t_start: float
    t_stop: float
    minutes: float


@dataclass
class ExperimentState:
    program_id: Optional[int] = None
    run_id: Optional[int] = None
    run_index: Optional[int] = None
    steps: List[ProgramStep] = field(default_factory=list)
    step_index: int = 0
    step_started_monotonic: Optional[float] = None
    started_monotonic: Optional[float] = None
    status: str = 'Idle'
    last_target_k: Optional[float] = None


def total_program_duration_s(steps: List[ProgramStep]) -> float:
    return sum(max(0.0, float(s.minutes) * 60.0) for s in steps)


def format_duration_hms(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    total_sec = int(seconds)
    hours, rem = divmod(total_sec, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f'{hours}:{minutes:02d}:{secs:02d}'
    return f'{minutes}:{secs:02d}'


def program_elapsed_s(state: ExperimentState) -> float:
    """Elapsed experiment time from completed steps + current step (core scheduler clock)."""
    if state.program_id is None or not state.steps:
        return 0.0
    completed_s = sum(
        max(0.0, float(s.minutes) * 60.0) for s in state.steps[: max(0, state.step_index)]
    )
    step_elapsed_s = 0.0
    if state.step_started_monotonic is not None:
        step_elapsed_s = max(0.0, time.monotonic() - float(state.step_started_monotonic))
    return completed_s + step_elapsed_s


def experiment_timing(state: ExperimentState) -> Dict[str, Any]:
    if state.program_id is None or not state.steps:
        return {
            'active': False,
            'elapsed_s': 0.0,
            'remaining_s': 0.0,
            'total_s': 0.0,
            'elapsed_text': '0:00',
            'remaining_text': '0:00',
            'total_text': '0:00',
            'progress_percent': 0.0,
        }

    total_s = total_program_duration_s(state.steps)
    elapsed_s = program_elapsed_s(state)
    remaining_s = max(0.0, total_s - elapsed_s)
    progress = (elapsed_s / total_s * 100.0) if total_s > 0 else 0.0

    return {
        'active': True,
        'program_id': state.program_id,
        'run_index': state.run_index,
        'run_id': state.run_id,
        'step_index': state.step_index + 1,
        'step_count': len(state.steps),
        'elapsed_s': round(elapsed_s, 1),
        'remaining_s': round(remaining_s, 1),
        'total_s': round(total_s, 1),
        'elapsed_text': format_duration_hms(elapsed_s),
        'remaining_text': format_duration_hms(remaining_s),
        'total_text': format_duration_hms(total_s),
        'progress_percent': round(min(100.0, progress), 1),
    }


class ProgramScheduler:
    @staticmethod
    def interpolate_target(step: ProgramStep, elapsed_s: float) -> float:
        duration_s = max(0.0, float(step.minutes) * 60.0)
        if duration_s <= 0.0:
            return float(step.t_stop)
        alpha = min(max(elapsed_s / duration_s, 0.0), 1.0)
        return float(step.t_start) + (float(step.t_stop) - float(step.t_start)) * alpha

    def tick(self, state: ExperimentState) -> Dict[str, Any]:
        if state.program_id is None or not state.steps:
            return {'active': False}

        if state.step_index >= len(state.steps):
            return {'active': False, 'finished': True, 'program_id': state.program_id}

        step = state.steps[state.step_index]
        if state.step_started_monotonic is None:
            state.step_started_monotonic = time.monotonic()
            return {
                'active': True,
                'program_id': state.program_id,
                'target_k': float(step.t_start),
                'step_started': True,
                'reset_integral': True,
                'step_index': state.step_index,
                'step_count': len(state.steps),
            }

        elapsed_s = max(0.0, time.monotonic() - float(state.step_started_monotonic))
        target_k = self.interpolate_target(step, elapsed_s)
        state.last_target_k = target_k
        state.status = f'Running step {state.step_index + 1}/{len(state.steps)}'

        duration_s = max(0.0, float(step.minutes) * 60.0)
        if elapsed_s >= duration_s:
            state.step_index += 1
            state.step_started_monotonic = time.monotonic()
            if state.step_index >= len(state.steps):
                return {
                    'active': False,
                    'finished': True,
                    'program_id': state.program_id,
                    'target_k': target_k,
                }
            next_step = state.steps[state.step_index]
            return {
                'active': True,
                'program_id': state.program_id,
                'target_k': float(next_step.t_start),
                'reset_integral': True,
                'advanced_step': True,
                'step_index': state.step_index,
                'step_count': len(state.steps),
            }

        return {
            'active': True,
            'program_id': state.program_id,
            'target_k': target_k,
            'step_index': state.step_index,
            'step_count': len(state.steps),
        }

    @staticmethod
    def parse_steps(rows: List[str]) -> List[ProgramStep]:
        steps: List[ProgramStep] = []
        for raw_row in rows:
            parts = str(raw_row).split('^')
            if len(parts) < 4:
                continue
            steps.append(
                ProgramStep(
                    step_id=int(parts[0]),
                    t_start=float(parts[1]),
                    t_stop=float(parts[2]),
                    minutes=float(parts[3]),
                )
            )
        steps.sort(key=lambda item: item.step_id)
        return steps


def state_to_public_dict(state: ExperimentState) -> Dict[str, Any]:
    timing = experiment_timing(state)
    mode = 'idle'
    if state.program_id is not None:
        mode = 'program_running'
    label = None
    if state.program_id is not None and state.run_index is not None:
        label = f'{state.program_id}.{state.run_index}'
    return {
        'mode': mode,
        'state': 'running' if state.program_id is not None else 'idle',
        'program_id': state.program_id,
        'run_id': state.run_id,
        'run_index': state.run_index,
        'run_label': label,
        'status': state.status,
        'last_target_k': state.last_target_k,
        'timing': timing,
    }
