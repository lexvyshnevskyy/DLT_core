"""Legacy-inspired temperature control helpers ported from the Delphi application."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ProfileSegment:
    start_temp_c: float
    end_temp_c: float
    duration_s: float


@dataclass
class ProgramState:
    segments: List[ProfileSegment]
    segment_index: int = 0
    elapsed_in_segment_s: float = 0.0
    total_elapsed_s: float = 0.0
    finished: bool = False


class RampHoldProgram:
    """Port of the Delphi `rezym()` helper.

    It creates a two-step program:
    1. Ramp from current temperature to target temperature.
    2. Hold target temperature for a long period.
    """

    def __init__(self, ramp_seconds_per_degree: float = 6.0, hold_seconds: float = 86400.0) -> None:
        self.ramp_seconds_per_degree = max(0.0, float(ramp_seconds_per_degree))
        self.hold_seconds = max(0.0, float(hold_seconds))
        self.state: Optional[ProgramState] = None

    def start(self, current_temp_c: float, target_temp_c: float) -> None:
        ramp_duration = abs(target_temp_c - current_temp_c) * self.ramp_seconds_per_degree
        self.state = ProgramState(
            segments=[
                ProfileSegment(
                    start_temp_c=float(current_temp_c),
                    end_temp_c=float(target_temp_c),
                    duration_s=float(ramp_duration),
                ),
                ProfileSegment(
                    start_temp_c=float(target_temp_c),
                    end_temp_c=float(target_temp_c),
                    duration_s=float(self.hold_seconds),
                ),
            ]
        )

    def reset(self) -> None:
        self.state = None

    def is_active(self) -> bool:
        return self.state is not None and not self.state.finished

    def step(self, dt_s: float) -> Optional[float]:
        if self.state is None:
            return None

        state = self.state
        if state.finished:
            return state.segments[-1].end_temp_c

        segment = state.segments[state.segment_index]
        duration_s = max(0.0, segment.duration_s)

        if duration_s == 0.0:
            setpoint = segment.end_temp_c
        else:
            fraction = min(1.0, state.elapsed_in_segment_s / duration_s)
            setpoint = segment.start_temp_c + (segment.end_temp_c - segment.start_temp_c) * fraction

        state.elapsed_in_segment_s += dt_s
        state.total_elapsed_s += dt_s

        if state.elapsed_in_segment_s > duration_s:
            if state.segment_index + 1 < len(state.segments):
                state.segment_index += 1
                state.elapsed_in_segment_s = 0.0
            else:
                state.finished = True
                setpoint = segment.end_temp_c

        return setpoint

    def snapshot(self) -> dict:
        if self.state is None:
            return {
                'active': False,
                'finished': False,
            }

        current = self.state.segments[self.state.segment_index]
        return {
            'active': not self.state.finished,
            'finished': self.state.finished,
            'segment_index': self.state.segment_index,
            'segment_start_c': current.start_temp_c,
            'segment_end_c': current.end_temp_c,
            'segment_duration_s': current.duration_s,
            'segment_elapsed_s': self.state.elapsed_in_segment_s,
            'total_elapsed_s': self.state.total_elapsed_s,
        }


class LegacyPidController:
    """Incremental PID port close to the Delphi `reg()` implementation."""

    def __init__(
        self,
        k: float = 100.0,
        ti: float = 3000.0,
        td: float = 5.0,
        sample_time_model: float = 100.0,
        output_min: int = 0,
        output_max: int = 1000,
    ) -> None:
        self.k = float(k)
        self.ti = float(ti)
        self.td = float(td)
        self.sample_time_model = float(sample_time_model)
        self.output_min = int(output_min)
        self.output_max = int(output_max)

        self.output = 0
        self.e0 = 0.0
        self.e1 = 0.0
        self.e2 = 0.0

    def reset(self, output: int = 0) -> None:
        self.output = self._clamp(output)
        self.e0 = 0.0
        self.e1 = 0.0
        self.e2 = 0.0

    def update(self, setpoint_c: float, measured_c: float) -> int:
        t = self.sample_time_model
        if t <= 0.0:
            raise ValueError('sample_time_model must be > 0')
        if self.ti == 0.0:
            raise ValueError('ti must not be 0')

        self.e2 = self.e1
        self.e1 = self.e0
        self.e0 = float(setpoint_c) - float(measured_c)

        d_u = (
            (self.k + self.k * (self.td / t)) * self.e0
            + (self.k * (t / self.ti) - 2.0 * self.k * (self.td / t) - self.k) * self.e1
            + (self.k * (self.td / t)) * self.e2
        )

        self.output = self._clamp(round(self.output + d_u))
        return self.output

    def snapshot(self) -> dict:
        return {
            'k': self.k,
            'ti': self.ti,
            'td': self.td,
            'sample_time_model': self.sample_time_model,
            'output': self.output,
            'e0': self.e0,
            'e1': self.e1,
            'e2': self.e2,
        }

    def _clamp(self, value: int) -> int:
        return max(self.output_min, min(self.output_max, int(value)))
