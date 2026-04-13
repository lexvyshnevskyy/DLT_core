"""Math helper ported from the legacy package."""

from __future__ import annotations

from typing import List, Tuple


def clamp(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


class ExpTempController:
    def __init__(self) -> None:
        self.k = 100.0
        self.ti = 3000.0
        self.td = 5.0

    def compute_setpoint(
        self,
        temp_start: float,
        temp_end: float,
        ramp_time_s: float,
        calc_rate_hz: float,
        tick: int,
    ) -> float:
        total_ticks = int(ramp_time_s * calc_rate_hz)
        if total_ticks <= 0:
            return temp_end
        delta_per_tick = (temp_end - temp_start) / total_ticks
        sp = temp_start + tick * delta_per_tick
        if sp > temp_end:
            return temp_end
        if sp < temp_start:
            return temp_start
        return sp

    def pid_step(
        self,
        t_cur: float,
        t_model_time: float,
        global_counter: float,
        global_row_counter: int,
        exp_matrix: List[List[float]],
        control_matrix: List[List[float]],
        u1: int,
        u2: int,
        t_step: float = 100.0,
    ) -> Tuple[int, int]:
        def t_exp(offset: float) -> float:
            row = global_row_counter
            return ((global_counter + offset) * exp_matrix[row][3] + exp_matrix[row][0])

        def e(n: int) -> float:
            idx = abs(n) + 1
            return t_exp(-n) - control_matrix[idx][0]

        d_u = (
            (self.k + self.k * (self.td / t_step)) * e(0)
            + (self.k * (t_model_time / self.ti) - 2 * self.k * (self.td / t_model_time) - self.k) * e(-1)
            + (self.k * (self.td / t_model_time)) * e(-2)
        )

        u1_new = clamp(round(u1 + d_u), 0, 1000)
        if 200 <= t_cur <= 371:
            u2_new = 200
        else:
            u2_new = clamp(round(1530 - t_exp(0) * 6.7), 0, 1000)
        return u1_new, u2_new
