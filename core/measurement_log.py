from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

DbQueryFn = Callable[[Dict[str, Any]], Dict[str, Any]]


def e720_measure_values(
    e720: Optional[Dict[str, Any]],
    *,
    updated_monotonic: float = 0.0,
    max_age_sec: float = 1.0,
    now: Optional[float] = None,
) -> Tuple[float, float, float]:
    if not e720:
        return 0.0, 0.0, 0.0
    if now is None:
        now = time.monotonic()
    if updated_monotonic <= 0.0 or (now - float(updated_monotonic)) > float(max_age_sec):
        return 0.0, 0.0, 0.0
    return (
        float(e720.get('frequency', 0.0) or 0.0),
        float(e720.get('firstvalue', 0.0) or 0.0),
        float(e720.get('secondvalue', 0.0) or 0.0),
    )


def build_measurement_row(
    program_id: int,
    e720: Dict[str, Any],
    control_value: float,
    monitor_value: float,
    target_k: Optional[float],
    *,
    run_id: Optional[int] = None,
    elapsed_s: Optional[float] = None,
    e720_updated_monotonic: float = 0.0,
    e720_max_age_sec: float = 1.0,
    include_ltm: bool = True,
) -> Dict[str, Any]:
    freq, measure_ch1, measure_ch2 = e720_measure_values(
        e720,
        updated_monotonic=e720_updated_monotonic,
        max_age_sec=e720_max_age_sec,
    )
    row: Dict[str, Any] = {
        'program_id': program_id,
        'freq': freq,
        'measure_ch1': measure_ch1,
        'measure_ch2': measure_ch2,
        't_ch1': float(control_value) if include_ltm else 0.0,
        't_ch2': float(monitor_value) if include_ltm else 0.0,
        't_exp': float(target_k if target_k is not None else 0.0),
    }
    if run_id is not None and int(run_id) > 0:
        row['run_id'] = int(run_id)
    if elapsed_s is not None:
        row['elapsed_s'] = max(0.0, float(elapsed_s))
    return row


def insert_measurement_immediate(db_query: DbQueryFn, row: Dict[str, Any]) -> bool:
    """One row, one commit — no batching."""
    response = db_query({'cmd': 'measurement_insert', **row})
    return str(response.get('result', '')).lower() == 'ok' and int(response.get('ID', 0) or 0) > 0
