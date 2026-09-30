from __future__ import annotations

import statistics
import time
from typing import Any, Callable, Dict, List, Optional

# One stored row is the median of every real sample collected in this long.
MEASUREMENT_BIN_S = 2.0

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
    monitor_value: Optional[float],
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
        # None means "no monitor sample". Do not substitute 0 K — that is a
        # real temperature and would enter the median window.
        't_ch2': (
            float(monitor_value)
            if include_ltm and monitor_value is not None
            else (None if include_ltm else 0.0)
        ),
        't_exp': float(target_k if target_k is not None else 0.0),
    }
    if run_id is not None and int(run_id) > 0:
        row['run_id'] = int(run_id)
    if elapsed_s is not None:
        row['elapsed_s'] = max(0.0, float(elapsed_s))
    return row


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(statistics.median(values))


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


class MeasurementMedianFilter:
    """Collect every sample for a short bin, then store one median row.

    Control still uses each raw reading. The database receives the middle of
    the bin, so a few bad frames cannot become the saved value.
    """

    def __init__(self, window_s: float = MEASUREMENT_BIN_S) -> None:
        self._window_s = max(0.1, float(window_s))
        self._run_id: Optional[int] = None
        self._opened: Optional[float] = None
        self._rows: List[Dict[str, Any]] = []

    def reset(self) -> None:
        self._run_id = None
        self._opened = None
        self._rows.clear()

    def add(self, row: Dict[str, Any], now: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Append one sample. Return the finished bin when 2 s have been collected."""
        if now is None:
            now = time.monotonic()
        now = float(now)
        run_id = row.get('run_id')
        run_key = int(run_id) if run_id is not None else None
        if run_key != self._run_id:
            self._rows.clear()
            self._opened = None
            self._run_id = run_key
        if self._opened is not None and (now - self._opened) >= self._window_s and self._rows:
            done = self._summarize()
            self._rows = [dict(row)]
            self._opened = now
            return done
        if self._opened is None:
            self._opened = now
        self._rows.append(dict(row))
        return None

    def flush(self) -> Optional[Dict[str, Any]]:
        """Close a short tail bin when the run ends."""
        if not self._rows:
            self._opened = None
            return None
        done = self._summarize()
        self._rows.clear()
        self._opened = None
        return done

    def _summarize(self) -> Dict[str, Any]:
        out = dict(self._rows[-1])
        t_ch1: List[float] = []
        t_ch2: List[float] = []
        t_exp: List[float] = []
        elapsed: List[float] = []
        freq: List[float] = []
        measure_ch1: List[float] = []
        measure_ch2: List[float] = []
        for sample in self._rows:
            value = _as_float(sample.get('t_ch1'))
            if value is not None:
                t_ch1.append(value)
            value = _as_float(sample.get('t_ch2'))
            if value is not None:
                t_ch2.append(value)
            value = _as_float(sample.get('t_exp'))
            if value is not None:
                t_exp.append(value)
            value = _as_float(sample.get('elapsed_s'))
            if value is not None:
                elapsed.append(value)
            frequency = _as_float(sample.get('freq')) or 0.0
            if abs(frequency) < 1e-9:
                continue
            primary = _as_float(sample.get('measure_ch1'))
            secondary = _as_float(sample.get('measure_ch2'))
            if primary in (None, 0.0) and secondary in (None, 0.0):
                continue
            freq.append(frequency)
            if primary not in (None, 0.0):
                measure_ch1.append(float(primary))
            if secondary not in (None, 0.0):
                measure_ch2.append(float(secondary))
        if t_ch1:
            out['t_ch1'] = _median(t_ch1)
        out['t_ch2'] = _median(t_ch2)
        if t_exp:
            out['t_exp'] = _median(t_exp)
        if elapsed:
            out['elapsed_s'] = _median(elapsed)
        if freq:
            out['freq'] = _median(freq)
            out['measure_ch1'] = _median(measure_ch1) if measure_ch1 else 0.0
            out['measure_ch2'] = _median(measure_ch2) if measure_ch2 else 0.0
        else:
            out['freq'] = 0.0
            out['measure_ch1'] = 0.0
            out['measure_ch2'] = 0.0
        return out


def insert_measurement_immediate(db_query: DbQueryFn, row: Dict[str, Any]) -> bool:
    """One row, one commit — no batching."""
    response = db_query({'cmd': 'measurement_insert', **row})
    return str(response.get('result', '')).lower() == 'ok' and int(response.get('ID', 0) or 0) > 0
