#!/usr/bin/env python3
"""Threaded PWM helper using pigpio."""

from __future__ import annotations

import threading
import time
from typing import Optional

try:
    import pigpio
except ImportError:  # pragma: no cover - environment specific
    pigpio = None


class PwmController:
    def __init__(self, pwm_pin: int = 18, frequency: int = 100, pwm_range: int = 1000, update_period: float = 0.01):
        self.range = pwm_range
        self.frequency = frequency
        self.pwm_pin = pwm_pin
        self.duty_cycle = 200
        self.update_period = update_period
        self._thread: Optional[threading.Thread] = None
        self._running = False

        if pigpio is None:
            raise RuntimeError('pigpio Python module is not installed')

        self.pi = pigpio.pi()
        if not self.pi.connected:
            raise RuntimeError('pigpio daemon is not running')

        self.pi.set_PWM_frequency(self.pwm_pin, self.frequency)
        self.pi.set_PWM_range(self.pwm_pin, self.range)

        self._running = True
        self._thread = threading.Thread(target=self._duty_cycle_loop, daemon=True)
        self._thread.start()

    def _duty_cycle_loop(self) -> None:
        while self._running:
            self.pi.set_PWM_dutycycle(self.pwm_pin, self.duty_cycle)
            time.sleep(self.update_period)

    def set_duty_cycle(self, duty_cycle: int = 0) -> None:
        self.duty_cycle = max(0, min(int(duty_cycle), self.range))

    def stop(self) -> None:
        self.pi.set_PWM_dutycycle(self.pwm_pin, 0)
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self.pi.stop()
