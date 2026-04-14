#!/usr/bin/env python3
"""Simple pigpio-backed PWM helper for Raspberry Pi outputs."""

from __future__ import annotations

from typing import Optional

try:
    import pigpio
except ImportError:  # pragma: no cover - environment specific
    pigpio = None


class PwmController:
    def __init__(self, pwm_pin: int = 18, frequency: int = 100, pwm_range: int = 1000):
        self.range = int(pwm_range)
        self.frequency = int(frequency)
        self.pwm_pin = int(pwm_pin)
        self.duty_cycle = 0
        self.pi: Optional["pigpio.pi"] = None

        if pigpio is None:
            raise RuntimeError('pigpio Python module is not installed')

        self.pi = pigpio.pi()
        if not self.pi.connected:
            raise RuntimeError('pigpio daemon is not running')

        self.pi.set_PWM_frequency(self.pwm_pin, self.frequency)
        self.pi.set_PWM_range(self.pwm_pin, self.range)
        self.pi.set_PWM_dutycycle(self.pwm_pin, 0)

    def set_duty_cycle(self, duty_cycle: int = 0) -> None:
        duty_cycle = max(0, min(int(duty_cycle), self.range))
        self.duty_cycle = duty_cycle
        if self.pi is not None:
            self.pi.set_PWM_dutycycle(self.pwm_pin, duty_cycle)

    def stop(self) -> None:
        if self.pi is not None:
            self.pi.set_PWM_dutycycle(self.pwm_pin, 0)
            self.pi.stop()
            self.pi = None
