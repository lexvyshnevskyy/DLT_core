#!/usr/bin/env python3
"""PWM helper for Raspberry Pi GPIO via pigpio."""

from __future__ import annotations

try:
    import pigpio
except ImportError:  # pragma: no cover - environment specific
    pigpio = None


class PwmController:
    def __init__(
        self,
        pwm_pin: int = 18,
        frequency: int = 10,
        pwm_range: int = 1000,
        initial_duty_cycle: int = 0,
    ) -> None:
        self.range = int(pwm_range)
        self.frequency = int(frequency)
        self.pwm_pin = int(pwm_pin)
        self.duty_cycle = 0

        if pigpio is None:
            raise RuntimeError('pigpio Python module is not installed')

        self.pi = pigpio.pi()
        if not self.pi.connected:
            raise RuntimeError('pigpio daemon is not running')

        self.pi.set_PWM_frequency(self.pwm_pin, self.frequency)
        self.pi.set_PWM_range(self.pwm_pin, self.range)
        self.set_duty_cycle(initial_duty_cycle)

    def set_duty_cycle(self, duty_cycle: int = 0) -> None:
        self.duty_cycle = max(0, min(int(duty_cycle), self.range))
        self.pi.set_PWM_dutycycle(self.pwm_pin, self.duty_cycle)

    def stop(self) -> None:
        try:
            self.pi.set_PWM_dutycycle(self.pwm_pin, 0)
        finally:
            self.pi.stop()
