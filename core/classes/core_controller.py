"""Core actuator helper for a single heater channel."""

from __future__ import annotations

from .pwm import PwmController


class CoreController:
    def __init__(
        self,
        pwm_pin: int = 18,
        pwm_frequency_hz: int = 10,
        pwm_range: int = 1000,
    ) -> None:
        self.heater_pwm = PwmController(
            pwm_pin=pwm_pin,
            frequency=pwm_frequency_hz,
            pwm_range=pwm_range,
            initial_duty_cycle=0,
        )

    def set_heater_output(self, duty_cycle: int) -> None:
        self.heater_pwm.set_duty_cycle(duty_cycle)

    def snapshot(self) -> dict:
        return {
            'heater_pwm': self.heater_pwm.duty_cycle,
            'pwm_pin': self.heater_pwm.pwm_pin,
            'pwm_frequency_hz': self.heater_pwm.frequency,
            'pwm_range': self.heater_pwm.range,
        }

    def stop(self) -> None:
        self.heater_pwm.stop()
