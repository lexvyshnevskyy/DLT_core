"""Core actuator helper for a single heater channel."""

from __future__ import annotations

from typing import Optional

from .pwm import PwmController


class CoreController:
    def __init__(
        self,
        pwm_pin: int = 18,
        pwm_pin_ch2: Optional[int] = None,
        pwm_frequency_hz: int = 10,
        pwm_range: int = 1000,
    ) -> None:
        self.heater_pwm = PwmController(
            pwm_pin=pwm_pin,
            frequency=pwm_frequency_hz,
            pwm_range=pwm_range,
            initial_duty_cycle=0,
        )
        self.heater_pwm_ch2: Optional[PwmController] = None
        if pwm_pin_ch2 is not None and int(pwm_pin_ch2) != int(pwm_pin):
            self.heater_pwm_ch2 = PwmController(
                pwm_pin=int(pwm_pin_ch2),
                frequency=pwm_frequency_hz,
                pwm_range=pwm_range,
                initial_duty_cycle=0,
            )

    def set_heater_output(self, duty_cycle: int) -> None:
        duty = int(duty_cycle)
        self.heater_pwm.set_duty_cycle(duty)
        if self.heater_pwm_ch2 is not None:
            self.heater_pwm_ch2.set_duty_cycle(duty)

    def snapshot(self) -> dict:
        data = {
            'heater_pwm': self.heater_pwm.duty_cycle,
            'pwm_pin': self.heater_pwm.pwm_pin,
            'pwm_frequency_hz': self.heater_pwm.frequency,
            'pwm_range': self.heater_pwm.range,
            'pwm_backend': self.heater_pwm.backend,
        }
        if self.heater_pwm_ch2 is not None:
            data['heater_pwm_ch2'] = self.heater_pwm_ch2.duty_cycle
            data['pwm_pin_ch2'] = self.heater_pwm_ch2.pwm_pin
        return data

    def stop(self) -> None:
        self.heater_pwm.stop()
        if self.heater_pwm_ch2 is not None:
            self.heater_pwm_ch2.stop()
