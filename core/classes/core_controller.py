"""Core actuator helper preserved from the legacy package."""

from __future__ import annotations

from .pwm import PwmController


class CoreController:
    def __init__(self) -> None:
        self.pwm_ch1 = PwmController(pwm_pin=22)
        self.pwm_ch1.set_duty_cycle(0)
        self.pwm_ch2 = PwmController(pwm_pin=23)
        self.pwm_ch2.set_duty_cycle(0)

    def set_channel_1(self, duty_cycle: int) -> None:
        self.pwm_ch1.set_duty_cycle(duty_cycle)

    def set_channel_2(self, duty_cycle: int) -> None:
        self.pwm_ch2.set_duty_cycle(duty_cycle)

    def snapshot(self) -> dict:
        return {
            'ch1': self.pwm_ch1.duty_cycle,
            'ch2': self.pwm_ch2.duty_cycle,
        }

    def stop(self) -> None:
        self.pwm_ch1.stop()
        self.pwm_ch2.stop()
