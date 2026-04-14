"""PWM output helper used by the ROS 2 core node."""

from __future__ import annotations

from typing import Optional

from .pwm import PwmController


class CoreController:
    def __init__(
        self,
        pwm_ch1_pin: int = 18,
        pwm_ch2_pin: Optional[int] = None,
        pwm_frequency: int = 100,
        pwm_range: int = 1000,
    ) -> None:
        self.pwm_ch1 = PwmController(
            pwm_pin=pwm_ch1_pin,
            frequency=pwm_frequency,
            pwm_range=pwm_range,
        )
        self.pwm_ch1.set_duty_cycle(0)

        self.pwm_ch2 = None
        if pwm_ch2_pin is not None:
            self.pwm_ch2 = PwmController(
                pwm_pin=pwm_ch2_pin,
                frequency=pwm_frequency,
                pwm_range=pwm_range,
            )
            self.pwm_ch2.set_duty_cycle(0)

    def set_channel_1(self, duty_cycle: int) -> None:
        self.pwm_ch1.set_duty_cycle(duty_cycle)

    def set_channel_2(self, duty_cycle: int) -> None:
        if self.pwm_ch2 is None:
            raise RuntimeError('PWM channel 2 is not configured')
        self.pwm_ch2.set_duty_cycle(duty_cycle)

    def snapshot(self) -> dict:
        result = {
            'ch1': self.pwm_ch1.duty_cycle,
        }
        if self.pwm_ch2 is not None:
            result['ch2'] = self.pwm_ch2.duty_cycle
        return result

    def stop(self) -> None:
        self.pwm_ch1.stop()
        if self.pwm_ch2 is not None:
            self.pwm_ch2.stop()
