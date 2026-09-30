"""Core actuator helper: CH1 heater + CH2 cooler PWM."""

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
        cooler_min_duty: int = 0,
        cooler_max_duty: Optional[int] = None,
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
        # Gas cooler (CH2): idle / "zero" command = cooler_min_duty (e.g. 20%).
        # Cooling increases flow above that floor up to cooler_max_duty. True off
        # (duty 0) is only allowed when explicitly requested (stop / disable).
        self.cooler_min_duty = max(0, int(cooler_min_duty))
        self.cooler_max_duty = int(cooler_max_duty) if cooler_max_duty is not None else int(pwm_range)
        # pigpiod keeps the last hardware duty across process restarts — force
        # true-off at construction so orphan heater/cooler never survive reboot.
        self.set_thermal_output(0, cooler_off=True)

    def clamp_cooler_duty(self, duty: int, *, allow_off: bool = False) -> int:
        """Map cooler duty: idle floor [min, max], or true 0 when allow_off."""
        value = int(duty)
        lo = max(0, int(self.cooler_min_duty))
        hi = max(lo if lo > 0 else 1, int(self.cooler_max_duty))
        if allow_off and value <= 0:
            return 0
        # Control "zero" and any below-min request → idle gas flow (min PWM).
        if value <= 0 or (lo > 0 and value < lo):
            return lo if lo > 0 else 0
        return max(lo, min(value, hi))

    def set_thermal_output(self, signed_duty: int, *, cooler_off: bool = False) -> None:
        """Bipolar: +heat CH1, −cool CH2. CH2 idle floor stays on unless cooler_off."""
        value = int(signed_duty)
        if value > 0:
            # Heat: cooler at idle floor (gas always flowing at min when enabled).
            self.set_heater_outputs(duty_ch1=value, duty_ch2=0, cooler_off=cooler_off)
        elif value < 0:
            self.set_heater_outputs(duty_ch1=0, duty_ch2=abs(value), cooler_off=cooler_off)
        else:
            self.set_heater_outputs(duty_ch1=0, duty_ch2=0, cooler_off=cooler_off)

    def set_heater_output(self, duty_cycle: int) -> None:
        """Legacy: treat non-negative as heater-only; negative as cooler-only."""
        self.set_thermal_output(int(duty_cycle))

    def set_heater_outputs(
        self,
        duty_ch1: Optional[int] = None,
        duty_ch2: Optional[int] = None,
        *,
        cooler_off: bool = False,
    ) -> None:
        """Set CH1 (heater) / CH2 (cooler) independently.

        CH2 (gas cooler): idle/"zero" → cooler_min_duty; cool → [min, max].
        Pass cooler_off=True only when shutting the experiment down.
        """
        if duty_ch1 is not None:
            self.heater_pwm.set_duty_cycle(int(duty_ch1))
        if duty_ch2 is not None:
            if self.heater_pwm_ch2 is None:
                raise RuntimeError('PWM CH2 is not configured (pwm_pin_ch2).')
            self.heater_pwm_ch2.set_duty_cycle(
                self.clamp_cooler_duty(int(duty_ch2), allow_off=cooler_off)
            )

    def snapshot(self) -> dict:
        data = {
            'heater_pwm': self.heater_pwm.duty_cycle,
            'pwm_pin': self.heater_pwm.pwm_pin,
            'pwm_frequency_hz': self.heater_pwm.frequency,
            'pwm_range': self.heater_pwm.range,
            'pwm_backend': self.heater_pwm.backend,
            'role_ch1': 'heater',
            'role_ch2': 'cooler',
        }
        if self.heater_pwm_ch2 is not None:
            data['heater_pwm_ch2'] = self.heater_pwm_ch2.duty_cycle
            data['cooler_pwm'] = self.heater_pwm_ch2.duty_cycle
            data['pwm_pin_ch2'] = self.heater_pwm_ch2.pwm_pin
        return data

    def stop(self) -> None:
        self.heater_pwm.stop()
        if self.heater_pwm_ch2 is not None:
            self.heater_pwm_ch2.stop()
