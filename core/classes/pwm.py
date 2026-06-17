#!/usr/bin/env python3
"""PWM helper for Raspberry Pi GPIO (pigpio on Pi 4 and earlier, lgpio on Pi 5)."""

from __future__ import annotations

import os
import re
from typing import Optional

try:
    import pigpio
except ImportError:  # pragma: no cover - environment specific
    pigpio = None

try:
    import lgpio
except ImportError:  # pragma: no cover - environment specific
    lgpio = None


def _read_pi_revision() -> str:
    try:
        with open('/proc/cpuinfo', encoding='ascii', errors='replace') as handle:
            for line in handle:
                if line.lower().startswith('revision'):
                    return line.split(':', 1)[-1].strip().lower()
    except OSError:
        pass
    return ''


def is_raspberry_pi_5(revision: str = '') -> bool:
    rev = (revision or _read_pi_revision()).lower()
    return bool(re.match(r'^[dc]04', rev))


def resolve_pwm_backend(preferred: str = 'auto') -> str:
    normalized = (preferred or 'auto').strip().lower()
    if normalized in ('pigpio', 'lgpio'):
        return normalized
    if is_raspberry_pi_5():
        return 'lgpio'
    return 'pigpio'


class _PigpioPwm:
    def __init__(
        self,
        pwm_pin: int,
        frequency: int,
        pwm_range: int,
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


_lgpio_chip_handle: Optional[int] = None
_lgpio_chip_refs = 0


def _lgpio_acquire() -> int:
    global _lgpio_chip_handle, _lgpio_chip_refs
    if lgpio is None:
        raise RuntimeError('lgpio Python module is not installed (required on Raspberry Pi 5)')
    if _lgpio_chip_handle is None:
        handle = lgpio.gpiochip_open(0)
        if handle < 0:
            raise RuntimeError(
                f'lgpio gpiochip_open(0) failed ({handle}). '
                'On Pi 5 add dtoverlay=pwm to /boot/firmware/config.txt and reboot.'
            )
        _lgpio_chip_handle = handle
    _lgpio_chip_refs += 1
    return _lgpio_chip_handle


def _lgpio_release() -> None:
    global _lgpio_chip_handle, _lgpio_chip_refs
    if _lgpio_chip_handle is None:
        return
    _lgpio_chip_refs = max(0, _lgpio_chip_refs - 1)
    if _lgpio_chip_refs == 0:
        try:
            lgpio.gpiochip_close(_lgpio_chip_handle)
        finally:
            _lgpio_chip_handle = None


class _LgpioPwm:
    def __init__(
        self,
        pwm_pin: int,
        frequency: int,
        pwm_range: int,
        initial_duty_cycle: int = 0,
    ) -> None:
        self.range = int(pwm_range)
        self.frequency = max(1, int(frequency))
        self.pwm_pin = int(pwm_pin)
        self.duty_cycle = 0
        self._pwm_active = False
        self._handle = _lgpio_acquire()
        self._claim_output()
        self.set_duty_cycle(initial_duty_cycle)

    def _claim_output(self) -> None:
        try:
            lgpio.gpio_claim_output(self._handle, self.pwm_pin, 0)
        except lgpio.error as exc:
            if 'busy' not in str(exc).lower():
                raise RuntimeError(f'lgpio claim output GPIO {self.pwm_pin} failed: {exc}') from exc

    def _duty_to_percent(self, duty: int) -> float:
        return max(0.0, min(100.0, 100.0 * float(duty) / float(self.range)))

    def _drive_off(self) -> None:
        if self._pwm_active:
            try:
                lgpio.tx_pwm(self._handle, self.pwm_pin, 0, 0)
            except lgpio.error:
                pass
            self._pwm_active = False
        lgpio.gpio_write(self._handle, self.pwm_pin, 0)

    def set_duty_cycle(self, duty_cycle: int = 0) -> None:
        self.duty_cycle = max(0, min(int(duty_cycle), self.range))
        percent = self._duty_to_percent(self.duty_cycle)
        if percent <= 0.0:
            self._drive_off()
            return
        lgpio.tx_pwm(self._handle, self.pwm_pin, self.frequency, percent)
        self._pwm_active = True

    def stop(self) -> None:
        try:
            self._drive_off()
        finally:
            _lgpio_release()


class PwmController:
    def __init__(
        self,
        pwm_pin: int = 18,
        frequency: int = 10,
        pwm_range: int = 1000,
        initial_duty_cycle: int = 0,
        backend: str = 'auto',
    ) -> None:
        selected = resolve_pwm_backend(os.environ.get('DELATOMETRY_PWM_BACKEND', backend))
        if selected == 'lgpio':
            self._impl = _LgpioPwm(pwm_pin, frequency, pwm_range, initial_duty_cycle)
        else:
            self._impl = _PigpioPwm(pwm_pin, frequency, pwm_range, initial_duty_cycle)
        self.backend = selected
        self.range = self._impl.range
        self.frequency = self._impl.frequency
        self.pwm_pin = self._impl.pwm_pin
        self.duty_cycle = self._impl.duty_cycle

    def set_duty_cycle(self, duty_cycle: int = 0) -> None:
        self._impl.set_duty_cycle(duty_cycle)
        self.duty_cycle = self._impl.duty_cycle

    def stop(self) -> None:
        self._impl.stop()
