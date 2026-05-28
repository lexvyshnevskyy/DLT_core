from __future__ import annotations

from typing import Any, Dict


def e720_from_msg(msg: Any) -> Dict[str, Any]:
    if msg is None:
        return {'online': False}

    def field(name: str) -> Any:
        value = getattr(msg, name, None)
        return getattr(value, 'data', value)

    frame_id = ''
    if hasattr(msg, 'header') and hasattr(msg.header, 'frame_id'):
        frame_id = str(msg.header.frame_id)

    online = frame_id != 'e720_offline'
    return {
        'online': online,
        'frequency': float(field('frequency') or 0.0),
        'firstvalue': float(field('firstvalue') or 0.0),
        'secondvalue': float(field('secondvalue') or 0.0),
    }
