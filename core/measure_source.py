from __future__ import annotations

from typing import Dict

MEASURE_SOURCES = ('e720', 'im3536')


def normalize_measure_source(source: str) -> str:
    normalized = (source or 'e720').strip().lower()
    return normalized if normalized in MEASURE_SOURCES else 'e720'


def resolve_measure_topic(
    source: str,
    *,
    e720_topic: str = '/measure_device',
    im3536_topic: str = '/im3536',
) -> str:
    topic = im3536_topic if normalize_measure_source(source) == 'im3536' else e720_topic
    return topic if topic.startswith('/') else f'/{topic}'


def resolve_measure_topics_from_env(env: Dict[str, str]) -> Dict[str, str]:
    source = normalize_measure_source(env.get('DELATOMETRY_MEASURE_SOURCE', 'e720'))
    e720_topic = env.get('DELATOMETRY_MEASURE_TOPIC_E720', '/measure_device')
    im3536_topic = env.get('DELATOMETRY_MEASURE_TOPIC_IM3536', '/im3536')
    command_topic = env.get('DELATOMETRY_MEASURE_COMMAND_TOPIC', '/measure_device/command')
    measure_topic = resolve_measure_topic(
        source,
        e720_topic=e720_topic,
        im3536_topic=im3536_topic,
    )
    if not command_topic.startswith('/'):
        command_topic = f'/{command_topic}'
    return {
        'source': source,
        'measure_topic': measure_topic,
        'measure_command_topic': command_topic,
    }
