"""Create local topics, publish acknowledged archives, and freeze finite replay offsets."""
import argparse
import json
import time
from collections import Counter
from pathlib import Path
from .contract import TOPICS, canonical, digest, read_archive

PARTITIONS = 3
RETENTION_MS = 7 * 24 * 60 * 60 * 1000


def initialize(bootstrap):
    from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        for topic in TOPICS.values():
            try:
                admin.create_topics([NewTopic(topic, PARTITIONS, 1, topic_configs={
                    'retention.ms': str(RETENTION_MS), 'cleanup.policy': 'delete',
                    'message.timestamp.type': 'LogAppendTime'})])
            except TopicAlreadyExistsError:
                pass
        # Reject pre-existing topics with a timestamp policy inconsistent with availability.
        resources = [ConfigResource(ConfigResourceType.TOPIC, t) for t in TOPICS.values()]
        for reply in admin.describe_configs(resources):
            for resource in reply.resources:
                if resource[0]:
                    raise RuntimeError(f'cannot inspect topic config: {resource}')
                configs = {entry[0]: entry[1] for entry in resource[4]}
                if configs.get('message.timestamp.type') != 'LogAppendTime' or configs.get('cleanup.policy') != 'delete':
                    raise RuntimeError('topic must use LogAppendTime and delete-only retention')
    finally:
        admin.close()


def publish(bootstrap, envelopes):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks='all', retries=5,
                             max_in_flight_requests_per_connection=1,
                             request_timeout_ms=30000, max_block_ms=30000)
    counts, offsets = Counter(), {}
    started = time.perf_counter()
    try:
        for e in envelopes:
            value = canonical(e).encode()
            if len(value) > 900_000:
                raise ValueError('archive message exceeds safe 900 KB limit; split resource groups')
            topic = TOPICS[e['signal']]
            # Acknowledged per record; deliberately simple reliable replay, not peak producer throughput.
            metadata = producer.send(topic, key=e['run_id'].encode(), value=value).get(timeout=60)
            counts[topic] += 1
            offsets[f'{topic}:{metadata.partition}'] = metadata.offset + 1
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=10)
    return {'acknowledged': dict(counts), 'last_acknowledged_end_offsets': offsets,
            'seconds': time.perf_counter()-started}


def snapshot(bootstrap):
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                             request_timeout_ms=30000)
    try:
        parts = []
        for topic in TOPICS.values():
            partitions = consumer.partitions_for_topic(topic)
            if partitions != set(range(PARTITIONS)):
                raise RuntimeError(f'{topic}: expected {PARTITIONS} partitions, found {partitions}')
            parts.extend(TopicPartition(topic, p) for p in sorted(partitions))
        starts, ends = consumer.beginning_offsets(parts), consumer.end_offsets(parts)
        def nested(values):
            return {t: {str(p.partition): values[p] for p in parts if p.topic == t} for t in TOPICS.values()}
        return {'schema_version': '1.0', 'bootstrap': bootstrap, 'startingOffsets': nested(starts),
                'endingOffsets': nested(ends), 'captured_ns': time.time_ns(),
                'message_count': sum(ends[p]-starts[p] for p in parts)}
    finally:
        consumer.close()


def write_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as f:
        f.write(json.dumps(value, indent=2, sort_keys=True)+'\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bootstrap', default='localhost:9092')
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('init')
    pub = sub.add_parser('publish')
    pub.add_argument('archive', type=Path)
    pub.add_argument('--run-id', required=True)
    kind = pub.add_mutually_exclusive_group(required=True)
    kind.add_argument('--synthetic', action='store_true')
    kind.add_argument('--recorded', action='store_true')
    pub.add_argument('--observed-ns', type=int)
    pub.add_argument('--receipt', type=Path, required=True)
    snap = sub.add_parser('snapshot')
    snap.add_argument('output', type=Path)
    a = p.parse_args()
    if a.command == 'init':
        initialize(a.bootstrap)
    elif a.command == 'publish':
        if a.receipt.exists():
            p.error('receipt already exists; choose a new receipt before publishing')
        records = list(read_archive(a.archive, a.run_id, a.synthetic, a.observed_ns))
        if not records:
            p.error('archive contains no messages')
        receipt = publish(a.bootstrap, records)
        receipt.update(run_id=a.run_id, synthetic=a.synthetic, archive_sha256=digest(records))
        write_new(a.receipt, receipt)
        print(json.dumps(receipt, indent=2))
    else:
        value = snapshot(a.bootstrap)
        write_new(a.output, value)
        print(json.dumps(value, indent=2))


if __name__ == '__main__':
    main()
