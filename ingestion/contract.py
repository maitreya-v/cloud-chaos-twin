"""Versioned envelopes for archived OTLP JSON; no network dependencies."""
import hashlib
import json
from pathlib import Path

SIGNALS = {'traces': 'resourceSpans', 'metrics': 'resourceMetrics', 'logs': 'resourceLogs', 'experiments': None}
TOPICS = {name: 'cct.' + name for name in SIGNALS}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def envelope(run_id, signal, payload, synthetic, observed_ns=None):
    result = dict(schema_version='1.0', run_id=run_id, signal=signal,
                  synthetic=synthetic, source_observed_ns=observed_ns, payload=payload)
    validate(result)
    return result


def validate(e):
    if e['schema_version'] != '1.0' or not isinstance(e['run_id'], str) or not e['run_id']:
        raise ValueError('invalid envelope version or run identity')
    if type(e['synthetic']) is not bool or e['signal'] not in SIGNALS:
        raise ValueError('invalid signal or synthetic flag')
    observed = e['source_observed_ns']
    if observed is not None and (type(observed) is not int or not 0 < observed < 2**63):
        raise ValueError('invalid source observation time')
    p = e['payload']
    key = SIGNALS[e['signal']]
    if not isinstance(p, dict) or (key and not isinstance(p.get(key), list)):
        raise ValueError('expected nested OTLP JSON payload')
    if key is None and p.get('run_id') != e['run_id']:
        raise ValueError('experiment run identity mismatch')
    return e


def read_archive(directory, run_id, synthetic, observed_ns=None):
    """One OTLP resource group per message, keeping large archives below broker limits."""
    for signal, key in SIGNALS.items():
        path = Path(directory) / (signal + '.json')
        if not path.exists():
            continue
        obj = json.loads(path.read_text())
        if key is None:
            yield envelope(run_id, signal, obj, synthetic, observed_ns)
        else:
            if not isinstance(obj.get(key), list):
                raise ValueError(f'{path}: missing {key}')
            for group in obj[key]:
                yield envelope(run_id, signal, {key: [group]}, synthetic, observed_ns)
