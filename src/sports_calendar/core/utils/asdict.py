from dataclasses import asdict, is_dataclass


def deep_asdict(obj):
    if is_dataclass(obj):
        return {k: deep_asdict(v) for k, v in asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: deep_asdict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_asdict(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(deep_asdict(v) for v in obj)
    return obj
