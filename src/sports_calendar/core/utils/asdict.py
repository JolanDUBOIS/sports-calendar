from dataclasses import is_dataclass, asdict


def deep_asdict(obj):
    if is_dataclass(obj):
        return {k: deep_asdict(v) for k, v in asdict(obj).items()}
    elif isinstance(obj, dict):
        return {k: deep_asdict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_asdict(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(deep_asdict(v) for v in obj)
    else:
        return obj
