from typing import get_origin, get_args, Any, List, Dict, Tuple, Union


def coerce(value: any, to_type: type) -> any:
    """ Recursively coerce a value to a specified type hint (supports List, Dict, Tuple, Optional, basic types). """
    origin = get_origin(to_type)
    args = get_args(to_type)

    # Optional / Union with None
    if origin is Union and type(None) in args:
        actual_type = next(a for a in args if a is not type(None))
        if value is None:
            return None
        return coerce(value, actual_type)

    # Lists
    if origin is list or origin is List:
        if not isinstance(value, list):
            # Try to wrap a single value into a list
            value = [value]
        item_type = args[0] if args else Any
        return [coerce(v, item_type) for v in value]

    # Tuples
    if origin is tuple or origin is Tuple:
        if not isinstance(value, (tuple, list)):
            raise TypeError(f"Cannot coerce {value!r} to tuple")
        if len(args) == 2 and args[1] is ...:
            # Tuple of variable length Tuple[T, ...]
            return tuple(coerce(v, args[0]) for v in value)
        if len(args) != len(value):
            raise TypeError(f"Tuple length mismatch: expected {len(args)}, got {len(value)}")
        return tuple(coerce(v, t) for v, t in zip(value, args))

    # Dicts
    if origin is dict or origin is Dict:
        if not isinstance(value, dict):
            raise TypeError(f"Cannot coerce {value!r} to dict")
        key_type, val_type = args if args else (Any, Any)
        return {coerce(k, key_type): coerce(v, val_type) for k, v in value.items()}

    # Basic types
    try:
        if isinstance(value, to_type):
            return value
        return to_type(value)
    except Exception as e:
        raise TypeError(f"Cannot coerce {value!r} to {to_type}: {e}") from e
