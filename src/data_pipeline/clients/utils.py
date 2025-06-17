def remove_keys(obj: any, keys_to_remove: list[str]) -> any:
    """ Recursively remove specified keys from a dictionary or list. """
    if isinstance(obj, dict):
        return {
            k: remove_keys(v, keys_to_remove)
            for k, v in obj.items()
            if k not in keys_to_remove
        }
    elif isinstance(obj, list):
        return [remove_keys(item, keys_to_remove) for item in obj]
    else:
        return obj
