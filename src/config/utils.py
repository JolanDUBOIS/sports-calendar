def set_attribute(obj: any, key: str, value: any, type_: type):
    """ Sets an attribute of an object with a value, ensuring the value is of the correct type. """

    # 1. Validate the key name (no spaces, starts with a letter or underscore)
    if not key[0].isalpha() and key[0] != '_':
        raise AttributeError(f"Invalid key name '{key}'. It must start with a letter or underscore.")
    if ' ' in key:
        raise AttributeError(f"Invalid key name '{key}'. It must not contain spaces.")

    # 2. Try to convert the value to the specified type
    try:
        converted_value = type_(value) if value is not None else None
    except (TypeError, ValueError) as e:
        raise ValueError(f"Failed to convert value '{value}' to type {type_}. Error: {e}")

    # 3. Set the attribute on the object
    setattr(obj, key, converted_value)
