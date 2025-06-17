from datetime import datetime, timedelta

import yaml


def date_offset_constructor(loader, node):
    """ Custom YAML constructor to handle date offsets. """
    days = int(node.value)
    return (datetime.today() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

yaml.add_constructor('!date_offset', date_offset_constructor, Loader=yaml.loader.SafeLoader)
