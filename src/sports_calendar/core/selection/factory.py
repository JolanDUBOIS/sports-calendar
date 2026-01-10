from . import logger
from .specs import SelectionSpec, SelectionItemSpec, SelectionFilterSpec
from ..utils import validate


class SelectionFilterSpecFactory:
    """ Create concrete SelectionFilterSpec instances from dicts. """

    _registry: dict[str, type[SelectionFilterSpec]] = {}

    @classmethod
    def _build_registry(cls):
        if cls._registry:
            return
        for subclass in SelectionFilterSpec.__subclasses__():
            key = getattr(subclass, "key", None)
            if key is None:
                continue
            cls._registry[key] = subclass

    @classmethod
    def create_filter(cls, data: dict) -> SelectionFilterSpec:
        """
        data: dict with at least "key" and other fields
        """
        cls._build_registry()
        key = data.get("key")
        validate(bool(key), "Filter dict must have a 'key' field", logger)
        filter_cls = cls._registry.get(key)
        validate(bool(filter_cls), f"Unknown filter key '{key}'", logger)
        kwargs = {k: v for k, v in data.items() if k != "key"}
        return filter_cls(**kwargs)

class SelectionSpecFactory:

    @classmethod
    def from_dict(cls, data: dict) -> SelectionSpec:
        """
        data: {
            "name": "my selection",
            "items": [
                {
                    "sport": "football",
                    "entity": "team",
                    "entity_id": 1,
                    "filters": [ {...}, {...} ]
                },
                ...
            ]
        }
        """
        items = []
        for item_data in data.get("items", []):
            filters = [
                SelectionFilterSpecFactory.create_filter(f_data)
                for f_data in item_data.get("filters", [])
            ]
            items.append(
                SelectionItemSpec(
                    sport=item_data["sport"],
                    entity=item_data.get("entity"),
                    entity_id=item_data.get("entity_id"),
                    filters=filters
                )
            )
        return SelectionSpec(
            name=data["name"],
            items=items
        )
