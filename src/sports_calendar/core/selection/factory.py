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
    def create_filter(cls, sport: str, filter_type: str, **kwargs) -> SelectionFilterSpec:
        """
        data: dict with at least "key" and other fields
        """
        cls._build_registry()
        filter_cls = cls._registry.get(filter_type)
        validate(
            filter_cls is not None,
            f"Unknown filter type: {filter_type}",
            logger
        )
        logger.debug(f"Creating filter of type {filter_type} for sport {sport} with args {kwargs}")
        logger.debug(f"Using filter class: {filter_cls.__name__}")
        return filter_cls(sport=sport, **kwargs)

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
                SelectionFilterSpecFactory.create_filter(sport=item_data["sport"], **f_data)
                for f_data in item_data.get("filters", [])
            ]
            items.append(
                SelectionItemSpec(
                    sport=item_data["sport"],
                    filters=filters
                )
            )
        return SelectionSpec(
            name=data["name"],
            items=items
        )
