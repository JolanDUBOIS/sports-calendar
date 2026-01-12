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
        """ TODO """
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
    """ Factory to create SelectionSpec instances from dicts. """

    @classmethod
    def from_dict(cls, data: dict) -> SelectionSpec:
        """ TODO """
        items = []
        for item_data in data.get("items", []):
            filters = [
                SelectionFilterSpecFactory.create_filter(sport=item_data["sport"], **f_data)
                for f_data in item_data.get("filters", [])
            ]
            item_kwargs = {k: v for k, v in item_data.items() if k != "filters"}
            items.append(
                SelectionItemSpec(
                    **item_kwargs,
                    filters=filters
                )
            )
        data_kwargs = {k: v for k, v in data.items() if k != "items"}
        return SelectionSpec(
            **data_kwargs,
            items=items
        )
