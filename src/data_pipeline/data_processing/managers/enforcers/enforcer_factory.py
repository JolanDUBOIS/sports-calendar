from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .enforcers import ConstraintEnforcer, UniqueEnforcer, NonNullableEnforcer
if TYPE_CHECKING:
    from src.specs import ConstraintSpec


class ConstraintEnforcerFactory:
    """ Factory to create constraint enforcers based on specifications. """

    enforcers_mapping = {
        "unique": UniqueEnforcer,
        "non-nullable": NonNullableEnforcer
    }

    @classmethod
    def create_enforcer(cls, spec: ConstraintSpec) -> ConstraintEnforcer:
        """ Create an enforcer based on the provided specification. """
        if spec.kind not in cls.enforcers_mapping:
            logger.error(f"Unknown constraint type: {spec.kind}")
            raise ValueError(f"Unknown constraint type: {spec.kind}")
        enforcer_class = cls.enforcers_mapping[spec.kind]
        return enforcer_class(spec)
