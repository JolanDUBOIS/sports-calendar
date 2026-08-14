from __future__ import annotations

from typing import TypeAlias

# The type sport-index uses to identify its entities (competitions, competitors, etc.).
# Kept as a single alias so a future change in sport-index's own ID representation
# only needs to be reflected here, not at every call site across sports_calendar.
EntityId: TypeAlias = str


def raw_entity_id(entity_id: EntityId) -> str:
    """ The provider's own id, with sport-index's view prefix stripped.

    sport-index gives the same underlying entity different ids depending on how
    it was reached: a club is `team:1644` when searched for or resolved, and
    `t-cpt:1644` as the side of a fixture. Comparing those as strings silently
    matches nothing, so identity comparisons use the raw id they share.

    Compound ids (`stgc:40:stgs:214140`) keep only their final segment, which is
    the entity's own id — the leading part identifies its parent.
    """
    return entity_id.rsplit(":", 1)[-1]
