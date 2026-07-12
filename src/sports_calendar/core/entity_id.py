from __future__ import annotations

from typing import TypeAlias

# The type sport-index uses to identify its entities (competitions, competitors, etc.).
# Kept as a single alias so a future change in sport-index's own ID representation
# only needs to be reflected here, not at every call site across sports_calendar.
EntityId: TypeAlias = str
