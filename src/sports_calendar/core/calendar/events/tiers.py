""" Human labels for stage tiers.

The provider's own session names are inconsistent across championships — the
same `RACE` tier comes back as "Grand Prix" in F1 and "Race" elsewhere, and
qualifying alternates between "Qualifying" and "Qualification". The tier is the
stable thing, so labels are keyed off it.

This lives in `core` rather than in the GUI's `copy.py` because both the
calendar summaries and the sessions dropdown need the same words, and they are
the same words for the same reason. `copy.py` imports from here.

Only the tiers a user would ever pick are listed: `StageTier` also carries
structural levels (SPORT, SEASON, EVENT, LAP) that describe where a stage sits
in the tree, not what kind of session it is.
"""

from sportindex import StageTier

STAGE_TIER_LABELS: dict[StageTier, str] = {
    StageTier.PRACTICE: "Practice",
    StageTier.QUALIFYING: "Qualifying",
    StageTier.QUALIFYING_PART: "Qualifying (part)",
    StageTier.SPRINT_QUALIFYING: "Sprint qualifying",
    StageTier.SPRINT_RACE: "Sprint race",
    StageTier.RACE: "Race",
    StageTier.PROLOGUE: "Prologue",
    StageTier.STAGE: "Stage",
}


def stage_tier_label(tier: StageTier | None, fallback: str = "") -> str:
    """ The stable label for a stage tier, or `fallback` when it has none.

    The fallback is normally the provider's own name for the stage, which is
    still better than nothing for a tier we do not label.
    """
    if tier is None:
        return fallback
    return STAGE_TIER_LABELS.get(tier, fallback)
