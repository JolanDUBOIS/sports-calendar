# Sports Calendar - Project Restart Status (July 2026)

This document captures where the project currently stands so work can resume quickly without re-discovering context.

Scope note: this status intentionally focuses on backend/domain/pipeline work. UI redesign work is not the priority for this branch.

## Confirmed Project Decisions

- sport-index is the mandatory sports data access layer for this project. It is not a fallback and not optional.
- Sports data access is intentionally separated into the sport-index project.
- sport-index is the source of truth for entity identity: entities are identified by the prefixed string IDs it assigns (e.g. `trnc:7`, `t-cpt:1644`), and sports_calendar stores/passes those strings through as-is rather than re-deriving or re-encoding them. See `EntityId` below.
- The old GUI is deprecated. It is kept in the repository only to preserve useful code blocks for reuse during migration.

## 1. Current Direction (What This Branch Became)

The branch started with a UI goal but effectively shifted into a backend refactor with these main directions:

- Selection model migrated toward typed, sport-index-driven entities.
- Resolution pipeline now uses a generic resolver + executors architecture.
- Calendar event generation is mapped from sport-index event types.
- Selection persistence is now more structured through service/registry/storage layers.
- Runtime setup (paths/logging/bootstrap) has been centralized.
- Entity IDs across the selection/filter layer are now sport-index-native strings (`EntityId`, a `str` type alias), not raw ints — this landed on 2026-07-12 alongside a same-day breaking change in sport-index itself (see section 3.4).

Practical takeaway: the project is no longer in a "new feature" phase; the backend migration itself is largely stabilized as of 2026-07-12 (see section 2), with a few smaller, well-scoped items left (section 3).

## 2. What Is In Good Shape

### 2.1 Core architecture seams are clear

- Selection domain models are centralized and typed.
- Filter fields/definitions map cleanly to filter types.
- Executor dispatch is explicit and easy to extend.

Key files:
- [src/sports_calendar/core/selection/models.py](src/sports_calendar/core/selection/models.py)
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)
- [src/sports_calendar/core/selection/filters/definitions.py](src/sports_calendar/core/selection/filters/definitions.py)
- [src/sports_calendar/core/entity_id.py](src/sports_calendar/core/entity_id.py)
- [src/sports_calendar/infra/engine/executors/mapping.py](src/sports_calendar/infra/engine/executors/mapping.py)

### 2.2 End-to-end selection workflow exists and is tested

- CLI sync command triggers selection resolution and calendar sync.
- Event conversion from sport-index collections to internal calendar event models exists.
- The resolver integration test (`tests/integration/infra/test_engine.py`) is re-enabled and passing against live sport-index data.
- All executors (`empty`, `competitions`, `competitors`, `min_ranking`, `sessions`) have integration tests passing against live sport-index data, plus one fully-mocked unit test for the sessions executor's tier-matching logic.

Key files:
- [src/sports_calendar/interfaces/cli/sync.py](src/sports_calendar/interfaces/cli/sync.py)
- [src/sports_calendar/application/workflows/run_selection.py](src/sports_calendar/application/workflows/run_selection.py)
- [src/sports_calendar/core/calendar/events/mapping.py](src/sports_calendar/core/calendar/events/mapping.py)
- [tests/integration/infra/test_engine.py](tests/integration/infra/test_engine.py)
- [tests/integration/infra/test_executors.py](tests/integration/infra/test_executors.py)
- [tests/unit/infra/engine/test_sessions_executor.py](tests/unit/infra/engine/test_sessions_executor.py)

### 2.3 Canonical config examples use the current schema

[config/selections/dev.yml](config/selections/dev.yml) reflects the current canonical shape (`sport_id` + `EntityId` strings for competitions/competitors, `StageTier` ints for sessions) and round-trips cleanly through `make validate-selections`.

Note: `config/selections/dev-1.yml`, previously referenced in this doc, does not exist — that was a stale claim from an earlier version of this status doc. `dev.yml` is the only (and sufficient) canonical example.

### 2.4 Environment bootstrap is mostly clean

- Paths/logging setup is centralized and called from CLI startup.
- `pyproject.toml`'s editable path to the sibling `sport-index` project was fixed (`../../sport-index` → `../sport-index`) — it pointed one directory too high and made `uv run`/`uv sync` fail outright.

Key files:
- [src/sports_calendar/infra/config/paths.py](src/sports_calendar/infra/config/paths.py)
- [src/sports_calendar/infra/setup/initialize.py](src/sports_calendar/infra/setup/initialize.py)
- [pyproject.toml](pyproject.toml)

### 2.5 Sessions filter is fixed and consistent

Previously, `SessionsExecutor.fetch()` matched `substage.name` (a display string like "Qualifying 1") against `filter_fields.sessions` (typed `list[StageTier]`, an int enum) — a comparison that could never succeed, so the sessions filter silently returned zero events whenever it ran first in a filter chain. Fixed to match on `substage.tier` instead, consistent with `apply()`, which also resolves the old "Qualifying 1 vs Qualifying 2" partial-match TODO for free (they share the same `StageTier` value).

Key files:
- [src/sports_calendar/infra/engine/executors/sessions.py](src/sports_calendar/infra/engine/executors/sessions.py)

## 3. Work In Progress / Remaining

### 3.1 GUI (`interfaces/gui/catalog.py`) has the same sport-index ID-format gap, unfixed

sport-index's `client.get()` signature changed on 2026-07-12 (entity ID now first-and-required-string, class second-and-optional). All backend executors were updated to match, plus the accompanying `EntityId`-based type migration. `interfaces/gui/catalog.py`'s `client.get(Competition, competition_id)` / `client.get(Competitor, competitor_id)` calls have the *same* broken argument order, but weren't fixed, because:
- The GUI handles arbitrary sports generically, so it can't statically pick a competition/competitor prefix (`trnc` vs `stgc`, etc.) the way sport-specific executors can.
- Its `FilterSearchProvider` Protocol and dict-key typing (`dict[int, str]`) still assume plain int IDs throughout — a deeper, GUI-specific type audit, not a one-line fix.
- No tests currently cover this file, so nothing catches it either way.
- UI work is explicitly deprioritized per the decision log below.

`get_sport_name` (same file) *was* fixed — `Sport` is a single concrete class with its own prefix, unlike `Competition`/`Competitor`, so `Sport.encode_id(sport_id)` works standalone.

Files:
- [src/sports_calendar/interfaces/gui/catalog.py](src/sports_calendar/interfaces/gui/catalog.py)

### 3.2 Remaining documented TODOs (unchanged, still deferred)

- Switch from `TeamsFilterFields`-style thinking to `CompetitorsFilterFields` with an explicit competitor type (team vs individual), since the client provides a unified interface for both.

File:
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)

### 3.3 UI split remains (new gui + old gui)

Both trees coexist:
- [src/sports_calendar/interfaces/gui](src/sports_calendar/interfaces/gui)
- [src/sports_calendar/interfaces/old_gui](src/sports_calendar/interfaces/old_gui)

Current policy:
- New development should not target old_gui.
- old_gui is deprecated and kept as a code-reference bank only.
- Backend stabilization remains the priority before any UI consolidation work.

### 3.4 Context: sport-index's ID scheme changed today (2026-07-12)

Two commits in the sibling `sport-index` project (`f22ae30`, `3f0886c`, both 2026-07-12) reworked entity identity: entities are now addressed by prefixed string IDs (`trnc:7`, `t-cpt:1644`, `stgc:40`, ...) via `client.get(entity_id: str, entity_cls=None)`, instead of the old `client.get(entity_cls, entity_id)` convention. This broke every `client.get(...)` call site in sports_calendar (all executors, plus GUI's `catalog.py`) on the same day it was made. The executor-side fallout is fixed as of this session (section 2.5, 2.1); the GUI-side fallout is tracked in 3.1.

Prefix reference (from sport-index):

| Prefix | Entity | Prefix | Entity |
|---|---|---|---|
| `spt` | Sport | `stg` | Event (stage) |
| `ctr` | Country | `trnc` | Competition (tournament) |
| `cat` | Category | `stgc` | Competition (stage) |
| `chl` | Channel | `trns` | Season (tournament) |
| `ref` | Referee | `stgs` | Season (stage) |
| `mng` | Manager | `t-cpt` | Competitor (team) |
| `vnu` | Venue | `p-cpt` | Competitor (player) |
| `stgv` | Venue (stage) | `team` | Team (resolved) |
| `mch` | Event (match) | `t-ath` | Athlete (team-sport) |
| | | `p-ath` | Athlete (individual) |

IDs can be composite/nested (`parent_id:prefix:raw_id`) — see sport-index's `domain/base.py` decode logic.

## 4. Known Risks

1. GUI catalog.py ID handling (section 3.1) — real but low-blast-radius since untested/deprioritized.
2. Sessions filter now genuinely works end-to-end (previously reported as "semantic mismatch" — that's resolved), but only race/stage sports are supported by it; that's a scope choice, not a bug.
3. sport-index is under active development (its own ID scheme changed mid-session today) — expect more of this class of breakage as it evolves. Worth a quick sanity check (`make validate-selections`, or `pytest tests/integration`) after pulling sport-index changes.

## 5. Future Work (After Stabilization)

Based on existing notes and codebase direction:

1. Expand sport coverage and source capabilities through sport-index integration improvements.
2. Audit and fix `interfaces/gui/catalog.py`'s ID handling (section 3.1), if/when UI work resumes.
3. Add release hygiene (versioning, release branch flow).
4. Simplify metadata/timestamps model if still desired.
5. Finish or remove deprecated/legacy paths once backend is stable.

Reference idea notes:
- [docs/ideas.md](docs/ideas.md)

## 6. Decision Log (Confirmed)

These decisions are confirmed and should be treated as project constraints:

1. sport-index is required for sports data access in this project (no fallback path planned).
2. data access separation into the sport-index project is intentional and considered an architectural improvement.
3. sport-index's own entity ID strings are the canonical identity format for competitions/competitors throughout sports_calendar (see `EntityId`, section 3.4) — never re-derive or pad these.
4. config selection file [config/selections/dev.yml](config/selections/dev.yml) reflects the canonical schema. There is no `dev-1.yml`.
5. old_gui is deprecated and retained only to avoid losing reusable code blocks.
6. UI work is currently deprioritized in favor of backend migration stabilization.

## 7. Quick Operational Commands

From project root:

- Setup: `make setup`
- Validate and rewrite selections: `make validate-selections`
- Sync calendar for dev selection: `make sync-calendar`
- Clear calendar: `make clear-calendar`
- Run tests: `uv run pytest tests` (integration tests hit live sport-index data / recorded fixtures under `tests/mock_data`, so expect a few minutes)

Reference:
- [Makefile](Makefile)

## 8. First Task To Execute Next Time

The backend migration's core loop (selection → resolver → executors → events) is now stable and tested end-to-end. When resuming, the highest-value next steps are:

1. If you're doing UI work: audit `interfaces/gui/catalog.py`'s ID handling (section 3.1) before building on top of it.
2. Otherwise: pick from Future Work (section 5) — sport coverage, release hygiene, or old_gui retirement are all reasonable next threads, none of them blocking.
