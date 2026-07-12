# Sports Calendar - Project Restart Status (July 2026)

This document captures where the project currently stands so work can resume quickly without re-discovering context.

Scope note: this status intentionally focuses on backend/domain/pipeline work. UI redesign work is not the priority for this branch.

## Confirmed Project Decisions

- sport-index is the mandatory sports data access layer for this project. It is not a fallback and not optional.
- Sports data access is intentionally separated into the sport-index project.
- The old GUI is deprecated. It is kept in the repository only to preserve useful code blocks for reuse during migration.

## 1. Current Direction (What This Branch Became)

The branch started with a UI goal but effectively shifted into a backend refactor with these main directions:

- Selection model migrated toward typed, sport-index-driven entities.
- Resolution pipeline now uses a generic resolver + executors architecture.
- Calendar event generation is mapped from sport-index event types.
- Selection persistence is now more structured through service/registry/storage layers.
- Runtime setup (paths/logging/bootstrap) has been centralized.

Practical takeaway: the project is no longer in a "new feature" phase; it is in a "stabilize and finish migration" phase.

## 2. What Is In Good Shape

### 2.1 Core architecture seams are clear

- Selection domain models are centralized and typed.
- Filter fields/definitions map cleanly to filter types.
- Executor dispatch is explicit and easy to extend.

Key files:
- [src/sports_calendar/core/selection/models.py](src/sports_calendar/core/selection/models.py)
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)
- [src/sports_calendar/core/selection/filters/definitions.py](src/sports_calendar/core/selection/filters/definitions.py)
- [src/sports_calendar/infra/engine/executors/mapping.py](src/sports_calendar/infra/engine/executors/mapping.py)

### 2.2 End-to-end selection workflow exists

- CLI sync command triggers selection resolution and calendar sync.
- Event conversion from sport-index collections to internal calendar event models exists.

Key files:
- [src/sports_calendar/interfaces/cli/sync.py](src/sports_calendar/interfaces/cli/sync.py)
- [src/sports_calendar/application/workflows/run_selection.py](src/sports_calendar/application/workflows/run_selection.py)
- [src/sports_calendar/core/calendar/events/mapping.py](src/sports_calendar/core/calendar/events/mapping.py)

### 2.3 Canonical config examples already use new shape

Your dev selections in config are already in the new model style (sport_id + numeric IDs):
- [config/selections/dev.yml](config/selections/dev.yml)
- [config/selections/dev-1.yml](config/selections/dev-1.yml)

This is important because it gives a concrete source of truth for migration.

### 2.4 Environment bootstrap is mostly clean

- Paths/logging setup is centralized and called from CLI startup.

Key files:
- [src/sports_calendar/infra/config/paths.py](src/sports_calendar/infra/config/paths.py)
- [src/sports_calendar/infra/setup/initialize.py](src/sports_calendar/infra/setup/initialize.py)

## 3. Work In Progress (WIP)

### 3.1 Migration alignment is incomplete across docs/tests/code

Core code has moved forward, but supporting layers are partially old:

- Docs still describe an older selection shape (`sport`, `entity`, `id`).
- Some test fixtures still use legacy schema assumptions.
- Integration coverage around resolver was disabled/commented.

Files showing this mismatch:
- [docs/selections.md](docs/selections.md)
- [tests/conftest.py](tests/conftest.py)
- [tests/integration/infra/test_engine.py](tests/integration/infra/test_engine.py)

### 3.2 Sessions filter logic is half-migrated

The sessions filter is currently inconsistent between type model and runtime behavior.

- Field model expects StageTier-like values.
- Executor still has TODO logic and name-based matching shortcuts.
- Tests use plain string sessions, not typed values.

Files:
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)
- [src/sports_calendar/infra/engine/executors/sessions.py](src/sports_calendar/infra/engine/executors/sessions.py)
- [tests/integration/infra/test_executors.py](tests/integration/infra/test_executors.py)

### 3.3 UI split remains (new gui + old gui)

Both trees coexist:
- [src/sports_calendar/interfaces/gui](src/sports_calendar/interfaces/gui)
- [src/sports_calendar/interfaces/old_gui](src/sports_calendar/interfaces/old_gui)

Current policy:
- New development should not target old_gui.
- old_gui is deprecated and kept as a code-reference bank only.
- Backend stabilization remains the priority before any UI consolidation work.

## 4. What Is Not Working / Risky Right Now

These are the highest confidence current risks based on code review:

1. Schema drift risk
- Different project layers imply different canonical selection formats.
- Risk: silent errors, confusing validation, and hard-to-debug behavior.

2. Sessions semantic mismatch
- sessions field typing vs executor matching logic is inconsistent.
- Risk: wrong filtering behavior for motorsport/session-based competitions.

3. Reduced integration confidence
- Resolver integration test is commented out.
- Risk: regressions can pass unit-level checks while breaking end-to-end selection resolution.

4. TODO-heavy edges in core execution
- Several TODOs remain in executors and event descriptions.
- Risk: hidden behavior assumptions and incomplete edge-case handling.

5. Documentation drift
- User-facing selection docs do not reflect canonical runtime shape.
- Risk: onboarding friction and invalid configs.

## 5. Future Work (After Stabilization)

Based on existing notes and codebase direction:

1. Expand sport coverage and source capabilities through sport-index integration improvements.
2. Improve test strategy (unit + integration + command-level checks).
3. Add release hygiene (versioning, release branch flow).
4. Simplify metadata/timestamps model if still desired.
5. Finish or remove deprecated/legacy paths once backend is stable.

Reference idea notes:
- [docs/ideas.md](docs/ideas.md)

## 6. Recommended Restart Plan (For You or an AI)

This order minimizes churn and restores confidence quickly.

### Step 1 - Freeze canonical selection schema

Declare one source of truth and apply it everywhere:
- Use core model + filter field definitions as canonical.
- Align docs and tests to match this exact shape.

Primary files:
- [src/sports_calendar/core/selection/models.py](src/sports_calendar/core/selection/models.py)
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)
- [docs/selections.md](docs/selections.md)
- [tests/conftest.py](tests/conftest.py)

### Step 2 - Fix sessions filter contract end-to-end

Pick one representation and enforce consistency:
- Field serialization/deserialization
- Executor filtering behavior
- Test fixtures/assertions

Primary files:
- [src/sports_calendar/core/selection/filters/fields.py](src/sports_calendar/core/selection/filters/fields.py)
- [src/sports_calendar/infra/engine/executors/sessions.py](src/sports_calendar/infra/engine/executors/sessions.py)
- [tests/integration/infra/test_executors.py](tests/integration/infra/test_executors.py)

### Step 3 - Restore resolver integration coverage

Bring back and stabilize resolver integration test(s):
- Start with one deterministic fixture.
- Validate full path: selection -> resolver -> collections.

Primary file:
- [tests/integration/infra/test_engine.py](tests/integration/infra/test_engine.py)

### Step 4 - Clean low-risk inconsistencies

- Remove stale TODO stubs where behavior is already decided.
- Align minor naming/type inconsistencies.
- Keep UI out of scope unless explicitly needed.

## 7. Quick Operational Commands

From project root:

- Setup: `make setup`
- Validate and rewrite selections: `make validate-selections`
- Sync calendar for dev selection: `make sync-calendar`
- Clear calendar: `make clear-calendar`

Reference:
- [Makefile](Makefile)

## 8. Decision Log (Confirmed)

These decisions are confirmed and should be treated as project constraints:

1. sport-index is required for sports data access in this project (no fallback path planned).
2. data access separation into the sport-index project is intentional and considered an architectural improvement.
3. config selection files in [config/selections/dev.yml](config/selections/dev.yml) and [config/selections/dev-1.yml](config/selections/dev-1.yml) reflect the intended new schema direction.
4. old_gui is deprecated and retained only to avoid losing reusable code blocks.
5. UI work is currently deprioritized in favor of backend migration stabilization.

## 9. First Task To Execute Next Time

When resuming, do this first:

1. Update [docs/selections.md](docs/selections.md) to match current canonical selection schema.
2. Update [tests/conftest.py](tests/conftest.py) fixtures to same schema.
3. Re-enable and fix [tests/integration/infra/test_engine.py](tests/integration/infra/test_engine.py).

This gives immediate clarity + testable progress with limited risk.
