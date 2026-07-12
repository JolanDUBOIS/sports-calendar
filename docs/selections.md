# Selections

The `selections` YAML files define which sports, competitions, and competitors Sports Calendar will track for a given Google Calendar. Each selection is linked **one-to-one** with a key in `.secrets/gcal_ids.yml` via the `name` field in the selection file (e.g., `keyA` → `main-selection.yml`).

> Each key in `gcal_ids.yml` must have a corresponding selection file. At least one selection is required.

## Creating a Selection File

1. Create the folder if it doesn't exist:

```bash
mkdir -p ~/.config/sports-calendar/selections
```

1. Create a new YAML file:

```bash
nano ~/.config/sports-calendar/selections/main-selection.yml
```

1. Basic structure:

```yml
name: keyA  # must match the key in gcal_ids.yml

items:
  - sport_id: 1  # sports_calendar's own local sport identifier (1 = football, 11 = f1, ...)
    filters:
      - fields:
          filter_type: sessions
          competition_id: stgc:40
          sessions: [6, 10]  # StageTier values, see below
```

- `name` links the selection to a Google Calendar key.
- `items` lists the sports you want to track. Each item groups one or more `filters` that narrow down which events to pull.
- Each filter's `fields.filter_type` selects which kind of filter it is, and determines which other keys are required (see below).

See [`config/selections/dev.yml`](../config/selections/dev.yml) for a complete, real, multi-filter example.

## Entity IDs

Competition and competitor IDs are **not** plain sofascore integers — they are the string IDs that the `sport-index` dependency assigns to its own entities, in the form `prefix:raw_id` (e.g. `trnc:7` for a tournament competition, `t-cpt:1644` for a team-sport competitor). sports_calendar stores and passes these strings through as-is; it never invents or re-encodes them. The prefixes you'll run into in selection files:

| Prefix  | Entity                                                                 |
|---------|-------------------------------------------------------------------------|
| `trnc`  | Competition (tournament-style, e.g. league/cup football competitions)   |
| `stgc`  | Competition (stage-style, e.g. motorsport/cycling race series)         |
| `t-cpt` | Competitor (team-sport, e.g. football teams)                           |
| `p-cpt` | Competitor (player-sport, e.g. tennis players)                         |

`sport_id` (on `items` and their `filters`) is the exception — it's sports_calendar's own small local dispatch key (`1` = football, `11` = f1, ...), not a sport-index ID.

## Filter Types

Each filter's `fields` dict requires `filter_type` plus the keys below:

- **`empty`** — no other keys. Matches nothing on `fetch`, passes everything through on `apply`.
- **`min_ranking`** — `ranking` (int), `competition_ids` (list of `trnc:`/`stgc:` IDs), optional `selection_rule` (see below).
- **`competitions`** — `competition_ids` (list of `trnc:`/`stgc:` IDs), optional `stage`.
- **`competitors`** — `competitor_ids` (list of `t-cpt:`/`p-cpt:` IDs), optional `selection_rule`.
- **`sessions`** — `competition_id` (a single `stgc:` ID — race/stage sports only), `sessions` (list of `StageTier` int values, see below).

`selection_rule` (used by `min_ranking` and `competitors`) has a `rule` (`any` / `both` / `opponent`) and, only for `opponent`, a `reference` (a single competitor ID).

### `sessions` values

`sessions` values are `StageTier` ints from `sport-index`, identifying the *kind* of session rather than matching on its display name:

| Value | Tier              |
|-------|-------------------|
| 3     | PRACTICE          |
| 4     | QUALIFYING        |
| 5     | QUALIFYING_PART   |
| 6     | RACE              |
| 9     | PROLOGUE          |
| 10    | SPRINT_RACE       |
| 12    | SPRINT_QUALIFYING |

## Notes

- Manually creating selections is currently **technical and error-prone**; IDs must match `sport-index`'s own entities.
- For now, if you want a custom selection, the easiest approach is to **ask the maintainer** or look at the `dev.yml` example in `./config/selections/`.
- A **UI for creating selections** is planned in a future release.
- Run `make validate-selections` after editing a selection file by hand — it validates the file and rewrites it in canonical form.

> Each selection is independent: you can create multiple selections and link them to different calendars. This allows you to maintain separate calendars for different sports, teams, or competitions.
