# Selections

The `selections` YAML files define which sports, teams, and competitions Sports Calendar will track for a given Google Calendar. Each selection is linked **one-to-one** with a key in `.secrets/gcal_ids.yml` via the `name` field in the selection file (e.g., `keyA` → `main-selection.yml`).

> Each key in `gcal_ids.yml` must have a corresponding selection file. At least one selection is required.

## Creating a Selection File

1. Create the folder if it doesn’t exist:

```bash
mkdir -p ~/.config/sports-calendar/selections
```

2. Create a new YAML file:

```bash
nano ~/.config/sports-calendar/selections/main-selection.yml
```

3. Basic structure:

```yml
name: keyA  # must match the key in gcal_ids.yml

items:
  - sport: football
    entity: team
    id: 160  # PSG
  # Add more items as needed
```
- `name` links the selection to a Google Calendar key.
- `items` lists the entities (teams, competitions, etc.) you want to track.

## Notes

- Manually creating selections is currently **technical and error-prone**; IDs must match the source APIs.
- For now, if you want a custom selection, the easiest approach is to **ask the maintainer** or look at the `dev.yml` example in `./config/selections/`.
- A **UI for creating selections** is planned in a future release.

> Each selection is independent: you can create multiple selections and link them to different calendars. This allows you to maintain separate calendars for different sports, teams, or competitions.
