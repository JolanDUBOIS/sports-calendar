# Google Calendar IDs Setup

Sports Calendar needs **at least one Google Calendar ID** to link your selections. Each calendar ID is associated with a **key** in the configuration file, and this key corresponds to the `name` field in the selection YAML file. The link is always 1:1 — one calendar per selection.

> Important: Any manually added events in these calendars will likely be deleted when running sync-calendar. Do not add manual events to calendars used for Sports Calendar.

## 1 - Create a Dedicated Calendar

1. Open [Google Calendar](https://calendar.google.com/).
2. On the left sidebar under **Other calendars**, click **+** → **Create new calendar**.
3. Give it a name (e.g., `Football Events`) and optionally a description.
4. Click **Create calendar**.

## 2 - Find the Calendar ID

Once the calendar is created:
1. Click on the calendar in the left sidebar → **Settings and sharing**.
2. Scroll to **Integrate calendar**.
3. Copy the **Calendar ID** — it usually looks like `example@gmail.com` or `xxxx@group.calendar.google.com`.
4. This is the value you will use in your `gcal_ids.yml` configuration.

## 3 - Configure `gcal_ids.yml`

Create the `.secrets` folder in your user config directory and add a YAML file:

```bash
mkdir -p ~/.config/sports-calendar/.secrets
nano ~/.config/sports-calendar/.secrets/gcal_ids.yml
```

Example structure:

```yml
keyA: YOUR_FIRST_CALENDAR_ID@group.calendar.google.com
keyB: YOUR_SECOND_CALENDAR_ID@group.calendar.google.com
```
- The key is linked to a selection (`name` in the YAML file).
- You can create as many keys as you need, but each must be unique.
- At least one key must be present for Sports Calendar to function.

## 4 - Notes

- Each key must have a **dedicated calendar**, as sync-calendar always delete future events before adding new ones.
- You can share this dedicated calendar with your main account if you want to see the events elsewhere.
- The IDs are only needed for the calendars used by Sports Calendar; other calendars remain untouched.
