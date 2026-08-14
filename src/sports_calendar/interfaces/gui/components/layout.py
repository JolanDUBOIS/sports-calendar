from contextlib import contextmanager

from nicegui import context, ui

from .. import theme


def _switch_preset(name: str) -> None:
    """ Reload the current page under a different preset. """
    try:
        path = context.client.request.url.path
    except (AttributeError, RuntimeError):
        path = "/selections"
    ui.navigate.to(f"{path}?theme={name}")


def _theme_picker(current: str) -> None:
    """ A control for comparing looks.

    Here to answer "which of these actually looks better" by looking at them
    rather than by describing them. It is not a user preference — nothing is
    persisted, and the choice lives in the URL so two presets can be held open
    side by side in two tabs. Drop it, or promote it to a real setting, once the
    look is settled.

    A menu rather than a `ui.select` deliberately: a select here would be the
    second one on a page whose only other select is the sport picker, and tests
    reasonably reach for "the select on the page".
    """
    with ui.button(icon="palette").props("flat dense no-caps").mark("theme-picker"):
        ui.label(theme.PRESETS[current].label).classes("ml-1")
        with ui.menu():
            for name, preset in theme.PRESETS.items():
                ui.menu_item(preset.label, on_click=lambda n=name: _switch_preset(n))


def global_header(preset_name: str):
    with ui.header().classes("items-center justify-between w-full px-6 py-3"):
        ui.label("Sports Calendar").classes(
            f"{theme.BRAND} cursor-pointer hover:opacity-70"
        ).on("click", lambda: ui.navigate.to("/"))

        with ui.row().classes("items-center gap-2"):
            _theme_picker(preset_name)
            ui.button("Language", icon="language").props("flat dense no-caps")
            ui.button("Settings", icon="settings").props("flat dense no-caps")
            ui.button("Help", icon="help").props("flat dense no-caps")


@contextmanager
def base_layout():
    # Before anything it styles is built, so no element renders unthemed first.
    preset_name = theme.apply()
    global_header(preset_name)
    with ui.column().classes("w-full max-w-5xl mx-auto px-6 py-8 gap-0"):
        yield
