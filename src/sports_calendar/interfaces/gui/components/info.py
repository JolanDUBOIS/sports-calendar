""" Small inline explainers.

The app is aimed at people who do not know the vocabulary of the domain, so
almost every label can carry an optional explanation. These are deliberately
tiny and unobtrusive: an "i" badge that reveals a sentence on hover.
"""

from nicegui import ui


def info_icon(text: str, *, size: str = "xs") -> ui.icon:
    """ An "i" badge revealing `text` on hover. """
    icon = ui.icon("info", size=size).classes(
        "text-gray-400 hover:text-primary cursor-help"
    )
    with icon:
        ui.tooltip(text).classes("text-sm max-w-xs whitespace-normal")
    return icon


def label_with_info(label: str, help_text: str | None = None, *, classes: str = "") -> None:
    """ A label followed by an optional "i" badge. """
    with ui.row().classes(f"items-center gap-1 {classes}"):
        ui.label(label)
        if help_text:
            info_icon(help_text)


def explanation(text: str) -> ui.label:
    """ A muted sentence used to introduce a screen or a choice. """
    return ui.label(text).classes("text-sm text-gray-500 leading-snug")
