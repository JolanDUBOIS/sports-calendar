""" Small inline explainers.

The app is aimed at people who do not know the vocabulary of the domain, so
almost every label can carry an optional explanation. These are deliberately
tiny and unobtrusive: an "i" badge that reveals a sentence on hover.
"""

from nicegui import ui

from .. import theme


def info_icon(text: str, *, size: str = "xs") -> ui.icon:
    """ An "i" badge revealing `text` on hover. """
    icon = ui.icon("info", size=size).classes(theme.INFO_ICON)
    with icon:
        ui.tooltip(text).classes("text-sm max-w-xs whitespace-normal")
    return icon


def explanation(text: str) -> ui.label:
    """ A muted sentence used to introduce a screen or a choice. """
    return ui.label(text).classes(theme.MUTED)
