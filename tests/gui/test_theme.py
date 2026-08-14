""" The theme layer.

Two things are worth pinning down. A preset has to be *complete* — a token a
preset forgets to set becomes a `var()` with no value, which does not error, it
just silently drops the rule and leaves that one element unstyled. And an
unknown preset name arrives from a URL a person typed, so it must not be able
to break the page.

What the presets look like is not tested and should not be: it is a judgement
call, and a test that asserts a colour only makes changing that colour harder.
"""

import re

import pytest
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import SelectionFilter
from sports_calendar.interfaces.gui import theme


def _a_preset_that_is_not_the_default() -> str:
    """ Any preset but the default, so the test proves the name was honoured
    rather than passing on the fallback. Named indirectly: presets get renamed
    while the look is being settled, and that should not break these. """
    return next(n for n in sorted(theme.PRESETS) if n != theme.DEFAULT_PRESET)


@pytest.mark.parametrize("name", sorted(theme.PRESETS))
def test_every_preset_sets_every_token(name: str) -> None:
    """ A missing token silently unstyles whatever used it. """
    variables = theme.PRESETS[name].as_css_variables()

    expected = {f.replace("_", "-") for f in theme.Preset.__dataclass_fields__ if f != "label"}
    declared = {
        line.split(":", 1)[0].strip().removeprefix("--sc-")
        for line in variables.splitlines()
        if line.strip()
    }
    assert declared == expected


@pytest.mark.parametrize("name", sorted(theme.PRESETS))
def test_no_preset_leaves_a_token_empty(name: str) -> None:
    for line in theme.PRESETS[name].as_css_variables().splitlines():
        _, _, value = line.partition(":")
        assert value.strip().rstrip(";"), f"empty token in preset '{name}': {line}"


def test_the_stylesheet_only_ever_names_tokens() -> None:
    """ A literal colour here would be invisible to every preset but one. """
    assert "#" not in theme._STYLESHEET  # noqa: SLF001


def test_the_stylesheet_uses_no_token_a_preset_does_not_define() -> None:
    """ Catches a `var(--sc-typo)` that would quietly render as nothing. """
    used = set(re.findall(r"var\(--sc-([a-z-]+)\)", theme._STYLESHEET))  # noqa: SLF001
    defined = {f.replace("_", "-") for f in theme.Preset.__dataclass_fields__ if f != "label"}
    assert used <= defined, f"undefined tokens: {sorted(used - defined)}"


def test_an_unknown_name_falls_back_to_the_default() -> None:
    assert theme.resolve("nonsense") == (theme.DEFAULT_PRESET, theme.PRESETS[theme.DEFAULT_PRESET])
    assert theme.resolve(None) == (theme.DEFAULT_PRESET, theme.PRESETS[theme.DEFAULT_PRESET])


def test_a_known_name_is_honoured() -> None:
    name = _a_preset_that_is_not_the_default()
    assert theme.resolve(name) == (name, theme.PRESETS[name])


async def test_a_page_carries_the_default_preset(user: User) -> None:
    await user.open("/selections")
    styles = "".join(user.client.head_html)
    assert "--sc-accent" in styles
    assert theme.PRESETS[theme.DEFAULT_PRESET].accent in styles


async def test_a_page_honours_the_theme_in_the_url(user: User) -> None:
    name = _a_preset_that_is_not_the_default()
    await user.open(f"/selections?theme={name}")
    styles = "".join(user.client.head_html)
    assert theme.PRESETS[name].page in styles


async def test_row_actions_leave_their_colour_to_the_theme(user: User) -> None:
    """ A `color` prop on a button renders Quasar's `.text-<colour>`, which is
    declared `!important` and so beats every selector in `theme.py`. NiceGUI
    adds `color='primary'` unless told otherwise, which silently pinned the edit
    and delete icons to the accent no matter what the theme asked for. Nothing
    about that failure is visible except by looking at the screen. """
    SelectionService.add_empty_selection("my-sel")
    item = SelectionService.add_empty_item("my-sel", 1, name="Football")
    SelectionService.add_filter(item.uid, SelectionFilter(sport_id=1, name="a", uid="a"))
    await user.open("/selections/my-sel")

    for marker in ("card-edit", "card-delete"):
        buttons = list(user.find(marker=marker).elements)
        assert buttons, f"no {marker} button on the page"
        for button in buttons:
            assert "color" not in button._props, (  # noqa: SLF001
                f"{marker} carries a color prop; theme.py can no longer colour it"
            )


async def test_a_nonsense_theme_in_the_url_still_renders(user: User) -> None:
    """ The page has to survive a mistyped query string, not 500. """
    SelectionService.add_empty_selection("my-sel")
    await user.open("/selections?theme=chartreuse")
    await user.should_see("my-sel")
