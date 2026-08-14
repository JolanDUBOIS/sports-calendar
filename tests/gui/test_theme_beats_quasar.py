""" Theme rules have to actually win.

Quasar's stylesheets load *after* ours. CSS breaks specificity ties by source
order, so any rule of ours that merely *matches* a Quasar rule loses — silently,
with our class present in the DOM and our declaration sitting there in
`theme.py` looking correct.

This has now happened three times: buttons stayed UPPERCASE at a 4px radius, and
the edit and delete icons stayed grey. Every one of them was invisible to the
test suite and only findable by looking at the screen.

So this test reads Quasar's own bundled CSS and checks, property by property,
that nothing in `_STYLESHEET` is quietly overruled. It is deliberately narrow:
it only inspects our single-class `.q-*` rules, which are the ones that can tie.
Doubled selectors (`.q-card.sc-card`) and `body`-prefixed ones already outrank
anything Quasar declares on one class, and our own `.sc-*` classes have no
competitor at all.
"""

import re
from pathlib import Path

import pytest

from sports_calendar.interfaces.gui import theme

# Properties Quasar sets that we never contest, or contest on purpose elsewhere.
# `background` on the header is fought with `!important`, which this test allows.
_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}")


def _quasar_stylesheets() -> list[Path]:
    import nicegui

    static = Path(nicegui.__file__).parent / "static"
    return sorted(static.glob("quasar*.prod.css"))


def _declarations(body: str) -> dict[str, bool]:
    """ property -> whether it was declared `!important`. """
    found = {}
    for declaration in body.split(";"):
        name, separator, value = declaration.partition(":")
        if not separator:
            continue
        found[name.strip().lower()] = "!important" in value.lower()
    return found


def _rules(css: str) -> list[tuple[str, dict[str, bool]]]:
    return [
        (selector.strip(), _declarations(body))
        for selector, body in _RULE.findall(css)
        # Skip at-rule preludes (@media, @supports); their inner rules are
        # matched separately by the same pattern.
        if selector.strip() and not selector.strip().startswith("@")
    ]


def _single_quasar_class(selector: str) -> str | None:
    """ The class name, if `selector` is exactly one `.q-*` class and nothing else. """
    stripped = selector.strip()
    if re.fullmatch(r"\.q-[a-z0-9-]+", stripped):
        return stripped
    return None


@pytest.fixture(scope="module")
def quasar_rules() -> list[tuple[str, dict[str, bool]]]:
    sheets = _quasar_stylesheets()
    if not sheets:
        pytest.skip("Quasar's bundled CSS was not found; nothing to compare against")
    return [rule for sheet in sheets for rule in _rules(sheet.read_text(encoding="utf-8"))]


def _quasar_declares(quasar_rules, class_selector: str) -> set[str]:
    """ Properties Quasar sets at a specificity our single class cannot beat.

    That means the class itself (`.q-btn`) *and* its modifier classes
    (`.q-btn--rectangle`, `.q-btn--round`): a modifier is still one class, so it
    ties with ours and wins on source order. The button radius was lost exactly
    there — `.q-btn--rectangle { border-radius: 4px }` — and a check that only
    looked at the base class would have called that rule healthy.
    """
    modifier = re.compile(rf"{re.escape(class_selector)}(--[a-z0-9-]+)?$")
    properties: set[str] = set()
    for selector, declarations in quasar_rules:
        parts = [part.strip() for part in selector.split(",")]
        if any(modifier.fullmatch(part) for part in parts):
            properties |= set(declarations)
    return properties


def test_no_theme_rule_is_silently_overruled_by_quasar(quasar_rules) -> None:
    losses = []
    for selector, declarations in _rules(theme._STYLESHEET):  # noqa: SLF001
        class_selector = _single_quasar_class(selector)
        if class_selector is None:
            continue

        contested = _quasar_declares(quasar_rules, class_selector)
        losses += [
            f"{selector} {{ {prop} }} ties with Quasar and loses on source order"
            for prop, is_important in declarations.items()
            if prop in contested and not is_important
        ]

    assert not losses, (
        "These declarations do nothing. Outrank Quasar by doubling the class "
        "(.q-card.sc-card), prefixing with `body`, or using !important:\n  "
        + "\n  ".join(losses)
    )


def test_the_check_would_catch_a_regression(quasar_rules) -> None:
    """ Guards the guard: a bare `.q-btn { text-transform }` must be reported.

    Without this, a change to Quasar's packaging that made `_quasar_declares`
    return nothing would turn the test above into one that always passes.
    """
    contested = _quasar_declares(quasar_rules, ".q-btn")
    assert "text-transform" in contested
    assert "color" in contested
