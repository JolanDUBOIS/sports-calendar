""" The one place the app decides what it looks like.

Everything visual lives here: colours, radii, shadows, borders, the surfaces
things sit on. Views elsewhere name *what a thing is* — `theme.CARD`,
`theme.MUTED` — and never what it looks like. That is the whole point: before
this module the answer to "what grey do we use for secondary text?" was
`text-gray-500`, written out at nine separate call sites across four
directories, and changing it meant finding all nine.

Layout classes (`w-full`, `items-center`, `gap-2`) stay at the call site. They
describe structure, not style, and a theme has no opinion about them.

## How it works

A preset is a set of design tokens. `apply()` writes them onto `:root` as CSS
custom properties and then loads one stylesheet — the same stylesheet for every
preset — whose rules are written entirely in terms of those properties. So
swapping the look swaps a dict of about twenty strings; no rule is duplicated
per preset, and no preset can drift out of step with the others.

`ui.colors()` is called with the same accent so Quasar's own components
(buttons, spinners, focus rings) follow along instead of staying on NiceGUI's
default blue.

## Adding a preset

Add a `Preset` to `PRESETS`. That is the entire procedure — if a new look needs
a new *rule* rather than new values, the rule belongs in `_STYLESHEET` written
against a token both presets can set.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, fields

from nicegui import context, ui

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Preset:
    """ One complete look, as values only.

    Field names become CSS custom properties: `text_muted` is written out as
    `--sc-text-muted`, and `_STYLESHEET` reads it back as
    `var(--sc-text-muted)`.
    """

    label: str

    # Surfaces, from the back of the page forwards.
    page: str            # the page itself
    surface: str         # cards, header, dialogs — what sits on the page
    surface_inset: str   # areas pushed *into* a card: its body, the drawer

    # Hairlines. `border` is the default; `border_strong` is for a border that
    # has to be seen on its own rather than merely separate two surfaces.
    border: str
    border_strong: str

    text: str
    text_muted: str

    accent: str           # primary actions, focus, the drag indicator
    accent_contrast: str  # text placed on top of `accent`
    accent_soft: str      # accent at low opacity, for hovers and tints
    # The two row actions. They are hues rather than shades of the paper because
    # they are the one place a glance has to tell two things apart instantly —
    # but desaturated, so they read as part of the paper's register rather than
    # as warning lights stuck onto it.
    edit: str
    danger: str

    radius_lg: str  # cards
    radius_md: str  # rules, inputs, buttons
    radius_sm: str  # chips

    shadow: str         # a resting card
    shadow_raised: str  # a dialog, or a card being dragged

    def as_css_variables(self) -> str:
        """ The preset as a `:root` block. `label` is metadata, not a token. """
        return "\n".join(
            f"  --sc-{f.name.replace('_', '-')}: {getattr(self, f.name)};"
            for f in fields(self)
            if f.name != "label"
        )


# Three variations on one warm register. They differ in how much warmth the
# paper carries and in how soft the shapes are — LINEN is bright and crisp,
# CLAY is sandy and rounded, SAND sits between them. Picking one is a single
# decision along that axis rather than a choice between unrelated designs.
#
# The steps are deliberately large enough to see. An earlier version separated
# them by about one percent of lightness, which is a difference you can measure
# and cannot perceive.
#
# In all three the accent is a darker shade of the paper rather than a second
# hue. Colour is spent only where it means something: blue for edit, red for
# delete.

LINEN = Preset(
    label="Linen",
    page="#faf9f7",
    surface="#ffffff",
    surface_inset="#f6f5f2",
    border="rgba(40, 38, 34, 0.12)",
    border_strong="rgba(40, 38, 34, 0.26)",
    text="#1f1e1b",
    text_muted="#6a655e",
    accent="#3a3733",
    accent_contrast="#ffffff",
    accent_soft="rgba(58, 55, 51, 0.055)",
    edit="#4d7fb3",
    danger="#b45a52",
    radius_lg="10px",
    radius_md="8px",
    radius_sm="6px",
    shadow="0 1px 2px rgba(40, 38, 34, 0.05)",
    shadow_raised="0 8px 24px rgba(40, 38, 34, 0.12)",
)

SAND = Preset(
    label="Sand",
    page="#f5f3ee",
    surface="#fffefb",
    surface_inset="#f8f6f1",
    border="rgba(55, 48, 38, 0.13)",
    border_strong="rgba(55, 48, 38, 0.27)",
    text="#26231f",
    text_muted="#6d6760",
    accent="#453f38",
    accent_contrast="#ffffff",
    accent_soft="rgba(69, 63, 56, 0.07)",
    edit="#4b7aab",
    danger="#ad584f",
    radius_lg="12px",
    radius_md="10px",
    radius_sm="8px",
    shadow="0 1px 2px rgba(55, 48, 38, 0.05), 0 1px 3px rgba(55, 48, 38, 0.05)",
    shadow_raised="0 8px 24px rgba(55, 48, 38, 0.13)",
)

CLAY = Preset(
    label="Clay",
    page="#ece6d9",
    surface="#faf7f0",
    surface_inset="#f2ede2",
    border="rgba(70, 58, 40, 0.16)",
    border_strong="rgba(70, 58, 40, 0.32)",
    text="#2c2720",
    text_muted="#6f6656",
    accent="#544c3f",
    accent_contrast="#ffffff",
    accent_soft="rgba(84, 76, 63, 0.08)",
    edit="#4a75a2",
    danger="#a6564d",
    radius_lg="14px",
    radius_md="12px",
    radius_sm="10px",
    shadow="0 1px 2px rgba(70, 58, 40, 0.06), 0 2px 6px rgba(70, 58, 40, 0.06)",
    shadow_raised="0 10px 28px rgba(70, 58, 40, 0.16)",
)

PRESETS: dict[str, Preset] = {
    "linen": LINEN,
    "sand": SAND,
    "clay": CLAY,
}

DEFAULT_PRESET = "sand"


# ---- what views name instead of naming a colour ---- #

BRAND = "sc-brand"
PAGE_TITLE = "sc-page-title"
SECTION_TITLE = "sc-section-title"
ICON_BUTTON = "sc-icon-button"
ICON_BUTTON_DANGER = "sc-icon-button sc-icon-button--danger"

CARD = "sc-card"
CARD_INTERACTIVE = "sc-card sc-card--interactive"
CARD_TITLE = "sc-card-title"
CARD_SUBTITLE = "sc-card-subtitle"
CARD_BODY = "sc-card-body"
CARD_HEADER = "sc-card-header"

RULE = "sc-rule"
ADD_CARD = "sc-add-card"
ADD_CARD_GLYPH = "sc-add-card-glyph"
DRAG_HANDLE = "sc-drag-handle"

MUTED = "sc-muted"          # secondary prose
HINT = "sc-hint"            # smaller still: field help, counts, timestamps
DANGER_TEXT = "sc-danger"
FIELD_LABEL = "sc-field-label"
PICKER = "sc-picker"        # the click-to-search box
CHIP_TRAY = "sc-chip-tray"  # the box multi-select chips sit in
INSET = "sc-inset"          # a panel pushed into the page, e.g. the drawer
SCROLL_PANEL = "sc-scroll-panel"
INFO_ICON = "sc-info-icon"


# The stylesheet is preset-independent by construction: every value it sets is
# a `var(--sc-*)`. If you find yourself wanting a literal colour here, it is a
# token that has not been named yet.
#
# ## The source-order trap
#
# Quasar's stylesheets are loaded *after* this one. So a rule of ours that ties
# with a Quasar rule on specificity loses — silently, with the class present in
# the DOM and the declaration visible in the file. Three rules here were written
# as a bare `.q-btn` or `.q-card` and did nothing at all until this was noticed.
#
# Anything competing with a Quasar single-class rule (`.q-btn`, `.q-card`,
# `.q-btn--rectangle`) therefore has to outrank it rather than match it: double
# the class up (`.q-card.sc-card`) or prefix with `body`. `!important` is the
# last resort, used only against Quasar's own `!important` rules — `.text-*`
# and the header's `bg-primary`.
#
# Note that a `color=` prop on a NiceGUI button renders `.text-<colour>`, which
# Quasar declares `!important`. No selector here can beat it; the button has to
# be built with `color=None` instead. See `_busy_icon_button`.
_STYLESHEET = """
/* ---- page ---- */
body,
.nicegui-content,
.q-page-container,
.q-layout {
  background: var(--sc-page);
  color: var(--sc-text);
}

/* ---- header: neutral chrome, colour saved for actions ---- */
.q-header {
  background: var(--sc-surface) !important;
  color: var(--sc-text) !important;
  border-bottom: 1px solid var(--sc-border);
  box-shadow: none !important;
}
.q-header .q-btn {
  color: var(--sc-text-muted) !important;
  font-weight: 500;
}
.q-header .q-btn:hover {
  color: var(--sc-text) !important;
}

/* ---- typography ---- */
.sc-brand {
  font-size: 1.3rem;
  font-weight: 680;
  letter-spacing: -0.02em;
  color: var(--sc-text);
}
.sc-page-title {
  font-size: 1.6rem;
  font-weight: 650;
  letter-spacing: -0.02em;
  color: var(--sc-text);
}
.sc-section-title {
  font-size: 1.05rem;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: var(--sc-text);
}
.sc-card-title {
  font-size: 0.975rem;
  font-weight: 600;
  color: var(--sc-text);
}
.sc-card-subtitle,
.sc-muted {
  font-size: 0.825rem;
  color: var(--sc-text-muted);
  line-height: 1.45;
}
.sc-hint {
  font-size: 0.75rem;
  color: var(--sc-text-muted);
}
.sc-danger {
  font-size: 0.75rem;
  color: var(--sc-danger);
}
.sc-field-label {
  font-size: 0.8rem;
  font-weight: 550;
  color: var(--sc-text);
}

/* ---- cards ---- */
/* Doubled selectors (.q-card.sc-card) rather than !important: Quasar sets its
   own radius and shadow on .q-card, and two classes outrank one. */
.q-card.sc-card,
.q-expansion-item.sc-card {
  background: var(--sc-surface);
  border: 1px solid var(--sc-border);
  border-radius: var(--sc-radius-lg);
  box-shadow: var(--sc-shadow);
  overflow: hidden;
  transition: border-color 0.15s ease, box-shadow 0.15s ease, background-color 0.15s ease;
}
.q-card.sc-card--interactive:hover,
.q-expansion-item.sc-card--interactive:hover {
  border-color: var(--sc-border-strong);
  box-shadow: var(--sc-shadow-raised);
}
/* The hover has to be painted on Quasar's own header row, not on the column we
   put inside it: that column is nested in a `.q-item__section` with padding
   around it, so tinting it drew a floating grey rectangle that stopped short of
   the card's edges and of the expand chevron. */
.sc-card .q-expansion-item__container > .q-item:hover {
  background: var(--sc-accent-soft);
}
/* Quasar paints its own hover and ripple through this overlay. Left in, it
   shows through as a second, differently-sized grey. */
.sc-card .q-expansion-item__container > .q-item .q-focus-helper {
  display: none;
}
/* The body stays on the card's own surface. Tinting it *and* giving each rule
   a surface of its own stacked two separations on top of each other, which
   read as clutter; the rules' borders already do the job. */
.sc-card-body {
  background: var(--sc-surface);
  /* Commented out to see it without: the gap between the header and the first
     rule may already separate them well enough on its own. */
  /* border-top: 1px solid var(--sc-border); */
}

/* ---- rules: their own surface, so they read as separate objects ---- */
/* White on the card body's grey, not grey on grey — a tint alone is not enough
   separation once there are three of them stacked up. */
.q-card.sc-rule {
  background: var(--sc-surface);
  border: 1px solid var(--sc-border);
  border-radius: var(--sc-radius-md);
  box-shadow: none;
  transition: border-color 0.15s ease, background-color 0.15s ease;
}
.q-card.sc-rule:hover {
  border-color: var(--sc-border-strong);
}
/* Dragging state. Owned here so the drag script carries behaviour only. */
.filter-row.sc-drag-source {
  opacity: 0.45;
}
.filter-row.sc-drop-before {
  box-shadow: inset 0 2px 0 0 var(--sc-accent);
}
.filter-row.sc-drop-after {
  box-shadow: inset 0 -2px 0 0 var(--sc-accent);
}
.sc-drag-handle {
  color: var(--sc-text-muted);
  opacity: 0.55;
  transition: opacity 0.15s ease;
}
.sc-drag-handle:hover {
  opacity: 1;
}

/* ---- the add button, as a ghost card ---- */
.q-card.sc-add-card {
  background: transparent;
  border: 1px dashed var(--sc-border-strong);
  border-radius: var(--sc-radius-lg);
  box-shadow: none;
  transition: border-color 0.15s ease, background-color 0.15s ease;
}
.q-card.sc-add-card:hover {
  background: var(--sc-accent-soft);
  border-color: var(--sc-accent);
}
.sc-add-card-glyph {
  font-size: 1.4rem;
  font-weight: 300;
  line-height: 1;
  color: var(--sc-text-muted);
}
.q-card.sc-add-card:hover .sc-add-card-glyph {
  color: var(--sc-accent);
}

/* ---- inputs and pickers ---- */
.sc-picker,
.sc-chip-tray {
  background: var(--sc-surface);
  border: 1px solid var(--sc-border);
  border-radius: var(--sc-radius-md);
  transition: border-color 0.15s ease;
}
.sc-picker:hover,
.sc-chip-tray:hover {
  border-color: var(--sc-border-strong);
}
.sc-scroll-panel {
  border: 1px solid var(--sc-border);
  border-radius: var(--sc-radius-md);
  background: var(--sc-surface);
}
.sc-scroll-panel .q-item:hover {
  background: var(--sc-accent-soft);
}
/* `body` prefix: Quasar's own rule is `.q-field--outlined .q-field__control`,
   which outranks a bare `.q-field__control` and lands later in the cascade. */
body .q-field .q-field__control {
  border-radius: var(--sc-radius-md) var(--sc-radius-md) 0 0;
}

/* ---- panels ---- */
.sc-inset {
  background: var(--sc-surface-inset);
}
.q-drawer {
  border-color: var(--sc-border) !important;
}

/* ---- dialogs and buttons ---- */
.q-dialog .q-card {
  border-radius: var(--sc-radius-lg);
  box-shadow: var(--sc-shadow-raised);
}
/* `body` prefix so this beats Quasar's `.q-btn` and `.q-btn--rectangle`, which
   are the same specificity and load later. Without it every button here stayed
   UPPERCASE at a 4px radius, and each one had to ask for `no-caps` by hand. */
body .q-btn {
  border-radius: var(--sc-radius-md);
  text-transform: none;
  letter-spacing: 0;
  font-weight: 550;
}
body .q-btn--round {
  border-radius: 50%;
}

/* ---- row actions ---- */
/* Coloured at rest, not only on hover: with icons in place of words, the colour
   is what tells the two apart before you have read the tooltip.
   `.q-btn` is doubled up because Quasar sets `.q-btn { color: inherit }` — one
   class, same specificity as ours, in a stylesheet loaded after ours, so a
   single class here loses on source order and the icons stay grey. */
.q-btn.sc-icon-button {
  color: var(--sc-edit);
  opacity: 0.85;
  transition: opacity 0.15s ease;
}
.q-btn.sc-icon-button:hover {
  opacity: 1;
}
.q-btn.sc-icon-button--danger {
  color: var(--sc-danger);
}

.sc-info-icon {
  color: var(--sc-text-muted);
  opacity: 0.7;
  cursor: help;
}
.sc-info-icon:hover {
  color: var(--sc-accent);
  opacity: 1;
}
"""


def resolve(name: str | None) -> tuple[str, Preset]:
    """ The preset `name` asks for, falling back to the default.

    Never raises: an unknown name arrives from a URL someone typed, and a
    mis-typed query string should show the app rather than an error page.
    """
    if name and name in PRESETS:
        return name, PRESETS[name]
    if name:
        logger.debug("Unknown theme '%s'; falling back to '%s'", name, DEFAULT_PRESET)
    return DEFAULT_PRESET, PRESETS[DEFAULT_PRESET]


def requested_preset_name() -> str | None:
    """ The `?theme=` on the current request, if there is one.

    Lets two presets be compared side by side in two tabs. Returns None under
    the test harness and anywhere else without a real request, which `resolve`
    turns into the default.
    """
    try:
        request = context.client.request
    except (AttributeError, RuntimeError):
        return None
    if request is None:
        return None
    return request.query_params.get("theme")


def apply(name: str | None = None) -> str:
    """ Install a preset on the current page. Returns the name actually used.

    Must run inside a page context, before anything it styles is built.
    """
    resolved_name, preset = resolve(name if name is not None else requested_preset_name())

    ui.add_css(f":root {{\n{preset.as_css_variables()}\n}}\n{_STYLESHEET}")
    # So Quasar's own components — button fills, spinners, focus rings — use the
    # preset's accent instead of NiceGUI's default blue.
    ui.colors(primary=preset.accent, negative=preset.danger)

    return resolved_name
