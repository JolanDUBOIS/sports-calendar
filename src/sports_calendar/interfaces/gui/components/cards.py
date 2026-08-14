import inspect
from collections.abc import Callable

from nicegui import ui

from .. import theme


def _busy_icon_button(
    icon: str, tooltip: str, handler: Callable, *, marker: str, danger: bool = False
) -> ui.button:
    """ An icon action that refuses to fire again until its handler has finished.

    "Edit" opens a dialog that has to look things up before it is usable. Even
    with the dialog appearing immediately, any handler that awaits leaves a
    window in which a second click is accepted — and two clicks meant two
    dialogs, stacked on top of each other.

    Icon-only, so it carries a tooltip and a marker: the tooltip because the app
    is aimed at people who should not have to guess, and the marker because
    there is no longer a label for a test to search for.
    """
    # `color=None` matters: NiceGUI defaults a button to `color='primary'`,
    # which renders Quasar's `.text-primary` — and that rule is `!important`, so
    # no selector in `theme.py` can outrank it. Dropping the prop lets the theme
    # own the colour, which is where it belongs.
    button = ui.button(
        icon=icon, color=None
    ).props("flat dense round size=sm").classes(
        theme.ICON_BUTTON_DANGER if danger else theme.ICON_BUTTON
    ).mark(marker)
    with button:
        # Above the icon rather than below it: below, the tooltip lands on the
        # next rule down and covers the thing you are about to compare it to.
        ui.tooltip(tooltip).props('anchor="top middle" self="bottom middle"')

    async def run() -> None:
        button.disable()
        try:
            result = handler()
            if inspect.isawaitable(result):
                await result
        finally:
            button.enable()

    button.on_click(run)
    return button


class BaseCard:
    """A standard card. Can be used alone or as a context manager to add items below the header."""

    def __init__(self, title: str, subtitle: str = None, on_delete: Callable = None, on_edit: Callable = None, draggable: bool = False):
        self.title = title
        self.subtitle = subtitle
        self.on_delete = on_delete
        self.on_edit = on_edit
        self.draggable = draggable
        self.drag_handle = None

        self.container = self._create_container()
        with self.container:
            self._build_header()
            self._build_body()

    def _create_container(self):
        """Creates the outer shell. Overridden by subclasses."""
        return ui.card().classes(f'w-full p-0 {theme.CARD}')

    def _build_header(self):
        """Builds the shared title and button layout."""
        header_row = ui.row().classes('w-full items-center justify-between px-4 py-3')
        if self.draggable:
            # `.nicegui-row` puts a 1rem gap on every row. Inline so it wins
            # without depending on stylesheet order.
            header_row.style('gap: 0.25rem')

        with header_row:
            if self.draggable:
                # The conventional six dots. Only the handle starts a drag, so
                # selecting the title text still works normally.
                #
                # The margins look far too large for the gap they close, and
                # have to be: `drag_indicator` draws its dots in the middle of a
                # square glyph box with transparent space either side, so a
                # margin has to swallow that whitespace before it moves anything
                # visible. Measured against the box, not against the dots.
                self.drag_handle = ui.icon('drag_indicator', size='26px').classes(
                    f'drag-handle {theme.DRAG_HANDLE} cursor-grab active:cursor-grabbing'
                # Asymmetric on purpose: the left margin fights the row's padding
                # as well as the glyph's whitespace, the right one only the
                # whitespace. Equal numbers here would not look equal on screen.
                #
                # The left margin is smaller than it looks like it should be
                # because a rule now has a visible border of its own: the dots
                # have to clear that edge, where before they only had to line up
                # with the padding of the panel behind them.
                ).style('margin-left: -14px; margin-right: 4px; padding: 0').mark('drag-handle')

            with ui.column().classes('gap-0.5'):
                self.title_label = ui.label(self.title).classes(theme.CARD_TITLE)
                # Always created, hidden while empty: cards are updated in place
                # rather than re-rendered, so a subtitle that only exists when
                # it started non-empty can never be corrected later.
                self.subtitle_label = ui.label(self.subtitle or '').classes(theme.CARD_SUBTITLE)
                self.subtitle_label.set_visibility(bool(self.subtitle))

            if self.on_edit or self.on_delete:
                with ui.row().classes('gap-1 ml-auto items-center'):
                    # click.stop so the click does not also toggle the expansion
                    # this header belongs to.
                    if self.on_edit:
                        _busy_icon_button(
                            'edit', 'Edit', self.on_edit, marker='card-edit',
                        ).on('click.stop', lambda: None)
                    if self.on_delete:
                        _busy_icon_button(
                            'delete', 'Delete', self.on_delete, marker='card-delete', danger=True,
                        ).on('click.stop', lambda: None)

    def set_subtitle(self, subtitle: str | None) -> None:
        """ Update the subtitle in place, hiding it when there is nothing to say. """
        self.subtitle = subtitle
        self.subtitle_label.set_text(subtitle or '')
        self.subtitle_label.set_visibility(bool(subtitle))

    def _build_body(self):
        """Creates the mount point for nested elements."""
        # Hidden by default unless used with 'with'
        self.body_container = ui.column().classes('w-full px-4 pb-4 hidden')

    def __enter__(self):
        # When used with 'with', reveal the body and enter its context
        self.body_container.classes(remove='hidden')
        self.body_container.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.body_container.__exit__(exc_type, exc_val, exc_tb)


class InteractiveCard(BaseCard):
    def __init__(self, title: str, subtitle: str = None, on_click: Callable | None = None, on_delete: Callable = None, on_edit: Callable = None):
        self.on_click = on_click
        super().__init__(title=title, subtitle=subtitle, on_delete=on_delete, on_edit=on_edit)

    def _create_container(self):
        card = super()._create_container()
        if self.on_click:
            card.on('click', self.on_click).classes(f'cursor-pointer {theme.CARD_INTERACTIVE}')
        return card


class ExpandableCard(BaseCard):
    """Inherits from BaseCard but overrides the container to be an expansion item."""

    def __init__(self, title: str, subtitle: str = None, default_open: bool = False, on_delete: Callable = None, on_edit: Callable = None):
        self.default_open = default_open
        super().__init__(title, subtitle, on_delete=on_delete, on_edit=on_edit)

    def _create_container(self):
        """Override to use NiceGUI's expansion element instead of a normal card."""
        self.exp = ui.expansion(value=self.default_open).classes(f'w-full {theme.CARD}')
        return self.exp

    def _build_header(self):
        """Override to inject the parent's header specifically into the expansion slot."""
        with self.exp.add_slot('header'), ui.column().classes(f'w-full cursor-pointer {theme.CARD_HEADER}'):
            super()._build_header()

    def _build_body(self):
        """Override to style the drop-down section."""
        # Narrower at the sides than the header above it: the rules inside carry
        # their own borders, so a wide gutter around them just wastes width.
        self.body_container = ui.column().classes(f'w-full px-2 pb-3 pt-1 {theme.CARD_BODY}')


class FilterBlock(BaseCard):
    """One rule, inside an ExpandableCard's body.

    It gets a surface of its own rather than sitting transparent on the body:
    with three of them stacked up, a shared background made them read as one
    undifferentiated block instead of three things you can act on separately.
    """

    def _create_container(self):
        return ui.card().classes(f'w-full p-0 {theme.RULE}')


class AddCard(InteractiveCard):
    """A clickable full-width card used for add actions."""

    def __init__(self, label: str = '+', on_click: Callable = None):
        self.label = label
        super().__init__(title='', subtitle=None, on_click=on_click)
        # Marker so tests can target the add button rather than guessing at
        # card ordering.
        self.container.mark('add-card')

    def _create_container(self):
        # A dashed ghost rather than a solid card: it is an affordance, not
        # content, and should not compete with the real cards above it.
        card = ui.card().classes(f'w-full p-0 {theme.ADD_CARD}')
        if self.on_click:
            card.on('click', self.on_click).classes('cursor-pointer')
        return card

    def _build_header(self):
        return None

    def _build_body(self):
        with ui.row().classes('w-full items-center justify-center py-3'):
            ui.label(self.label).classes(theme.ADD_CARD_GLYPH)
