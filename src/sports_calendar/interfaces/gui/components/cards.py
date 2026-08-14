import inspect
from collections.abc import Callable

from nicegui import ui


def _busy_button(label: str, handler: Callable, props: str) -> ui.button:
    """ A button that refuses to fire again until its handler has finished.

    "Edit" opens a dialog that has to look things up before it is usable. Even
    with the dialog appearing immediately, any handler that awaits leaves a
    window in which a second click is accepted — and two clicks meant two
    dialogs, stacked on top of each other.
    """
    button = ui.button(label).props(props)

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
        return ui.card().classes('w-full mb-2 shadow-sm border border-gray-200 transition-colors duration-200 p-0')

    def _build_header(self):
        """Builds the shared title and button layout."""
        header_row = ui.row().classes('w-full items-center justify-between p-4')
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
                    'drag-handle text-gray-400 hover:text-gray-600 '
                    'cursor-grab active:cursor-grabbing'
                # Asymmetric on purpose: the left margin fights the row's padding
                # as well as the glyph's whitespace, the right one only the
                # whitespace. Equal numbers here would not look equal on screen.
                ).style('margin-left: -22px; margin-right: 4px; padding: 0').mark('drag-handle')

            with ui.column().classes('gap-0'):
                self.title_label = ui.label(self.title).classes('text-lg font-bold')
                # Always created, hidden while empty: cards are updated in place
                # rather than re-rendered, so a subtitle that only exists when
                # it started non-empty can never be corrected later.
                self.subtitle_label = ui.label(self.subtitle or '').classes('text-sm text-gray-500 italic')
                self.subtitle_label.set_visibility(bool(self.subtitle))

            if self.on_edit or self.on_delete:
                with ui.row().classes('gap-2 ml-auto'):
                    # click.stop so the click does not also toggle the expansion
                    # this header belongs to.
                    if self.on_edit:
                        _busy_button('Edit', self.on_edit, 'color=primary flat dense').on('click.stop', lambda: None)
                    if self.on_delete:
                        _busy_button('Delete', self.on_delete, 'color=red flat dense').on('click.stop', lambda: None)

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
            card.on('click', self.on_click).classes('cursor-pointer hover:bg-gray-50')
        return card


class ExpandableCard(BaseCard):
    """Inherits from BaseCard but overrides the container to be an expansion item."""

    def __init__(self, title: str, subtitle: str = None, default_open: bool = False, on_delete: Callable = None, on_edit: Callable = None):
        self.default_open = default_open
        super().__init__(title, subtitle, on_delete=on_delete, on_edit=on_edit)

    def _create_container(self):
        """Override to use NiceGUI's expansion element instead of a normal card."""
        expansion_classes = 'w-full mb-2 bg-white shadow-sm border border-gray-200 rounded-md overflow-hidden'
        self.exp = ui.expansion(value=self.default_open).classes(expansion_classes)
        return self.exp

    def _build_header(self):
        """Override to inject the parent's header specifically into the expansion slot."""
        with self.exp.add_slot('header'):
            header_classes = 'w-full cursor-pointer hover:bg-gray-50 transition-colors duration-200'
            with ui.column().classes(header_classes):
                super()._build_header()

    def _build_body(self):
        """Override to style the drop-down section."""
        self.body_container = ui.column().classes('w-full p-4 bg-gray-50 border-t border-gray-200')


class FilterBlock(BaseCard):
    """A borderless card component for displaying individual filters inside an ExpandableCard."""

    def _create_container(self):
        # Override the parent method to remove borders, shadows, and default backgrounds.
        return ui.card().classes('w-full bg-transparent shadow-none p-0 transition-colors duration-200')


class AddCard(InteractiveCard):
    """A clickable full-width card used for add actions."""

    def __init__(self, label: str = '+', on_click: Callable = None):
        self.label = label
        super().__init__(title='', subtitle=None, on_click=on_click)
        # Marker so tests can target the add button rather than guessing at
        # card ordering.
        self.container.mark('add-card')

    def _build_header(self):
        return None

    def _build_body(self):
        with ui.row().classes('w-full items-center justify-center py-3'):
            ui.label(self.label).classes('text-3xl font-light text-gray-500 leading-none')
