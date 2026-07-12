from collections.abc import Callable

from nicegui import ui


class BaseCard:
    """A standard card. Can be used alone or as a context manager to add items below the header."""

    def __init__(self, title: str, subtitle: str = None, on_delete: Callable = None, on_edit: Callable = None):
        self.title = title
        self.subtitle = subtitle
        self.on_delete = on_delete
        self.on_edit = on_edit

        self.container = self._create_container()
        with self.container:
            self._build_header()
            self._build_body()

    def _create_container(self):
        """Creates the outer shell. Overridden by subclasses."""
        return ui.card().classes('w-full mb-2 shadow-sm border border-gray-200 transition-colors duration-200 p-0')

    def _build_header(self):
        """Builds the shared title and button layout."""
        with ui.row().classes('w-full items-center justify-between p-4'):
            with ui.column().classes('gap-0'):
                ui.label(self.title).classes('text-lg font-bold')
                if self.subtitle:
                    ui.label(self.subtitle).classes('text-sm text-gray-500 italic')

            if self.on_edit or self.on_delete:
                with ui.row().classes('gap-2 ml-auto'):
                    if self.on_edit:
                        ui.button('Edit', on_click=self.on_edit).props('color=primary flat dense').on('click.stop', lambda: None)
                    if self.on_delete:
                        ui.button('Delete', on_click=self.on_delete).props('color=red flat dense').on('click.stop', lambda: None)

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

    def _build_header(self):
        return None

    def _build_body(self):
        with ui.row().classes('w-full items-center justify-center py-3'):
            ui.label(self.label).classes('text-3xl font-light text-gray-500 leading-none')
