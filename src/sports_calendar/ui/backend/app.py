from pathlib import Path

from flask import Flask

from .routes.selections import bp as selections_bp
from .routes.items import bp as items_bp
from .routes.filters import bp as filters_bp
from .routes.lookups import bp as lookups_bp
from sports_calendar.core import Paths
from sports_calendar.core.db import setup_repo_path
from sports_calendar.core.selection import SelectionRegistry, SelectionStorage


FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

def create_app(config: dict | None = None) -> Flask:
    """Create and configure the Flask app."""
    app = Flask(
        __name__,
        template_folder=FRONTEND_DIR / "templates",
        static_folder=FRONTEND_DIR / "static",
        static_url_path="/static",
    )
    setup_repo_path(Paths.DB_DIR)
    SelectionRegistry.initialize(
        SelectionStorage.load_all()
    )

    if config:
        app.config.update(config)

    # Register all resource blueprints
    app.register_blueprint(selections_bp)
    app.register_blueprint(items_bp)
    app.register_blueprint(filters_bp)
    app.register_blueprint(lookups_bp)

    return app


def launch(host="127.0.0.1", port=5000, debug=True, **kwargs):
    """
    Simple helper to launch the Flask app.
    Can be called from CLI.
    """
    app = create_app(kwargs.get("config"))
    app.run(host=host, port=port, debug=debug)
