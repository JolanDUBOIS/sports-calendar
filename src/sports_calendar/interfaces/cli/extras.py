""" Helpers for commands that depend on an optional dependency extra.

The CLI itself lives in the shared base install, so subcommands import their
heavy dependencies lazily (inside the command body) and translate a missing
extra into an actionable message instead of an ImportError traceback.
"""

import typer


def missing_extra(extra: str, exc: ImportError) -> typer.Exit:
    """ Report a missing optional extra and return an Exit to raise. """
    typer.echo(
        f"Error: this command requires the '{extra}' extra, which is not installed.\n"
        f"  Missing dependency: {exc.name}\n"
        f"  Install it with:    uv sync --extra {extra}\n"
        f"  (or, with pip:      pip install 'sports-calendar[{extra}]')",
        err=True,
    )
    return typer.Exit(code=1)
