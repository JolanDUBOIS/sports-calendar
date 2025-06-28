from __future__ import annotations
from typing import TYPE_CHECKING, cast

from .container import Config


class _ConfigProxy:
    _instance: Config | None = None

    def set(self, instance: Config):
        self._instance = instance

    def is_set(self):
        return self._instance is not None

    def __getattr__(self, attr):
        if self._instance is None:
            raise RuntimeError("Config accessed before being set.")
        return getattr(self._instance, attr)

    def __getattribute__(self, name: str):
        if name == '__class__' and self._instance is not None:
            return type(self._instance)
        return super().__getattribute__(name)

config = _ConfigProxy()

# For static type checkers, cast the proxy to Config
config: Config = cast(Config, config)