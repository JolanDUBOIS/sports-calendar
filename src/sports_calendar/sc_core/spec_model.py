from __future__ import annotations
import json
from pathlib import Path
from typing import TypeVar, Any
from dataclasses import dataclass

import cattrs
from yaml import dump

from sports_calendar.sc_core import load_yml


T = TypeVar('T', bound='SpecModel')
_converter = cattrs.Converter()

@dataclass
class SpecModel:
    """ TODO """

    def __post_init__(self):
        self.validate()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.to_dict()})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.to_dict()})"

    def validate(self) -> None:
        """ Validate the model. """
        pass

    def to_dict(self, deep: bool = True) -> dict[str, Any]:
        """ TODO """
        if not deep:
            return self.__dict__.copy()
        return _converter.unstructure(self)

    def to_yaml(self, path: Path | str) -> None:
        """ Convert the model to a YAML file. """

        path = Path(path) if isinstance(path, str) else path
        data = self.to_dict()
        with path.open("w", encoding="utf-8") as file:
            dump(data, file, default_flow_style=False, allow_unicode=True)

    def to_json(self,path: Path | str) -> None:
        """ Convert the model to a JSON file. """
        path = Path(path) if isinstance(path, str) else path
        data = self.to_dict()
        with path.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """ Create an instance of the model from a dictionary. """
        return _converter.structure(data, cls)

    @classmethod
    def from_yaml(cls: type[T], path: Path | str) -> T:
        """ Create an instance of the model from a YAML file. """
        path = Path(path) if isinstance(path, str) else path
        data = load_yml(path)
        return cls.from_dict(data)

    @classmethod
    def from_json(cls: type[T], path: Path | str) -> T:
        """ Create an instance of the model from a JSON file. """
        path = Path(path) if isinstance(path, str) else path
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return cls.from_dict(data)
