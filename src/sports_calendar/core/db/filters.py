from __future__ import annotations
from typing import Any

import pandas as pd

from . import logger


class Filter:
    """ Represents a single column filter. """
    def __init__(self, col: str, op: str, value: Any):
        self.col = col
        self.op = op
        self.value = value

    def apply(self, df: pd.DataFrame) -> pd.Series:
        logger.debug(f"Applying filter on column {self.col} with operation {self.op} and value {self.value}")
        if self.col not in df.columns:
            return pd.Series([True] * len(df))  # ignore missing columns
        series = df[self.col]
        if self.op == "==":
            return series == self.value
        elif self.op == ">=":
            return series >= self.value
        elif self.op == "<=":
            return series <= self.value
        elif self.op == "in":
            return series.isin(self.value)
        elif self.op == "contains":
            return series.str.contains(self.value, na=False)
        else:
            raise ValueError(f"Unsupported operator: {self.op}")

    # Allow combining filters with AND (&) and OR (|)
    def __and__(self, other: Filter) -> CombinedFilter:
        return CombinedFilter(self, other, op="and")

    def __or__(self, other: Filter) -> CombinedFilter:
        return CombinedFilter(self, other, op="or")


class CombinedFilter:
    """ Represents a combination of two filters with AND/OR. """
    def __init__(self, left: Filter | CombinedFilter, right: Filter | CombinedFilter, op: str):
        self.left = left
        self.right = right
        self.op = op

    def apply(self, df: pd.DataFrame) -> pd.Series:
        logger.debug(f"Applying combined filter with operation {self.op}")
        left_mask = self.left.apply(df)
        right_mask = self.right.apply(df)
        if self.op == "and":
            return left_mask & right_mask
        elif self.op == "or":
            return left_mask | right_mask
        else:
            raise ValueError(f"Unsupported combine operator: {self.op}")
