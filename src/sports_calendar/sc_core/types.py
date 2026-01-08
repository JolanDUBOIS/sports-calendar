from typing import TypeAlias

import pandas as pd


IOContent: TypeAlias = list[dict] | pd.DataFrame | None
