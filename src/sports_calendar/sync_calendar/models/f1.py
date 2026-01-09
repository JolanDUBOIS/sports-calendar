import pandas as pd

from .base import BaseTable


class F1EventsTable(BaseTable):
    """ Table for storing F1 event data. """
    __file_name__ = "events.csv"
    __columns__ = {
        "id": {"type": "int", "source": "session_id"},
        "name": {"type": "str", "source": "short_name"},
        "session": {"type": "str", "source": "session_type"},
        "date_time": {"type": "str", "source": "session_date"},
        "city": {"type": "str", "source": "circuit_city"},
        "country": {"type": "str", "source": "circuit_country"},
    }
    __sport__ = "f1"

    @classmethod
    def query(
        cls,
        ids: list[int] | None = None,
        date_from: str | None = None,
        date_to: str | None = None
    ) -> pd.DataFrame:
        """ Query F1 events by IDs and optional date range. """
        df = cls.get_table()
        if ids is not None:
            df = df[df['id'].isin(ids)]
        if date_from is not None:
            df = df[df['date_time'] >= date_from]
        if date_to is not None:
            df = df[df['date_time'] <= date_to]
        return df
