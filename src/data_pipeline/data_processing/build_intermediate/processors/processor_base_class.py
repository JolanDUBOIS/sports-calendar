from abc import ABC, abstractmethod

import pandas as pd


class Processor(ABC):
    """ TODO """

    def __init__(self):
        """ TODO """
        pass

    @abstractmethod
    def process(self, sources: dict[str, dict|pd.DataFrame], **kwargs) -> pd.DataFrame:
        """ TODO """
        pass
