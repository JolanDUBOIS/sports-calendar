from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from .processors import (
    ExtractJson,
    ReshapeMatches,
    DateNormalization,
    Parsing,
    ExtractTable,
    CreateRegistry
)


SourceType = dict | list[dict] | pd.DataFrame
SourceBundleType = dict[str, SourceType]

class PipelineBaseClass(ABC):
    """ TODO """

    @classmethod
    @abstractmethod
    def run(cls, sources: SourceBundleType, **kwargs) -> pd.DataFrame:
        """ TODO """
        pass

    @staticmethod
    def _check_json_data(json_data: list[dict]) -> None:
        """ TODO """
        if not isinstance(json_data, list) and all(isinstance(i, dict) for i in json_data):
            # Maybe too computationally expensive to go through all the elements already
            logger.error("The source should be a list of dictionaries.")
            raise ValueError("The source should be a list of dictionaries.")

    @staticmethod
    def _check_dataframe(data: pd.DataFrame) -> None:
        """ TODO """
        if not isinstance(data, pd.DataFrame):
            logger.error("The source should be a pandas DataFrame.")
            raise ValueError("The source should be a pandas DataFrame.")


class ESPNMatchPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "espn_matches"

    @classmethod
    def run(cls, sources: dict[str, list[dict]], columns_mapping: dict[str, str], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running ESPNMatchPipeline")
        json_data = sources.get(cls.source_key)
        if json_data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if not json_data:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_json_data(json_data)
        
        data = ExtractJson().process(json_data, columns_mapping, **kwargs)
        cls._check_dataframe(data)

        data = ReshapeMatches().process(data, cls.source_key, **kwargs)
        cls._check_dataframe(data)

        data = DateNormalization().process(data, cls.source_key, **kwargs)
        return data


class FootballDataMatchesPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "football_data_matches"

    @classmethod
    def run(cls, sources: dict[str, list[dict]], columns_mapping: dict[str, str], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running FootballDataMatchesPipeline")
        json_data = sources.get(cls.source_key)
        if json_data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if not json_data:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_json_data(json_data)
        
        data = ExtractJson().process(json_data, columns_mapping, **kwargs)
        cls._check_dataframe(data)

        data = DateNormalization().process(data, cls.source_key, **kwargs)
        return data


class FootballDataStandingsPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "football_data_standings"

    @classmethod
    def run(cls, sources: dict[str, list[dict]], columns_mapping: dict[str, str], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running FootballDataStandingsPipeline")
        json_data = sources.get(cls.source_key)
        if json_data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if not json_data:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_json_data(json_data)
        
        data = ExtractJson().process(json_data, columns_mapping, **kwargs)
        return data


class FootballDataTeamsPipeline(PipelineBaseClass): # TODO - Same as the previous one
    """ TODO """
    
    source_key = "football_data_teams"

    @classmethod
    def run(cls, sources: dict[str, list[dict]], columns_mapping: dict[str, str], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running FootballDataTeamsPipeline")
        json_data = sources.get(cls.source_key)
        if json_data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if not json_data:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_json_data(json_data)
        
        data = ExtractJson().process(json_data, columns_mapping, **kwargs)
        return data


class FootballRankingFifaRankingsPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "football_ranking_fifa_rankings"

    @classmethod
    def run(cls, sources: dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running FootballRankingFifaRankingsPipeline") # Change log
        data = sources.get(cls.source_key)
        if data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_dataframe(data)
        
        data = Parsing().process(data, cls.source_key, **kwargs)
        return data


class LiveSoccerMatchesPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "live_soccer_matches"

    @classmethod
    def run(cls, sources: dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running LiveSoccerMatchesPipeline")
        data = sources.get(cls.source_key)
        if data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_dataframe(data)
        
        data = Parsing().process(data, cls.source_key, **kwargs)
        cls._check_dataframe(data)
        
        data = DateNormalization().process(data, cls.source_key, **kwargs)
        return data


class LiveSoccerStandingsPipeline(PipelineBaseClass):
    """ TODO """
    
    source_key = "live_soccer_standings"

    @classmethod
    def run(cls, sources: dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info("Running LiveSoccerStandingsPipeline")
        data = sources.get(cls.source_key)
        if data is None:
            logger.error(f"No data found for source key: {cls.source_key}")
            raise ValueError(f"No data found for source key: {cls.source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {cls.source_key}")
            return pd.DataFrame()
        cls._check_dataframe(data)
        
        data = Parsing().process(data, cls.source_key, **kwargs)
        return data


class ExtractTablePipeline(PipelineBaseClass):
    """ TODO """

    @classmethod
    def run(cls, sources: dict[str, pd.DataFrame], source_key: str, output_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Running ExtractTeamsPipeline for {output_key}")
        data = sources.get(source_key)
        if data is None:
            logger.error(f"No data found for source key: {source_key}")
            raise ValueError(f"No data found for source key: {source_key}")
        if data.empty:
            logger.info(f"No new data found for source key: {source_key}")
            return pd.DataFrame()
        cls._check_dataframe(data)

        data = ExtractTable().process(data, output_key, **kwargs)
        return data


class CreateRegistryPipeline(PipelineBaseClass):
    """ TODO """

    @classmethod
    def run(cls, sources: dict[str, pd.DataFrame], output_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Running CreateRegistryPipeline for {output_key}")
        data = CreateRegistry().process(sources, output_key, **kwargs)
        return data
