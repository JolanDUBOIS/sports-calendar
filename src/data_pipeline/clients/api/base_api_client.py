import time
import traceback

import requests

from . import logger


class BaseApiClient:
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """

    def query_api(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
        max_retries: int = 3,
        delay: int = 15,
        request_interval: int = 0
    ) -> dict|None:
        """ TODO """
        retries = 0
        while retries < max_retries:
            try:
                time.sleep(request_interval)
                response = requests.get(url, params=params, headers=headers)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded (status code 429). Attempt {retries}/{max_retries}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    retries += 1
                else:
                    logger.error(f"Failed to fetch data from URL: {url}. Status code: {response.status_code}")
                    logger.debug(f"Response content: {response.content}")
                    return None

            except Exception as e:
                logger.error(f"Failed to fetch data from URL: {url}. Error: {e}")
                logger.debug(traceback.format_exc())
                break

        if retries >= max_retries:
            logger.error(f"Max retries reached for URL: {url}. Failed to fetch data.")

        return None

    @staticmethod
    def flatten_dict(d: dict, parent_key: str, prefix: str = '') -> dict:
        """ TODO """
        nested_item = d.get(parent_key, {})
        if not isinstance(nested_item, dict):
            logger.warning(f"Expected a dictionary for key '{parent_key}', but got {type(nested_item)}.")
            return d
        new_keys = [f"{prefix}{k}" for k in nested_item.keys()]
        for k in d.keys():
            if k in new_keys:
                logger.warning(f"Key '{k}' already exists in the dictionary. It will be overwritten.")
        return {**d, **{f"{prefix}{k}": v for k, v in nested_item.items()}}
