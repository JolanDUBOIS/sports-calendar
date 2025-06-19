import re
import unicodedata

from rapidfuzz import fuzz # type: ignore

from . import logger


def strict_is_in(strA: str, strB: str) -> bool:
    """ Check if one string is contained as a whole word within another, case-insensitive. """
    if not isinstance(strA, str) or not isinstance(strB, str):
        logger.debug(f"strA: {strA}, strB: {strB}")
        logger.debug(f"strA type: {type(strA)}, strB type: {type(strB)}")
        logger.error("The function strict_is_in only accepts strings.")
        raise TypeError("The function strict_is_in only accepts strings.")

    strA = strA.strip().lower()
    strB = strB.strip().lower()

    patternA = r'\b' + re.escape(strA) + r'\b'
    patternB = r'\b' + re.escape(strB) + r'\b'

    return bool(re.search(patternA, strB)) or bool(re.search(patternB, strA))

def remove_substring(string: str, substring: str) -> str:
    """ Remove a substring from a string. """
    pattern = r'\b' + re.escape(substring) + r'\b'
    return re.sub(pattern, '', string).strip()

def remove_substrings(strings: list[str], substrings: list[str]) -> list[str]:
    """ Remove multiple substrings from a list of strings. """
    if not substrings:
        return strings
    for substring in substrings:
        strings = [remove_substring(string, substring) for string in strings]
    return strings

def delete_duplicates(list_: list) -> list:
    """ Remove duplicates from a list. """
    return list(dict.fromkeys(list_))

def normalize_string(string: str) -> str:
    """ Normalize a string by removing accents, converting to ASCII, replacing hyphens with spaces, and removing dots. """
    try:
        string = unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode('utf-8')
        return string.replace("-", " ").replace(".", "").strip()
    except Exception as e:
        print(f"Error normalizing string '{string}': {e}")
        raise e

def normalize_list(strings: list[str]) -> list[str]:
    """ Normalize a list of strings. """
    return [normalize_string(str(string)) for string in strings]

def calculate_string_similarity_score(strA: str, strB: str) -> float:
    """ Calculate the similarity score between two strings. """
    if strA == strB:
        return 100
    score = 90 if strict_is_in(strA, strB) else 0
    score = max(score, fuzz.ratio(strA, strB))
    return score

def calculate_list_similarity_score(listA: list[str], listB: list[str], tokens_to_remove: list[str] = None) -> float:
    """ Calculate the similarity score between two lists of strings. """
    scores = []
    listA = delete_duplicates(remove_substrings(normalize_list(listA), tokens_to_remove))
    listB = delete_duplicates(remove_substrings(normalize_list(listB), tokens_to_remove))
    for strA in listA:
        if not isinstance(strA, str):
            continue
        for strB in listB:
            if not isinstance(strB, str):
                continue
            score = calculate_string_similarity_score(strA, strB)
            scores.append(score)
    return max(scores) if scores else 0.0
