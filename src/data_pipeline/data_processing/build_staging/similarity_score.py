import re
import unicodedata

from rapidfuzz import fuzz # type: ignore

from src.data_pipeline.data_processing import logger


GENERIC_TEAM_TOKENS = [
    "FC", "Football Club", "Soccer Club", "SC", "Sporting Club",
    "AC", "Club", "Associazione Calcio", "SV", "Sportverein",
    "SS", "Società Sportiva", "CF", "Club de Fútbol", "SK", "Sportklub",
    "US", "Unione Sportiva", "Union Sportive", "CD", "Club Deportivo", "AFC",
    "Association Football Club", "AS", "Associazione Sportiva", "Association Sportive",
]

def strict_is_in(strA: str, strB: str) -> bool:
    """ TODO """
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
    """ TODO """
    try:
        string = unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode('utf-8')
        return string.replace("-", " ").replace(".", "").strip()
    except Exception as e:
        print(f"Error normalizing string '{string}': {e}")
        raise e

def normalize_list(strings: list[str]) -> list[str]:
    """ Normalize a list of strings. """
    return [normalize_string(string) for string in strings]

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
    logger.debug(f"List A variations: {listA}")
    logger.debug(f"List B variations: {listB}")
    for strA in listA:
        for strB in listB:
            score = calculate_string_similarity_score(strA, strB)
            scores.append(score)

    return max(scores) if scores else 0.0
