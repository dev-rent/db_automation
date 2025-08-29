import re
import unicodedata
from typing import Iterable

from rapidfuzz import fuzz


def normalise_string(string: str, *, digits=False) -> str:
    """Return cleaned string for comparison.
    Param:
        - digits, set True if digits need to remain.
    """
    string = string.lower()
    string = unicodedata.normalize("NFKD", string)
    string = "".join(c for c in string if not unicodedata.combining(c))
    string = string.replace("ß", "ss")

    if digits:
        string = re.sub(r"[^a-z0-9]", "", string)
    else:
        string = re.sub(r"[^a-z]", "", string)
    return string


def fuzzy_equal(a: str, b: str, threshold: int = 90) -> bool:
    score = fuzz.ratio(a, b)
    return score >= threshold


def fuzzy_keys(key: tuple, keys: Iterable) -> tuple:
    """
    Fuzzy match a key with existings keys and return matching key if exists.
    """
    length = len(key)

    for k in keys:
        for i in range(length):
            threshold = 90 if len(key[i]) > 4 else 80
            if not fuzzy_equal(key[i], k[i], threshold):
                break
        else:
            return True, k
    return False, None
