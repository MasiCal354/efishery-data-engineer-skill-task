from collections import Counter
from copy import deepcopy
from difflib import SequenceMatcher
from string import punctuation
from typing import Dict

from pandas import DataFrame, Series


def replace_punctuation(s: str) -> str:
    for p in punctuation:
        s = s.replace(p, " ")
        while "  " in s:
            s = s.replace("  ", " ")
    s = s.strip()
    return s


def replace_stopwords(s: str) -> str:
    stop_words = ["dan", "ikan", "ikn"]
    for sw in stop_words:
        s = s.replace(f"{sw} ", " ")
        s = s.replace(f" {sw}", " ")
        while "  " in s:
            s = s.replace("  ", " ")
    s = s.strip()
    return s


def numeric_only(s: str) -> str:
    s = "".join([c for c in s if c.isdigit() or c == " "])
    while "  " in s:
        s = s.replace("  ", " ")
    s = s.strip()
    return s


def get_komoditas_mapping(
    df: DataFrame,
    min_komoditas_count_quantile: float,
    min_komoditas_matching_ratio: float,
) -> Dict[str, str]:
    dict_komoditas = dict(Counter(df["vector_komoditas"].sum()))
    komoditas_mapping = {}

    series_komoditas = Series(dict_komoditas)
    quantile_komoditas = series_komoditas.quantile(
        min_komoditas_count_quantile
    )

    for i, ic in dict_komoditas.items():
        for j, jc in dict_komoditas.items():
            if (
                i != ""
                and (
                    i in j
                    or SequenceMatcher(None, i, j).ratio()
                    >= min_komoditas_matching_ratio
                )
                and ic >= quantile_komoditas
            ):
                if j in komoditas_mapping:
                    if ic > dict_komoditas[komoditas_mapping[j]]:
                        komoditas_mapping[j] = i
                else:
                    komoditas_mapping[j] = i
    final_komoditas_mapping = deepcopy(komoditas_mapping)
    for k, v in komoditas_mapping.items():
        final_komoditas_mapping[k] = komoditas_mapping[v]
    return final_komoditas_mapping


def get_total_berat_komoditas(df: DataFrame) -> Dict[str, int]:
    komoditas_berat = []
    for k, b in zip(df["vector_komoditas"], df["vector_berat"]):
        if len(k) == len(b):
            komoditas_berat.append(dict(zip(k, b)))
        elif len(b) > 0:
            komoditas_berat.append({i: b[0] for i in k})

    return (
        DataFrame(komoditas_berat).sum().sort_values(ascending=False).to_dict()
    )


def show_total_berat_komoditas(total_berat_komoditas: Dict[str, int]) -> None:
    i = 1
    for k, v in total_berat_komoditas.items():
        if k is None:
            continue
        print(f"{i}. {k}: {int(v)}kg")
        i += 1
