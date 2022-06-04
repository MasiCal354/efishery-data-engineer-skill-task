from typing import Dict, List
from functions import replace_punctuation, replace_stopwords, numeric_only


MIN_KOMODITAS_COUNT_QUANTILE = 0.75
MIN_KOMODITAS_MATCHING_RATIO = 0.75

PIPELINE_ARGS: List[Dict] = [
    {
        "input_columns": "komoditas",
        "function": replace_punctuation,
        "output_columns": "clean_komoditas"
    },
    {
        "input_columns": "berat",
        "function": replace_punctuation,
        "output_columns": "clean_berat"
    },
    {
        "input_columns": "clean_komoditas",
        "function": replace_stopwords,
        "output_columns": "clean_komoditas"
    },
    {
        "input_columns": "clean_berat",
        "function": replace_stopwords,
        "output_columns": "clean_berat"
    },
    {
        "input_columns": "clean_komoditas",
        "function": lambda x: x.strip().split(" "),
        "output_columns": "vector_komoditas"
    },
    {
        "input_columns": "clean_berat",
        "function": lambda x: x.strip().split(" "),
        "output_columns": "vector_berat"
    },
    {
        "input_columns": "vector_berat",
        "function": lambda x: [i for i in x if i != "" and i[0].isdigit()],
        "output_columns": "vector_berat"
    },
    {
        "input_columns": "vector_berat",
        "function": lambda x: [int(numeric_only(i)) for i in x],
        "output_columns": "vector_berat"
    },
    
]