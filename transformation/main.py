from collections import Counter
from copy import deepcopy
from difflib import SequenceMatcher
from string import punctuation
from pathlib import Path
import pandas as pd

MIN_KOMODITAS_COUNT_QUANTILE = 0.75
MIN_KOMODITAS_MATCHING_RATIO = 0.75

df = pd.read_json(Path(__file__).parent.resolve() / "raw.json")

def replace_punctuation(s: str):
    for p in punctuation:
        s = s.replace(p, " ")
        while "  " in s:
            s = s.replace("  ", " ")
    s = s.strip()
    return s

def replace_stopwords(s: str):
    stop_words = ["dan", "ikan", "ikn"]
    for sw in stop_words:
        s = s.replace(f"{sw} ", " ")
        s = s.replace(f" {sw}", " ")
        while "  " in s:
            s = s.replace("  ", " ")
        s = s.strip()
    return s

def numeric_only(s: str):
    s = "".join([c for c in s if c.isdigit() or c == " "])
    while "  " in s:
        s = s.replace("  ", " ")
    s = s.strip()
    return s

df["clean_komoditas"] = df["komoditas"].apply(replace_punctuation)
df["clean_berat"] = df["berat"].apply(replace_punctuation)
df["clean_komoditas"] = df["clean_komoditas"].apply(replace_stopwords)
df["clean_berat"] = df["clean_berat"].apply(replace_stopwords)

df["vector_komoditas"] = df["clean_komoditas"].apply(lambda x: x.strip().split(" "))
df["vector_berat"] = df["clean_berat"].apply(lambda x: x.strip().split(" "))
df["vector_berat"] = df["vector_berat"].apply(lambda x: [i for i in x if i != "" and i[0].isdigit()])
df["vector_berat"] = df["vector_berat"].apply(lambda x: [int(numeric_only(i)) for i in x])

dict_komoditas = dict(Counter(df["vector_komoditas"].sum()))
komoditas_mapping = {}

series_komoditas = pd.Series(dict_komoditas)
quantile_komoditas = series_komoditas.quantile(MIN_KOMODITAS_COUNT_QUANTILE)

for i, ic in dict_komoditas.items():
    for j, jc in dict_komoditas.items():
        if i != "" and (i in j or SequenceMatcher(None, i, j).ratio() >= MIN_KOMODITAS_MATCHING_RATIO) and ic >= quantile_komoditas:
            if j in komoditas_mapping:
                if ic > dict_komoditas[komoditas_mapping[j]]:
                    komoditas_mapping[j] = i
            else:
                komoditas_mapping[j] = i
komoditas_mapping
final_komoditas_mapping = deepcopy(komoditas_mapping)
for k, v in komoditas_mapping.items():
    final_komoditas_mapping[k] = komoditas_mapping[v]

df["vector_komoditas"] = df["vector_komoditas"].apply(lambda x: [final_komoditas_mapping.get(i) for i in x])

komoditas_berat = []
for k, b in zip(df["vector_komoditas"], df["vector_berat"]):
    if len(k) == len(b):
        komoditas_berat.append(dict(zip(k, b)))
    elif len(b) > 0:
        komoditas_berat.append({i: b[0] for i in k})

agg = pd.DataFrame(komoditas_berat).sum().sort_values(ascending=False).to_dict()
i = 1
for k, v in agg.items():
    if k is None:
        continue
    print(f"{i}. {k}: {int(v)}kg")
    i += 1
