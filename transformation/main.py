from argparse import ArgumentParser, Namespace
from typing import Dict

from pandas import DataFrame, read_json

from pipeline import apply_pipeline
from config import MIN_KOMODITAS_COUNT_QUANTILE, MIN_KOMODITAS_MATCHING_RATIO, PIPELINE_ARGS
from functions import get_komoditas_mapping, get_total_berat_komoditas, show_total_berat_komoditas

parser: ArgumentParser = ArgumentParser()
parser.add_argument("--file-path", help="File Path to the json data input")
args: Namespace = parser.parse_args()

def main():
    df: DataFrame = read_json(args.file_path)

    df = apply_pipeline(df, *PIPELINE_ARGS)

    komoditas_mapping = get_komoditas_mapping(df, MIN_KOMODITAS_COUNT_QUANTILE, MIN_KOMODITAS_MATCHING_RATIO)

    df["vector_komoditas"] = df["vector_komoditas"].apply(
        lambda x: [komoditas_mapping.get(i) for i in x]
    )

    total_berat_komoditas: Dict[str, int] = get_total_berat_komoditas(df)

    show_total_berat_komoditas(total_berat_komoditas)

if __name__ == "__main__":
    main()