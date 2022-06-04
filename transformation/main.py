from argparse import ArgumentParser, Namespace
from typing import Dict

from config import (
    MIN_KOMODITAS_COUNT_QUANTILE,
    MIN_KOMODITAS_MATCHING_RATIO,
    PIPELINE_ARGS,
)
from functions import (
    get_komoditas_mapping,
    get_total_berat_komoditas,
    show_total_berat_komoditas,
)
from pandas import DataFrame, read_json
from pipeline import apply_pipeline


def main(args: Namespace):
    dataframe: DataFrame = read_json(args.file_path)

    dataframe = apply_pipeline(dataframe, *PIPELINE_ARGS)

    komoditas_mapping = get_komoditas_mapping(
        dataframe, MIN_KOMODITAS_COUNT_QUANTILE, MIN_KOMODITAS_MATCHING_RATIO
    )

    dataframe["vector_komoditas"] = dataframe["vector_komoditas"].apply(
        lambda x: [komoditas_mapping.get(i) for i in x]
    )

    total_berat_komoditas: Dict[str, int] = get_total_berat_komoditas(dataframe)

    show_total_berat_komoditas(total_berat_komoditas)


if __name__ == "__main__":
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument("--file-path", help="File Path to the json data input")
    args: Namespace = parser.parse_args()
    main(args)
