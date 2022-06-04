from pandas import DataFrame


def apply_pipeline(dataframe: DataFrame, *args):
    """Function to apply list of functions to pandas
        dataframe using apply method sequentially.
    """
    for d in args:
        if len(d["input_columns"]) == 1 or isinstance(d["input_columns"], str):
            dataframe[d["output_columns"]] = dataframe[d["input_columns"]].apply(d["function"])
        else:
            dataframe[d["output_columns"]] = dataframe[d["input_columns"]].apply(d["function"], axis=1)
    return dataframe
