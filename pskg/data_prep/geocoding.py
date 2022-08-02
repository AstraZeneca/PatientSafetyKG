###
### Geocoding Support Functions
###

import pandas as pd
from . import s3_utils


def raw_load(input_bucket=None, input_key=None, file_path=None):
    """
    General utility function for loading fixed geocoding data, such as countries to continents

    Parameters
    ----------
    input_bucket: object
        S3 bucket containing country mapping data
    country_key: str
        Key for data file in input_bucket

    Returns
    -------
    dataframe
        A dataframe with
    """

    if not file_path and not (input_bucket or input_key):
        raise ValueError(
            "geocoding.raw_load(): Either a bucket and key, or file_path is required."
        )

    if file_path:
        df = pd.read_csv(
            file_path,
            encoding="utf-8",
            parse_dates=None,
            dtype=str,
            keep_default_na=False,
        )
    else:
        df = s3_utils.csv_file_to_data_frame(
            bucket=input_bucket,
            key=input_key,
            encoding="utf-8",
            date_cols=None,
            dtypes=str,
            keep_na=False,
        )

    return df
