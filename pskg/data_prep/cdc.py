from datetime import timedelta
import pandas as pd
from . import s3_utils

_CDC_DATA_TYPES = {
    "Vaccine": str,
    "VAX_NAME": str,
    "Total_Administered": int,
    "Posted": str,
    "Updated_Raw": str,
    "URL": str,
    "ExposureId": str,
}

_PARSE_OPTIONS = {
    "parse_dates": ["Datetime"],
}


def derive_exposure_id(input_row):
    return "{0}|{1}".format(
        input_row["Vaccine"], input_row["Datetime"].strftime("%Y%m%d")
    )


def get_dtypes():
    return _CDC_DATA_TYPES


def get_date_parser():
    return _PARSE_OPTIONS["parse_dates"]


def derive_start_date(input_row):
    end_date = input_row["Date"]
    start_date = end_date - timedelta(days=1)

    return start_date


def standardize_vaccine_names(input_name):
    """
    Map CDC specific names to standardized values

    Parameters
    ----------
    input_name: str
        Original name

    Returns
    -------
    str
        Standardized name
    """
    if input_name.upper() == "PFIZER-BIONTECH":
        return "Pfizer"
    elif input_name.upper() == "PFIZER\BIONTECH":
        return "Pfizer"
    elif input_name.upper() == "PFIZER":
        return "Pfizer"
    elif input_name.upper() == "J&J/JANSSEN":
        return "Janssen"
    elif input_name.upper() == "JANSSEN":
        return "Janssen"
    elif input_name.upper() == "MODERNA":
        return "Moderna"
    else:
        return input_name


def raw_load(input_bucket=None, input_key=None, file_path=None):
    """
    Load and transform CDC exposure data from specified S3 bucket and key,
    or from a local file.

    Parameters
    ----------
    input_bucket: str, optional
        S3 Bucket reference object
    input_key: str, optional
        Key for CDC exposure data within input_bucket
    file_path: str, optional
        Path to local CDC exposure file, exclusive with input_bucket/input_key

    Returns
    dataframe
        Dataframe with CDC exposure data
    """
    if not file_path and not (input_bucket or input_key):
        raise ValueError(
            "cdc.raw_load(): Either input_bucket and input_key, or file_path must be specified."
        )

    if file_path:
        cdc_df = pd.read_csv(
            file_path,
            dtype=get_dtypes(),
            parse_dates=get_date_parser(),
        )
    else:
        cdc_df = s3_utils.csv_file_to_data_frame(
            bucket=input_bucket,
            key=input_key,
            dtypes=get_dtypes(),
            date_cols=get_date_parser(),
        )

    # Derive a new unique ID for the Vaccine records
    cdc_df["Vaccine"] = cdc_df["Vaccine"].apply(standardize_vaccine_names)
    cdc_df["ExposureId"] = cdc_df.apply(derive_exposure_id, axis=1)
    cdc_df["Date"] = cdc_df["Datetime"].dt.date

    return cdc_df
