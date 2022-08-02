from boto3 import exceptions
import pandas as pd
from pathlib import Path
import logging

from . import s3_utils

logger = logging.getLogger("pskg_loader.meddra")

_meddra_column_maps = {
    "llt": [
        "llt_code",
        "llt_name",
        "pt_code",
        "llt_whoarts_code",
        "llt_harts_code",
        "llt_costart_sym",
        "llt_icd9_code",
        "llt_icd9cm_code",
        "llt_ic10_code",
        "llt_currency",
        "llt_jart_code",
        "blank",
    ],
    "mdhier": [
        "pt_code",
        "hlt_code",
        "hlgt_code",
        "soc_code",
        "pt_name",
        "hlt_name",
        "hlgt_name",
        "soc_name",
        "soc_abbrev",
        "null_field",
        "pt_soc_code",
        "primary_soc_fg",
        "blank",
    ],
    "smq_content": [
        "smq_code",
        "term_code",
        "term_level",
        "term_scope",
        "term_category",
        "term_weight",
        "term_Status",
        "term_addition_version",
        "term_last_modified_version",
        "blank",
    ],
    "meddra_release": [
        "version",
        "language",
        "null_field1",
        "null_field2",
        "null_field3",
        "blank",
    ],
    "smq_list": [
        "smq_code",
        "smq_name",
        "smq_level",
        "smq_description",
        "smq_source",
        "smq_note",
        "MedDRA_version",
        "status",
        "smq_algorithm",
        "blank",
    ],
    "smq_content": [
        "smq_code",
        "term_code",
        "term_level",
        "term_scope",
        "term_category",
        "term_weight",
        "term_status",
        "term_addition_version",
        "term_last_modified_version",
        "blank",
    ],
}


def generate_meddra_id(data_row, meddra_type, meddra_code_column):
    return "{0}:{1}".format(meddra_type, data_row[meddra_code_column])


def read_raw(
    meddra_file_type,
    input_bucket=None,
    input_key=None,
    file_path=None,
):
    """
    Read in a raw MedDRA format file (asc) and return a data frame,
    from either an S3 bucket/key or local data path.  Use
    columns from the supplied medra_file_Type.

    Parameters
    ----------
    input_bucket: str, optional
        Name of S3 input bucket, defaults to None
    input_key: str, optional
        Key inside input_bucket, defaults to None
    file_path: Path or str, optional
        Path to meddra file

    Returns
    -------
    pd.Dataframe
        A dataframe with columns from provided type
    """
    if meddra_file_type not in _meddra_column_maps:
        msg = f"Unknown MedDRA file type: {meddra_file_type}, must be one of {_meddra_column_maps.keys()}"
        logger.error(msg)
        raise ValueError(msg)

    if file_path:
        if isinstance(file_path, str):
            file_path = Path(file_path)
        logger.info(f"Reading {file_path}")
        df = pd.read_csv(
            file_path, header=None, names=_meddra_column_maps[meddra_file_type], sep="$"
        )
    elif input_bucket and input_key:
        logger.info(f"Reading s3://{input_bucket}/{input_key}")
        try:
            df = s3_utils.asc_file_to_data_frame(
                input_bucket, input_key, names=_meddra_column_maps[meddra_file_type]
            )
        except Exception as x:
            logger.error(f"Failed to read: s3://{input_bucket}/{input_key} ({str(x)}")
            raise
    else:
        raise ValueError(
            "Either file_path or (input_bucket and input_key) must be supplied."
        )

    return df


def generate_smq_edge_id(data_row):
    if data_row["TermLevel"] == 0:
        # this is an SMQ Code, so no prefix
        return str(data_row["MeddraCode"])
    elif data_row["TermLevel"] == 4:
        # this is link to a PT (level 4)
        return "{0}:{1}".format("PT", data_row["MeddraCode"])
    elif data_row["TermLevel"] == 5:
        # this is a link to an LLT (level 5)
        return "{0}:{1}".format("LLT", data_row["MeddraCode"])


def generate_type_meddra_id(data_row, column_name, meddra_type):
    return "{0}:{1}".format(meddra_type, data_row[column_name])


def generate_meddra_term_df(data_frame, columns, meddra_type, meddra_version):
    df = data_frame[columns].copy()  # [MeddraCode, Name]

    df.insert(loc=1, column="MeddraType", value=meddra_type)

    df.columns = ["MeddraCode", "MeddraType", "Name"]
    meddra_ids = df.apply(generate_meddra_id, axis=1)

    df.insert(loc=0, column="MeddraId", value=meddra_ids)

    df.insert(loc=4, column="MeddraVersion", value=meddra_version)
    df.columns = ["MeddraId", "MeddraCode", "MeddraType", "Name", "MeddraVersion"]

    return df


def generate_meddra_link_df(data_frame, columns, from_type, to_type):
    df = data_frame[columns].copy()
    df.columns = ["MeddraCodeFrom", "MeddraCodeTo"]
    df["MeddraIdFrom"] = df.apply(
        generate_type_meddra_id, args=("MeddraCodeFrom", from_type), axis=1
    )
    df["MeddraIdTo"] = df.apply(
        generate_type_meddra_id, args=("MeddraCodeTo", to_type), axis=1
    )

    df = df.drop(columns=["MeddraCodeFrom", "MeddraCodeTo"])

    df.columns = ["MeddraIdFrom", "MeddraIdTo"]
    return df


def generate_meddra_smq_df(data_frame):
    """
    Add columns headers to dataframe extracted from smq_content.asc

    Parameters
    ----------
    data_frame: dataframe
        dataframe containing columns from the smq_content.asc

    Returns
    -------
    df
        dataframe with column names set
    """
    published_columns = [
        "MeddraSmqCode",
        "Name",
        "MeddraSmqLevel",
        "MeddraSmqDescription",
        "MeddraSmqSource",
        "MeddraSmqNote",
        "MeddraSmqVersion",
        "MeddraSmqAlgorithm",
        "MeddraSmqStatus",
    ]
    # foolishness required because MedDRA puts a dollar sign at the end of every line...
    df = data_frame.loc[:, range(0, len(published_columns))].copy()
    df.columns = published_columns
    return df


def generate_meddra_smq_contains_df(data_frame):
    """
    Add column headers to dataframe extracted from smq_content.asc

    Parameters
    ----------
    data_frame: dataframe
        dataframe containing columns from the smq_content.asc

    Returns
    -------
    df
        dataframe with column names set
    """
    published_columns = [
        "MeddraSmqCode",
        "MeddraCode",
        "TermLevel",
        "Scope",
        "Category",
        "Weight",
        "Status",
        "AdditionVersion",
        "LastModifiedVersion",
    ]
    # foolishness required because MedDRA puts a dollar sign at the end of every line...
    df = data_frame.loc[:, range(0, len(published_columns))].copy()
    df.columns = published_columns
    return df
