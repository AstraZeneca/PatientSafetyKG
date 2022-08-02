###
###
###

import re
from zipfile import ZipFile
from datetime import datetime as dt
import logging
import io
from pathlib import Path

import pandas as pd

from . import s3_utils
from .outcomes import OutcomeMapper

logger = logging.getLogger("pskg_loader.vaers")


def _vaers_yesno_as_bool(value, true=["Y"], false=["N"]):
    if (not value) or (value in false):
        return False
    elif value in true:
        return True
    else:
        return None


_VAERS_TRADE_NAMES = {
    "COVID19 (COVID19 (PFIZER-BIONTECH))": "Comirnaty",
    "COVID19 (COVID19 (MODERNA))": "Spikevax",
    "COVID19 (COVID19 (JANSSEN))": "Janssen",
    "COVID19 (COVID19 (UNKNOWN))": "Unknown",
}

_VAERS_GENERIC_NAMES = {
    "COVID19 (COVID19 (PFIZER-BIONTECH))": "Tozinameran",
    "COVID19 (COVID19 (MODERNA))": "Moderna",
    "COVID19 (COVID19 (JANSSEN))": "Janssen",
    "COVID19 (COVID19 (UNKNOWN))": "Unknown",
}

_VAERS_RXCUI = {
    "COVID19 (COVID19 (PFIZER-BIONTECH))": 2468235,
    "COVID19 (COVID19 (MODERNA))": 2470234,
    "COVID19 (COVID19 (JANSSEN))": 2479835,
}

_VAERS_DATA_TYPES = {
    "VAERSDATA": {
        "VAERS_ID": int,  # 0
        "STATE": str,  # 2
        "AGE_YRS": float,  # 3
        "CAGE_YR": float,  # 4
        "CAGE_MO": float,  # 5
        "SEX": str,  # 6
        "SYMPTOM_TEXT": str,  # 7
        "SPLTTYPE": str,  # 8
        "HOSPDAYS": float,  # 13
        "NUMDAYS": float,  # 19
        "LAB_DATA": str,  # 20
        "V_ADMINBY": str,  # 21
        "V_FUNDBY": str,  # 22
        "OTHER_MEDS": str,  # 23
        "CUR_ILL": str,  # 24
        "HISTORY": str,  # 25
        "PRIOR_VAX": str,  # 26
        "SPLTTYPE": str,  # 27
        "FORM_VERS": float,  # 28
        "ALLERGIES": str,  # 32
    },
    "VAERSVAX": {
        "VAERS_ID": int,
        "VAX_TYPE": str,
        "VAX_MANU": str,
        "VAX_LOT": str,
        "VAX_DOSE_SERIES": str,
        "VAX_ROUTE": str,
        "VAX_SITE": str,
        "VAX_NAME": str,
    },
    "VAERSSYMPTOMS": {
        "VAERS_ID": int,
        "SYMPTOM1": str,
        "SYMPTOMVERSION1": float,
        "SYMPTOM2": str,
        "SYMPTOMVERSION2": float,
        "SYMPTOM3": str,
        "SYMPTOMVERSION3": float,
        "SYMPTOM4": str,
        "SYMPTOMVERSION4": float,
        "SYMPTOM5": str,
        "SYMPTOMVERSION5": float,
    },
}

_PARSE_OPTIONS = {
    "VAERSDATA": {
        "converters": {
            "DIED": _vaers_yesno_as_bool,
            "DISABLE": _vaers_yesno_as_bool,
            "L_THREAT": _vaers_yesno_as_bool,
            "ER_VISIT": _vaers_yesno_as_bool,
            "HOSPITAL": _vaers_yesno_as_bool,
            "X_STAY": _vaers_yesno_as_bool,
            "RECOVD": _vaers_yesno_as_bool,
            "BIRTH_DEFECT": _vaers_yesno_as_bool,
            "OFC_VISIT": _vaers_yesno_as_bool,
            "ER_ED_VISIT": _vaers_yesno_as_bool,
        },
        "parse_dates": [
            "RECVDATE",
            "RPT_DATE",
            "DATEDIED",
            "VAX_DATE",
            "ONSET_DATE",
            "TODAYS_DATE",
        ],
    },
    "VAERSVAX": {"converters": None, "parse_dates": None},
    "VAERSSYMPTOMS": {"converters": None, "parse_dates": None},
}

_OUTCOME_MAPPING = pd.DataFrame.from_records(
    (
        ("OFC_VISIT", "doctor visit"),
        ("ER_VISIT", "er visit"),
        ("ER_ED_VISIT", "er visit"),
        ("HOSPITAL", "hospitalization"),
        ("X_STAY", "prolonged hospitalization"),
        ("L_THREAT", "life threatening"),
        ("DIED", "death"),
        ("BIRTH_DEFECT", "birth defect"),
        ("DISABLE", "disabled"),
        ("RECOVD", "recovered"),
    ),
    columns=["dataset_outcomes", "standard_outcomes"],
)
OutcomeMapper.register_outcome_mapping("VAERS", _OUTCOME_MAPPING)

# Regex for identifying component zip files within an AllVAERSDATACSVS.zip file
_vaers_combined_zip_file = re.compile(
    r"(?P<vfile>(?P<tag>\d{4,4}|NonDomestic)(?P<file_type>[A-Z]+)\.csv)"
)

# Regex for identifying VAERS zip files in folder
_vaers_multiple_zip_file = re.compile(
    r"(?P<vfile>(?P<tag>\d{4,4}|NonDomestic)(?P<file_type>VAERSData)\.zip)"
)

# Regex for identifying component files in a zip archive
_vaers_multiple_internal_file = re.compile(
    r"(?P<vfile>(?P<tag>\d{4,4}|NonDomestic)(?P<file_type>[A-Z]+)\.csv)"
)


def derive_case_id(input_row):
    return "{0}:{1}".format("VAERS", input_row["VAERS_ID"])


def derive_vax_id(input_row):
    return "{0}|{1}|{2}".format(
        input_row["VAX_MANU"], input_row["VAX_TYPE"], input_row["VAX_NAME"]
    )


def get_dtypes(file_name):
    return _VAERS_DATA_TYPES[file_name]


def get_date_parser(file_name):
    return _PARSE_OPTIONS[file_name]["parse_dates"]


def get_converters(file_name):
    return _PARSE_OPTIONS[file_name]["converters"]


def standardize_manufacturer_names(input_name):
    """
    Simple function to address various synonyms used in data sources
    and resolve to a simple canonical name.

    Parameters
    ----------
    input_name: str
        Raw vaccine name

    Returns
    str
        Standardized name if available, otherwise return input_name unaltered
    """
    if input_name.upper() == "PFIZER-BIONTECH":
        return "Pfizer"
    if input_name.upper() == "PFIZER\BIONTECH":
        return "Pfizer"
    if input_name.upper() == "PFIZER":
        return "Pfizer"
    elif input_name.upper() == "J&J/JANSSEN":
        return "Janssen"
    elif input_name.upper() == "JANSSEN":
        return "Janssen"
    if input_name.upper() == "MODERNA":
        return "Moderna"
    else:
        return input_name


def get_trade_name(input_name):
    """
    Provide optional mappings from VAERS style VAX_NAMEs to
    trade names (brand names).

    Parameters
    ----------
    input_name: str
        Vaccine name (VAX_NAME)

    Returns
    -------
    str
        Standardized tradename, or original name if no mapping exists
    """
    return _VAERS_TRADE_NAMES.get(input_name) or input_name


def get_generic_name(input_name):
    """
    Provide optional mappings from VAERS style VAX_NAMEs to
    generic names.

    Parameters
    ----------
    input_name: str
        Vaccine name (VAX_NAME)

    Returns
    -------
    str
        Standardized generic name, or original name if no mapping exists
    """
    return _VAERS_GENERIC_NAMES.get(input_name) or input_name


def get_rxcui(input_name):
    """
    Provide optional mappings from VAERS style VAX_NAMEs to
    RxCUIs.

    Parameters
    ----------
    input_name: str
        Vaccine name (VAX_NAME)

    Returns
    -------
    str
        RxCUI or None if no mapping exists.
    """
    return _VAERS_RXCUI.get(input_name)


def get_components_from_combined(input_bucket=None, input_key=None, file_path=None):
    """
    VAERS data are published as multiple zip files (individual case), or in a single combined
    zip file containing all years.  This function opens the combined zip file, and identifies
    all available files inside of it and returns them as a dataframe:

        | key | file_path | internal_file_name | tag | modified |


    Parameters
    ----------
    input_bucket: str
        Bucket for source file
    input_key: str
        Key within specified bucket
    file_path: str
        Path to local zip file (exclusive with input_bucket/input_key)

    Returns
    -------
    pd.Dataframe
        Returns a dataframe with information for all data contained with the zip
    """
    result = {
        "key": [],
        "file_path": [],
        "internal_file_name": [],
        "tag": [],
        "modified": [],
    }
    if file_path:
        if isinstance(file_path, str):
            file_path = Path(file_path)
        with ZipFile(file_path) as zf:
            for zi in zf.infolist():
                m = _vaers_combined_zip_file.match(zi.filename)
                if m is not None:
                    result["key"].append(None)
                    result["file_path"].append(file_path.name)
                    result["internal_file_name"].append(zi.filename)
                    result["tag"].append(m.group("tag"))
                    result["modified"].append(dt(*zi.date_time))
                else:
                    logger.warn(
                        f"WARNING: Unrecognized file in VAERS archive: {zi.filename} ignored."
                    )
    elif input_bucket and input_key:
        # S3 case
        zip_object = None
        try:
            zip_object = s3_utils.get_object(input_bucket, input_key)
        except Exception:
            logger.error(
                f"Failure to read zip data from bucket: {input_bucket} and key: {input_key}"
            )
        with io.BytesIO(zip_object["Body"].read()) as zipf:
            # rewind the file
            zipf.seek(0)
            # Read the file as a zipfile and process the members
            with ZipFile(zipf, mode="r") as zf:
                for zi in zf.infolist():
                    m = _vaers_combined_zip_file.match(zi.filename)
                    if m is not None:
                        result["key"].append(input_key)
                        result["file_path"].append(None)
                        result["internal_file_name"].append(zi.filename)
                        result["tag"].append(m.group("tag"))
                        result["modified"].append(dt(*zi.date_time))
                    else:
                        logger.warn(
                            f"WARNING: Unrecognized file in VAERS archive: {zi.filename} ignored."
                        )
    else:
        raise ValueError(
            "Either file_path or (input_bucket and input_key) are required."
        )

    return pd.DataFrame.from_dict(result)


def get_components_from_individual(input_bucket=None, input_key=None, folder_path=None):
    """
    VAERS data are published as multiple zip files (individual case), or in a single combined
    zip file containing all years.  This function opens the folder or key containing multiple
    individual zip files, and identifies all available data tags inside of it.

        | key | file_path | internal_file_name | tag | modified |

    input_bucket: str
        Bucket containing zip file
    input_key: str
        Key within bucket where VAERS zip files are located
    folder_path: str
        Path to local folder containing VAERS zip files

    Returns
    -------
    pd.Dataframe
        Returns a dataframe with information for all data contained with the zip
    """
    result = {
        "key": [],
        "file_path": [],
        "internal_file_name": [],
        "tag": [],
        "modified": [],
    }
    if folder_path:
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)
        for file_path in folder_path.glob("*.zip"):
            m = _vaers_multiple_zip_file.match(file_path.name)
            if m is not None:
                with ZipFile(file_path, mode="r") as zf:
                    for zi in zf.infolist():
                        m = _vaers_multiple_internal_file.match(zi.filename)
                        if m is not None:
                            result["key"].append(None)
                            result["file_path"].append(file_path)
                            result["internal_file_name"].append(zi.filename)
                            result["tag"].append(m.group("tag"))
                            result["modified"].append(dt(*zi.date_time))
                        else:
                            logger.warn(
                                f"Unknown file type in VAERS archive: {zi.filename} ignored."
                            )
            else:
                logger.warn(
                    f"Unknown file type in VAERS folder: {file_path.name} ignored."
                )

    else:
        vaers_bucket = s3_utils.get_bucket(input_bucket)
        for obj in vaers_bucket.objects.filter(Prefix=input_key + "/"):
            file_only = obj.key[len(input_key)+1:]
            if not file_only:
                continue
            m = _vaers_multiple_zip_file.match(file_only)
            if m is not None:
                zip_object = s3_utils.get_object(input_bucket, obj.key)
                with io.BytesIO(zip_object["Body"].read()) as zipf:
                    # rewind the file
                    zipf.seek(0)
                    with ZipFile(zipf, mode="r") as zf:
                        for zi in zf.infolist():
                            m = _vaers_multiple_internal_file.match(zi.filename)
                            if m is not None:
                                result["key"].append(obj.key)
                                result["file_path"].append(None)
                                result["internal_file_name"].append(zi.filename)
                                result["tag"].append(m.group("tag"))
                                result["modified"].append(dt(*zi.date_time))
                            else:
                                logger.warn(
                                    f"Unknown file type in VAERS archive: {zi.filename} ignored."
                                )
            else:
                logger.warn(f"Unknown file type in {input_key} key: {file_only} ignored.")
                exit(0)

    return pd.DataFrame.from_dict(result)


def raw_load(
    internal_file_name,
    file_type,
    input_bucket=None,
    input_key=None,
    file_path=None,
):
    """
    Load VAERS file type from provided zip file

    Parameters
    ----------
    input_bucket: str
        Bucket for source file
    input_key: str
        Key within specified bucket
    file_path: str
        Path to local zip file (exclusive with input_bucket/input_key)
    internal_file_name: str
        Name of file within enclosing zip archive
    file_type: str
        VAERS File Type, e.g. "VAERSDATA"

    Returns
    pd.DataFrame
        Dataframe containing raw VAERS data, with dates parsed and basic conversions applied
    """

    if file_path:
        with ZipFile(file_path) as zf:
            df = pd.read_csv(
                zf.open(internal_file_name),
                encoding="ISO-8859-1",
                on_bad_lines="error",
                dtype=get_dtypes(file_type),
                parse_dates=get_date_parser(file_type),
                converters=get_converters(file_type),
            )
    else:
        df = s3_utils.zip_file_to_data_frame(
            bucket_name=input_bucket,
            zip_file_key=input_key,
            internal_file_name=internal_file_name,
            dtypes=get_dtypes(file_type),
            date_parser=get_date_parser(file_type),
            convert_funcs=get_converters(file_type),
        )

    return df


def raw_load_types(input_bucket, input_key, file_path):
    """
    Load vaccine type information--additional descriptive information from VAERS

    Parameters
    ----------
    input_bucket: str, optional
        S3 bucket containing country mapping data
    input_key: str, optional
        Key for data file in input_bucket
    file_path: str
        Path to local VAERS types file, exclusive with input_bucket/input_key


    Returns
    -------
    dataframe
        A dataframe with
    """

    if not file_path and not (input_bucket or input_key):
        raise ValueError(
            "vaers.raw_load_types(): Either a bucket and key, or file_path is required."
        )

    if file_path:
        df = pd.read_csv(
            file_path, encoding="utf-8", date_cols=None, dtypes=str, keep_na=False
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
