import numpy as np
import pandas as pd
from . import s3_utils

_EXPOSURE_DATA_TYPES = {
    "DosesAdministered": "Int64",
    "SpotfireCountry": str,
    "InformationSource": str,
    "VaccineSource": str,
    "MONTHADMINISTERED": str,
    "Comments": str,
    "grouping": str,
    "dose_1_gender_male": "Int64",
    "dose_1_gender_female": "Int64",
    "dose_1_gender_undefined": "Int64",
    "dose_2_gender_male": "Int64",
    "dose_2_gender_female": "Int64",
    "dose_2_gender_undefined": "Int64",
    "dose_unknown_gender_male": "Int64",
    "dose_unknown_gender_female": "Int64",
    "dose_unknown_gender_undefined": "Int64",
    "CalculatedTotalDosesAdministered": "Int64",
    "Total_all_genders_dose_1": "Int64",
    "Total_all_genders_dose_2": "Int64",
    "Total_male_all_doses": "Int64",
    "Total_female_all_doses": "Int64",
    "Total_gender_undefined_all_doses": "Int64",
    "Total_all_genders_dose_unknown": "Int64",
    "total_all_sub_population_counts": "Int64",
}


_PARSE_OPTIONS = {
    "parse_dates": ["DateReported", "Modified"],
}

_age_group_mappings = {
    "Age Group 6 (80+)": [80, 999],
    "Age Group 5 (70-79)": [70, 79],
    "Age Group 4 (60-69)": [60, 69],
    "Age Group 3 (50-59)": [50, 59],
    "Other priority": [0, 0],
    "Unknown (aka delta)": [0, 0],
    "Age Group 1 (18-24)": [18, 24],
    "Age Group 2 (25-49)": [25, 49],
}


def get_dtypes():
    return _EXPOSURE_DATA_TYPES


def get_date_parser():
    return _PARSE_OPTIONS["parse_dates"]


def derive_exposure_id(input_row):
    return "{0}|{1}|{2}".format(
        input_row["SpotfireCountry"],
        input_row["MONTHADMINISTERED"],
        input_row["grouping"],
    )


def derive_min_age(age_range):
    return _age_group_mappings.get(age_range)[0]


def derive_max_age(age_range):
    return _age_group_mappings.get(age_range)[1]


def derive_gender(dose_gender_value):
    return dose_gender_value.split("_")[3]


def derive_dose(dose_gender_value):
    return dose_gender_value.split("_")[1]


def process_exposure_subgrouping(data_frame, grouping_cols, value_cols):
    """
    Helper function to process the exposure records that are tracked by
    age, dosage and gender. These are also split out by country as well.
    The process to gather the values involves unpivoting a data frame and then
    splitting out the variables from the column headings

    Parameters
    ----------
    reaction_list: a data frame to be processed. This should contain only
    counts which are split out by age, gender and dosage

    Returns
    -------
    A transformed data frame
    """
    data_frame = data_frame.dropna(subset=["grouping"])

    melted = data_frame.melt(id_vars=grouping_cols, value_vars=value_cols)

    melted = melted.dropna(subset=["value"])

    melted["GroupAgeMin"] = melted["grouping"].apply(derive_min_age)
    melted["GroupAgeMax"] = melted["grouping"].apply(derive_max_age)
    melted["GroupGender"] = melted["variable"].apply(derive_gender)
    melted["DoseIdentifier"] = melted["variable"].apply(derive_dose)

    final = melted[
        [
            "ExposureId",
            "StartDate",
            "EndDate",
            "value",
            "InformationSource",
            "GroupAgeMin",
            "GroupAgeMax",
            "GroupGender",
            "DoseIdentifier",
            "SpotfireCountry",
        ]
    ].copy()

    final.insert(loc=9, column="GroupRace", value="")
    final.insert(loc=10, column="GroupCondition", value="")
    final.insert(loc=11, column="Subregion", value="")
    final["Vaccine"] = "AstraZeneca"

    final.reset_index()

    return final


def process_exposure_top_level(data_frame):
    """
    Helper function to process the exposure records that are tracked solely at one
    level. These are at a country level without any sub categorisation

    Parameters
    ----------
    reaction_list: a data frame to be processed. This should contain only top level counts

    Returns
    -------
    A transformed data frame
    """
    data_frame = data_frame.dropna(subset=["DosesAdministered"])

    final = data_frame[
        [
            "ExposureId",
            "StartDate",
            "EndDate",
            "DosesAdministered",
            "InformationSource",
            "SpotfireCountry",
        ]
    ].copy()

    final.insert(loc=5, column="GroupAgeMin", value=np.NAN)
    final.insert(loc=6, column="GroupAgeMax", value=np.NAN)
    final.insert(loc=7, column="GroupGender", value="")
    final.insert(loc=8, column="DoseIdentifier", value="")
    final.insert(loc=9, column="GroupRace", value="")
    final.insert(loc=10, column="GroupCondition", value="")
    final.insert(loc=11, column="Subregion", value="")
    final["Vaccine"] = "AstraZeneca"

    final.reset_index()

    return final


def raw_load(input_bucket=None, input_key=None, file_path=None):
    """
    Load and transform AZ exposure data for processing from an s3 bucket/key
    or local file

    Parameters
    ----------
    input_bucket: object
        Reference to S3 bucket
    exposure_key: str
        Key for exposure data within input_bucket
    file_path: str
        Path to local file, exclusive with input_bucket/input_key

    """
    if not file_path and not (input_bucket or input_key):
        raise ValueError(
            "az_exposure.raw_load(): Either input_bucket and input_key, or file_path must be specified."
        )

    if file_path:
        az_exposure_df = pd.read_excel(
            file_path,
            dtype=get_dtypes(),
            parse_dates=get_date_parser(),
            skiprows=[0, 1, 2, 3],
        )
    else:
        az_exposure_df = s3_utils.excel_file_to_data_frame(
            bucket=input_bucket,
            key=input_key,
            dtype=get_dtypes(),
            parse_dates=get_date_parser(),
            skiprows=[0, 1, 2, 3],
        )

    az_exposure_df["ExposureId"] = az_exposure_df.apply(derive_exposure_id, axis=1)
    az_exposure_df["EndDate"] = pd.to_datetime(
        az_exposure_df["MONTHADMINISTERED"], format="%Y%m"
    ).dt.date
    az_exposure_df["StartDate"] = az_exposure_df["EndDate"] - pd.DateOffset(months=1)
    az_exposure_df["StartDate"] = az_exposure_df["StartDate"].dt.date

    top_level_cols = [
        "ExposureId",
        "StartDate",
        "EndDate",
        "DosesAdministered",
        "InformationSource",
        "SpotfireCountry",
    ]
    age_dose1_cols = [
        "ExposureId",
        "StartDate",
        "EndDate",
        "grouping",
        "dose_1_gender_male",
        "dose_1_gender_female",
        "dose_1_gender_undefined",
        "InformationSource",
        "SpotfireCountry",
    ]
    age_dose2_cols = [
        "ExposureId",
        "StartDate",
        "EndDate",
        "grouping",
        "dose_2_gender_male",
        "dose_2_gender_female",
        "dose_2_gender_undefined",
        "InformationSource",
        "SpotfireCountry",
    ]

    top_level_counts_df = az_exposure_df[top_level_cols].copy()
    age_dose1_counts_df = az_exposure_df[age_dose1_cols].copy()
    age_dose2_counts_df = az_exposure_df[age_dose2_cols].copy()

    top_level_final_df = process_exposure_top_level(top_level_counts_df)
    age_dose1_grouped_df = process_exposure_subgrouping(
        age_dose1_counts_df,
        [
            "ExposureId",
            "StartDate",
            "EndDate",
            "grouping",
            "InformationSource",
            "SpotfireCountry",
        ],
        ["dose_1_gender_male", "dose_1_gender_female", "dose_1_gender_undefined"],
    )
    age_dose2_grouped_df = process_exposure_subgrouping(
        age_dose2_counts_df,
        [
            "ExposureId",
            "StartDate",
            "EndDate",
            "grouping",
            "InformationSource",
            "SpotfireCountry",
        ],
        ["dose_2_gender_male", "dose_2_gender_female", "dose_2_gender_undefined"],
    )

    columns = [
        "ExposureId",
        "StartDate",
        "EndDate",
        "Count",
        "DataSource",
        "GroupAgeMin",
        "GroupAgeMax",
        "GroupGender",
        "DoseIdentifier",
        "GroupRace",
        "GroupCondition",
        "SubRegion",
        "Country",
        "Vaccine",
    ]

    top_level_final_df.columns = columns
    age_dose1_grouped_df.columns = columns
    age_dose2_grouped_df.columns = columns

    all_az_exposure_df = pd.concat(
        [top_level_final_df, age_dose1_grouped_df, age_dose2_grouped_df]
    )

    return all_az_exposure_df
