###
### Utility Functions for alignment of identifiers between data sources
###

# Standardized vaccine names.  Currently COVID-19 vaccines
# in VAERS and Eudravigilance can be aligned (i.e. are the same vaccine)
# using standardized manufacter names.  Any other values are assigned
# an id for that data source.
_aligned_vaccine_name_ids = {
    "COVID19 (COVID19 (PFIZER-BIONTECH))": "Pfizer",
    "COVID19 (COVID19 (JANSSEN))": "Janssen",
    "COVID19 (COVID19 (MODERNA))": "Moderna",
    "COVID19 (COVID19 (UNKNOWN))": "Unknown",
    "TOZINAMERAN": "Pfizer",
    "COMIRNATY": "Pfizer",
    "PFIZER COVID-19": "Pfizer",
    "MODERNA": "Moderna",
    "CHADOX1": "AstraZeneca",
    "COVID-19 VACCINE ASTRAZENECA": "AstraZeneca",
    "VAXZEVRIA": "AstraZeneca",
    "SPIKEVAX": "Moderna",
    "[COVID-19 VACCINE AD26.COV2.S]": "Janssen",
}

_aligned_vaccine_manufacturer_ids = {
    "Pfizer": "Pfizer",
    "Janssen": "Janssen",
    "Moderna": "Moderna",
    "AstraZeneca": "AstraZeneca",
}

_aligned_medication_name_ids = {
    "VAXZEVRIA": "AstraZeneca",
    "CHADOX1": "AstraZeneca",
    "TOZINAMERAN": "Pfizer",
    "COMIRNATY": "Pfizer",
    "SPIKEVAX": "Moderna",
}

_aligned_medication_manufacturer_ids = {
    "Pfizer": "Pfizer",
    "Janssen": "Janssen",
    "Moderna": "Moderna",
    "AstraZeneca": "AstraZeneca",
}

_aligned_ds = "ALIGNED"

# NOTE: Future medication and vaccine identifier harmonization leveraging a controlled
# vocabulary will be added here


def get_vaccine_id(
    row, data_source, drug_column="drug", manufacturer_column="Manufacturer"
):
    """
    Return an appropriate identifier for a given vaccine.  If an id appears in the aligned set,
    use a common prefix to indicate that the id is valid accross data sets.  Otherwise the
    returned id is scoped to the given data_source.

    Parameters
    ----------
    row: dict
        A row dictionary (as called from apply axis=1)
    data_source: str
        A string identifying the data source (e.g. VAERS)
    drug_column: str, optional
        Name of column containing drug name, defaults to "drug"
    manufacturer_column: str
        Name of column in row with manufacturer information, defaults to "Manufacturer"
    """
    if row[drug_column] in _aligned_vaccine_name_ids:
        return f"{_aligned_ds}:{_aligned_vaccine_name_ids[row[drug_column]]}"
    if row[manufacturer_column] in _aligned_vaccine_manufacturer_ids:
        return f"{_aligned_ds}:{_aligned_vaccine_manufacturer_ids[row[manufacturer_column]]}"
    else:
        return f"{data_source}:{row[drug_column]}"


def get_medication_id(
    row, data_source, drug_column="drug", manufacturer_column="Manufacturer"
):
    """
    Return an appropriate identifier for a given medication.  If an id appears in the aligned set,
    use a common prefix to indicate that the id is valid accross data sets.
    """

    if row[drug_column] in _aligned_medication_name_ids:
        return f"{_aligned_ds}:{_aligned_medication_name_ids[row[drug_column]]}"
    if row[manufacturer_column] in _aligned_medication_manufacturer_ids:
        return f"{_aligned_ds}:{_aligned_medication_manufacturer_ids[row[manufacturer_column]]}"
    else:
        return f"{data_source}:{row[drug_column]}"
