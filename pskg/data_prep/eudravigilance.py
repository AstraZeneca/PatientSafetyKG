import pandas as pd
import re
import math
import xml.sax
from pathlib import Path
import logging

from . import s3_utils
from .outcomes import OutcomeMapper

# Logging
logger = logging.getLogger(f"pskg_loader.eudravigilance")


# Important column names
_world_wide_case_id = "Worldwide Unique Case Identification"
_eu_local_number = "EU Local Number"
_gateway_receipt_date = "EV Gateway Receipt Date"
_suspect_drug_list = "Suspect/interacting Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])"
_concomitant_drug_list = "Concomitant/Not Administered Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])"
_primary_source_column = "Primary Source Country for Regulatory Purposes"

# Regex to parse term format, this is sufficient for ae terms
_term_re = re.compile(r"(?P<term>.+) [(](?P<dur>.+) - (?P<outcome>.+) - (?P<sc>.*)[)]")

# Regex to handle: Concomitant/Not Administered Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])
_simple_drug_re = re.compile(
    r"^(?P<drug>.+) \((?P<char>.+?) - (?P<ind>.+?) - (?P<action>.+?)(?P<dpres> - ){0,1}(?(dpres)\[(?P<dur>.+?) - (?P<dose>.+?) - (?P<route>.+?)\])\).*$"
)

_drug_manufacturer_mappings = {
    "Pfizer": ["TOZINAMERAN", "COMIRNATY", "PFIZER COVID-19 VACCINE"],
    "Moderna": ["MODERNA", "SPIKEVAX"],
    "AstraZeneca": ["CHADOX1", "COVID-19 VACCINE ASTRAZENECA", "VAXZEVRIA"],
    "Janssen": ["JANSSEN", "AD26.COV2.S"],
    "Sinopharm": ["COVID-19 VACCINE INACT (VERO)"],
}

# Standardize names for specific vaccines (note: VAERS names will take precedence on aligned drugs)
_exact_drug_name_mappings = {
    "[COVID-19 VACCINE]": {
        "GenericName": "Unknown Covid-19 Vaccine",
        "TradeName": "Unknown",
    },
    "[COVID-19 VACCINE AD26.COV2.S]": {
        "GenericName": "Janssen",
        "TradeName": "Janssen",
    },
    "COVID-19 VACCINE ASTRAZENECA (CHADOX1 NCOV-19) [COVID-19 VACCINE ASTRAZENECA (CHADOX1 NCOV-19)]": {
        "TradeName": "Vaxzevria",
        "GenericName": "COVID-19 VACCINE ASTRAZENECA (CHADOX1 NCOV-19)",
    },
    "VAXZEVRIA [COVID-19 VACCINE ASTRAZENECA (CHADOX1 NCOV-19)]": {
        "TradeName": "Vaxzevria",
        "GenericName": "COVID-19 VACCINE ASTRAZENECA (CHADOX1 NCOV-19)",
    },
    "[COVID-19 MRNA VACCINE (NUCLEOSIDE-MODIFIED)]": {
        "GenericName": "Unknown MNRA Covid-19 Vaccine",
        "TradeName": "Unknown",
    },
    "[COVID-19 VACCINE INACT (VERO) HB02]": {"TradeName": "Sinopharm"},
    "COMIRNATY [TOZINAMERAN]": {"TradeName": "Comirnaty", "GenericName": "Tozinameran"},
    "[TOZINAMERAN]": {"TradeName": "Comirnaty", "GenericName": "Tozinameran"},
    "TOZINAMERAN [TOZINAMERAN]": {
        "TradeName": "Comirnaty",
        "GenericName": "Tozinameran",
    },
    "COVID-19 VACCINE JANSSEN (AD26.COV2.S) [COVID-19 VACCINE JANSSEN (AD26.COV2.S)]": {
        "GenericName": "Janssen",
        "TradeName": "Janssen",
    },
    "SPIKEVAX [COVID-19 MRNA VACCINE MODERNA (CX-024414)]": {
        "GenericName": "Moderna",
        "TradeName": "SPIKEVAX",
    },
    "COVID-19 MRNA VACCINE MODERNA (CX-024414) [COVID-19 MRNA VACCINE MODERNA (CX-024414)]": {
        "GenericName": "Moderna",
        "TradeName": "SPIKEVAX",
    },
}

# Drug type mappings
_vax_type_mappings = {"COVID-19": "COVID19", "TOZINAMERAN": "COVID19"}


_EU_DATA_TYPES = {
    _eu_local_number: str,
    _world_wide_case_id: str,
    "Report Type": str,
    "Primary Source Qualification": str,
    _primary_source_column: str,
    "Literature Reference": str,
    "Patient Age Group": str,
    "Patient Age Group (as per reporter)": str,
    "Patient Sex": str,
    "Parent Child Report": str,
    "Reaction List PT (Duration – Outcome - Seriousness Criteria)": str,
    _suspect_drug_list: str,
    _concomitant_drug_list: str,
    "ICSR Form": str,
}

_PARSE_OPTIONS = {
    "parse_dates": [_gateway_receipt_date],
}

_OUTCOME_MAPPING = pd.DataFrame.from_records(
    (
        ("Caused/Prolonged Hospitalisation", "prolonged hospitalization"),
        ("Congenital Anomaly", "birth defect"),
        ("Disabling", "disabled"),
        ("Fatal", "death"),
        ("Life Threatening", "life threatening"),
        ("Other Medically Important Condition", "other medically important condition"),
        ("Recovering/Resolving", "in recovery"),
        ("Recovered/Resolved With Sequelae", "sequelae"),
        ("Recovered/Resolved", "recovered"),
        ("Results in Death", "death"),
        ("Unknown", "unknown"),
    ),
    columns=["dataset_outcomes", "standard_outcomes"],
)
OutcomeMapper.register_outcome_mapping(dataset="EUDRAVIGILANCE", mapping=_OUTCOME_MAPPING)

ALL_EU_OUTCOMES = (
    pd.pivot_table(
        _OUTCOME_MAPPING[["dataset_outcomes"]].copy().assign(tmpid=0),
        index="tmpid",
        columns="dataset_outcomes",
        aggfunc=lambda x: False,
    )
    .reset_index()
    .drop("tmpid", axis=1)
)
logger.info(f"All outcomes: {ALL_EU_OUTCOMES}")


def find_line_listing_files(
    data_set_tag, input_bucket=None, prefix="EudraVigilance", folder_path=None, 
):
    """
    Locate source files in the specified S3 bucket, data_version and
    with optional prefix.  Returns keys for matching files.

    Parameters
    ----------
    input_bucket: str
        S3 bucket URI
    data_set_tag: str
        Data version, typically a string formatted as a date, e.g. 10Sep2021
    prefix: str, optional
        Default prefix for EudraVigilance data, defaults to "EudraVigilance"

    Returns
    -------
    pd.Dataframe
        Dataframe with columns key (in input_bucket) or file_path matching data_tag and either an excel or xml pattern.
    """

    excel_pattern = "^.*(xlsx|xlsm|xltx|xltm)$"
    xml_pattern = "^.*\.xml$"

    interim_result = {"key": [], "file_path": []}
    result = None

    if folder_path:
        logger.info(f"Searching local folder: {folder_path} for {data_set_tag}")
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)

        for f in folder_path.glob("*.*"):
            if data_set_tag in f.name and (
                re.match(excel_pattern, f.name) or re.match(xml_pattern, f.name)
            ):
                if "~$" in f.name:
                    logger.warning(f"Skipping {f.name} (temporary file)")
                    continue
                interim_result["key"].append(None)
                interim_result["file_path"].append(f)
        result = pd.DataFrame.from_dict(interim_result)
        if len(result) == 0:
            logger.warning(f"No files found in {folder_path}.")
    else:
        # EUDRAVigilance (exported in Excel format or XML format in S3)
        eu_bucket = s3_utils.get_bucket(input_bucket)
        for obj in eu_bucket.objects.filter(Prefix=prefix):
            if data_set_tag in obj.key and (
                re.match(excel_pattern, obj.key) or re.match(xml_pattern, obj.key)
            ):
                interim_result["key"].append(obj.key)
                interim_result["file_path"].append(None)
        result = pd.DataFrame.from_dict(interim_result)
        if len(result) == 0:
            logger.warning(
                f"No files found in s3://{input_bucket} with prefix {prefix}."
            )

    return result


def get_dtypes():
    return _EU_DATA_TYPES


def get_date_parser():
    return _PARSE_OPTIONS["parse_dates"]


def derive_case_id(
    row, native_id_column=_eu_local_number, receipt_date_column=_gateway_receipt_date    
):
    """
    Convert a native case id to a PSKG case id.  Since EV files can contain updated cases (i.e. same
    worldwide case id, but multiple dates)

    Parameters
    ----------
    row: dict
        This function is intended to be called using apply
    native_id_column: str, optional
        Column to use for native row identifier (defaults to world wide case id)
    receipt_date_column: date
        Column containing receipt date

    """
    rd = row[receipt_date_column].strftime("%Y%m%d")       

    return f"EUDRAVIGILANCE:{row[native_id_column]}-{rd}"

def ev_split_break(column):
    """
    Break up text column delimited on ",<BR><BR>"
    """
    return column.split(",<BR><BR>") if type(column) == str else None


def ev_extract_term(reaction_list):
    """
    Helper function to split reaction_list (a string) into a list. The reaction list column contains
    these data elements:

    * PT
    * Duration
    * Outcome
    * Seriousness Criteria (n.b. this is a list)

    Outcome and seriousness criteria are used to determine if the given PT reaction is serious, and
    if the entire case should be considered serious

    Parameters
    ----------
    reaction_list: str

    Returns
    -------
    Series with components broken out
    """
    terms = []
    duration = []
    outcome = []
    seriousness_criteria = []
    if reaction_list is not None:
        for s in reaction_list:
            if s.lower() == "not reported":
                continue
            m = _term_re.match(s)
            if m is not None:
                terms.append(m["term"])
                duration.append(m["dur"])
                outcome.append(m["outcome"])
                seriousness_criteria.append([t.strip() for t in m["sc"].split(",")])
            else:
                raise ValueError(
                    f"Warning: could not match {s}, reaction_list={reaction_list}."
                )
    return pd.Series(
        [terms, duration, outcome, seriousness_criteria],
        index=[
            "terms_list",
            "duration_list",
            "outcome_list",
            "seriousness_criteria_list",
        ],
    )


def ev_extract_drug_details(
    df,
    drug_column,
    case_id_column="CaseId",
    primary_source_column=_primary_source_column,
    gateway_date_column=_gateway_receipt_date,
):
    """
    Helper function useful for extracting information from a drug column
    that has been previously split into a list.
    Attempts to isolate drug name (which is often messy), along with
    additional information including the characterization (concomitant, suspect,
    interacting, not administered), indication preferred term, duration

    Parameters
    ----------
    drug_column: str
        A column containing a list of drug information
    case_id_column: str, optional
        Name of unique identifier for this case, defaults to "CaseId"
    primary_source_column: str, optional
        Name of primary source column, defaults to _primary_source_column constant
    gateway_date_column: str, optional
        Gateway receipt date column, defaults to _gateway_receipt_date constant

    Returns
    -------
    pd.Dataframe
        A dataframe organized by case id and gateway date
    """
    case_id = []
    primary_source = []
    gateway_date = []
    characterization = []
    drug = []
    indication = []
    action = []
    dose = []
    duration = []
    route = []
    errors = []

    for _, row in df[
        [case_id_column, gateway_date_column, drug_column, primary_source_column]
    ].iterrows():
        for d in row[drug_column]:
            clean_d = d.strip()
            if clean_d:
                if clean_d == "Not reported" or clean_d == "[Not reported]":
                    continue
                case_id.append(row[case_id_column])
                gateway_date.append(row[gateway_date_column])
                primary_source.append(row[primary_source_column])
                details = _simple_drug_re.match(d)
                if details is None:
                    drug.append(None)
                    characterization.append(None),
                    indication.append(None)
                    action.append(None)
                    dose.append(None)
                    duration.append(None)
                    route.append(None)
                    errors.append(f"could not parse: '{clean_d}'")
                else:
                    drug.append(details.group("drug"))
                    characterization.append(details.group("char")),
                    indication.append(details.group("ind"))
                    action.append(details.group("action"))
                    dose.append(details.group("dose"))
                    duration.append(details.group("dur"))
                    route.append(details.group("route"))
                    errors.append("")

    return pd.DataFrame(
        list(
            zip(
                case_id,
                primary_source,
                gateway_date,
                drug,
                characterization,
                indication,
                action,
                dose,
                duration,
                route,
                errors,
            )
        ),
        columns=[
            case_id_column,
            primary_source_column,
            gateway_date_column,
            "drug",
            "characterization",
            "indication",
            "action",
            "dose",
            "duration",
            "route",
            "errors",
        ],
    )


def ev_simple_classify_manufacturer(
    drug, drug_name_mappings=_drug_manufacturer_mappings, verbose=False
):
    """
    Bare bones drug classifier, look for key strings and returns appropriate manufacturer
    based on the drug_name_mappings suplied.  Similar to ev_classify_drug, but assumes that
    drug names appear in the supplied column.  Drug names not in the map are returned unaltered.

    Parameters
    ----------
    drug: str
        Drug name to map
    drug_name_mappings: dict
        Mapping of strings in names to friendly names
    verbose: boolean, optional
        Logs mapping information, and exits

    Returns
    -------
    str
        Manufacter if mapping is available, or Unknown
    """
    if verbose:
        logger.info(f"Mappings: {drug_name_mappings}")
        return None
    if drug is None or (drug != drug):
        logger.warn(f"drug is None.")
        return drug

    for k, v in drug_name_mappings.items():
        for t in v:
            if t in drug:
                return k
    return drug


def ev_simple_standardize_generic_drug_names(
    drug, drug_name_mapping=_exact_drug_name_mappings
):
    """
    Standardize specific drug names found in EV data, if a match is not found, the given
    drug name is returned.

    Parameters
    ----------
    drug: str
        Name of drug as it appears in EudraVigilance data
    drug_name_mapping: dict, optional
        Exact mapping of name to GenericName

    Returns
    str
        Mapped name, or drug if not present in mapping
    """
    if drug in drug_name_mapping and "GenericName" in drug_name_mapping[drug]:
        return drug_name_mapping[drug]["GenericName"]
    return drug


def ev_simple_standardize_trade_drug_name(
    drug, drug_name_mapping=_exact_drug_name_mappings
):
    """
    Get trade names for specific drug names found in EV data, if a match is not found, empty
    string is returned

    Parameters
    ----------
    drug: str
        Name of drug as it appears in EudraVigilance data
    drug_name_mapping: dict, optional
        Exact mapping of name to GenericName

    Returns
    str
        Mapped name, or drug if not present in mapping
    """
    if drug in drug_name_mapping and "TradeName" in drug_name_mapping[drug]:
        return drug_name_mapping[drug]["TradeName"]
    return ""


def ev_simple_vax_type(drug, vax_type_mapping=_vax_type_mappings):
    """
    EudraVigilance data does not include a notion of type for vaccines, however, all Covid-19 drugs
    contain the string "COVID-19".  This function returns the given type if found in
    the map
    """
    for k, v in vax_type_mapping.items():
        if k in drug:
            return v
    else:
        return ""


def derive_country(case_id):
    """
    Determine case country from case identifier (i.e. column 'Worldwide Unique Case Identification'), which is currently formatted
    with an ISO 2-character country code followed by a unique identifier.

    Parameters
    ----------
    input_string: str
        Case identifier in country-identifier format

    Returns
    -------
    str
        Iso-country code
    """
    return case_id.split("-")[0]


def get_min_age(age_string):
    """
    Return the minmum age from a EudraVigilance age range

    Parameters
    ----------
    age_string: str
        A EudraVigilance age category

    Returns
    -------
    int
        Minimum age in years

    """
    if age_string == "18-64 Years":
        return 18
    elif age_string == "12-17 Years":
        return 12
    elif age_string == "65-85 Years":
        return 65
    elif age_string == "Not Specified":
        return ""
    elif age_string == "More than 85 Years":
        return 85
    elif age_string == "3-11 Years":
        return 3
    elif age_string == "2 Months - 2 Years":
        return 0
    elif age_string == "0-1 Month":
        return 0


def get_max_age(age_string):
    """
    Return the maximum age from a EudraVigilance age range

    Parameters
    ----------
    age_string: str
        A EudraVigilance age category

    Returns
    -------
    int
        Minimum age in years
    """
    if age_string == "18-64 Years":
        return 64
    elif age_string == "12-17 Years":
        return 17
    elif age_string == "65-85 Years":
        return 85
    elif age_string == "Not Specified":
        return 0
    elif age_string == "More than 85 Years":
        return 999
    elif age_string == "3-11 Years":
        return 11
    elif age_string == "2 Months - 2 Years":
        return 2
    elif age_string == "0-1 Month":
        return 0


def merge_lists(x):
    """
    Helper function (used with apply) to merge nested lists into a single list
    with only unique values.  Example:
        [['a','b','c'],['a','1','2']]
        ==> ['a', 'b', 'c', '1', '2']

    Parameter
    x: list
        A list containing sub-lists

    Returns
    list
        A single level list containing unique elements from each sub-list
    """

    merged = []
    for item in x:
        for inner_item in item:
            merged.append(inner_item)
    return list(set(merged))


def simplify_list(x):
    """
    Helper function to reduce a three-level list (such as seriousness criteria) to a
    simple list.  Example:

    [[[''], ['Caused/Prolonged Hospitalisation'], ['Caused/Prolonged Hospitalisation', 'Other Medically Important Condition']]]
    ==>  ['Caused/Prolonged Hospitalisation', 'Other Medically Important Condition']

    Returns
    -------
    list
        Simple one level list
    """
    return list(set([l3 for l1 in x for l2 in l1 for l3 in l2 if l3]))


def join_and_merge_lists(input_row):
    """
    Helper function (used with apply) to combine two lists of lists into
    a list with the pairwise elements of the source lists
    concatenated.  Example:
        [['1', '5'], ['2', '6'], ['3', '7']]
        [['a', 'g'], ['b', 'h'], ['c', 'i']]
    ==> [['1:a', '5:g'], ['2:b', '6:h'], ['3:c', '7:i']]

    input_row is a row dictionary, containing keys "terms_list" and
    "duration_list"

    Parameters
    ----------
    input_row: dict
        Dictionary with keys "terms_list" and "duration_list"

    returns:
    list
        Combined lists as described above
    """
    list1 = input_row["terms_list"]
    list2 = input_row["duration_list"]

    z = [zip(x, y) for x, y in zip(list1, list2)]

    return [[f + ":" + g for f, g in aa] for aa in z]


def single_value(input_list):
    """
    Helper function to extract the last non-null value of a list, typically used
    with case data from multiple versions collected as a list (latest
    data element is last).

    Parameters
    ----------
    input_list: list
        A list of items (can by any type)

    Returns
    -------
    obj
        Returns last non-null item in the list
    """
    non_nulls = [x for x in input_list if x]
    return non_nulls[-1]


def split_string(s):
    """
    Helper function for dealing with duration expression strings, separating
    duration from interval (e.g. wk == weeks)

    Parameters
    ----------
    s: str
        String containing a number followed by a interval specifier

    Returns
    -------
    tuple (head, tail)
        head is the numeric portion, tail is the interval specifier string
    """
    tail = s.lstrip("0123456789")
    head = s[: len(s) - len(tail)]
    return head, tail


def convert_duration_to_days(duration):
    """
    Parse duration strings present in some columns, converting various intervals to
    days

    Parameters
    ----------
    duration: str
        String formatted duration, e.g. 7wk == 7 weeks

    Returns
    -------
    float
        duration rounded to days
    """
    if duration is None:
        return None

    number_part, text_part = split_string(str(duration))

    if number_part:
        number = float(number_part)
    else:
        return 0

    if text_part == "d":
        return math.ceil(number)
    elif text_part == "wk":
        return math.ceil(number * 7)
    elif text_part == "h":
        return math.ceil(number / 24)
    elif text_part == "min":
        return math.ceil(number / 24 / 60)
    elif text_part == "s":
        return math.ceil(number / 24 / 60 / 60)
    else:
        return 0


###
### Line Listing Excel Support
###


def raw_load(input_bucket=None, input_key=None, file_path=None, columns=None):
    """
    Read either an S3 or local line listing format file (in XL or XML format) and
    produce a dataframe with only the specified columns

    Parameters
    ----------
    input_bucket: str
        S3 bucket name
    input_key: str
        S3 bucket key containing an EV source file.
    file_path: str
        Path to local file, exclusive with input_bucket/input_key
    columns: list, optional
        Gather only specified columns

    Returns
    -------
    pd.Dataframe
        Dataframe from all matching source files, limited to columns specified
    """
    if not file_path and not (input_bucket or input_key):
        raise ValueError(
            "az_exposure.raw_load(): Either a bucket and key, or file_path is required."
        )

    if file_path:
        if not isinstance(file_path, Path):
            file_path = Path(file_path)
        is_xml = file_path.suffix.lower() == "xml"
        if is_xml:
            # Load EV XML Line Listing format data
            parser = xml.sax.make_parser()
            parser.setContentHandler(EudravigilanceStreamHandler())
            logger.info(f"Loading: file://{file_path.as_posix()} (xml)...")
            try:
                parser.parse(file_path)
                eu_df = parser.getContentHandler().getDataframe().loc[:, columns]
            except Exception as x:
                logger.error(f"Failed to load: {file_path}")
                raise

        else:
            # Load XL Line Listing format data
            logger.info(f"Loading: file://{file_path.as_posix()}...")
            try:
                eu_df = pd.read_excel(
                    file_path,
                    dtype=get_dtypes(),
                    parse_dates=get_date_parser(),
                    usecols=columns,
                )
            except Exception as x:
                logger.error(f"Failed to load: {file_path}")
                raise
    else:
        if not input_key.endswith(".xml"):
            # Load XL Line Listing format data
            logger.info(f"Loading: {input_bucket}/{input_key}...")
            try:
                eu_df = s3_utils.excel_file_to_data_frame(
                    bucket=input_bucket,
                    key=input_key,
                    dtype=get_dtypes(),
                    parse_dates=get_date_parser(),
                    usecols=columns,
                )
            except Exception as x:
                logger.error(f"Failed to load: {input_bucket}/{input_key}")
                raise
        else:
            # Load EV XML Line Listing format data
            parser = xml.sax.make_parser()
            parser.setContentHandler(EudravigilanceStreamHandler())
            logger.info(f"Loading: {input_bucket}/{input_key} (xml)...")
            try:
                parser.parse(s3_utils.get_file_contents(input_bucket, input_key))
                eu_df = parser.getContentHandler().getDataframe().loc[:, columns]
            except Exception as x:
                logger.info(f"Failed to load: {input_bucket}/{input_key}")
                raise

    # Gather all source dfs into a single list

    return eu_df


###
### Line Listing XML Sax Support
###


class EudravigilanceStreamHandler(xml.sax.handler.ContentHandler):
    """
    Subclass of xml.sax.handler.ContentHandler for extracting data from a source
    stream in EudraVigilance XML Line Listing format.
    """

    def __init__(self, verbose=False):
        """
        Class init method.
        """
        super().__init__()
        # Columns defined in schema--maps internal name like "C0" to column name
        self.column_names = {}
        self.column_types = {}
        self.records = {}
        self.parsed = False
        self._verbose = verbose

        # Parser status
        self.in_schema = False
        self.in_type = False
        self.in_records = False

        # Manage current record data
        self.current_record = {}
        self.current_column = None

    @property
    def verbose(self):
        return self._verbose

    @verbose.setter
    def verbose(self, verbose):
        self._verbose = bool(verbose)

    def startElement(self, name, attrs):
        """
        Method called whenever the Sax parser encounters a new element with name as the tag and attribures attrs (dict).

        Parameters
        ----------
        name: str
            Tag for newly identified element in stream

        attr: dict
            Dictionary of attributes with name spaces
        """
        if name == "RS" and self._verbose:
            print("Rowset...")
        elif name == "xsd:schema":
            if self._verbose:
                print("  Schema")
            self.in_schema = True
        elif name == "xsd:element" and self.in_schema:
            if self._verbose:
                print(
                    f"    Column: {attrs['name']}: {attrs['saw-sql:columnHeading']} ({attrs['type']})"
                )
            self.column_names[attrs["name"]] = attrs["saw-sql:columnHeading"]
            self.column_types[attrs["saw-sql:columnHeading"]] = attrs["type"]
            in_type = True
        elif name in "R":
            self.in_records = True
        elif name in self.column_names and self.in_records:
            self.current_column = self.column_names[name]

    def endElement(self, name):
        """
        Method called whenever the Sax parser encounters a closing element with name as the tag.

        Parameters
        ----------
        name: str
            Tag for newly identified element in stream
        """

        if name == "xsd:element":
            in_type = False
        elif name == "xsd:schema":
            if self.verbose:
                print("Schema parse complete.")
            in_schema = False
        elif name == "R":
            # clean up white space junk...
            if self.current_record:
                for k, v in self.current_record.items():
                    self.current_record[k] = " ".join(self.current_record[k].split())
                    if k in self.records:
                        self.records[k].append(self.current_record[k])
                    else:
                        self.records[k] = [self.current_record[k]]
            self.current_record = {}
            self.current_column = None
        elif name == "RS":
            self.parsed = True
            if self._verbose:
                print("Complete!")

    def characters(self, content):
        """
        Method called as content is read from the source stream.

        Parameters
        ----------
        content: str
            Text content
        """
        if content and self.in_records:
            if self.current_column in self.current_record:
                self.current_record[self.current_column] += content
            elif self.current_column is not None:
                self.current_record[self.current_column] = content
            else:
                # this is content (usually white space) outside column definitions
                pass

    def getDataframe(self):
        """
        Return a dataframe of records found, converting columns in xsd:dateTime format
        datetime[64].  This function must be called after data are parsed completely,
        otherwise an exception will be raised.

        Parameters
        ----------
        self

        Returns
        -------
        dataframe
            Dataframe representation of data previously parsed from the XML source
        """
        if self.parsed:
            df = pd.DataFrame.from_dict(self.records)
            for k, v in self.column_types.items():
                if v == "xsd:dateTime":
                    df[k] = pd.to_datetime(df[k], format="%Y-%m-%dT%H:%M:%S")
            return df
        else:
            raise ValueError("No records parsed yet.")
