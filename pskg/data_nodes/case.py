###
### Class Defining Case.tsv structure
###

from graph_objects import utils


class Case(utils.Generator):
    _output_columns = [
        "CaseId",
        "SourceCaseId",
        "DataSource",
        "Tag",
        "ReportedDate",
        "ReceivedDate",
        "PatientAgeRangeMin",
        "PatientAgeRangeMax",
        "PatientGender",
        "PatientOutcome",
        "PatientRecovered",
        "DeathDate",
        "HospitalizationLengthInDays",
        "ReportType",
    ]
