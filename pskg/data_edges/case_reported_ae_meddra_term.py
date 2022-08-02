###
### Class Defining CaseReportedAEMeddraTerm.tsv
###

from graph_objects import utils


class CaseReportedAEMeddraTerm(utils.Generator):
    _output_columns = ["CaseId", "MeddraTerm", "OnsetDate", "LengthInDays"]
