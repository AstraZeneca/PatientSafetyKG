###
### Class Defining CaseReportedFrom.tsv
###

from graph_objects import utils


class CaseReportedFrom(utils.Generator):
    _output_columns = ["CaseId", "Country", "SubRegion"]
