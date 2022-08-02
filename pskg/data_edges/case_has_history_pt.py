###
### Placeholder for HasHistoryOf relationship
###

from graph_objects import utils


class CaseGroup(utils.Generator):
    _output_columns = ["CaseId", "MeddraId", "StartDate", "StopDate", "Evidence"]
