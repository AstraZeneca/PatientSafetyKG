###
### Class Defining Exposure.tsv
###

from graph_objects import utils


class Exposure(utils.Generator):
    _output_columns = [
        "ExposureId",
        "DataSource",
        "StartDate",
        "EndDate",
        "Count",
        "GroupAgeMin",
        "GroupAgeMax",
        "GroupGender",
        "GroupRace",
        "GroupCondition",
        "DoseIdentifier",
        "SubRegion",
    ]
