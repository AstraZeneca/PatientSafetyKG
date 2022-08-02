###
### Class Defining CountryHasExposure.tsv
###

from graph_objects import utils


class CountryHasExposure(utils.Generator):
    _output_columns = ["ExposureId", "CountryCode"]
