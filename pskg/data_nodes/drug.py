###
### Class Defining Vaccine.tsv and Medication.tsv
###

from graph_objects import utils


class Medication(utils.Generator):
    _output_columns = [
        "MedicationId",
        "TradeName",
        "GenericName",
        "OriginalName",
        "Manufacturer",
        "RxNormCui",
        "Description",
    ]


class Vaccine(utils.Generator):
    _output_columns = [
        "VaccineId",
        "VaxType",
        "TradeName",
        "GenericName",
        "OriginalName",
        "Manufacturer",
        "RxNormCui",
        "Description",
    ]
