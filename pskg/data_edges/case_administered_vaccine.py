###
### Class Defining CaseAdministeredVaccine.tsv
###

from graph_objects import utils


class CaseAdministeredVaccine(utils.Generator):
    _output_columns = [
        "CaseId",
        "VaccineId",
        "VaccineDate",
        "VaccineLot",
        "VaccineRoute",
        "VaccineSite",
        "Indication",
        "Characterization",
        "Dosage",
        "Units",
        "Duration",
    ]
