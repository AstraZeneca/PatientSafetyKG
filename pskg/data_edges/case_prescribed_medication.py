###
### Prescribed medication
###

from graph_objects import utils


class CasePrescribedMedication(utils.Generator):
    _output_columns = [
        "CaseId",
        "MedicationId",
        "StartDate",
        "StopDate",
        "Duration",
        "Dosage",
        "Units",
        "Route",
        "Indication",
        "Evidence",
        "Characterization",
    ]
