from pathlib import Path
from graph_objects import utils
import pandas as pd
import logging


class CaseGroup(utils.Generator):
    _output_columns = ["CaseGroupId", "Name", "Abbreviation", "Description"]


###
### This class can be moved to its own file in the event more complex case groups are needed
###
class EudraVigilanceCaseGroup(CaseGroup):
    """
    Create case groups for EV
    """

    case_group_df = pd.DataFrame.from_dict(
        {
            "CaseGroupId": ["EU-EEA", "EU-Non-EEA"],
            "Name": ["EEA", "Non-EEA"],
            "Abbreviation": ["EEA", "Non-EEA"],
            "Description": [
                "Eudravigilance case whose Primary Source Country for Regulatory Purposes is in the European Economic Association",
                "Eudravigilance case whose Primary Source Country for Regulatory Purposes is NOT in the European Economic Association",
            ],
        }
    )
    data_tag = "in-line"

    def __init__(self):
        """
        Currently EV case groups are defined in-line
        """
        fp = Path(__file__)
        self.source_url = "file://" + fp.as_posix()

        self.manifest_data = [
            self.get_manifest_data(self.case_group_df, file_path=fp, tag=self.data_tag)
        ]

        self.logger = logging.getLogger(f"pskg_loader.EV Case Groups({self.data_tag})")

    def write_objects(self, output_stream):
        self.case_group_df.to_csv(
            output_stream, index=False, header=False, sep="\t", mode="a"
        )

        self.logger.info(f"{len(self.case_group_df)} rows written.")
