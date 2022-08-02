###
### Collect mappings from case groups to cases
###

import logging
from pathlib import Path

from pandas.io.formats.format import set_eng_float_format
from graph_objects import utils
from data_prep import eudravigilance as eu
from data_nodes.case_group import EudraVigilanceCaseGroup


class CaseGroup(utils.Generator):
    _output_columns = ["CaseGroupId", "CaseId"]


class EudraVigilanceCaseGroupCase(CaseGroup):
    # Gather case group definitions
    case_group_df = EudraVigilanceCaseGroup.case_group_df
    data_source = "EUDRAVIGILANCE"

    # Columns needed for identifying case group
    eu_raw_id_column = "Worldwide Unique Case Identification"
    eu_gateway_date = "EV Gateway Receipt Date"
    eu_report_country = "Primary Source Country for Regulatory Purposes"
    raw_eu_columns = [
        eu_raw_id_column,
        eu_gateway_date,
        eu_report_country,
    ]

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
        """
        Create a new EudraVigilance case group contains object.  Either an s3_bucket and s3_key
        are required, or a file_path.

        Parameters
        data_set_tag:   str
            Data set
        s3_bucket: str, optional
        s3_key: str, optional
            key within s3_bucket to data zip file
        file_path: str, optional
            Path to local eudravigilance data file
        ev_source: str
            EudraVigilance data source: "Public" or "EVDAS"; configured in config.yml, indicates EV data source is public site or from the EVDAS system. 
            public site does not contain "Worldwide Unique Case Identification" used to identiy case country.
            If data source is Public "Worldwide Unique Case Identification" is replaced with "EU Local Number"
        """

        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(
            f"pskg_loader.EV Case Group Case({self.data_set_tag})"
        )
        self.ev_source = ev_source

    def write_objects(self, output_stream):
        """
        Construct case data from a EudraVigilance Line Listing format file and write it to an existing open output_stream.  Caller is responsible for
        creating the output stream and eventually closing it.

        Parameters
        ----------
        output_stream: object
            Open stream for output, data will be appended to this stream

        Returns
        -------
        None
        """

        # Clear out old data
        self.manifest_data = []
        if self.ev_source == "Public":
            self.eu_raw_id_column = "EU Local Number"
            self.raw_eu_columns[0] = self.eu_raw_id_column
 
        # Gather only columns needed for case nodes
        eu_case_df = eu.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            columns=self.raw_eu_columns,
        )
        self.manifest_data.append(
            self.get_manifest_data(df=eu_case_df, tag=self.data_set_tag)
        )

        result_df = eu_case_df.merge(
            self.case_group_df,
            left_on=self.eu_report_country,
            right_on="Name",
            how="left",
        )
        result_df["CaseId"] = result_df.apply(eu.derive_case_id, axis=1)

        final_df = result_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, sep="\t", index=False, header=None, mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
