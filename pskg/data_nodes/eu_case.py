import logging

import numpy as np
import pandas as pd

from data_prep import eudravigilance as eu
from data_prep import s3_utils
from data_prep.outcomes import OutcomeMapper
from . import case


class EudraVigilanceCase(case.Case):
    """
    Generator class for EudraVigilance cases
    """

    data_source = "EUDRAVIGILANCE"

    reaction_list_column = (
        "Reaction List PT (Duration – Outcome - Seriousness Criteria)"
    )
    
    eu_raw_id_column = "Worldwide Unique Case Identification"
    raw_eu_columns = [
        eu_raw_id_column,
        "EV Gateway Receipt Date",
        "Report Type",
        "Primary Source Country for Regulatory Purposes",
        "Patient Age Group",
        "Patient Sex",
        reaction_list_column,
    ]

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
        """
        Create a new EudraVigilance generator object.  Either an s3_bucket and s3_key
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
            EudraVigilance data source: public or EVDAS, configured in config.yml, indicates EV data source is public site or from the EVDAS system. 
            public site does not contain "Worldwide Unique Case Identification" used to identiy case country.
            If data source is Public "Worldwide Unique Case Identification" is replaced with "EU Local Number"
        """

        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(f"pskg_loader.EudraVigilanceCase")
        self.logger.info(f"Created {self}")
        OutcomeMapper.show_mappings(dataset=self.data_source)
        self.ev_source = ev_source
        
        if self.ev_source == "Public":
            self.eu_raw_id_column = "EU Local Number"
            self.raw_eu_columns[0] = self.eu_raw_id_column
 
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

        # Gather only columns needed for case nodes
        #set CaseId to Worldwide Unique Case Identification if present in EV data
            
        eu_case_df = eu.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            columns=self.raw_eu_columns,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=eu_case_df, tag=self.data_set_tag)
        )

        eu_case_df["reaction_list_pt"] = eu_case_df[self.reaction_list_column].apply(
            eu.ev_split_break
        )

        eu_case_df[
            ["terms_list", "duration_list", "outcome_list", "seriousness_criteria_list"]
        ] = eu_case_df[["reaction_list_pt"]].apply(
            lambda x: eu.ev_extract_term(x["reaction_list_pt"]), axis=1
        )

        # Build up columns for export
        
        eu_case_df["CaseId"] = eu_case_df.apply(eu.derive_case_id, native_id_column=self.eu_raw_id_column, axis=1)
        eu_case_df["SourceCaseId"] = eu_case_df[self.eu_raw_id_column]
        eu_case_df["DataSource"] = self.data_source
        eu_case_df["ReportedDate"] = pd.to_datetime(
            "", errors="coerce"
        )  # EV doesn't have this value
        eu_case_df["ReceivedDate"] = eu_case_df["EV Gateway Receipt Date"].dt.date
        eu_case_df["Tag"] = eu_case_df["EV Gateway Receipt Date"].dt.date
        eu_case_df["PatientAgeRangeMin"] = eu_case_df["Patient Age Group"].apply(
            eu.get_min_age
        )
        eu_case_df["PatientAgeRangeMax"] = eu_case_df["Patient Age Group"].apply(
            eu.get_max_age
        )
        eu_case_df["PatientGender"] = eu_case_df["Patient Sex"]
        eu_case_df["DeathDate"] = pd.to_datetime(
            "", errors="coerce"
        )  # EV doesn't have this value
        eu_case_df["ReportType"] = eu_case_df["Report Type"]

        # Standardize patient outcomes.

        eu_case_df["outcome_list"] = eu_case_df["outcome_list"] + eu_case_df[
            "seriousness_criteria_list"
        ].apply(eu.merge_lists)

        eu_outcome_tdf = eu_case_df[["CaseId", "outcome_list"]].explode(
            column="outcome_list"
        )

        # A given data file may not contain all possible outcome values, so
        # make sure all columns are available in the pivot
        eu_outcome_tdf = pd.pivot_table(
            eu_outcome_tdf,
            index="CaseId",
            columns="outcome_list",
            aggfunc=lambda x: True,
        ).reset_index()

        # Add any missing outcome columns
        for c in list(eu.ALL_EU_OUTCOMES.columns):
            if c not in eu_outcome_tdf.columns:
                self.logger.info(f"Adding missing column: {c}")
                eu_outcome_tdf[c] = np.NaN

        eu_outcome_tdf["PatientOutcome"] = eu_outcome_tdf.apply(
            OutcomeMapper.derive_outcomes,
            axis=1,
            dataset=self.data_source,
        )

        eu_outcome_tdf["PatientRecovered"] = (
            eu_outcome_tdf["Recovered/Resolved"]
            | eu_outcome_tdf["Recovered/Resolved With Sequelae"]
        )

        eu_case_df = eu_case_df.merge(
            eu_outcome_tdf[["CaseId", "PatientOutcome", "PatientRecovered"]],
            on=["CaseId"],
            how="left",
        )

        eu_case_df["HospitalizationLengthInDays"] = np.NaN
        eu_case_df["ReportType"] = eu_case_df["Report Type"]

        final_df = eu_case_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
