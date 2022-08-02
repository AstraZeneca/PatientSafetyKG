###
### Build Case to Adverse Event Edges from EudraVigilance data
###

import pandas as pd
import logging
from . import case_reported_ae_meddra_term
from data_prep import eudravigilance as eu
from data_prep.meddra import generate_type_meddra_id


class EudraVigilanceCaseReportedAEMeddraTerm(
    case_reported_ae_meddra_term.CaseReportedAEMeddraTerm
):
    """
    Manage conversion from raw data to CaseReportedAEMeddraTerm.tsv
    """

    data_source = "EUDRAVIGILANCE"
    data_set = "EUDRAVIGILANCE"
    reaction_column_name = (
        "Reaction List PT (Duration – Outcome - Seriousness Criteria)"
    )
    case_id_column_name = "Worldwide Unique Case Identification"
    case_date_column_name = "EV Gateway Receipt Date"

    def __init__(
        self,
        data_set_tag,
        s3_bucket=None,
        s3_key=None,
        file_path=None,
        prefix="EudraVigilance",
        ev_source=None
    ):
        """
        Create a new EudraVigilanceCaseReportedAEMeddraTerm object, used
        for building up the case--[:REPORTED_AE]->meddra relationship

        Parameters
        ----------
        s3_bucket: str
            S3 bucket name
        prefix: str, optional
            Prefix to search in s3_bucket for data files, defaults to EudraVigilance
        ev_source: str
            EudraVigilance data source: public or EVDAS, configured in config.yml, indicates EV data source is public site or from the EVDAS system. 
            public site does not contain "Worldwide Unique Case Identification" used to identiy case country.
            If data source is Public "Worldwide Unique Case Identification" is replaced with "EU Local Number"
        """
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.s3_prefix = prefix
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(
            "pskg_loader.EudraVigilanceCaseReportedAEMeddraTerm"
        )
        self.logger.info(f"Created {self}")
        self.ev_source = ev_source

    def write_objects(self, output_stream):
        """
        Constrct a dataframe with reported AEs, and write to provided output_stream

        Parameters
        ----------
        output_stream: object
            Open stream for output, data will be appended to this stream

        Returns
        -------
        None
        """
        self.manifest_data = []

        # Load required columns from raw data
        if self.ev_source == "Public":
            self.case_id_column_name = "EU Local Number"

        eu_df = eu.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            columns=[
                self.case_id_column_name,
                self.case_date_column_name,
                self.reaction_column_name,
            ],
        )

        self.manifest_data.append(self.get_manifest_data(df=eu_df))

        # Get CaseID, and turn AEs into a list, and then split that list into usable components
        eu_df["CaseId"] = eu_df.apply(eu.derive_case_id, native_id_column=self.case_id_column_name, axis=1)
        eu_df["reaction_list_pt"] = eu_df[self.reaction_column_name].apply(
            eu.ev_split_break
        )

        eu_df[
            ["terms_list", "duration_list", "outcome_list", "seriousness_criteria_list"]
        ] = eu_df[["reaction_list_pt"]].apply(
            lambda x: eu.ev_extract_term(x["reaction_list_pt"]), axis=1
        )

        result = {"CaseId": [], "MeddraTerm": [], "LengthInDays": []}
        for _, row in eu_df.iterrows():
            for m, d in list(zip(row["terms_list"], row["duration_list"])):
                result["CaseId"].append(row["CaseId"])
                result["MeddraTerm"].append(m)
                result["LengthInDays"].append(eu.convert_duration_to_days(d))

        eu_all_ae_df = pd.DataFrame.from_dict(result)

        # Explode to:
        # CaseId | terms_list
        # id     | term:duration

        eu_all_ae_df["OnsetDate"] = ""

        final_df = eu_all_ae_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, sep="\t", header=False, mode="a")

        self.logger.info(f"{len(eu_all_ae_df)} written.")
