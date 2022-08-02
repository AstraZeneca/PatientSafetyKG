###
### Build Case to Adverse Event Edges from VAERS data
###

import logging
from typing import MutableSequence

import pandas as pd

from . import case_reported_ae_meddra_term

from data_prep import vaers
from data_prep.meddra import generate_meddra_id


class VaersCaseReportedAEMeddraTerm(
    case_reported_ae_meddra_term.CaseReportedAEMeddraTerm
):
    """
    Manage conversion from raw data to CaseReportedAEMeddraTerm.tsv
    """

    data_source = "VAERS"
    data_set = "VAERS"
    data_file_type = "VAERSDATA"
    symptom_data_file_type = "VAERSSYMPTOMS"

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None):
        """
        Create a new VaersCaseReportedAEMeddraTerm object.

        Parameters
        data_set_tag:   str
            VAERS data set name, e.g. 2021.
        s3_bucket: obj, optional
            S3 bucket, defaults to None
        s3_key: str, optional
            key within s3_bucket to data zip file
        file_path: Path
            Path to local file system zip file
        """
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)

        self.data_file = f"{data_set_tag}{self.data_file_type}.csv"
        self.symptom_data_file = f"{data_set_tag}{self.symptom_data_file_type}.csv"
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger("pskg_loader.VaersCaseReportedAEMeddraTerm")
        self.logger.info(f"Created {data_set_tag} {self}")

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

        vaers_df = vaers.raw_load(
            internal_file_name=self.data_file,
            file_type=self.data_file_type,
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=vaers_df, tag=self.data_file)
        )

        vaers_symptoms_df = vaers.raw_load(
            internal_file_name=self.symptom_data_file,
            file_type=self.symptom_data_file_type,
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=vaers_df, tag=self.symptom_data_file_type)
        )

        # Collapse multiple SYMPTOM columns to
        # | VAERS_ID | MeddraId |
        melted_df = pd.melt(
            vaers_symptoms_df,
            id_vars=["VAERS_ID"],
            value_vars=["SYMPTOM1", "SYMPTOM2", "SYMPTOM3", "SYMPTOM4", "SYMPTOM5"],
            var_name="Symptom",
            value_name="MeddraTerm",
        ).dropna()

        # Gather result structure:
        # | VAERS_ID | ONSET_DATE | NUM_DAYS | values
        # Reset columns to:
        # | VAERS_ID | OnsetDate | LengthInDays | MeddraId
        result_df = vaers_df[["VAERS_ID", "ONSET_DATE", "NUMDAYS"]].merge(
            melted_df[["VAERS_ID", "MeddraTerm"]], on=["VAERS_ID"]
        )
        result_df.columns = ["VAERS_ID", "OnsetDate", "LengthInDays", "MeddraTerm"]
        result_df["CaseId"] = result_df.apply(vaers.derive_case_id, axis=1)

        final_df = result_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, header=False, index=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} written.")
