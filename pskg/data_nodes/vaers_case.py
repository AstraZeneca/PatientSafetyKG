###
### Build Cases from VAERS data
###

import logging

from data_prep import vaers
from data_prep.outcomes import OutcomeMapper
from . import case


class VaersCase(case.Case):
    """
    Manage conversion from raw data source structure Case.tsv output structure
    """

    data_source = "VAERS"
    data_set = "VAERS"
    vaers_data_component = "VAERSDATA"

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None):
        """
        Create a new VaersCase object.

        Parameters
        data_set_tag:   str
            VAERS data set name, e.g. 2021.
        s3_bucket: obj, optional
            S3 bucket, defau
            lts to None
        s3_key: str, optional
            key within s3_bucket to data zip file
        file_path: Path
            Path to local file system zip file
        """
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)

        self.data_file = f"{data_set_tag}{self.vaers_data_component}.csv"
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(f"pskg_loader.VaersCase")
        self.logger.info(f"Created {self.data_set_tag} {self}")

    def write_objects(self, output_stream):
        """
        Construct VAERS case data from given zip file and write it to an existing open output_stream.  Caller is responsible for
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

        self.logger.info(f"Loading: {self.data_file} from {self.source_url}")
        vaers_df = vaers.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            internal_file_name=self.data_file,
            file_type=self.vaers_data_component,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=vaers_df, tag=self.data_file)
        )

        vaers_outcomes = vaers_df.apply(
            OutcomeMapper.derive_outcomes, axis=1, dataset=self.data_set
        )

        vaers_df["CaseId"] = vaers_df.apply(vaers.derive_case_id, axis=1)
        vaers_df["SourceCaseId"] = vaers_df["VAERS_ID"]
        vaers_df["Tag"] = self.data_set_tag
        vaers_df["DataSource"] = self.data_source
        vaers_df["ReportedDate"] = vaers_df["RPT_DATE"].dt.date
        vaers_df["ReceivedDate"] = vaers_df["RECVDATE"].dt.date
        vaers_df["PatientAgeRangeMin"] = vaers_df["AGE_YRS"]
        vaers_df["PatientAgeRangeMax"] = vaers_df["AGE_YRS"]
        vaers_df["PatientGender"] = vaers_df["SEX"]
        vaers_df["PatientOutcome"] = vaers_outcomes
        vaers_df["DeathDate"] = vaers_df["DATEDIED"].dt.date
        vaers_df["HospitalizationLengthInDays"] = vaers_df["NUMDAYS"]
        vaers_df["ReportType"] = ""
        vaers_df["PatientRecovered"] = vaers_df["RECOVD"]

        final_df = vaers_df[self._output_columns].drop_duplicates()
        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
