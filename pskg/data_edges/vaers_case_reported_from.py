###
### Build Case to country links, VAERS
###
### TODO: Bring in state level stratification.
###


import logging
from . import case_reported_from
from data_prep import vaers


class VaersCaseReportedFrom(case_reported_from.CaseReportedFrom):
    """
    Manage conversion from raw data to CaseReportedFrom.tsv
    """

    data_source = "VAERS"
    data_set = "VAERS"
    data_file_type = "VAERSDATA"

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None):
        """
        Create a new VaersCaseReportedFrom object.

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
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger("pskg_loader.VaersCaseReportedFrom")
        self.logger.info(f"Create {data_set_tag} {self}")

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

        # Gather result structure:
        # | VAERS_ID | ONSET_DATE | NUM_DAYS | values
        # Reset columns to:
        # | VAERS_ID | OnsetDate | LengthInDays | MeddraId
        result_df = vaers_df[["VAERS_ID", "STATE"]].copy()
        result_df["CaseId"] = result_df.apply(vaers.derive_case_id, axis=1)

        # FYI in NonDomesticVaers state will be "FR"
        result_df["SubRegion"] = result_df["STATE"]
        result_df["Country"] = "US"

        final_df = result_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, header=False, index=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} written.")
