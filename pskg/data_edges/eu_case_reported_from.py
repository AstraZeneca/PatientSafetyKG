###
### Build Case to Adverse Event Edges from EudraVigilance data
###

import logging
from data_prep.outcomes import OutcomeMapper

from . import case_reported_from
from data_prep import eudravigilance as eu
from data_prep.meddra import generate_type_meddra_id


class EudraVigilanceCaseReportedFrom(case_reported_from.CaseReportedFrom):
    """
    Manage conversion from raw data to CaseReportedAEMeddraTerm.tsv
    """

    data_source = "EUDRAVIGILANCE"
    data_set = "EUDRAVIGILANCE"
    case_id_column_name = "Worldwide Unique Case Identification"
#    case_id_column_name = "EU Local Number"
    case_date_column_name = "EV Gateway Receipt Date"

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
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
        self.logger = logging.getLogger("pskg_loader.EudraVigilanceCaseReportedFrom")
        self.data_set_tag = data_set_tag
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
            columns=[self.case_id_column_name, self.case_date_column_name],
        )

        self.manifest_data.append(
            self.get_manifest_data(df=eu_df, tag=self.data_set_tag)
        )

        # Get CaseID, and turn AEs into a list, and then split that list into usable components
        eu_df["CaseId"] = eu_df.apply(eu.derive_case_id,  native_id_column=self.case_id_column_name, axis=1)
        eu_df["Country"] = eu_df[self.case_id_column_name].apply(eu.derive_country)
        eu_df["SubRegion"] = ""

        final_df = eu_df[self._output_columns].drop_duplicates()
        final_df.to_csv(output_stream, header=False, index=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} written.")
