###
### Build Case to Adverse Event Edges from EudraVigilance data
###

import logging

import numpy as np

from data_nodes.eu_drug import EudraVigilanceHelper
from data_prep import eudravigilance as eu
from data_prep import id_management as idm
from . import case_prescribed_medication


class EudraVigilanceCasePrescribedMedication(
    case_prescribed_medication.CasePrescribedMedication, EudraVigilanceHelper
):
    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
        """
        Create a new EudraVigilance case prescribed medication contains object.  Either an s3_bucket and s3_key
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
        super().__init__(s3_bucket, s3_key, file_path)
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(
            f"pskg_loader.EudraVigilanceCasePrescribedMedication"
        )
        self.logger.info(f"Created {self}")
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

        _, eu_concom_med_df = self.get_all_drugs_df(drug_filter="medication", ev_source=self.ev_source)

        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_concom_med_df, tag=f"{self.data_set_tag}.RxConcomMedications"
            )
        )

        if not eu_concom_med_df.empty:
            eu_concom_med_df["MedicationId"] = eu_concom_med_df.apply(
                idm.get_medication_id, data_source=self.data_source, axis=1
            )
            eu_concom_med_df["Evidence"] = ""
            eu_concom_med_df["StartDate"] = ""
            eu_concom_med_df["StopDate"] = ""
            eu_concom_med_df["Duration"] = eu_concom_med_df["duration"].apply(
                eu.convert_duration_to_days
            )

            eu_concom_med_df["Characterization"] = np.where(
                eu_concom_med_df["characterization"].fillna("") == "",
                eu_concom_med_df["characterization"],
                eu_concom_med_df["characterization"].map(
                    {"I": "Interacting", "S": "Suspect", "C": "Concomitant"}
                ),
            )

            eu_concom_med_df[["Dosage", "Units"]] = eu_concom_med_df.apply(
                self._extract_dose_unit, axis=1
            )
            eu_concom_med_df["Route"] = eu_concom_med_df["route"]

            concom_final_df = eu_concom_med_df[self._output_columns].drop_duplicates()
            if not concom_final_df.empty:
                concom_final_df.to_csv(
                    output_stream, header=False, index=False, sep="\t", mode="a"
                )

                self.logger.info(f"{len(concom_final_df)} rows written (concomitant).")
        else:
            self.logger.info(f"No concomitant medications found in {self.source_url}.")
