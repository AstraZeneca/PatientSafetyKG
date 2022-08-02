import logging

import numpy as np

from data_nodes.eu_drug import EudraVigilanceHelper
from data_prep import eudravigilance as eu
from data_prep import id_management as idm
from . import case_administered_vaccine


class EudraVigilanceAdministeredVaccine(
    case_administered_vaccine.CaseAdministeredVaccine, EudraVigilanceHelper
):
    """
    Manage conversion from raw data to vaccine administered
    """

    data_source = "EUDRAVIGILANCE"
    data_set = "EUDRAVIGILANCE"
    reaction_column_name = (
        "Reaction List PT (Duration – Outcome - Seriousness Criteria)"
    )
    suspect_column_name = "Suspect/interacting Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])"
    case_id_column_name = "Worldwide Unique Case Identification"

    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
        """
        Create a new class EudraVigilanceAdministeredVaccine object.

        Parameters
        ----------
        data_version: str
            formatted date string for identifying specific files within the given bucket/key, e.g. 10Oct2021
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
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger("pskg_loader.EudraVigilanceAdministeredVaccine")
        self.logger.info(f"Created {self}")
        self.ev_source = ev_source
        if self.ev_source == "Public":
            self.case_id_column_name = "EU Local Number"

    def write_objects(self, output_stream):

        self.manifest_data = []

        eu_suspect_med_df, eu_concom_med_df = self.get_all_drugs_df(
            drug_filter="vaccine", ev_source=self.ev_source
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_suspect_med_df, tag=f"{self.data_set_tag}.AdmSuspectVaccines"
            )
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_concom_med_df, tag=f"{self.data_set_tag}.AdmConcomVaccines"
            )
        )

        if not eu_suspect_med_df.empty:
            eu_suspect_med_df["VaccineId"] = eu_suspect_med_df.apply(
                idm.get_vaccine_id, data_source=self.data_source, axis=1
            )

            eu_suspect_med_df["VaccineDate"] = ""
            eu_suspect_med_df["VaccineLot"] = ""

            eu_suspect_med_df[["Dosage", "Units"]] = eu_suspect_med_df.apply(
                self._extract_dose_unit, axis=1
            )
            eu_suspect_med_df["VaccineSite"] = ""
            eu_suspect_med_df["VaccineRoute"] = eu_suspect_med_df["route"]
            eu_suspect_med_df["Duration"] = eu_suspect_med_df["duration"].apply(
                eu.convert_duration_to_days
            )
            eu_suspect_med_df["Characterization"] = np.where(
                eu_suspect_med_df["characterization"].fillna("") == "",
                eu_suspect_med_df["characterization"],
                eu_suspect_med_df["characterization"].map(
                    {"I": "Interacting", "S": "Suspect", "C": "Concomitant"}
                ),
            )

            suspect_final_df = eu_suspect_med_df[self._output_columns].drop_duplicates()
            if not suspect_final_df.empty:
                suspect_final_df.to_csv(
                    output_stream, header=None, index=False, sep="\t", mode="a"
                )

                self.logger.info(f"{len(suspect_final_df)} rows written (suspect).")
        else:
            self.logger.warning(f"No suspect vaccines found in {self.source_url}.")

        if not eu_concom_med_df.empty:
            eu_concom_med_df["VaccineId"] = eu_concom_med_df.apply(
                idm.get_vaccine_id, data_source=self.data_source, axis=1
            )

            eu_concom_med_df["VaccineDate"] = ""
            eu_concom_med_df["VaccineLot"] = ""

            eu_concom_med_df[["Dosage", "Units"]] = eu_concom_med_df.apply(
                self._extract_dose_unit, axis=1
            )

            eu_concom_med_df["VaccineSite"] = ""
            eu_concom_med_df["Duration"] = eu_concom_med_df["duration"].apply(
                eu.convert_duration_to_days
            )
            eu_concom_med_df["VaccineRoute"] = eu_concom_med_df["route"]
            eu_concom_med_df["Characterization"] = np.where(
                eu_concom_med_df["characterization"].fillna("") == "",
                eu_concom_med_df["characterization"],
                eu_concom_med_df["characterization"].map(
                    {"I": "Interacting", "S": "Suspect", "C": "Concomitant"}
                ),
            )

            concom_final_df = eu_concom_med_df[self._output_columns].drop_duplicates()
            if not concom_final_df.empty:
                concom_final_df.to_csv(
                    output_stream, header=None, index=False, sep="\t", mode="a"
                )

                self.logger.info(f"{len(concom_final_df)} rows written (concomitant).")
        else:
            self.logger.info(f"No concomitant medications found in {self.source_url}.")
