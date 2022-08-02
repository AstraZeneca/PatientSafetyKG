###
### Build Vaccines from EudraVigilance data
###

import logging
from pathlib import Path
import re

import pandas as pd
import numpy as np

from . import drug
from data_prep import id_management as idm, eudravigilance as eu


class EudraVigilanceHelper(object):
    data_source = "EUDRAVIGILANCE"
    suspect_list_column = "Suspect/interacting Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])"
    concom_list_column = "Concomitant/Not Administered Drug List (Drug Char - Indication PT - Action taken - [Duration - Dose - Route])"
    eu_raw_id_column = "Worldwide Unique Case Identification"
    primary_source_column = "Primary Source Country for Regulatory Purposes"
    eu_gateway_receipt_date = "EV Gateway Receipt Date"
    raw_eu_columns = [
        eu_raw_id_column,
        concom_list_column,
        suspect_list_column,
        eu_gateway_receipt_date,
        primary_source_column,
    ]

    _dose_re = re.compile("^(?P<dose>[0-9]+)(\.[0-9]*){0,1}(?P<unit>.*)")

    allowed_drug_type_filters = ["medication", "vaccine"]

    def get_all_drugs_df(self, drug_filter=None, ev_source=None):
        """
        Load raw EudraVigilance case data into a dataframe, and break out
        suspect and concomittant medications in separate dataframes, optionally
        filtering to vaccine or medications only

        Parameters
        ----------
        drug_type: str
            Filter to given type, only "vaccine" or "medication" are supported
        ev_source: str
            EudraVigilance data source: public or EVDAS, configured in config.yml, indicates EV data source is public site or from the EVDAS system. 
            public site does not contain "Worldwide Unique Case Identification" used to identiy case country.
            If data source is Public "Worldwide Unique Case Identification" is replaced with "EU Local Number"            
        """

        if drug_filter and drug_filter not in self.allowed_drug_type_filters:
            raise ValueError(
                f"Filter type '{drug_filter}', {self.allowed_drug_type_filters} are allowed"
            )

        if ev_source == "Public":
            self.eu_raw_id_column = "EU Local Number"
            self.raw_eu_columns[0] =  self.eu_raw_id_column

            # Gather only columns needed
        result_df = eu.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            columns=self.raw_eu_columns,
        )

        # suspect and concommitant lists can contain entries for drugs other than vaccines.  NOTE: this must be handled more
        # effectively using a controlled terminology

        result_df["CaseId"] = result_df.apply(eu.derive_case_id, native_id_column=self.eu_raw_id_column, axis=1)
        result_df["suspect_drug_list"] = result_df[self.suspect_list_column].apply(
            eu.ev_split_break
        )
        result_df["concom_drug_list"] = result_df[self.concom_list_column].apply(
            eu.ev_split_break
        )
        self.logger.info(f"concom_drug_list:  {result_df['concom_drug_list']}")

        # Extract vaccines and other medications
        # Concomittant drugs are processed every time, suspects are only considered for the vaccine filter
        eu_suspect_df = None
        eu_concom_df = eu.ev_extract_drug_details(
            result_df, drug_column="concom_drug_list"
        )

        if drug_filter == "vaccine":
            eu_suspect_df = eu.ev_extract_drug_details(
                result_df, drug_column="suspect_drug_list"
            )

            # filter vaccines (note: assume all suspect meds are vaccines until there
            # is a means to precisely differentiate them)
            eu_concom_df = eu_concom_df.loc[
                (eu_concom_df["indication"].str.contains("immunisation", case=False))
                | eu_concom_df["drug"].str.contains("vax", case=False)
                | (eu_concom_df["drug"].str.contains("vaccine", case=False))
            ].copy()

            # Assign key columns (suspect)
            eu_suspect_df["OriginalName"] = eu_suspect_df["drug"]
            eu_suspect_df["Indication"] = eu_suspect_df["indication"]
            eu_suspect_df["Manufacturer"] = eu_suspect_df["drug"].apply(
                eu.ev_simple_classify_manufacturer
            )

            eu_suspect_df["GenericName"] = eu_suspect_df["drug"].apply(
                eu.ev_simple_standardize_generic_drug_names
            )

            # NOTE: Replace thes with a webservice based lookup, e.g. NIH Daily Med
            eu_suspect_df["TradeName"] = eu_suspect_df["drug"].apply(
                eu.ev_simple_standardize_trade_drug_name
            )
            eu_suspect_df["RxNormCui"] = ""
            eu_suspect_df["Description"] = ""

        elif drug_filter == "medication":
            # filter to just medications, again suspect are just
            # assumed to be vaccines so do not include them at all here
            eu_concom_df = eu_concom_df.loc[
                ~(
                    eu_concom_df["indication"].str.contains("immunisation", case=False)
                    | eu_concom_df["drug"].str.contains("vax", case=False)
                    | (eu_concom_df["drug"].str.contains("vaccine", case=False))
                )
            ].copy()

        # Assign key columns (concomitant)
        eu_concom_df["OriginalName"] = eu_concom_df["drug"]
        eu_concom_df["Indication"] = eu_concom_df["indication"]
        eu_concom_df["Manufacturer"] = eu_concom_df["drug"].apply(
            eu.ev_simple_classify_manufacturer
        )

        eu_concom_df["GenericName"] = eu_concom_df["drug"].apply(
            eu.ev_simple_standardize_generic_drug_names
        )

        # NOTE: Replace this with a webservice based lookup, e.g. NIH Daily Med
        eu_concom_df["TradeName"] = eu_concom_df["drug"].apply(
            eu.ev_simple_standardize_trade_drug_name
        )
        eu_concom_df["RxNormCui"] = ""
        eu_concom_df["Description"] = ""

        return eu_suspect_df, eu_concom_df

    def _extract_dose_unit(self, row):
        """
        Parse an EV Dose string an extract the dose and unit.
        This function is intended to be called via apply with axis =1

        Parameters
        ----------
        row: dict
            Row dictionary with a dose column

        Returns
        pd.Series
            with dose and unit as separate columns
        """

        if row["dose"] is None:
            row["Dose"] = ""
            row["Units"] = ""
        elif row["dose"] == "n/a":
            row["Dose"] = ""
            row["Units"] = ""
        else:
            m = self._dose_re.match(row["dose"])
            if m:
                row["Dose"] = m.group("dose")
                row["Units"] = m.group("unit")
            else:
                row["Dose"] = row["dose"]
                row["Units"] = ""

        return pd.Series(data=[row["Dose"], row["Units"]], index=["Dosage", "Unit"])


class EudraVigilanceVaccine(drug.Vaccine, EudraVigilanceHelper):
    def __init__(self, data_set_tag, s3_bucket=None, s3_key=None, file_path=None, ev_source=None):
        """
        Create a new EudraVigilanceVaccine generator object.  Either an s3_bucket and s3_key
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
        self.logger = logging.getLogger(f"pskg_loader.EudraVigilanceVaccine")
        self.ev_source = ev_source

    def write_objects(self, output_stream):
        """
        Construct vaccine data from a EudraVigilance Line Listing format file and write it to an existing open output_stream.  Caller is responsible for
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

        eu_suspect_vax_df, eu_concom_vax_df = self.get_all_drugs_df(
            drug_filter="vaccine", ev_source=self.ev_source
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_suspect_vax_df, tag=f"{self.data_set_tag}.SuspectVaccines"
            )
        )
        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_concom_vax_df, tag=f"{self.data_set_tag}.ConcomVaccines"
            )
        )

        if not eu_suspect_vax_df.empty:
            eu_suspect_vax_df["VaccineId"] = eu_suspect_vax_df.apply(
                idm.get_vaccine_id, data_source=self.data_source, axis=1
            )

            eu_suspect_vax_df["VaxType"] = eu_suspect_vax_df["drug"].apply(
                eu.ev_simple_vax_type
            )

            suspect_final_df = eu_suspect_vax_df[self._output_columns].drop_duplicates()
            if not suspect_final_df.empty:
                suspect_final_df.to_csv(
                    output_stream, header=False, index=False, sep="\t"
                )

                self.logger.info(f"{len(suspect_final_df)} rows written (suspect).")
        else:
            self.logger.warning(f"No suspect vaccines found in {self.source_url}.")

        if not eu_concom_vax_df.empty:
            eu_concom_vax_df["VaccineId"] = eu_concom_vax_df.apply(
                idm.get_vaccine_id, data_source=self.data_source, axis=1
            )
            eu_concom_vax_df["VaxType"] = eu_concom_vax_df["drug"].apply(
                eu.ev_simple_vax_type
            )

            concom_final_df = eu_concom_vax_df[self._output_columns].drop_duplicates()
            if not concom_final_df.empty:
                concom_final_df.to_csv(
                    output_stream, header=False, index=False, sep="\t"
                )

                self.logger.info(f"{len(concom_final_df)} rows written (concomitant).")
        else:
            self.logger.info(f"No concomitant vaccines found in {self.source_url}.")


class EudraVigilanceMedication(drug.Medication, EudraVigilanceHelper):
    """
    Generator class for EudraVigilance medications (i.e. concomitant medications)
    """

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
            Path to local eudravigilance  data file
        ev_source: str
            EudraVigilance data source: public or EVDAS, configured in config.yml, indicates EV data source is public site or from the EVDAS system. 
            public site does not contain "Worldwide Unique Case Identification" used to identiy case country.
            If data source is Public "Worldwide Unique Case Identification" is replaced with "EU Local Number"
        """

        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.data_set_tag = data_set_tag
        self.logger = logging.getLogger(
            f"pskg_loader.eudravigilance.EudraVigilanceMedication"
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

        _, eu_concom_med_df = self.get_all_drugs_df(drug_filter="medication", ev_source=self.ev_source)

        self.manifest_data.append(
            self.get_manifest_data(
                df=eu_concom_med_df, tag=f"{self.data_set_tag}.ConComMedication"
            )
        )

        if not eu_concom_med_df.empty:
            eu_concom_med_df["MedicationId"] = eu_concom_med_df.apply(
                idm.get_medication_id, data_source=self.data_source, axis=1
            )

            # Special case since VAERS does not have AZ1222 currently
            eu_concom_med_df.loc[
                eu_concom_med_df["MedicationId"] == "ALIGNED:AstraZeneca", ["VaxType"]
            ] = "COVID19"

            concom_final_df = eu_concom_med_df[self._output_columns].drop_duplicates()
            if not concom_final_df.empty:
                concom_final_df.to_csv(
                    output_stream, header=False, index=False, sep="\t"
                )

                self.logger.info(f"{len(concom_final_df)} rows written (concomitant).")
        else:
            self.logger.info(f"No concomitant medications found in {self.source_url}.")
