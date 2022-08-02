###
### Build case to administered vaccines
###

import logging

from . import case_administered_vaccine

from data_prep import id_management as idm, vaers


class VaersCaseAdministeredVaccine(case_administered_vaccine.CaseAdministeredVaccine):
    vaers_vax_component = "VAERSVAX"
    vaers_data_component = "VAERSDATA"
    data_source = "VAERS"

    def __init__(self, data_set_tag, s3_bucket, s3_key, file_path):
        super().__init__(s3_bucket, s3_key, file_path)

        self.logger = logging.getLogger(f"pskg_loader.VaersCaseAdministeredVaccine")
        self.vax_data_file = f"{data_set_tag}{self.vaers_vax_component}.csv"
        self.data_file = f"{data_set_tag}{self.vaers_data_component}.csv"
        self.logger.info(f"Created {data_set_tag} {self}")

    def write_objects(self, output_stream):
        """
        Construct vaccine data and write it to an existing open output_stream.  Caller is responsible for
        creating the output stream and eventually closing it.

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
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            internal_file_name=self.data_file,
            file_type=self.vaers_data_component,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=vaers_df, tag=self.data_file)
        )

        vaers_vax_df = vaers.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
            internal_file_name=self.vax_data_file,
            file_type=self.vaers_vax_component,
        )

        self.manifest_data.append(
            self.get_manifest_data(df=vaers_vax_df, tag=self.vax_data_file)
        )

        # TODO: Refactor name resolution in favor of standards based approach (i.e.
        # use UMLS or WHO data)
        vaers_vax_df["Manufacturer"] = vaers_vax_df["VAX_MANU"].apply(
            vaers.standardize_manufacturer_names
        )
        vaers_vax_df["TradeName"] = vaers_vax_df["VAX_NAME"].apply(vaers.get_trade_name)
        vaers_vax_df["GenericName"] = vaers_vax_df["VAX_NAME"].apply(
            vaers.get_generic_name
        )
        vaers_vax_df["VaccineRoute"] = vaers_vax_df["VAX_ROUTE"]
        vaers_vax_df["VaccineSite"] = vaers_vax_df["VAX_SITE"]
        vaers_vax_df["VaccineLot"] = vaers_vax_df["VAX_LOT"]

        # VAERS does not differentiate Suspect/Interacting/Concomittant so mark "Suspect"
        # Duration is not provided in VAERS (Vaccination date is provided)
        vaers_vax_df["Characterization"] = "Suspect"
        vaers_vax_df["Dosage"] = ""
        vaers_vax_df["Units"] = ""
        vaers_vax_df["Duration"] = ""

        vaers_vax_df["VaccineId"] = vaers_vax_df.apply(
            idm.get_vaccine_id,
            drug_column="VAX_NAME",
            manufacturer_column="Manufacturer",
            data_source=self.data_source,
            axis=1,
        )

        vaers_vax_df["CaseId"] = vaers_vax_df.apply(vaers.derive_case_id, axis=1)

        # This join is necessary for VAX_DATE
        tmp_df = vaers_vax_df.merge(
            vaers_df[["VAERS_ID", "VAX_DATE"]].fillna(""), on=["VAERS_ID"], how="left"
        )

        tmp_df["VaccineDate"] = tmp_df["VAX_DATE"]
        tmp_df["Indication"] = ""  # Currently this is not available in VAERS

        final_df = tmp_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
