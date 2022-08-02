###
### Build Vacines from VAERS data
###

import logging
from pathlib import Path

import pandas as pd

from . import drug
from data_prep import id_management, s3_utils, vaers


class VaersVaccine(drug.Vaccine):
    """
    Manage conversion from raw data source structure to Vaccine.tsv output structure
    """

    data_source = "VAERS"
    data_set = "VAERS"
    vaers_data_component = "VAERSDATA"
    vaers_vax_component = "VAERSVAX"
    vaers_desc_component = "DataUseGuide"

    def __init__(
        self,
        data_set_tag,
        s3_bucket=None,
        s3_key=None,
        file_path=None,
        desc_s3_key=None,
        desc_file_path=None,
    ):
        """
        Create a new VaersVaccine object.

        Parameters
        data_set_tag:   str
            VAERS data set name, e.g. 2021.
        s3_bucket: obj, optional
            S3 bucket, defaults to None
        s3_key: str, optional
            key within s3_bucket for data zip file
        file_path: Path
            Path to local file system zip file
        desc_s3_key: str, optional
            key within s3_bucket for supplimental data file
        desc_file_path: str, optional
            Path to local VAERS description file
        """

        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)

        self.vax_data_file = f"{data_set_tag}{self.vaers_vax_component}.csv"
        self.data_file = f"{data_set_tag}{self.vaers_data_component}.csv"

        self.has_description_file = True
        self.desc_s3_key = desc_s3_key
        self.desc_file_path = desc_file_path
        if desc_file_path:
            if isinstance(desc_file_path, Path):
                self.desc_file_path = desc_file_path
            else:
                self.desc_file_path = Path(desc_file_path)
            self.desc_source_url = f"file://{self.desc_file_path.as_posix()}"
        elif desc_s3_key:
            self.desc_source_url = f"s3://{self.s3_bucket}/{self.desc_s3_key}"
        else:
            self.has_description_file = False

        self.logger = logging.getLogger(f"pskg_loader.VaersVaccine")
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

        self.logger.info(f"Loading: {self.vax_data_file} from {self.source_url}")
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

        # Gather description data, if present
        if self.has_description_file:
            self.logger.info(f"Using description data {self.desc_source_url}.")
            if self.desc_file_path:
                desc_df = pd.read_excel(self.desc_file_path)
            elif self.desc_s3_key:
                # Load up description columns
                desc_df = s3_utils.excel_file_to_data_frame(
                    self.s3_bucket, self.desc_s3_key
                )

            self.manifest_data.append(
                self.get_manifest_data(
                    df=desc_df,
                    s3_bucket=self.s3_bucket,
                    s3_key=self.desc_s3_key,
                    file_path=self.desc_file_path,
                )
            )

            vaers_vax_df = vaers_vax_df.merge(desc_df, on="VAX_TYPE", how="left")
        else:
            self.logger.info(f"No description data available.")
            vaers_vax_df["Description"] = ""

        # TODO: Refactor name resolution in favor of standards based approach (i.e.
        # use UMLS or WHO data)
        vaers_vax_df["OriginalName"] = vaers_vax_df["VAX_NAME"]
        vaers_vax_df["Manufacturer"] = vaers_vax_df["VAX_MANU"].apply(
            vaers.standardize_manufacturer_names
        )
        vaers_vax_df["TradeName"] = vaers_vax_df["VAX_NAME"].apply(vaers.get_trade_name)
        vaers_vax_df["GenericName"] = vaers_vax_df["VAX_NAME"].apply(
            vaers.get_generic_name
        )
        vaers_vax_df["VaccineId"] = vaers_vax_df.apply(
            id_management.get_vaccine_id,
            drug_column="VAX_NAME",
            manufacturer_column="Manufacturer",
            data_source=self.data_source,
            axis=1,
        )
        vaers_vax_df["RxNormCui"] = vaers_vax_df["VAX_NAME"].apply(vaers.get_rxcui)
        vaers_vax_df["VaxType"] = vaers_vax_df["VAX_TYPE"]

        final_df = vaers_vax_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
