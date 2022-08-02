###
### Class Defining Case.tsv structure
###

import logging
import pandas as pd
import numpy as np
from graph_objects import utils
from data_prep import s3_utils, geocoding


class Country(utils.Generator):
    _output_columns = [
        "Name",
        "CountryCode",
        "Population",
        "AgeDistribution",
        "SocioEconomicDistribution",
        "GenderDistribution",
        "RacialDistribution",
        "InformationDate",
    ]

    data_tag = "static-country-geocoding"

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.logger = logging.getLogger(f"pskg_loader.{self.data_tag}")
        self.logger.info(f"Created {self}")

    def write_objects(self, output_stream):
        """
        Assemble country data from source file and persist to provided output stream.  Caller is responsible for
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

        df = geocoding.raw_load(
            input_bucket=self.s3_bucket,
            input_key=self.s3_key,
            file_path=self.file_path,
        )

        self.manifest_data.append(self.get_manifest_data(df=df))

        df["Name"] = df["name"]
        df["CountryCode"] = df["alpha-2"]
        df["Population"] = np.NAN
        df["AgeDistribution"] = ""
        df["SocioEconomicDistribution"] = ""
        df["GenderDistribution"] = ""
        df["RacialDistribution"] = ""
        df["InformationDate"] = ""
        df["InformationDate"] = df["InformationDate"].astype("datetime64[ns]")

        df[self._output_columns].to_csv(
            output_stream, index=False, header=False, sep="\t"
        )


class Continent(utils.Generator):
    _output_columns = [
        "ContinentCode",
        "Name",
    ]

    data_tag = "static-continent-geocoding"

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.logger = logging.getLogger(f"pskg_loader.{self.data_tag}")
        self.logger.info(f"Created {self}")

    def write_objects(self, output_stream):
        """
        Assemble continent data from source file and persist to provided output stream.  Caller is responsible for
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

        df = geocoding.raw_load(
            input_bucket=self.s3_bucket, input_key=self.s3_key, file_path=self.file_path
        )

        self.manifest_data.append(self.get_manifest_data(df=df))

        df["ContinentCode"] = df["region-code"].replace("", np.nan)
        df["Name"] = df["region"]
        final_df = (
            df[self._output_columns]
            .drop_duplicates()
            .reset_index(drop=True)
            .dropna(axis=0, how="any")
        )

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
