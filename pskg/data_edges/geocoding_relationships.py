###
### Class Defining CountryHasExposure.tsv
###

from graph_objects import utils
from data_prep import geocoding
import numpy as np
import logging


class CountryInContinent(utils.Generator):
    _output_columns = ["CountryCode", "ContinentCode"]

    data_tag = "static-country-incontinent-geocoding"

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

        df["CountryCode"] = df["alpha-2"].replace("", np.nan)
        df["ContinentCode"] = df["region-code"].replace("", np.nan)

        final_df = (
            df[self._output_columns]
            .drop_duplicates()
            .reset_index(drop=True)
            .dropna(axis=0, how="any")
        )

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
