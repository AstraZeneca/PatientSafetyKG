###
###
###

import logging
from numpy import DataSource
from . import country_has_exposure
from data_prep import cdc


class CDCCountryHasExposure(country_has_exposure.CountryHasExposure):
    data_source = "CDC"

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.country_code = "US"
        self.logger = logging.getLogger("pskg_loader.CDCCountryHasExposure")
        self.logger.info(f"Created: {self}")

    def write_objects(self, output_stream):
        """
        Assemble CDC expsoure data from source file and persist to provided output stream.  Caller is responsible for
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

        # Read in raw CDC data with minimal transformation applied
        df = cdc.raw_load(self.s3_bucket, self.s3_key, self.file_path)

        self.manifest_data.append(self.get_manifest_data(df=df))

        df["DataSource"] = self.data_source
        df["CountryCode"] = self.country_code

        final_df = df[self._output_columns].drop_duplicates()
        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} written.")
