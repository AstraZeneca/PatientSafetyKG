###
### Exposure data from AZ data source
###

from pathlib import Path
import logging
from . import vaccine_has_exposure
from data_prep import az_exposure, id_management as idm


class AZVaccineHasExposure(vaccine_has_exposure.VaccineHasExposure):
    data_source = "AZExposure"

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.logger = logging.getLogger("pskg_loader.AZVaccineHasExposure")
        self.logger.info(f"Created: {self}")

    def write_objects(self, output_stream):
        """
        Construct a dataframe with country and exposure data, and write to provided output_stream

        Parameters
        ----------
        output_stream: object
            Open stream for output, data will be appended to this stream

        Returns
        -------
        None
        """
        self.manifest_data = []

        # Read in raw AZ Exposure Data
        df = az_exposure.raw_load(self.s3_bucket, self.s3_key, self.file_path)

        self.manifest_data.append(self.get_manifest_data(df))

        df["drug"] = "VAXZEVRIA"
        df["Manufacturer"] = "AstraZeneca"
        df["VaccineId"] = df.apply(
            idm.get_vaccine_id, data_source=self.data_source, axis=1
        )

        final_df = df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} written.")
