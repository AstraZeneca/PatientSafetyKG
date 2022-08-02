###
### Exposure data from AZ data source
###

from pathlib import Path
import logging

from numpy import append
from . import country_has_exposure
from data_prep import az_exposure, geocoding


class AZCountryHasExposure(country_has_exposure.CountryHasExposure):
    def __init__(
        self,
        s3_bucket=None,
        s3_key=None,
        file_path=None,
        s3_country_ref_key=None,
        country_ref_file_path=None,
    ):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)

        if country_ref_file_path:
            if isinstance(country_ref_file_path, str):
                self.country_ref_file_path = Path(country_ref_file_path)
            elif not isinstance(country_ref_file_path, Path):
                raise ValueError(
                    f"country_ref_file_path must be str or Path object, got: {country_ref_file_path}"
                )
            else:
                self.country_ref_file_path = country_ref_file_path
            self.country_ref_source_url = (
                f"file://{self.country_ref_file_path.resolve()}"
            )
            self.s3_country_ref_key = None
        elif s3_country_ref_key:
            self.s3_country_ref_key = s3_country_ref_key
            self.country_ref_file_path = None
        else:
            raise ValueError(
                f"One of s3_country_ref_key or country_ref_file_path is required"
            )
        self.logging = logging.getLogger("pskg_loader.AZCountryHasExposure")
        self.logging.info(f"Created: {self}")

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

        # read in reference mapping data
        ref_df = geocoding.raw_load(
            self.s3_bucket, self.s3_country_ref_key, self.country_ref_file_path
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=ref_df,
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_country_ref_key,
                file_path=self.country_ref_file_path,
            )
        )

        result_df = df.merge(ref_df, left_on="Country", right_on="name", how="inner")[
            ["ExposureId", "alpha-2"]
        ]

        result_df.columns = ["ExposureId", "CountryCode"]

        final_df = result_df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logging.info(f"{len(final_df)} written.")
