import logging

from data_prep import az_exposure
from . import exposure


class AzExposure(exposure.Exposure):
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
        Path to local eudravigilance data file
    """

    data_source = "AZExposure"
    data_tag = "AZ Exposure"

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

        self.manifest_data = []

        # Read in raw AZ data with minimal transformation applied
        self.logger.info(f"Reading: {self.source_url}")

        df = az_exposure.raw_load(
            input_bucket=self.s3_bucket, input_key=self.s3_key, file_path=self.file_path
        )

        self.manifest_data.append(self.get_manifest_data(df=df, tag=self.data_tag))

        final_df = df[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, header=False, sep="\t", mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
