###
### Class Defining MeddraCqLinks.tsv structure
###

import logging
from graph_objects import utils
from data_prep import meddracq as mcqdp
from graph_objects import utils


class MeddraCqLink(utils.Generator):
    _output_columns = ["Name", "PT"]

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        if s3_bucket or s3_key:
            raise ValueError("Currently only a local project path can specified")
        self.logger = logging.getLogger(f"pskg_loader.meddra_cq_links")
        if not self.file_path.exists():
            self.logger.error(
                f"Specified file path {file_path.resolve()} does not exist"
            )
            raise ValueError(
                f"Specified file path {file_path.resolve()} does not exist"
            )
        self.logger.info(f"Created {self}")

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
        results = mcqdp.read_raw_links(self.file_path)

        self.manifest_data = []
        self.manifest_data.append(self.get_manifest_data(df=results))

        final_df = results.drop_duplicates()

        final_df.to_csv(output_stream, sep="\t", index=False, header=False, mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
