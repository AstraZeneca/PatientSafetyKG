###
### Class Defining MeddraCq.tsv structure
###

import logging

from data_prep import meddracq as mcqdp
from graph_objects import utils


class MeddraCq(utils.Generator):
    _output_columns = [
        "Name",
        "Abbreviation",
        "Description",
        "Authors",
        "CreatedDate",
        "Source",
    ]

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=file_path)
        self.logger = logging.getLogger("pskg_loader.MeddraCq")
        if not self.file_path.exists():
            self.logger.error(
                f"Specified file path {file_path.resolve()} does not exist"
            )
            raise ValueError(
                f"Specified file path {file_path.resolve()} does not exist"
            )
        self.logger.info(f"Created {self}")

    def write_objects(self, output_stream):
        results = mcqdp.read_raw_meta_data(self.file_path)

        self.manifest_data = []

        self.manifest_data.append(self.get_manifest_data(df=results))

        final_df = results[self._output_columns].drop_duplicates()

        final_df.to_csv(output_stream, index=False, sep="\t", header=False, mode="a")

        self.logger.info(f"{len(final_df)} rows written.")
