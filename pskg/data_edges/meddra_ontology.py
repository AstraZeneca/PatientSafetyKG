###
### Class to build Meddra Hierarchy
###

import logging
from pathlib import Path

from data_prep import meddra as raw_meddra
from graph_objects import utils


class MeddraOntology(utils.Generator):
    from_col = "MeddraIdFrom"
    to_col = "MeddraIdTo"
    primary = "PrimarySoc"

    _output_columns = [from_col, to_col, primary]

    def __init__(
        self,
        s3_bucket=None,
        s3_key=None,
        folder_path=None,
        mdhier_file="mdhier.asc",
        llt_file="llt.asc",
        release_file="meddra_release.asc",
    ):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=folder_path)

        if folder_path:
            if isinstance(folder_path, str):
                folder_path = Path(folder_path)

            self.mdhier_path = folder_path / mdhier_file
            self.mdhier_source_url = f"file://{self.mdhier_path.as_posix()}"
            self.mdhier_s3_key = None
            self.llt_path = folder_path / llt_file
            self.llt_source_url = f"file://{self.llt_path.as_posix()}"
            self.llt_s3_key = None
            self.release_path = folder_path / release_file
            self.release_source_url = f"file://{self.release_path.as_posix()}"
            self.release_s3_key = None
        else:
            base_key = f"{self.s3_key}"
            self.mdhier_s3_key = f"{base_key}/{mdhier_file}"
            self.mdhier_path = None
            self.mdhier_source_url = f"s3://{self.s3_bucket}/{self.mdhier_s3_key}"
            self.llt_s3_key = f"{base_key}/{llt_file}"
            self.llt_path = None
            self.llt_source_url = f"s3://{self.s3_bucket}/{self.llt_s3_key}"
            self.release_s3_key = f"{base_key}/{release_file}"
            self.release_source_url = f"s3://{self.s3_bucket}/{self.release_s3_key}"
            self.release_path = None

        self.logger = logging.getLogger(f"pskg_loader.MeddraOntology")
        self.logger.info(f"Created {self}")

    def write_objects(self, output_stream):
        """
        Construct Meddra ontology links and write them to an existing open output_stream.  Caller is responsible for
        creating the output stream and eventually closing it.

        Parameters
        ----------
        output_stream: object
            Open stream for output, data will be appended to this stream

        Returns
        -------
        None
        """

        # Hierarchy file contains all PT, HLT, HLGT, and SOC terms,
        # so read it in here.
        mdhier_df = raw_meddra.read_raw(
            input_bucket=self.s3_bucket,
            input_key=self.mdhier_s3_key,
            file_path=self.mdhier_path,
            meddra_file_type="mdhier",
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=mdhier_df,
                s3_bucket=self.s3_bucket,
                s3_key=self.mdhier_s3_key,
                file_path=self.mdhier_path,
            )
        )

        # Set up id columns
        mdhier_df["PT"] = mdhier_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="PT",
            meddra_code_column="pt_code",
            axis=1,
        )
        mdhier_df["HLT"] = mdhier_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="HLT",
            meddra_code_column="hlt_code",
            axis=1,
        )
        mdhier_df["HLGT"] = mdhier_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="HLGT",
            meddra_code_column="hlgt_code",
            axis=1,
        )
        mdhier_df["SOC"] = mdhier_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="SOC",
            meddra_code_column="soc_code",
            axis=1,
        )
        mdhier_df["BLANK"] = ""

        # This file links LLTs to PTs
        llt_df = raw_meddra.read_raw(
            input_bucket=self.s3_bucket,
            input_key=self.llt_s3_key,
            file_path=self.llt_path,
            meddra_file_type="llt",
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=llt_df,
                s3_bucket=self.s3_bucket,
                s3_key=self.llt_s3_key,
                file_path=self.llt_path,
            )
        )

        # Link LLTs to PTs
        llt_df[self.from_col] = llt_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="LLT",
            meddra_code_column="llt_code",
            axis=1,
        )
        llt_df[self.to_col] = llt_df.apply(
            raw_meddra.generate_meddra_id,
            meddra_type="PT",
            meddra_code_column="pt_code",
            axis=1,
        )
        llt_df[self.primary] = ""

        tmp_df = llt_df[self._output_columns].drop_duplicates()
        tmp_df.to_csv(output_stream, sep="\t", index=False, header=False, mode="a")
        self.logger.info(f"{len(tmp_df)} LLTs to PTs written.")

        pt_hlt_df = mdhier_df[["PT", "HLT", "primary_soc_fg"]].drop_duplicates()
        pt_hlt_df.to_csv(output_stream, sep="\t", index=False, header=False, mode="a")
        self.logger.info(f"{len(pt_hlt_df)} PTs to HLTs written.")

        hlt_hlgt_df = mdhier_df[["HLT", "HLGT", "BLANK"]].drop_duplicates()
        hlt_hlgt_df.to_csv(output_stream, sep="\t", header=False, index=False, mode="a")
        self.logger.info(f"{len(hlt_hlgt_df)} HLTs written.")

        hlgt_soc_df = mdhier_df[["HLGT", "SOC", "BLANK"]].drop_duplicates()
        hlgt_soc_df.to_csv(output_stream, sep="\t", header=False, index=False, mode="a")
        self.logger.info(f"{len(hlgt_soc_df)} HLTs written.")


class MeddraSMQContainsTerm(utils.Generator):
    _output_columns = [
        "MeddraSmqCode",
        "MeddraId",
        "TermLevel",
        "Scope",
        "Category",
        "Weight",
        "Status",
        "AdditionVersion",
        "LastModifiedVersion",
    ]

    _term_map = {
        "smq_code": "MeddraSmqCode",
        "term_code": "MeddraId",
        "term_level": "TermLevel",
        "term_scope": "Scope",
        "term_category": "Category",
        "term_weight": "Weight",
        "term_status": "Status",
        "term_addition_version": "AdditionVersion",
        "term_last_modified_version": "LastModifiedVersion",
    }

    def __init__(
        self,
        smq=True,
        s3_bucket=None,
        s3_key=None,
        folder_path=None,
        smq_content="smq_content.asc",
    ):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=folder_path)
        self.smq = smq
        if folder_path:
            if isinstance(folder_path, str):
                folder_path = Path(folder_path)

            self.smq_content_path = folder_path / smq_content
            self.smq_content_source_url = f"file://{self.smq_content_path.as_posix()}"
            self.smq_content_s3_key = None
        else:
            base_key = f"{self.s3_key}"
            self.smq_content_s3_key = f"{base_key}/{smq_content}"
            self.smq_content_path = None
            self.smq_content_source_url = (
                f"s3://{self.s3_bucket}/{self.smq_content_s3_key}"
            )

        self.logger = logging.getLogger(f"pskg_loader.MeddraSMQContains")
        self.logger.info(f"Created {self} SMQ Mode:{self.smq}")

    def write_objects(self, output_stream):
        """
        Construct SQM content (i.e. SMQs to terms and other SMQs) links and write them to an existing open output_stream.  Caller is responsible for
        creating the output stream and eventually closing it.

        Parameters
        ----------
        output_stream: object
            Open stream for output, data will be appended to this stream

        Returns
        -------
        None
        """
        self.logger.info(f"SMQ Mode: {self.smq}")

        smq_content_df = raw_meddra.read_raw(
            input_bucket=self.s3_bucket,
            input_key=self.smq_content_s3_key,
            file_path=self.smq_content_path,
            meddra_file_type="smq_content",
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=smq_content_df,
                s3_bucket=self.s3_bucket,
                s3_key=self.smq_content_s3_key,
                file_path=self.smq_content_path,
            )
        )

        smq_content_df.columns = [
            self._term_map.get(tc) or tc for tc in smq_content_df.columns
        ]

        if self.smq:
            final_df = smq_content_df[smq_content_df["TermLevel"].isin([0])][
                self._output_columns
            ].drop_duplicates()
        else:
            smq_content_df["MeddraId"] = smq_content_df.apply(
                raw_meddra.generate_meddra_id,
                meddra_type="PT",
                meddra_code_column="MeddraId",
                axis=1,
            )
            final_df = smq_content_df[smq_content_df["TermLevel"].isin([4, 5])][
                self._output_columns
            ].drop_duplicates()

        final_df.to_csv(output_stream, index=False, sep="\t", header=False, mode="a")

        self.logger.info(f"{len(final_df)} written.")
