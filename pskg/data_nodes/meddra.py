###
### Classes defining Meddra objects
###

import logging
from pathlib import Path

from numpy import isin

from data_prep import meddra as raw_meddra
from graph_objects import utils


class MeddraTerm(utils.Generator):
    _output_columns = [
        "MeddraCode",
        "MeddraId",
        "MeddraAbbreviation",
        "MeddraType",
        "MeddraVersion",
        "MeddraLanguage",
        "Name",
    ]

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
            self.check_content(self.s3_bucket, self.mdhier_s3_key)
            self.check_content(self.s3_bucket, self.llt_s3_key)
            self.check_content(self.s3_bucket, self.release_s3_key)

        self.logger = logging.getLogger(f"pskg_loader.MeddraTerm")
        self.logger.info(f"Created {self}")

    def gather_term_codes(
        self,
        df,
        meddra_type,
        code_column,
        name_column,
        abbreviation_column=None,
        version_column="version",
        language_column="language",
    ):
        """
        Helper function to assemble MedDRA terms for output.

        Parameters
        ----------
        df: pd.Dataframe
            Dataframe containing code data
        meddra_type: str
            Type of codes being exracted
        code_column: str
            Column containing codes
        name_column: str
            Column containting name
        abbreviaton_column: str, optional
            Column containing abbreviation information, defaults to None
        version_column: str, optional
            Column containing MedDRA version, defaults to "version"
        language_column: str, optional
            Column with MedDRA language version, defaults to "language"

        Returns
        -------
        pd.DataFrame
            Returns a dataframe in outputformat populated with terms for the given type

        """
        self.logger.info(
            f"gather_term_codes: meddra_type:{meddra_type}, code_column:{code_column}, name_column:{name_column}, abbreviation:{abbreviation_column}"
        )
        result_df = (
            df[
                [
                    c
                    for c in [
                        code_column,
                        name_column,
                        abbreviation_column,
                        version_column,
                        language_column,
                    ]
                    if c is not None
                ]
            ]
            .drop_duplicates()
            .assign(
                MeddraType=meddra_type,
                MeddraVersion=lambda x: x[version_column]
                if version_column is not None
                else "",
                MeddraLanguage=lambda x: x[language_column]
                if language_column is not None
                else "",
            )
        )
        result_df["Name"] = result_df[name_column]
        result_df["MeddraCode"] = result_df[code_column]
        result_df["MeddraId"] = result_df.apply(
            raw_meddra.generate_type_meddra_id,
            column_name=code_column,
            meddra_type=meddra_type,
            axis=1,
        )
        if abbreviation_column:
            result_df["MeddraAbbreviation"] = result_df[abbreviation_column]
        else:
            result_df["MeddraAbbreviation"] = ""
        if version_column:
            result_df["MeddraVersion"] = result_df[version_column]
        else:
            result_df["MeddraVersion"] = ""
        if language_column:
            result_df["MeddraLanguage"] = result_df[language_column]
        else:
            result_df["MeddraLanguage"] = ""

        return result_df[self._output_columns].drop_duplicates()

    def write_objects(self, output_stream):
        """
        Construct Meddra nodes and write them an existing open output_stream.  Caller is responsible for
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

        if self.release_path or self.release_s3_key:
            # gather version data, if available
            self.logger.info(
                f"Using MedDRA version data from {self.release_source_url}"
            )
            version_df = raw_meddra.read_raw(
                input_bucket=self.s3_bucket,
                input_key=self.release_s3_key,
                file_path=self.release_path,
                meddra_file_type="meddra_release",
            )

            self.manifest_data.append(
                self.get_manifest_data(
                    df=version_df,
                    s3_bucket=self.s3_bucket,
                    s3_key=self.release_s3_key,
                    file_path=self.release_path,
                )
            )

            mdhier_df = mdhier_df.merge(version_df, how="cross")
            llt_df = llt_df.merge(version_df, how="cross")
        else:
            self.logger.warn(f"No MedDRA version file available.")
            mdhier_df["version"] = None
            mdhier_df["language"] = None
            llt_df["version"] = None
            llt_df["language"] = None

        pt_codes_df = self.gather_term_codes(
            df=mdhier_df, meddra_type="PT", code_column="pt_code", name_column="pt_name"
        )

        pt_codes_df.to_csv(output_stream, sep="\t", header=False, index=False, mode="a")
        self.logger.info(f"{len(pt_codes_df)} PTs written.")

        hlt_codes_df = self.gather_term_codes(
            df=mdhier_df,
            meddra_type="HLT",
            code_column="hlt_code",
            name_column="hlt_name",
        )
        hlt_codes_df.to_csv(
            output_stream, sep="\t", header=False, index=False, mode="a"
        )
        self.logger.info(f"{len(hlt_codes_df)} HLTs written.")

        hlgt_codes_df = self.gather_term_codes(
            df=mdhier_df,
            meddra_type="HLGT",
            code_column="hlgt_code",
            name_column="hlgt_name",
        )
        hlgt_codes_df.to_csv(
            output_stream, sep="\t", header=False, index=False, mode="a"
        )
        self.logger.info(f"{len(hlgt_codes_df)} HLGTs written.")

        soc_codes_df = self.gather_term_codes(
            df=mdhier_df,
            meddra_type="SOC",
            code_column="soc_code",
            name_column="soc_name",
            abbreviation_column="soc_abbrev",
        )
        soc_codes_df.to_csv(
            output_stream, sep="\t", header=False, index=False, mode="a"
        )
        self.logger.info(f"{len(soc_codes_df)} SOCs written.")

        llt_codes_df = self.gather_term_codes(
            df=llt_df,
            meddra_type="LLT",
            code_column="llt_code",
            name_column="llt_name",
        )

        llt_codes_df.to_csv(
            output_stream, sep="\t", header=False, index=False, mode="a"
        )
        self.logger.info(f"{len(llt_codes_df)} LLTs written.")


class MeddraSMQ(utils.Generator):
    _output_columns = [
        "MeddraSmqCode",
        "Name",
        "SmqLevel",
        "SmqDescription",
        "SmqSource",
        "SmqNote",
        "SmqVersion",
        "SmqStatus",
        "SmqAlgorithm",
    ]

    def __init__(
        self,
        s3_bucket=None,
        s3_key=None,
        folder_path=None,
        smq_list_file="smq_list.asc",
    ):
        super().__init__(s3_bucket=s3_bucket, s3_key=s3_key, file_path=folder_path)
        if folder_path:
            if isinstance(folder_path, str):
                folder_path = Path(folder_path)
            self.smq_list_file_path = folder_path / smq_list_file
            self.smq_list_s3_key = None
            self.smq_list_source_url = f"file://{self.smq_list_file_path.as_posix()}"
        else:
            self.smq_list_file_path = None
            self.smq_list_s3_key = f"{self.s3_key}/{smq_list_file}"
            self.smq_list_source_url = f"s3://{self.s3_bucket}/{self.smq_list_s3_key}"
        self.logger = logging.getLogger(f"pskg_loader.MeddraSMQ")
        self.logger.info(f"Created {self}")

    def write_objects(self, output_stream):
        """
        Construct MeddraSMQ nodes and write them an existing open output_stream.  Caller is responsible for
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
        smq_list_df = raw_meddra.read_raw(
            input_bucket=self.s3_bucket,
            input_key=self.smq_list_s3_key,
            file_path=self.smq_list_file_path,
            meddra_file_type="smq_list",
        )

        self.manifest_data.append(
            self.get_manifest_data(
                df=smq_list_df,
                s3_bucket=self.s3_bucket,
                s3_key=self.smq_list_s3_key,
                file_path=self.smq_list_file_path,
            )
        )

        smq_list_df["MeddraSmqCode"] = smq_list_df["smq_code"]
        smq_list_df["Name"] = smq_list_df["smq_name"]
        smq_list_df["SmqLevel"] = smq_list_df["smq_level"]
        smq_list_df["SmqDescription"] = smq_list_df["smq_description"]
        smq_list_df["SmqSource"] = smq_list_df["smq_source"]
        smq_list_df["SmqVersion"] = smq_list_df["MedDRA_version"]
        smq_list_df["SmqStatus"] = smq_list_df["status"]
        smq_list_df["SmqNote"] = smq_list_df["smq_note"]
        smq_list_df["SmqAlgorithm"] = smq_list_df["smq_algorithm"]

        final_df = smq_list_df[self._output_columns].drop_duplicates()
        final_df.to_csv(output_stream, sep="\t", header=False, index=False, mode="a")

        self.logger.info(f"{len(final_df)} MeddraSMQs written.")
