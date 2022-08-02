###
### Define generic class for managing graph objects.
###
### Edge class is the base class for generating Neo4J edge definitions as a tab separated value file;
### the EdgePool class manages lists of Node classes and provides a single method for producing a
### output file from all registered classes
###

import abc
import datetime
import io
import logging
import os
import time
from pathlib import Path
from collections import namedtuple

import boto3
import pandas as pd
import s3fs

from data_prep.s3_utils import (
    get_file_content_last_modified,
    get_file_content_length,
    write_data_frame_to_S3,
)


class ImportPoolManager(object):
    """
    Class for managing a list of Pool objects, and producing all load files and the manifest
    """

    manifest_file = "Manifest.tsv"

    def __init__(
        self,
        s3_output_bucket=None,
        s3_output_key=None,
        output_folder=None,
        name="ImportPoolManager",
    ):
        """
        Build ImportPoolManager object.

        ----------
        s3_output_bucket: str, optional
            Name of output bucket
        s3_output_key: str, optional
            Name of key within output bucket
        output_folder: str, optional
            Name of local output folder (exclusive with s3_bucket/s3_key)

        Returns
        -------
        None
        """
        # check constructor args
        if s3_output_bucket and s3_output_key and output_folder:
            raise ValueError(
                "Only s3_bucket and s3_key, or folder_path may be specified."
            )
        elif s3_output_bucket and not s3_output_key:
            raise ValueError("s3_bucket must be specified with an s3_key.")
        elif not output_folder and not s3_output_key:
            raise ValueError(
                "One of output_folder or s3_output_bucket and s3_output_key must be specified."
            )

        self.registered_pools = []
        self.name = name

        if output_folder:
            if isinstance(output_folder, str):
                self.output_folder = Path(output_folder)
            else:
                self.output_folder = output_folder
            self.s3_bucket = None
            self.s3_key = None
            self.output_url = f"file://{self.output_folder.resolve().as_posix()}"
        else:
            self.s3_bucket = s3_output_bucket
            self.s3_key = s3_output_key
            self.output_url = f"s3://{self.s3_bucket}/{self.s3_key}"
            self.output_folder = None
        self.logger = logging.getLogger("pskg_loader.ImportPoolManager")
        self.logger.info(f"Created {self}")

    def __str__(self) -> str:
        return f"ImportPoolManager(name={self.name}, output_url={self.output_url}, registered_pools={self.registered_pools})"

    def __repr__(self) -> str:
        tmp = f"ImportPoolManager(name={self.name}, output_url={self.output_url})"
        for obj in self.registered_pools:
            tmp += f"\n  {str(obj)}"
        return tmp

    def register(self, pool):
        """
        Register a Pool object for output.
        """
        if not isinstance(pool, Pool):
            self.logger.error(
                f"Cannot register object of type {type(pool)}, must be subclass of Pool"
            )
            raise ValueError(
                f"ImportPoolManager: Cannot register object of type {type(pool)}, must be subclass of Pool"
            )
        if pool not in self.registered_pools:
            self.logger.info(f"Registered pool {pool}")
            self.registered_pools.append(pool)
        else:
            self.logger.warning(f"Pool {pool} already registered.")

    def create_output(self):
        """
        Iterate over all registered Pool objects, produce output files, and write out manifest
        """
        manifests_df = []
        for pool in self.registered_pools:
            self.logger.info(f"Writing objects from {pool} to {self.output_url}")
            pool.write_objects(
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                folder_path=self.output_folder,
            )

            t = pool.gather_manifest()
            if t is not None:
                manifests_df.append(t)
            else:
                self.logger.info(
                    f"No manifest data availble for {pool} (writing to: {self.output_url})"
                )

        final_manifest_df = pd.concat(manifests_df).drop_duplicates()

        if self.output_folder:
            manifest_path = self.output_folder / self.manifest_file
            self.logger.info(
                f"Writing manifest file: file://{manifest_path.resolve().as_posix()}"
            )
            final_manifest_df.to_csv(
                self.output_folder / self.manifest_file,
                sep="\t",
                index=False,
            )
        else:
            manifest_key = f"s3://{self.s3_bucket}/{self.s3_key}/{self.manifest_file}"
            self.logger.info(f"Writing manifest file: {manifest_key}")
            write_data_frame_to_S3(
                df=final_manifest_df,
                bucket_name=self.s3_bucket,
                file_name=f"{self.s3_key}/{self.manifest_file}",
                sep="\t",
                index=False,
            )


class Pool(object):
    """
    Class for managing generating classes
    """

    def __init__(self, name, output_file, output_buffer_mb=100):
        self.name = name
        self.output_file = output_file
        self.graph_object_list = []
        self.output_buffer_size_mb = float(output_buffer_mb)
        self.logger = logging.getLogger(f"pskg_loader.Pool.{name}")

    def __str__(self) -> str:
        return f"Pool(name={self.name}, output_file={self.output_file}, registered_objects={len(self.graph_object_list)})"

    def __repr__(self) -> str:
        tmp = f"Pool(name={self.name}, output_file={self.output_file}, output_buffer_size={self.output_buffer_size_mb} mb)"
        for obj in self.graph_object_list:
            tmp += f"\n  {str(obj)}"
        return tmp

    def register(self, node):
        """
        Register a node generating class
        """
        self.graph_object_list.append(node)

    def write_objects(self, s3_bucket=None, s3_key=None, folder_path=None):
        """
        Write out complete edge file from all registered classes.

        Parameters
        ----------
        s3_bucket: str, optional
            Name of output bucket
        s3_key: str, optional
            Name of key within output bucket
        folder_path: str, optional
            Name of local path (exclusive with s3_bucket/s3_key)

        Returns
        -------
        None
        """
        # Simple validation logic... folder_path or bucket/key
        if (s3_bucket or s3_key) and folder_path:
            raise ValueError(
                "Only s3_bucket and s3_key, or file_path may be specified."
            )
        elif s3_bucket and not s3_key:
            raise ValueError("s3_bucket must be specified with an s3_key.")
        elif not folder_path and not s3_key:
            raise ValueError(
                "One of file_path or s3_bucket and s3_key must be specified."
            )

        if folder_path:
            if isinstance(folder_path, str):
                # Local file system
                destination_path = Path(folder_path) / self.output_file
            elif isinstance(folder_path, Path):
                destination_path = folder_path / self.output_file
            else:
                raise ValueError("folder_path must be of type str or Path")

            if self.graph_object_list and len(self.graph_object_list) > 0:
                with open(destination_path, "wb") as f:
                    # Write header to TSV file
                    self.graph_object_list[0].write_header(f)
                    for graph_obj in self.graph_object_list:
                        # Write out data to TSV file
                        try:
                            graph_obj.write_objects(f)
                        except Exception:
                            self.logger.error(f"{graph_obj}.write_objects()")
                            raise
            else:
                self.logger.warn(f"{self}.write_objects(): No records to write.")
        else:
            # S3 file system
            destination_path = f"s3://{s3_bucket}/{s3_key}/{self.output_file}"
            destination_key = f"{s3_key}/{self.output_file}"
            fs = s3fs.S3FileSystem(anon=False)
            s3 = boto3.client("s3")
            part_info = {"Parts": []}
            total_graph_objects = len(self.graph_object_list)

            mpu = s3.create_multipart_upload(Bucket=s3_bucket, Key=destination_key)
            part_num = 0
            min_size_mb = self.output_buffer_size_mb * 1024 * 1024
            current_size_mb = 0.0
            start_time = time.time()

            # Assemble output in chunks of at least min_size_mb
            if len(self.graph_object_list) > 0:
                csv_buffer = io.BytesIO()
                self.graph_object_list[0].write_header(csv_buffer)

                for i, graph_obj in enumerate(self.graph_object_list):
                    object_file_start_time = time.time()
                    csv_buffer.seek(0, os.SEEK_END)
                    graph_obj.write_objects(csv_buffer)
                    current_size_mb = csv_buffer.getbuffer().nbytes / 1024.0 / 1024.0
                    csv_buffer.seek(0, os.SEEK_END)
                    object_file_stop_time = time.time()
                    self.logger.info(
                        f"Processed {i+1} of {total_graph_objects} for part {part_num + 1}, buffered {current_size_mb} mb, position: {csv_buffer.tell()}"
                        f" ({(object_file_stop_time - object_file_start_time) / 60.0:.2f} minutes)"
                    )

                    if csv_buffer.getbuffer().nbytes > min_size_mb:
                        part_num += 1
                        csv_buffer.seek(0, os.SEEK_SET)
                        part = s3.upload_part(
                            Bucket=s3_bucket,
                            Key=destination_key,
                            PartNumber=part_num,
                            UploadId=mpu["UploadId"],
                            Body=csv_buffer,
                        )
                        part_info["Parts"].append(
                            {"PartNumber": part_num, "ETag": part["ETag"]}
                        )
                        self.logger.info(f"Wrote part {part_num}: {current_size_mb} mb")
                        csv_buffer.close()
                        csv_buffer = io.BytesIO()
                        current_size_mb = 0

                if csv_buffer.getbuffer().nbytes > 0:
                    part_num += 1
                    csv_buffer.seek(0, os.SEEK_SET)
                    part = s3.upload_part(
                        Bucket=s3_bucket,
                        Key=destination_key,
                        PartNumber=part_num,
                        UploadId=mpu["UploadId"],
                        Body=csv_buffer,
                    )
                    part_info["Parts"].append(
                        {"PartNumber": part_num, "ETag": part["ETag"]}
                    )
                    csv_buffer.close()
                    self.logger.info(f"Wrote final {part_num}: {current_size_mb} mb")
                else:
                    self.logger.info(f"All parts written.")

                s3.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=destination_key,
                    UploadId=mpu["UploadId"],
                    MultipartUpload=part_info,
                )
            stop_time = time.time()

            self.logger.info(
                f"{self.output_file} complete, total time: {(stop_time - start_time ) / 60.0:.2f} minutes"
            )

    def gather_manifest(self):
        """
        Build a combined dataframe with all available manifest data from
        registered objects, and resolve duplicates.

        Returns
        -------
        pd.Dataframe
            A dataframe containing all manifest information.
        """
        manifest_dfs = []

        for graph_obj in self.graph_object_list:
            manifest_dfs.append(graph_obj.get_manifest())

        if manifest_dfs:
            df = pd.concat(manifest_dfs).drop_duplicates()
        else:
            df = None
        return df


class Generator(object):
    """
    Base class for all generation classes.  A generator class is used to output portions of
    a tab separated load file.
    """

    _output_columns = []
    _manifest_item = namedtuple(
        "ManifestItem", ["Path", "LastModified", "Tag", "Rows", "Size", "Md5"]
    )

    def __init__(self, s3_bucket=None, s3_key=None, file_path=None):
        """
        Create a new generator object.  Either an s3_bucket and s3_key
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
        if file_path:
            # If specified, file_path takes precedence over S3 bucket/key
            s3_key = None
            s3_bucket = None
        elif s3_bucket and not s3_key:
            raise ValueError("s3_bucket must be specified with an s3_key.")
        elif not file_path and not s3_key:
            raise ValueError(
                "One of file_path or s3_bucket and s3_key must be specified."
            )

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        if file_path:
            if isinstance(file_path, str):
                self.file_path = Path(file_path)
            elif not isinstance(file_path, Path):
                raise ValueError("file_path must be str or Path object")
            else:
                self.file_path = file_path
            self.source_url = "file://" + self.file_path.as_posix()
        else:
            self.file_path = None
            self.source_url = f"s3://{s3_bucket}/{s3_key}"

        self.manifest_data = []

    def __str__(self) -> str:
        cls_name = type(self).__name__
        return f"{cls_name} ({self.source_url})"

    def check_content(self, s3_bucket=None, s3_key=None, file_path=None):
        """
        Do a quick sanity check on the given file_path or bucket to see if it exists

        s3_bucket: str, optional
        s3_key: str, optional
            key within s3_bucket to data zip file
        file_path: str, optional
            Path to local eudravigilance data file
        """
        if file_path:
            if not file_path.exists():
                raise RuntimeError(f"Does not exist: {file_path}")
            else:
                return True
        else:
            try:
                s3 = boto3.client("s3")
                s3.head_object(Bucket=s3_bucket, Key=s3_key)
                return True
            except Exception as x:
                raise RuntimeError(f"Failed to read: {s3_bucket}/{s3_key} ({str(x)})")

    def get_manifest_data(
        self, df, s3_bucket=None, s3_key=None, file_path=None, tag="", md5=None
    ):
        """
        Gather manifest data and return it to the caller as named tuple.  If no parameters for
        the given data file are given (i.e. an s3_bucket and s3_key, or a file_path), the file
        is assumed to be the objects given

        Parameters
        ----------
        df: pd.Dataframe
            Dataframe containing rows from given bucket/key or path
        s3_bucket: str
            Name of S3 bucket, defaults to None
        s3_key: str
            Name of S3 key, defaults to None
        """
        if file_path:
            # file path specified
            if isinstance(file_path, str):
                file_path = Path(file_path)
            elif not isinstance(file_path, Path):
                raise ValueError("if specified, file_path must be str or Path object")
            content_length = file_path.stat().st_size
            last_modified = datetime.datetime.fromtimestamp(
                file_path.stat().st_mtime, datetime.timezone.utc
            )
            source_url = f"file://{file_path.resolve().as_posix()}"
        elif s3_bucket:
            # s3 bucket specified
            content_length = get_file_content_length(s3_bucket, s3_key)
            last_modified = get_file_content_last_modified(s3_bucket, s3_key)
            source_url = f"s://{s3_bucket}/{s3_key}"
        elif self.file_path:
            # Use objects self path
            content_length = self.file_path.stat().st_size
            last_modified = datetime.datetime.fromtimestamp(
                self.file_path.stat().st_mtime, datetime.timezone.utc
            )
            source_url = f"file://{self.file_path.resolve().as_posix()}"
        elif self.s3_key:
            content_length = get_file_content_length(
                bucket=self.s3_bucket, key=self.s3_key
            )
            last_modified = get_file_content_last_modified(self.s3_bucket, self.s3_key)
            source_url = f"s://{self.s3_bucket}/{self.s3_key}"

        return self._manifest_item(
            source_url,
            last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
            tag,
            len(df),
            content_length,
            Md5=md5,
        )

    def write_header(self, output_stream):
        """
        Create a TSV header on specified stream
        """
        if self._output_columns:
            pd.DataFrame(None, columns=self._output_columns).to_csv(
                output_stream, index=False, sep="\t"
            )
        else:
            raise RuntimeError("No column headers defined")

    @abc.abstractmethod
    def write_objects(self, output_stream):
        raise NotImplementedError

    def get_manifest(self):
        """
        Return manifest information (valid only after write_objects is called).
        """
        if not self.manifest_data:
            raise RuntimeError(
                f"{self}.get_manifest() must be called after write_objects()."
            )

        return pd.DataFrame(self.manifest_data)
