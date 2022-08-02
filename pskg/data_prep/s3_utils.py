# Use boto3 for all interactions with S3 from AI Bench
import boto3

# Use Pandas to load data for processing
import pandas as pd
import numpy as np

# This is used by boto for processing so is required to be loaded
import io
import zipfile as zp
from datetime import datetime


def get_bucket(bucket_name):
    """
    This returns the S3 bucket object using BOTO as the connection mechanism
    """
    s3 = boto3.resource("s3")
    return s3.Bucket(bucket_name)


def get_object(bucket_name, key):
    s3client = boto3.client("s3")
    response = s3client.get_object(Bucket=bucket_name, Key=key)

    return response


def datetime_to_date(input_date):

    output_date = datetime.strptime(input_date, "%m/%d/%Y %H:%M")
    return output_date.date()


def zip_file_to_data_frame(
    bucket_name, zip_file_key, internal_file_name, dtypes, date_parser, convert_funcs
):
    zip_object = get_object(bucket_name, zip_file_key)

    with io.BytesIO(zip_object["Body"].read()) as tf:
        # rewind the file
        tf.seek(0)
        # Read the file as a zipfile and process the members
        with zp.ZipFile(tf, mode="r") as zipf:
            for subfile in zipf.infolist():
                if subfile.filename == internal_file_name:
                    df = pd.read_csv(
                        zipf.open(subfile.filename),
                        encoding="latin",
                        dtype=dtypes,
                        parse_dates=date_parser,
                        converters=convert_funcs,
                    )

    return df


def get_file_contents(bucket, key):
    """
    This returns the contents of a single file, identified using the S3 key for that file
    """
    client = boto3.client("s3")

    response = client.get_object(Bucket=bucket, Key=key)
    return io.BytesIO(response["Body"].read())


def get_file_content_length(bucket, key):
    """
    Return the content length for give s3 bucket and key.
    """
    client = boto3.client("s3")
    response = client.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


def get_file_content_last_modified(bucket, key):
    client = boto3.client("s3")
    response = client.head_object(Bucket=bucket, Key=key)
    return response["LastModified"]


def csv_file_to_data_frame(
    bucket, key, dtypes, date_cols, encoding="latin-1", keep_na=True
):
    """
    Convert the file contents for a CSV file to a Pandas Data Frame

    TODO: Change this to suit your needs
    """
    content = get_file_contents(bucket, key)
    return pd.read_csv(
        content,
        encoding=encoding,
        dtype=dtypes,
        parse_dates=date_cols,
        keep_default_na=keep_na,
    )


def asc_file_to_data_frame(bucket, key, header=None, names=None):
    """
    Convert the file contents for a CSV file to a Pandas Data Frame

    TODO: Change this to suit your needs
    """
    content = get_file_contents(bucket, key)
    return pd.read_csv(content, sep="$", header=header, names=names)


def excel_file_to_data_frame(bucket, key, **kwargs):
    """
    Convert the file contents for a XLSX file to a Pandas Data Frame

    TODO: Change this to suit your needs
    """
    content = get_file_contents(bucket, key)
    return pd.read_excel(content, engine="openpyxl", **kwargs)


def write_data_frame_to_S3(df, bucket_name, file_name, **kwargs):
    """
    Write given dataframe to S3 bucket, optionally passing any keyword args to
    the to_csv() function of the datarame.

    Parameters
    ----------
    df: pd.Dataframe
        Dataframe to exposrt
    bucket_name: str
        Name of S3 bucket
    file_name:  str
        Key to filename within s3 bucket
    kwargs: dict
        Additional keyword arguments for export

    Returns
    -------
    None
    """
    bucket = get_bucket(bucket_name=bucket_name)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, **kwargs)

    bucket.put_object(Body=csv_buffer.getvalue(), Key=file_name)
    return None
