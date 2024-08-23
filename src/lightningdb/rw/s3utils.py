import re

import boto3


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """
    Parse an S3 URI into bucket and key components.

    Args:
        uri (str): The S3 URI to parse, in the format 's3://<bucket>/<key>'.

    Returns:
        tuple[str, str]: A tuple containing the bucket name and key.

    Raises:
        ValueError: If the provided URI is not a valid S3 URI.
    """
    pattern = r"^s3://([^/]+)/(.*)$"
    match = re.match(pattern, uri)
    if match is None:
        raise ValueError(f"Invalid S3 URI: {uri}. Expected format: s3://<bucket>/<key>")

    bucket = match.group(1)
    key = match.group(2)
    return bucket, key


def upload_file(fp, uri):
    """
    Upload a file-like object to an S3 bucket.

    Args:
        fp: A file-like object to be uploaded.
        uri (str): The S3 URI where the file will be uploaded.
    """
    bucket, key = parse_s3_uri(uri)
    s3 = boto3.client("s3")
    s3.upload_fileobj(fp, bucket, key)


def download_file(uri, fp):
    """
    Download a file from an S3 bucket to a file object.

    Args:
        uri (str): The S3 URI of the file to be downloaded.
        fp: A file-like object where the downloaded content will be written.
    """
    bucket, key = parse_s3_uri(uri)
    s3 = boto3.client("s3")
    s3.download_fileobj(bucket, key, fp)
