#!/usr/bin/env python3

# Read data from stdin and stream it to Amazon S3 by doing multipart uploads
#
# Example usage:
# 	tar -C / -cpjO /home | streamtos3 -k keyfile -b some-bucket -o some-obj
#
# Inspired by F. Nordmann, see
# https://www.vennedey.net/blog/1-Pipe-data-from-STDIN-to-Amazon-S3

import base64
import boto3
import botocore
import sys
import argparse
import time
import hashlib
import math
from io import BytesIO

__author__ = "Jan Gru"
__copyright__ = "Copyright (C) 2021 Jan Gru"
__license__ = "MIT License"
__version__ = "0.1"


class ETagMismatchError(Exception):
    pass


def parse_keyfile(keyfile):
    try:
        with open(keyfile, "r") as f:
            key_id, secret = f.read().strip(" \t\n\r").split(":")
            return (key_id, secret)
    except IOError:
        print("Please provide a readable keyfile containing AWS credentials.")
        sys.exit(2)
    except ValueError:
        print(
            "Please provide AWS credentials in the form <KEY_ID>:<SECRET_KEY> "
            "in keyfile {keyfile}."
        )
        sys.exit(3)


def clean_up(client, bucket, obj, upload_id):
    client.abort_multipart_upload(
        Bucket=bucket,
        Key=obj,
        UploadId=upload_id,
    )
    print(f"Aborted multipart upload")


def check_integrity(client, bucket, obj, hashes):
    response = None
    try:
        response = client.head_object(Bucket=bucket, Key=obj)
    except botocore.exceptions.ClientError:
        print("Error: Object was not created")
        sys.exit(8)

    remote_etag = response["ETag"].strip('""')  # strip off ""
    md5_of_md5s = hashlib.md5(b"".join([h.digest() for h in hashes])).hexdigest()
    local_etag = f"{md5_of_md5s}-{len(hashes)}"

    print(f"Local  etag: {local_etag}")
    print(f"Remote etag: {remote_etag}")

    if local_etag != remote_etag:
        print("Error: Mismatching etags")
        return False

    return True


def do_upload(client, bucket, obj, buf, chunksize, secs, retry, is_debug=False):
    upload_id = -1

    try:
        # Initiate the upload
        s3_upload = client.create_multipart_upload(Bucket=bucket, Key=obj)
        upload_id = s3_upload["UploadId"]
        print(upload_id)
    except botocore.exceptions.ClientError as e:
        print("Error: Multipart upload could not be initiated.")
        if is_debug:
            print(e)
        sys.exit(7)

    # The md5sum of each part is calculated and the md5sum of the concatenated
    # checksums is calculated on the way, in order to verify the integrity
    # after the upload by comparing calculated checksum with the eTag of the
    # uploaded object.
    num = 0

    # Calculates the amount of digits for string formatting
    kb_digits = math.ceil(math.log10(chunksize))
    # Store information of the last uploaded part for stream closure
    parts = []
    hashes = []
    # MD5 of the read data
    in_md5sum = hashlib.md5()

    # Read chunksize bytes from stdin and upload it as a multipart to S3.
    while True:
        chunk = buf.read(chunksize)
        if not chunk:
            print("All data read")
            break

        num += 1
        in_md5sum.update(chunk)
        part_md5sum = hashlib.md5(chunk)
        hashes.append(part_md5sum)
        # Keep track of hashes
        # If upload fails, try again.
        upload_try = 0
        while upload_try < retry:
            try:
                upload_try += 1

                part = client.upload_part(
                    Bucket=bucket,
                    Body=chunk,
                    ContentLength=len(chunk),
                    ContentMD5=base64.b64encode(part_md5sum.digest()).decode("utf-8"),
                    PartNumber=num,
                    UploadId=upload_id,
                    Key=obj,
                )
                parts.append({"PartNumber": num, "ETag": part["ETag"]})

                if part["ETag"][1:-1] != part_md5sum.hexdigest():
                    print(f"ETag of part {num} mismatches MD5...")
                    raise ETagMismatchError("ETag mismatching", "upload_part")

                if len(chunk) > 1024:
                    print(
                        f"Upload part {num:10} - {len(chunk)//1024:{kb_digits}} KiB "
                        f"- {part_md5sum.hexdigest()} - Try {upload_try}"
                    )
                else:
                    print(
                        f"Upload part {num:10} - {len(chunk):{kb_digits}} B "
                        f"- {part_md5sum.hexdigest()} - {upload_try} Try"
                    )
                break

            except (botocore.exceptions.ClientError, ETagMismatchError) as e:
                print(f"Error uploading part {num}. Trying again in {secs} seconds...")
                if is_debug:
                    print(e)
                time.sleep(secs)

    # No success clean up
    if upload_try >= retry:
        clean_up(client, bucket, obj, upload_id)
        sys.exit(8)

    else:
        # Complete upload and check integrity
        try:
            client.complete_multipart_upload(
                Bucket=bucket,
                UploadId=upload_id,
                Key=obj,
                MultipartUpload={"Parts": parts},
            )
            print(
                f"Completed uploading {num} parts and "
                "verified the reported checksums"
            )
        except botocore.exceptions.ClientError as e:
            print("Error: Error while completing upload.")
            if is_debug:
                print(e)
            sys.exit(8)

    return upload_id, in_md5sum, hashes


def create_client(keyfile, bucket, obj, is_debug=False):
    key_id, secret = parse_keyfile(keyfile)

    try:
        # Establish connection to S3
        client = boto3.client(
            "s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
        )

    except botocore.exceptions.ClientError as e:
        print("Error: Connection to S3 could not be established.")
        if is_debug:
            print(e)
        sys.exit(4)

    try:
        # Check if the bucket is available
        client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        print(f"Error: Bucket {bucket} is not available.")
        if is_debug:
            print(e)
        sys.exit(5)

    try:
        # Check, if object already exists
        client.head_object(Bucket=bucket, Key=obj)
        print(f"Error: Object {obj} exists in bucket {bucket}")
        sys.exit(6)
    except botocore.exceptions.ClientError:
        # This is expected to happen
        pass

    return client


def check_file_or_stdin(infile):

    # Checks, if interactive session or reading from a file
    if sys.stdin.isatty():
        if not infile == "-":
            print(f"Reading from {infile}")
            return False
        else:
            print("Supply data via STDIN or file!")
            sys.exit(1)

    elif infile != "-":
        print("Received data via STDIN and received a file! Aborting...")
        sys.exit(1)

    print(f"Received data from stdin")
    return True


def upload(infile, keyfile, bucket, obj, chunksize, secs, retry, is_debug=False):
    is_stdin = check_file_or_stdin(infile)

    client = create_client(keyfile, bucket, obj, is_debug)

    upload_id, md5sum = None, None

    if is_stdin:
        upload_id, md5sum, hashes = do_upload(
            client, bucket, obj, sys.stdin.buffer, chunksize, secs, retry, is_debug
        )
    else:
        with open(infile, "rb", buffering=chunksize) as f:

            upload_id, md5sum, hashes = do_upload(
                client, bucket, obj, f, chunksize, secs, retry, is_debug
            )

    print(f"Read data from stdin with MD5: {md5sum.hexdigest()}")
    print(f"Stored data as object {obj} in bucket {bucket}")

    if check_integrity(client, bucket, obj, hashes):
        sys.exit(0)
    else:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Store data from stdin into S3 bucket\n"
        "DISCLAIMER: This is an _unvalidated_ proof of concept, "
        "use at _your own risk_!"
    )
    parser.add_argument(
        "infile",
        nargs="?",
        default="-",
        help="Filepath or '-' for stdin"
        # "infile", nargs="?", type=argparse.FileType("rb"), default=sys.stdin
    )
    parser.add_argument(
        "-k",
        "--keyfile",
        help="File that contains: <AWS_KEY>:<AWS_SECRET>" " for S3 access.",
        required=True,
    )

    parser.add_argument(
        "-b", "--bucket", help="Name of the target bucket.", required=True
    )

    parser.add_argument(
        "-o",
        "--obj",
        help="Name of the object to write",
        required=True,
    )

    parser.add_argument(
        "-c",
        "--chunksize",
        type=int,
        default=8 * 1024 * 1024,
        help="Split upload in CHUNK_SIZE bytes. (Default: 8 MiB)",
    )
    parser.add_argument(
        "-s",
        "--secs-wait",
        type=int,
        default=5,
        help="Time in seconds to wait until retry upload a "
        "failed part again (Default: 5)",
    )
    parser.add_argument(
        "-r",
        "--retry",
        type=int,
        default=5,
        help="Retries until giving up (Default: 5)",
    )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        default=False,
        help="Print debug information",
    )

    args = parser.parse_args()

    upload(
        args.infile,
        args.keyfile,
        args.bucket,
        args.obj,
        args.chunksize,
        args.secs_wait,
        args.retry,
        args.debug,
    )


if __name__ == "__main__":
    main()
