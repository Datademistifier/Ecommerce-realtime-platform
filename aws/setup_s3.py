"""
setup_s3.py
-----------
Creates the S3 bucket structure for the data lake layer.
Run once before starting the pipeline.

Usage: python aws/setup_s3.py
"""

import boto3
import os
from botocore.exceptions import ClientError

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ecommerce-realtime-platform")
REGION      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Folder prefixes to create (S3 simulates folders with key prefixes)
FOLDERS = [
    "raw/orders/",
    "raw/clickstream/",
    "raw/inventory_updates/",
    "processed/",
    "archive/",
]


def create_bucket(s3_client, bucket_name: str, region: str):
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"✅ Created bucket: s3://{bucket_name}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"ℹ️  Bucket already exists: s3://{bucket_name}")
        else:
            raise


def set_lifecycle_policy(s3_client, bucket_name: str):
    """Move raw data to Glacier after 90 days to reduce storage costs."""
    lifecycle = {
        "Rules": [
            {
                "ID":     "archive-raw-after-90-days",
                "Filter": {"Prefix": "raw/"},
                "Status": "Enabled",
                "Transitions": [{
                    "Days":         90,
                    "StorageClass": "GLACIER",
                }],
                "Expiration": {"Days": 365},   # delete after 1 year
            }
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    print(f"✅ Lifecycle policy set: raw/ → Glacier at 90d, delete at 365d")


def enable_versioning(s3_client, bucket_name: str):
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print(f"✅ Versioning enabled on s3://{bucket_name}")


def create_folder_structure(s3_client, bucket_name: str):
    for folder in FOLDERS:
        s3_client.put_object(Bucket=bucket_name, Key=folder)
        print(f"  📁 Created: s3://{bucket_name}/{folder}")


def main():
    print(f"Setting up S3 data lake: s3://{BUCKET_NAME} in {REGION}\n")

    s3 = boto3.client("s3", region_name=REGION)

    create_bucket(s3, BUCKET_NAME, REGION)
    enable_versioning(s3, BUCKET_NAME)
    set_lifecycle_policy(s3, BUCKET_NAME)

    print("\nCreating folder structure:")
    create_folder_structure(s3, BUCKET_NAME)

    print(f"\n✅ S3 setup complete.")
    print(f"   Bucket:  s3://{BUCKET_NAME}")
    print(f"   Region:  {REGION}")
    print(f"   Folders: {', '.join(FOLDERS)}")


if __name__ == "__main__":
    main()
