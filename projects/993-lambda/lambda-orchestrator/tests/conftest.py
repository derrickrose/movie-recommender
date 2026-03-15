import os
from pathlib import Path

import boto3
import docker

region = "eu-central-1"
os.environ["Env"] = "test"
os.environ["AWS_REGION"] = region
bucket_name = "test-bucket"
test_folder_name = "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/"


def get_local_stack_ip_address():
    docker_client = docker.from_env()
    localstack_container = docker_client.containers.get("localstack")
    network_data = localstack_container.attrs["NetworkSettings"]
    return network_data["Networks"]["bridge"]["IPAddress"]


ip_address = (
    "localhost" if "/" != f"{os.path.sep}" else get_local_stack_ip_address()
)

local_host = f"http://{ip_address}:4566"

# Create a mock S3 client
S3_CLIENT = boto3.client(
    "s3",
    endpoint_url=local_host,
    aws_access_key_id="fake",
    aws_secret_access_key="fake",
    region_name=region,
    verify=False,
)


def init_localstack(s3_client):
    init_s3(s3_client)


def clean_localstack(s3_client):
    clean_s3(s3_client)


def init_s3(s3_client):
    try:
        clean_s3(s3_client)
    except Exception:
        print("No bucket to delete")
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": region},
    )
    test_resources_dir = os.path.join(os.path.dirname(__file__), "resources")
    for filename in os.listdir(test_resources_dir):
        print(filename)
        if os.path.isfile(os.path.join(test_resources_dir, filename)):
            with open(
                os.path.join(test_resources_dir, filename), "rb"
            ) as data:
                object_key = (
                    Path(test_folder_name).joinpath(filename).as_posix()
                )
                s3_client.upload_fileobj(data, bucket_name, object_key)


def clean_s3(s3_client):
    print("Cleaning s3 buckets...")
    try:
        keys = list_bucket(s3_client)
        for key in keys:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        print(e)
        print("The bucket is empty..")

    s3_client.delete_bucket(Bucket=bucket_name)


def list_bucket(s3_client, prefix=""):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    keys = []
    for obj in response["Contents"]:
        print("obj ", obj)
        keys.append(obj["Key"])
    return set(keys)


def test_init_stack():
    init_localstack(S3_CLIENT)
