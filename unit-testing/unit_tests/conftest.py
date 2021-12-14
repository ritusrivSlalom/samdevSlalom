import boto3
import os
import pytest

from moto import mock_s3, mock_sqs, mock_datasync


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    

@pytest.fixture(scope='function')
def s3_client(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn

@pytest.fixture(scope='function')
def sqs_client(aws_credentials):
    with mock_sqs():
        conn = boto3.client("sqs", region_name="us-east-1")
        yield conn

@pytest.fixture(scope='function')
def datasync_client(aws_credentials):
    with mock_datasync():
        conn = boto3.client("datasync", region_name="us-east-1")
        yield conn