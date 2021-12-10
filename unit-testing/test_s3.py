import pytest
from tempfile import NamedTemporaryFile

from s3 import MyS3Client


@pytest.fixture
def bucket_name():
    return "my_bucket_name"


@pytest.fixture
def s3_test(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    yield

def test_list_buckets(s3_client, s3_test):
    my_client = MyS3Client()
    buckets = my_client.list_buckets()
    assert buckets == ["my_bucket_name"]


def test_list_objects(s3_client, s3_test):
    file_text = "test"
    with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
        with open(tmp.name, "w", encoding="UTF-8") as f:
            f.write(file_text)

        s3_client.upload_file(tmp.name, "my_bucket_name", "myfile123")
        s3_client.upload_file(tmp.name, "my_bucket_name", "myfile2343")

    my_client = MyS3Client()
    objects = my_client.list_objects(bucket_name="my_bucket_name", prefix="myfile1")
    assert objects == ["myfile123"]

'''
Keep this test in comment, local dosent set the location for bucket
def test_get_bucket_location(s3_client, s3_test):
    my_client = MyS3Client()
    location = my_client.get_bucket_location(bucket_name="my_bucket_name")
    assert location == ["location"]
'''
