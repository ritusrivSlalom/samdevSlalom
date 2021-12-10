import pytest
from datasync import MyDataSyncClient



@pytest.fixture
def source_location_arn():
    return {"arn:aws:s3:::gh-fake-automation-bucket-dev"}

@pytest.fixture
def destination_location_arn():
    return {"bip-analysis-fake-bucket-dev"}

@pytest.fixture
def name():
    return "my_task_name"

@pytest.fixture
def get_arn(name):
    return "arn:aws:datasync:us-east-1|task:{0}".format(name)   

@pytest.fixture
def initial_options():
    return {
            "VerifyMode": "NONE",
            "Atime": "BEST_EFFORT",
            "Mtime": "PRESERVE",
        }

@pytest.fixture
def updated_options():
    return {
            "VerifyMode": "POINT_IN_TIME_CONSISTENT",
            "Atime": "BEST_EFFORT",
            "Mtime": "PRESERVE",
        }

def create_locations(client, create_smb=False, create_s3=False):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    smb_arn = None
    s3_arn = None
    if create_smb:
        # response = client.create_location_smb(
        #     ServerHostname="host",
        #     Subdirectory="somewhere",
        #     User="",
        #     Password="",
        #     AgentArns=["stuff"],
        # )
        #smb_arn = response["LocationArn"]
        smb_arn = "arn:aws:s3:::gh-pcluster-automation-bucket-dev"
    if create_s3:
        # response = client.create_location_s3(
        #     S3BucketArn="arn:aws:s3:::my_bucket",
        #     Subdirectory="dir",
        #     S3Config={"BucketAccessRoleArn": "role"},
        # )
        #s3_arn = response["LocationArn"]
        s3_arn = "bip-analysis-bucket-dev"
    return {"smb_arn": smb_arn, "s3_arn": s3_arn}

def test_creat_task(name, get_arn, initial_options):
    datasync_client = MyDataSyncClient()
    locations = create_locations(datasync_client, create_smb=True, create_s3=True)
    response = datasync_client.create_task(
        source_location_arn=locations["smb_arn"],
        destination_location_arn=locations["s3_arn"],
        name=name,
        metadata=initial_options
    )
    
    assert get_arn in response

def test_start_task_execution(name, get_arn, initial_options):
    datasync_client = MyDataSyncClient()
    locations = create_locations(datasync_client, create_smb=True, create_s3=True)
    response = datasync_client.start_task_execution(
            source_location_arn=locations["smb_arn"],
            destination_location_arn=locations["s3_arn"],
            name=name,
            metadata=initial_options
        )
    assert get_arn+"/execution/exec-{0}".format(str(1).zfill(17)) in response

def test_cancel_task_execution(name, get_arn, initial_options):
    datasync_client = MyDataSyncClient()
    locations = create_locations(datasync_client, create_smb=True, create_s3=True)
    response = datasync_client.cancel_task_execution(
            source_location_arn=locations["smb_arn"],
            destination_location_arn=locations["s3_arn"],
            name=name,
            metadata=initial_options
        )
    assert "AVAILABLE" == response

def test_delete_task(name, get_arn, initial_options):
    datasync_client = MyDataSyncClient()
    locations = create_locations(datasync_client, create_smb=True, create_s3=True)
    response = datasync_client.delete_task(
            task_arn=get_arn,
            source_location_arn=locations["smb_arn"],
            destination_location_arn=locations["s3_arn"],
            name=name,
            metadata=initial_options
        )
    assert "TaskDeleted" == response

def test_update_task(name, get_arn, initial_options, updated_options):
    datasync_client = MyDataSyncClient()
    locations = create_locations(datasync_client, create_smb=True, create_s3=True)
    response = datasync_client.update_task(
            task_arn=get_arn,
            source_location_arn=locations["smb_arn"],
            destination_location_arn=locations["s3_arn"],
            name=name,
            metadata=initial_options,
            u_metadata=updated_options
        )
    assert updated_options == response.metadata




    