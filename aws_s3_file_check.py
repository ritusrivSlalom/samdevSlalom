import boto3
from datetime import datetime, timezone

today = datetime.now(timezone.utc)

# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session
        #session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        session = boto3.Session()
        my_session = boto3.session.Session()
        REGION = my_session.region_name
        # Connect the resource
        conn_client = session.client(service, REGION)
    except Exception as e:
        print('Could not connect to region: %s and resources: %s , Exception: %s\n' % (REGION, service, e))
        conn_client = None
    return conn_client

def checkCompletedFile():
    bucket_name = "slalomtesting1"
    file_path = "test/"
    fileName = "copy_complete.txt"
    s3_client = aws_connect_client("s3")
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=f'{file_path}{fileName}')
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("The copy_complete.txt file is present")
        else:
            print("THe copy_complete.txt file is not present")
    except Exception as err:
        print(f"Unable to find the copy_complete.txt file in the bucket : {bucket_name}. Exception: {err}")
        raise err


def handler(event, context):
    # TODO implement
    print("Inside cronFunction..")
    print(event)
    checkCompletedFile()

# local testing
handler("test","test")