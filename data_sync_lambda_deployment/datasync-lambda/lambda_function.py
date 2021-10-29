import json
import boto3
import logging
import sys

# Define constants at once
taskARN="arn:aws:datasync:us-east-1:826587600013:task/task-00eae489ff5d99b5f"
FAILED_EXIT_CODE = 1

ACCESS_KEY=""
SECRET_KEY=""

# Enable the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S %Z')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session
        #session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        session = boto3.Session()
        my_session = boto3.session.Session()
        REGION = my_session.region_name
        # REGION = "us-east-1"
        # Connect the resource
        conn_client = session.client(service, REGION)
    except Exception as e:
        logger.error('Could not connect to region: %s and resources: %s , Exception: %s\n' % (REGION, service, e))
        conn_client = None
    return conn_client

def getTaskList():
    conn = aws_connect_client("datasync")
    try:
        response = conn.list_tasks()
        return response['Tasks'][0]['TaskArn']
    except Exception as err:
        logger.error(f"Unable to get the list of datasync task. Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)


def getTaskExection():
    conn = aws_connect_client("datasync")
    try:
        response = conn.list_task_executions(TaskArn=taskARN)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            for i in response['TaskExecutions']:
                if i['Status'] == "LAUNCHING":
                    logger.info(f"Please find the current execution: {i}")
            print("task initiated")
    except Exception as err:
        logger.error(f"Unable to get the list of datasync task. Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)

def startTask():
    conn = aws_connect_client("datasync")
    try:
        response = conn.start_task_execution(TaskArn=taskARN)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Task is initiated...")
            getTaskExection()
    except Exception as err:
        logger.error(f"Unable to get the list of datasync task. Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)


def lambda_handler(event, context):
    # TODO implement
    startTask()