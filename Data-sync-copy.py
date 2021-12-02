import sys
import os
import json
# import psycopg2
import boto3
import logging
from datetime import datetime

# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session

        #session = boto3.Session()
        my_session = boto3.session.Session()
        region_name = my_session.region_name
        # print(region_name)
        # Connect the resource
        conn_client = session.client(service, 'us-east-1')
    except Exception as e:
        print('Could not connect to region: %s and resources: %s , Exception: %s\n' % (region_name, service, e))
        conn_client = None
    return conn_client


def describe_execution(task_execution_arn):
    ## TO DO
    exec_status = 'success'
    try:
        resp_desc_task_exection = aws_datasync_conn.describe_task_execution(TaskExecutionArn=task_execution_arn)
        if resp_desc_task_exection["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(resp_desc_task_exection)
            print(resp_desc_task_exection["Status"])
            print("EstimatedFilesToTransfer: ", resp_desc_task_exection["EstimatedFilesToTransfer"])
            print("EstimatedBytesToTransfer: ", resp_desc_task_exection["EstimatedBytesToTransfer"])
            print("FilesTransferred: ", resp_desc_task_exection["FilesTransferred"])
            return resp_desc_task_exection["Status"]
    except Exception as err:
        print(f"Unable to describe the task execution")

# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    global sourceLocation
    global destinationLocation
    global gtaskName
    global aws_datasync_conn
    aws_datasync_conn = aws_connect_client("datasync")
    TASKNAME = "GH_BIP_TASK"

    try:
        sourceLocation = event['queryStringParameters']['src']
        destinationLocation = event['queryStringParameters']['dest']
        if (len(sourceLocation) == 0 or len(destinationLocation) == 0):
            print("Valid Parameters not defined!")
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")

    sourceVal = [x.strip() for x in sourceLocation.split('/') if x]
    gtaskName = f"{TASKNAME}_{sourceVal.pop()}"

    try:

        describe_execution("arn:aws:datasync:us-east-1:826587600013:task/task-0ac9917c1d00b9646/execution/exec-0b345126c15b8d288")
        # create_task()
        #start_exec("GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY")
        #createDSLocation("bip-analysis-bucket-vv1")
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)

event = {
  "queryStringParameters": {
    "src": "/ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/",
    "dest": "bip-analysis-bucket"
  }
}
handler(event,"")