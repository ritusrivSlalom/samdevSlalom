#
# Lambda function used to write inbound IoT sensor data to RDS PostgeSQL database
#
import sys
import os
import json
import psycopg2
import boto3
import logging
from datetime import datetime

region_name = "us-east-1"
TASKNAME = "GH_BIP_TASK"

def return_success(taskname, status, err):
    return {
        'statusCode': 200,
        'body': json.dumps({
            "Task_name": f"{taskname}",
            "Status": f"{status}",
            "ERROR": f"{err}"
        })
    }


def return_error(taskname, status, err):
    return {
        'statusCode': 500,
        'body': json.dumps({
            "Task_name": f"{taskname}",
            "Status": f"{status}",
            "ERROR": f"{err}"
        })
    }

# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session

        session = boto3.Session()
        my_session = boto3.session.Session()
        # region_name = my_session.region_name
        # print(region_name)
        # Connect the resource
        conn_client = session.client(service, region_name)
    except Exception as e:
        print('Could not connect to region: %s and resources: %s , Exception: %s\n' % (region_name, service, e))
        conn_client = None
    return conn_client


def getAccountID():
    conn = aws_connect_client("sts")
    try:
        account_id = conn.get_caller_identity()["Account"]
    except Exception as err:
        print(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id

def getDataSyncRolePolicy(bucket):
    policy_json = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Effect": "Allow",
                "Resource": f"arn:aws:s3:::{bucket}"
            },
            {
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObjectTagging",
                    "s3:GetObjectTagging",
                    "s3:PutObject"
                ],
                "Effect": "Allow",
                "Resource": f"arn:aws:s3:::{bucket}/*"
            }
        ]
    }
    return policy_json

def createPolicy(bucket):
    conn = aws_connect_client("iam")
    try:
        policy_list = conn.list_policies()
        policyname_list = [i['PolicyName'] for i in policy_list['Policies']]
        policy_json = getDataSyncRolePolicy(bucket)
        policy_name = f"AWSDataSyncS3BucketAccess-{bucket}"
        if policy_name in policyname_list:
            policyARN = [i['Arn'] for i in policy_list['Policies'] if policy_name in i['PolicyName']][0]
            print(f"The policy is already present : {policyARN}")
        else:
            response = conn.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_json)
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"The policy has been created. Name: {policy_name}")
                policyARN = [i['Arn'] for i in policy_list['Policies'] if policy_name in i['PolicyName']]
    except Exception as err:
        print(f"Unable to create a policy Exception: {err}")
    return policyARN

def createRole(bucket):
    datasync_policy_json = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "datasync.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    conn = aws_connect_client("iam")
    role_name = f"AWSDataSyncS3BucketAccess-{bucket}-role"
    try:
        role_list = conn.list_roles()
        rolename_list = [i['RoleName'] for i in role_list['Roles']]
        policyARN = createPolicy(bucket)

        if role_name in rolename_list:
            roleARN = [i['Arn'] for i in role_list['Roles'] if role_name in i['RoleName']][0]
            print(f"the role is already present : {roleARN}")
        else:
            source_create_role_response = conn.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(datasync_policy_json)
            )
            roleARN = source_create_role_response["Role"]["Arn"]
            if source_create_role_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("Role is created...")

            policy_resp = conn.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policyARN
            )
            if policy_resp['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("Attached policy into the role...")
            #roleARN = [i['Arn'] for i in role_list['Roles'] if role_name in i['RoleName']]
    except Exception as err:
        print(f"Unable to create a role, Exception: {err}")
    return roleARN

def createS3(bucket):
    conn = aws_connect_client("s3")
    try:
        s3list = conn.list_buckets()
        s3name_list = [i['Name'] for i in s3list['Buckets']]
        if bucket in s3name_list:
            S3ARN = f"arn:aws:s3:::{bucket}"
            print(f"the s3bucket is already present : {bucket} and {S3ARN} ")

        else:
            s3response = conn.create_bucket(
                Bucket=f'{bucket}'
            )
            S3ARN = f"arn:aws:s3:::{bucket}"
            if s3response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("S3 bucket is created...")
    except Exception as err:
        print(f"Unable to create a s3, Exception: {err}")
    return S3ARN


def createDSLocation(bucket):
    datasync_list = aws_datasync_conn.list_locations()
    get_datasync_lists = [i['LocationUri'] for i in datasync_list['Locations'] if "s3" in i['LocationUri']]
    s3arn = createS3(bucket)
    roleARN = createRole(bucket)
    try:
        if any(bucket in string for string in get_datasync_lists):
            print("The datasync location is already created")
        else:
            datasync_create = aws_datasync_conn.create_location_s3(S3BucketArn=f"{s3arn}", S3StorageClass="STANDARD",
                                                                S3Config={'BucketAccessRoleArn': f'{roleARN}'},
                                                                Tags=[
                                                                    {
                                                                        'Key': 'Name',
                                                                        'Value': f'DataSync-Location-{bucket}'
                                                                    },
                                                                ]
                                                    )
            if datasync_create['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("Datasync bucket is created...")
    except Exception as err:
        print(f"Unable to create a datasync location, Exception: {err}")


# Make AWS API call to AWS SSM Params
def get_parameter(name):
    try:
        conn = aws_connect_client("ssm")
        parameter = conn.get_parameter(Name=name)
    except Exception as err:
        print(f"Unable to get the params from SSM. Exception - {err}")
        sys.exit(1)
    return parameter['Parameter']['Value']


def getDBCredentails():
    print("Getting DB parameters...")
    db_endpoint = get_parameter("/gh-bip/" + region_name + "/db_endpoint")
    db_user = get_parameter("/gh-bip/" + region_name + "/db_username")

    try:
        # Create a Secrets Manager client
        print("getting secrets")
        conn = aws_connect_client("secretsmanager")
        get_secret_value_response = conn.get_secret_value(SecretId='bip_db_pass')

        if 'SecretString' in get_secret_value_response:
            db_password = json.loads(get_secret_value_response['SecretString'])

    except Exception as err:
        print(f"Unable to get the db credentails from SecretManger. Exception - {err}")
        sys.exit(1)
    return db_endpoint, db_user, db_password['bip_db_pass']


def getDBConnection():
    db_endpoint, db_user, db_password = getDBCredentails()
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='postgres', user=db_user,
                                password=db_password)
        # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        conn.autocommit = True
        print("connected ")
    except Exception as err:
        print(f"Unable to connnct the database. Exception {err}")
        sys.exit(1)

    return conn


# read the table
def readDB(readQuery):
    # global readRows
    # Need to read IDs in order to generate the ID pimary key
    # global readIDRows

    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("read rows", rows)
        if len(rows) > 0:
            status = rows[0][0]
            taskName = rows[0][1]
            # print(str(status), str(taskName))
        else:
            status = ""
            taskName = ""
            print("the source or destination are not present in the table")
    except Exception as err:
        print(f"Unable to read the data from the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return status, taskName


def getTaskList():
    try:
        response = aws_datasync_conn.list_tasks()
        # getTaskARN = [i['TaskArn'] for i in response['Tasks']]
        # getTaskNames = [i['Name'] for i in response['Tasks']]
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return response['Tasks']
    except Exception as err:
        print(f"Unable to list the tasks . Exception : {err}")

def readDBstatus(task_name):
    ## Check db if same source, location any task exists, If
    ## read all executions status
    readQuery = """SELECT status,task_name FROM gh_bip_data_copy_test WHERE sourcename = '%s' AND destinationname = '%s' ORDER BY "id" DESC LIMIT 1""" % (
        sourceLocation, destinationLocation)
    statusDB, taskName = readDB(readQuery)
    if taskName:
        print(f"The task is present in the DB. TaskName : {taskName}")
        response = aws_datasync_conn.list_tasks()
        getTaskARN = [i['TaskArn'] for i in getTaskList()]
        getTaskNames = [i['Name'] for i in getTaskList()]

        for i in getTaskList():
            if taskName == i['Name']:
                print("i m un")
                taskARN = i['TaskArn']
                resp_list_executions = aws_datasync_conn.list_task_executions(TaskArn=taskARN)
                taskExecutionArn = [ i['TaskExecutionArn'] for i in resp_list_executions['TaskExecutions']]
                taskExecutionStatus = [ i['Status'] for i in resp_list_executions['TaskExecutions']]
                print(taskExecutionArn,taskExecutionStatus)
    return taskExecutionArn,taskExecutionStatus

def create_task():
    ## TO DO
    #
    # return task_id
    SourceLocationArn="arn:aws:datasync:us-east-1:826587600013:location/loc-0e8755850d5ad1a80"
    DestinationLocationArn="arn:aws:datasync:us-east-1:826587600013:location/loc-02306782d5d2ddffa"
    try:
        resp_create_task = aws_datasync_conn.create_task(
            SourceLocationArn=SourceLocationArn,
            DestinationLocationArn=DestinationLocationArn)
        if resp_create_task['ResponseMetadata']['HTTPStatusCode'] == 200:
            return resp_create_task['TaskArn']
    except Exception as err:
        print(f"unable to create a new task. Exception: {err}")

def start_exec(task_name):
    try:
        for i in getTaskList():
            if task_name == i['Name']:
                taskARN = i['TaskArn']
                response = aws_datasync_conn.start_task_execution(TaskArn=taskARN)
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print("Task is initiated...")
                    TaskExecutionArn = response["TaskExecutionArn"]
                    return TaskExecutionArn
    except Exception as err:
        print(f"Unable to start task : {task_name}, Exception: {err}")

def get_task_status(task_name):

    try:
        for i in getTaskList():
            if task_name == i['Name']:
                taskARN = i['TaskArn']
                resp_list_executions = aws_datasync_conn.list_task_executions(TaskArn=taskARN)
                taskExecutionArn = [i['TaskExecutionArn'] for i in resp_list_executions['TaskExecutions']]
                taskExecutionStatus = [ i['Status'] for i in resp_list_executions['TaskExecutions']]
                print(taskExecutionArn,taskExecutionStatus)
    except Exception as err:
        print(f"unable to get list of task and task status. Exception: {err}")

    return taskExecutionArn,taskExecutionStatus

def describe_task(task_name):
    # `# TO DO
    try:
        for i in getTaskList():
            if task_name == i['Name']:
                taskARN = i['TaskArn']
                resp_desc_executions = aws_datasync_conn.describe_task(TaskArn=taskARN)
                # task_execution_arn = resp_list_executions["CurrentTaskExecutionArn"]
                print(resp_desc_executions)
                print(resp_desc_executions['Status'])
    except Exception as err:
        print(f"unable to get list of task and task status. Exception: {err}")

    return resp_desc_executions['Status']

def describe_execution(task_execution_arn):
    ## TO DO
    exec_status = 'success'
    try:
        resp_desc_task_exection = aws_datasync_conn.describe_task_execution(TaskExecutionArn=task_execution_arn)
        if resp_desc_task_exection["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(resp_desc_task_exection["Status"])
            return resp_desc_task_exection["Status"]
    except Exception as err:
        print(f"Unable to describe the task execution")

# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    global sourceLocation
    global destinationLocation
    global aws_datasync_conn
    aws_datasync_conn = aws_connect_client("datasync")

    try:
        sourceLocation = event['queryStringParameters']['src']
        destinationLocation = event['queryStringParameters']['dest']
        if (len(sourceLocation) == 0 or len(destinationLocation) == 0):
            print("Valid Parameters not defined!")
            return return_error(sourceLocation, "FAILED", "Valid Parameters not defined for src/dest!")
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        return return_error(sourceLocation, "FAILED", "Missing parameters")


    try:
        readDBstatus("GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY")
        #get_task_status("GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY")
        #describe_task("GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY")
        #describe_execution("arn:aws:datasync:us-east-1:826587600013:task/task-0ac9917c1d00b9646/execution/exec-0d4b8ad19b9d17da4")
        #create_task()
        #start_exec("GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY")
        #createDSLocation("bip-analysis-bucket-vv2")
    except Exception as err:
        print(f"Unable to execute the function. Exception : {err}")
        raise err
        sys.exit(1)
