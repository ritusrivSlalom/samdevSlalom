
import json
import sys
import os
import json
import boto3
import logging
from datetime import datetime

region_name = os.environ['AWS_REGION']

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
    client = boto3.client("sts", region_name=os.environ['AWS_REGION'])
    try:
        account_id = client.get_caller_identity()["Account"]
    except Exception as err:
        print(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id


def getARNSD():
    AgnetARN = "arn:aws:datasync:" + region_name + ":" + getAccountID() + ":agent/agent-05cfb5347b04ab37a"
    S3RoleArn = "arn:aws:iam::" + getAccountID() + ":role/s3_data_sync_access"
    return AgnetARN ,S3RoleArn

# def create_locations(client, src, dest):
#     """
#     Convenience function for creating locations.
#     Locations must exist before tasks can be created.
#     """
#     nfs_arn = None
#     s3_arn = None
#     dest = "datasync-target-bucket"
#     response = client.create_location_nfs(
#         ServerHostname="ghdevhome.ghdna.io",
#         Subdirectory=src,
#         OnPremConfig={
#         'AgentArns': [
#              AgnetARN,
#         ]
#     },
#      MountOptions={
#          'Version': 'AUTOMATIC'
#      }
#     )
#     print("nfs location")
#     print(response)
#     nfs_arn = response["LocationArn"]
## To be removed
# srcresponse = client.create_location_s3(
#     S3BucketArn="arn:aws:s3:::ghbi-bisre-terraform-state-royal-marrsden-testing",
#     S3Config={"BucketAccessRoleArn": S3RoleArn},
# )
# src_s3_arn = response["LocationArn"]
# nfs_arn = src_s3_arn
# sourceVal = [x.strip() for x in src.split('/') if x]
# print("S3 prefix")
# print(sourceVal.pop())
#
# response = client.create_location_s3(
#     S3BucketArn="arn:aws:s3:::" + dest,
#     Subdirectory=sourceVal.pop(),
#     S3Config={"BucketAccessRoleArn": S3RoleArn},
# )
# s3_arn = response["LocationArn"]
# return {"nfs_arn": nfs_arn, "s3_arn": s3_arn}

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

def createDSLocation(src, dest):
    '''
    Creates a Destination S3 Location in the Datasync
    :param src:
    :param dest:
    :return:
    '''
    datasync_list = aws_datasync_conn.list_locations()
    get_datasync_lists = [i['LocationUri'] for i in datasync_list['Locations'] if "s3" in i['LocationUri']]
    dest = "datasync-target-bucket"
    s3arn = createS3(dest)
    _, roleARN = getARNSD()
    try:
        if any(dest in string for string in get_datasync_lists):
            print("The datasync location is already created.please check once")
        else:
            datasync_create = aws_datasync_conn.create_location_s3(S3BucketArn=f"{s3arn}", S3StorageClass="STANDARD",
                                                                   S3Config={'BucketAccessRoleArn': f'{roleARN}'},
                                                                   Tags=[
                                                                       {
                                                                           'Key': 'Name',
                                                                           'Value': f'DataSync-Location-{dest}'
                                                                       },
                                                                   ]
                                                                   )
            if datasync_create['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("Datasync location is created...")
                ds_location_arn = datasync_create["LocationArn"]
    except Exception as err:
        print(f"Unable to create a datasync location, Exception: {err}")

    # return {"nfs_arn": nfs_arn, "s3_arn": ds_location_arn}
    return {"s3_arn": ds_location_arn}

def create_task():
    try:
        src = "/ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/"
        dest = "bip-analysis-bucket"
        client = boto3.client("datasync", region_name=os.environ['AWS_REGION'])
        locations = create_locations(client, src, dest)
        sourceVal = [x.strip() for x in src.split('/') if x]
        TASKNAME = "GH_BIP_TASK_" + sourceVal.pop()
        print(TASKNAME)
        response = client.create_task(
            SourceLocationArn=locations["nfs_arn"],
            DestinationLocationArn=locations["s3_arn"],
            Name=TASKNAME,
        )
        task_arn = response['TaskArn']
        print("Task ARN")
        print(task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")

# Make AWS API call to AWS SSM Params
def get_parameter(name):
    print(name)
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
            print(db_password)
    except Exception as err:
        print(f"Unable to get the db credentails from SecretManger. Exception - {err}")
        sys.exit(1)
    return db_endpoint, db_user, db_password['bip_db_pass']

def getDBConnection():
    db_endpoint, db_user, db_password = getDBCredentails()
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='bipanalysisdb', user=db_user,
                                password=db_password)
        # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        conn.autocommit = True
        print("connected ")
    except Exception as err:
        print(f"Unable to connnct the database. Exception {err}")
        sys.exit(1)

    return conn


def any_inprogress_task():
    dbConnection = getDBConnection()
    task_name, src, dest, status = "", "", "", ""
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT task_name, src, dest, status FROM gh_bip_data_copy WHERE status != '%s' and status != '%s' ORDER BY "id" DESC LIMIT 1""" % (
            'COMPLETED', 'CANCEL')
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("The number of parts: ", cur.rowcount)
        if len(rows) > 0:
            (task_name, src, dest, status) = rows[0]
            print(str(status))
    except Exception as err:
        print(f"Unable to read the status from the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return (task_name, src, dest, status)


def get_db_status(task_name):
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT status FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (
            task_name)
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("read status")
        if len(rows) > 0:
            status = rows[0][0]
            print(str(status))
        else:
            status = ""
    except Exception as err:
        print(f"Unable to read the data from the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return status


def update_db_status(task_name, newstatus):
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        updateQuery = """update gh_bip_data_copy SET status = '%s'  WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (
            newstatus, task_name)
        cur.execute(updateQuery)
        updated_rows = cur.rowcount
        # Commit the changes to the database
        dbConnection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the status in the database. Exception: {err}")
    finally:
        if dbConnection is not None:
            dbConnection.close()
    return updated_rows

def update_execution_id(task_name, executionID):
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        updateQuery = """update gh_bip_data_copy SET execution_id = '%s'  WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (
            executionID, task_name)
        cur.execute(updateQuery)
        updated_rows = cur.rowcount
        # Commit the changes to the database
        dbConnection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the executionID in the database. Exception: {err}")
    finally:
        if dbConnection is not None:
            dbConnection.close()
    return updated_rows

def getTaskList():
    try:
        response = aws_datasync_conn.list_tasks()
        print(response)
        getTaskARN = [i['TaskArn'] for i in response['Tasks']]
        getTaskNames = [i['Name'] for i in response['Tasks'] if i['Name'] ]
        print(getTaskNames)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return response['Tasks']
    except Exception as err:
        print(f"Unable to list the tasks . Exception : {err}")

def describe_task(task_name):
    # `# TO DO
    try:
        for i in getTaskList():
            if task_name == i['Name']:
                taskARN = i['TaskArn']
                resp_desc_executions = aws_datasync_conn.describe_task(TaskArn=taskARN)
                # task_execution_arn = resp_list_executions["CurrentTaskExecutionArn"]
                if resp_desc_executions['ResponseMetadata']['HTTPStatusCode'] == 200:
                    Status = resp_desc_executions['Status']
                    return Status
                else:
                    Status = ""
                    return Status
    except Exception as err:
        print(f"unable to get list of task and task status. Exception: {err}")

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

def handler(event, context):
    # TODO implement
    print("Inside cronFunction..")
    print(event)
    global aws_datasync_conn

    aws_datasync_conn = aws_connect_client("datasync")

    try:
        (task_name, src, dest, status) = any_inprogress_task()
        if task_name:
            print(task_name, src, dest, status)
            if status == 'TASK_CREATION':
                getAWSTaskStatus = describe_task(task_name)
                if len(getAWSTaskStatus) > 0 and getAWSTaskStatus and getAWSTaskStatus == "AVAILABLE":
                    TaskExecutionArn = start_exec()
                    exec_id = [x.strip() for x in TaskExecutionArn.split('/') if x].pop()
                    update_db_status(task_name ,exec_id)
                    status = "EXEC_INPROGRESS"
                    update_execution_id(task_name ,status)
                else:
                    pass

            #     '''
            #     Check db if any row with status != 'COMPLETED/CANCEL'
            #     If exists:
            #         get task_name, src and dest, status
            #         if status == 'TASK_CREATION'
            #         If task exists and query task status is 'AVAILABLE'
            #          #           exec_id, status = start_exec()
            #                   update db status as 'EXEC_INPROGRESS'
            #         else ## If task doesnt exists
            #          #       create_task()
            #          #       wait 30 secs
            #          #       describe_task(task_name)
            #          #       if task status is 'AVAILABLE'
            #          #       then exec_id, status = start_exec()
            #                    update db status as 'EXEC_INPROGRESS'
            #          elseif status == 'QUEUED'|'LAUNCHING'|'PREPARING'|'TRANSFERRING'|'VERIFYING'|'EXEC_INPROGRESS'
            #          #           Exestatus = describe_execution(task_execution_arn);
            #          #           update status in DB
            #        #           If status == 'ERROR'
            #                      cloudwatch Alarm
            #          elseif status == 'SUCCESS'|'ERROR'
            #          #           exec_id, status = start_exec()
            #                   update db status as 'EXEC_INPROGRESS'
            #                   If status == 'ERROR'
            #                      cloudwatch Alarm
            #  else:
            #     do nothing
            #     '''
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)