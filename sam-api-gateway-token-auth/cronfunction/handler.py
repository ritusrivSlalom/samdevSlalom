import json
import sys
import os
import json
import boto3
import logging
from datetime import datetime
import time
import psycopg2

SRC_BUCKET="gh-pcluster-automation-bucket-dev"      # Mainly for test - remove later

REGION = os.environ['AWS_REGION']

client = boto3.client("datasync", region_name=os.environ['AWS_REGION'])

singleton_connection = None # TODO: Create singleton class to replace this POC

def getAccountID():
    client = boto3.client("sts", region_name=os.environ['AWS_REGION'])
    try:
        account_id = client.get_caller_identity()["Account"]
    except Exception as err:
        print(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id

AgnetARN="arn:aws:datasync:eu-west-2:063935053328:agent/agent-05cfb5347b04ab37a"
accntid=getAccountID()
S3RoleArn="arn:aws:iam::"+accntid+":role/s3_data_sync_access"
is_dev = True

def get_parameter(name):
    try:
        conn = boto3.client("ssm", region_name=os.environ['AWS_REGION'])
        parameter = conn.get_parameter(Name=name)
    except Exception as err:
        print(f"Unable to get the params from SSM. Exception - {err}")
        sys.exit(1)
    return parameter['Parameter']['Value']


def getDBCredentials():
    print("Getting DB parameters...")
    db_endpoint = get_parameter("/gh-bip/" + REGION + "/db_endpoint")
    db_user = get_parameter("/gh-bip/" + REGION + "/db_username")

    try:
        # Create a Secrets Manager client
        print("getting secrets")
        conn = boto3.client("secretsmanager", region_name=os.environ['AWS_REGION'])
        get_secret_value_response = conn.get_secret_value(SecretId='bip_db_pass')

        # print(get_secret_value_response)
        db_password = get_secret_value_response['SecretString']
        if 'SecretString' in get_secret_value_response:
            db_password = json.loads(get_secret_value_response['SecretString'])
    except Exception as err:
        print(f"Unable to get the db credentails from SecretManger. Exception - {err}")
        sys.exit(1)
    return db_endpoint, db_user, db_password['bip_db_pass']


def getDBConnection():
    db_endpoint, db_user, db_password = getDBCredentials()
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='bipanalysisdb', user=db_user,
                                password=db_password)
        # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        conn.autocommit = True
        print("connected ")
    except Exception as err:
        print(f"Unable to connect to the database. Exception {err}")
        sys.exit(1)

    return conn


# A task ID is an indentifier referencing an executing DataSync task
def getTaskId(task_name):
    global singleton_connection
    print("Getting task id")
    try:
        cur = singleton_connection.cursor()
        readQuery = """SELECT task_id FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (task_name)
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("read status")
        if cur.rowcount > 0:
            task_id = rows[0][0]
            print(str(task_id))
        else:
            task_id = ""
    except Exception as err:
        print(f"Unable to read the task_id from the database. Exception: {err}")
        sys.exit(1)
    return task_id

# An execution_id ID is an indentifier referencing a running (in various states) DataSync task
def getExecutionId(task_name):
    global singleton_connection
    print("getting execution id")
    try:
        cur = singleton_connection.cursor()
        readQuery = """SELECT execution_id FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (task_name)
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("read status")
        if cur.rowcount > 0:
            execution_id = rows[0][0]
            print(str(execution_id))
        else:
            execution_id = ""
    except Exception as err:
        print(f"Unable to read the execution_id from the database. Exception: {err}")
        sys.exit(1)
    return execution_id

def updateTaskId(task_name, task_arn):
    global singleton_connection
    try:
        print("updating task Arn as :" + task_arn)
        taskid = [x.strip() for x in task_arn.split('/') if x]
        task_id=taskid.pop()
        print("task_id:" + task_id)
        cur = singleton_connection.cursor()
        updateQuery = """update gh_bip_data_copy SET task_id = '%s'  WHERE task_name = '%s'""" % (task_id, task_name)
        cur.execute(updateQuery)
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
        # Commit the changes to the database
        singleton_connection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the status in the database. Exception: {err}")
    return updated_rows

def updateExecId(task_name, execution_arn):
    global singleton_connection
    try:
        print("updating execution Arn as :"+execution_arn)
        taskid = [x.strip() for x in execution_arn.split('/') if x]
        execution_id = taskid.pop()
        cur = singleton_connection.cursor()
        updateQuery = """update gh_bip_data_copy SET execution_id = '%s'  WHERE task_name = '%s'""" % (execution_id, task_name)
        cur.execute(updateQuery)
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
        # Commit the changes to the database
        singleton_connection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the status in the database. Exception: {err}")
    return updated_rows


def update_db_status(task_name, newstatus):
    global singleton_connection
    try:
        print("updating task status as : " + newstatus)
        cur = singleton_connection.cursor()
        updateQuery = """update gh_bip_data_copy SET status = '%s'  WHERE task_name = '%s'""" % (newstatus.upper(), task_name)
        cur.execute(updateQuery)
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
        # Commit the changes to the database
        singleton_connection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the status in the database. Exception: {err}")
    return updated_rows


def create_locations(client, src, destbucket):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    nfs_arn = None
    s3_arn = None
    
    sourceVal = [x.strip() for x in src.split('/') if x]    # Extract the last segment of the folder(s) path
    sourceVal = sourceVal.pop()
    if is_dev:
        #S3RoleArn = createRole(dest)
        try:
            response = client.create_location_s3(
                S3BucketArn="arn:aws:s3:::" + SRC_BUCKET,
                Subdirectory=sourceVal,
                S3Config={"BucketAccessRoleArn": S3RoleArn},
            )
            nfs_arn = response["LocationArn"]
            print("S3 prefix:" + sourceVal)
        except Exception as err:
            print(f"Unable to create datasync source location. Exception: {err}")
            sys.exit(1)
    else:
        try:
            response = client.create_location_nfs(
                ServerHostname="ghdevhome.ghdna.io",
                Subdirectory=src,
                OnPremConfig={
            		'AgentArns': [
                	    AgnetARN,
            		]
        		},
        	    MountOptions={
        	        'Version': 'AUTOMATIC'
        	    }
            )
            print("nfs location")
            print(response)
            nfs_arn = response["LocationArn"]
        except Exception as err:
            print(f"Unable to create datasync source location. Exception: {err}")
            sys.exit(1)
    
    print("Creating src/dest Task locations :")
    try:
        response = client.create_location_s3(
            S3BucketArn="arn:aws:s3:::" + destbucket,
            Subdirectory=sourceVal, # Have the destination mimic the source path
            S3Config={"BucketAccessRoleArn": S3RoleArn},
        )
        s3_arn = response["LocationArn"]
    except Exception as err:
        print(f"Unable to create datasync destination location. Exception: {err}")
        sys.exit(1)
    return {"nfs_arn": nfs_arn, "s3_arn": s3_arn}


def create_task(src, dest):
    print("Creating a Task : src=" + src)
    print("Creating a Task : dest bucket=" + dest)
    try:
        locations = create_locations(client, src, dest)
        sourceVal = [x.strip() for x in src.split('/') if x]
        TASKNAME = "GH_BIP_TASK_" + sourceVal.pop()
        print(TASKNAME)
        response=client.create_task(
		        SourceLocationArn=locations["nfs_arn"],
		        DestinationLocationArn=locations["s3_arn"],
		        Name=TASKNAME,
		)
        task_arn=response['TaskArn']
        print("Task ARN")
        print(task_arn)
        updateTaskId(TASKNAME,task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")

def describe_task(task_arn):
    response = client.describe_task(TaskArn=task_arn)
    return response["Status"]

def start_exec(task_name,task_arn):
    print("Starting Task execution :"+task_name)
    try:
        response=client.start_task_execution(TaskArn=task_arn)
        task_execution_arn = response["TaskExecutionArn"]
        updateExecId(task_name, task_execution_arn)
        time.sleep(30) 
        response = client.describe_task_execution(TaskExecutionArn=task_execution_arn)
        return response["Status"]
    except Exception as err:
        print(f"Unable to start task : {task_name}, Exception: {err}")


def any_inprogress_task():
    global singleton_connection
    task_name, src, dest, status = "","","",""
    try:
        cur = singleton_connection.cursor()
        readQuery = """SELECT task_name, sourcename, destinationname, status, task_id FROM gh_bip_data_copy WHERE status != '%s' and status != '%s' ORDER BY "id" DESC""" % ('COMPLETED','CANCEL')
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("The number of rows returned: ", cur.rowcount)
        if cur.rowcount > 0:
            (task_name, src, dest, status, task_id) = rows[0]
            print(str(status))
    except Exception as err:
        print(f"Unable to read the status from the database. Exception: {err}")
        sys.exit(1)
    return rows

def get_db_status(task_name):
    global singleton_connection
    try:
        cur = singleton_connection.cursor()
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
    return status


def publish_message(error_msg):
    snsclient = boto3.client('sns')
    lambda_func_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
    try:
        message = ""
        message += "\nLambda error  summary" + "\n\n"
        message += "##########################################################\n"
        message += "# LogGroup Name:- " + os.environ['AWS_LAMBDA_LOG_GROUP_NAME'] + "\n"
        message += "# LogStream:- " + os.environ['AWS_LAMBDA_LOG_STREAM_NAME'] + "\n"
        message += "# Log Message:- " + "\n"
        message += "# \t\t" + str(error_msg.split("\n")) + "\n"
        message += "##########################################################\n"
        accntid=getAccountID()
        # Sending the notification...
        snsclient.publish(
            TargetArn="arn:aws:sns:"+REGION+":"+accntid+":gh-bip-notify",
            Subject=f'Execution error for Lambda - {lambda_func_name[3]}',
            Message=message
        )
    except Exception as e:
        print(f"Unable to publish message. Exception: {e}")


def handler(event, context):
    # TODO implement event parser
    global singleton_connection
    print("Inside CronFunction..")
    print(event)
    try:
        singleton_connection = getDBConnection()
        rows = any_inprogress_task()
        for (task_name, src, dest, status, task_id) in rows:
            print (task_name, src, dest, status)
            if status == 'TASK_CREATION' and task_id is not None:
                task_id=getTaskId(task_name)
                task_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id
                taskStatus = describe_task(task_arn)
                if taskStatus == 'AVAILABLE':
                    status=start_exec(task_name,task_arn)
                    update_db_status(task_name, 'EXEC_INPROGRESS')     
            elif status == 'TASK_CREATION' and task_id is None: ## If task doesnt exists
                task_arn=create_task(src, dest)
                wU = True
                while wU == True:
                    taskStatus=describe_task(task_arn)
                    print(taskStatus)
                    if taskStatus == 'AVAILABLE':
                        wU = False
                    else:
                        time.sleep(30)
                status=start_exec(task_name,task_arn)
                update_db_status(task_name, 'EXEC_INPROGRESS')
            elif status in ['QUEUED','LAUNCHING','PREPARING','TRANSFERRING','VERIFYING','EXEC_INPROGRESS']:
                task_id=getTaskId(task_name)
                exec_id=getExecutionId(task_name)
                task_execution_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id+"/execution/"+exec_id
                response = client.describe_task_execution(TaskExecutionArn=task_execution_arn)
                update_db_status(task_name, response["Status"])
                if response["Status"] == 'ERROR':
                    publish_message("Error while data copy in task "+task_name+" and Execution Id : "+exec_id)
            elif status in ['SUCCESS','ERROR']:    
                task_id=getTaskId(task_name)
                task_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id
                status=start_exec(task_name,task_arn)
                update_db_status(task_name, 'EXEC_INPROGRESS')
                task_id=getTaskId(task_name)
                exec_id=getExecutionId(task_name)
                task_execution_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id+"/execution/"+exec_id
                response = client.describe_task_execution(TaskExecutionArn=task_execution_arn)
                update_db_status(task_name, response["Status"])
                if response["Status"] == 'ERROR':
                    publish_message("Error while data copy in task "+task_name+" and Execution Id : "+exec_id)
                else:
                    # See if copy_complete.txt is in destination
                        # Construct the destination S3 bucket+folders via new construct_destination(task_name, src, dest)   # return destination_endpoint = dest's bucket + src's folders
                        # find_copy_complete(destination_endpoint)  # return copy_complete = true or false
                        # if (copy_complete):
                        #   if ( response["EstimatedFilesToTransfer"] >= response["FilesTransferred"]):  # 2nd stage: either all files transferred or there's more
                        #       if ( response["EstimatedBytesToTransfer"] <= response["BytesTransferred"] || response["EstimatedBytesToTransfer"] == 0  ):  # 3rd stage:  No more to copy over
                        #           update_db_status(task_name, 'SUCCESS')  # <= what should be the final status? 'DONE' ? 
                        #   (All else cases are just to let above cron logic cycle thru again)
                    pass
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        publish_message("Error in copy scheduler: "+err)
        raise err
        sys.exit(1)
    finally:
        try:
            singleton_connection.close()
        except:
            print(f"Unable to close DB connection. Exception : {err}")
            raise err
            sys.exit(1)
            
if __name__ == "__main()__":
    handler(None, None)
