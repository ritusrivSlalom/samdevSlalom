import json
import sys
import os
import json
import boto3
import logging
from datetime import datetime
import time
import psycopg2

REGION = os.environ['AWS_REGION']

datasyncclient = boto3.client("datasync", region_name=os.environ['AWS_REGION'])

def getAccountID():
    # client = boto3.client("sts", region_name=os.environ['AWS_REGION'])
    # try:
    #     account_id = client.get_caller_identity()["Account"]
    # except Exception as err:
    #     print(f"Unable to get Account ID. Exception: {err}")
    #     sys.exit(1)
    account_id = "063935053328"
    return account_id

AgnetARN="arn:aws:datasync:eu-west-2:063935053328:agent/agent-05cfb5347b04ab37a"
accntid=getAccountID()
S3RoleArn="arn:aws:iam::"+accntid+":role/s3_data_sync_access"
is_dev = False

def get_parameter(name):
    try:
        conn = boto3.client("ssm", region_name=os.environ['AWS_REGION'])
        parameter = conn.get_parameter(Name=name)
    except Exception as err:
        print(f"Unable to get the params from SSM. Exception - {err}")
        sys.exit(1)
    return parameter['Parameter']['Value']


def getDBCredentails():
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


def getTaskId(task_name):
    
    dbConnection = getDBConnection()
    print("Getting task id")
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT task_id FROM gh_bip_data_copy WHERE task_name = (%task_name) ORDER BY "id" DESC LIMIT 1"""
        cur.execute(readQuery, [task_name])
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
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return task_id

def getExecutionId(task_name):
    print("getting execution id")
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT execution_id FROM gh_bip_data_copy WHERE task_name = (%task_name) ORDER BY "id" DESC LIMIT 1"""
        cur.execute(readQuery, [task_name])
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
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return execution_id

def updateTaskId(task_name, task_arn):
    dbConnection = getDBConnection()
    try:
        print("updating task Arn as :"+task_arn)
        taskid = [x.strip() for x in task_arn.split('/') if x]
        print("task_id")
        task_id=taskid.pop()
        cur = dbConnection.cursor()
        updateQuery = """update gh_bip_data_copy SET task_id = (%task_id)  WHERE task_name = (%task_name)"""
        cur.execute(updateQuery, [task_id, task_name])
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
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

def updateExecId(task_name, execution_arn):
    dbConnection = getDBConnection()
    try:
        print("updating execution Arn as :"+execution_arn)
        taskid = [x.strip() for x in execution_arn.split('/') if x]
        execution_id = taskid.pop()
        task_id = taskid.pop(-2)
        cur = dbConnection.cursor()
        print(execution_id)
        print(task_id)
        updateQuery = """UPDATE gh_bip_data_copy SET execution_id = '(%execution_id)', task_id = '(%task_id)' WHERE task_name = '(%task_name)'"""
        cur.execute(updateQuery, [execution_id, task_id, task_name])
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
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


def update_db_status(task_name, newstatus):
    dbConnection = getDBConnection()
    try:
        print("updating task status as : "+newstatus)
        cur = dbConnection.cursor()
        newstatus_u= newstatus.upper()
        updateQuery = """UPDATE gh_bip_data_copy SET status = '(%newstatus_u)'  WHERE task_name = '(%task_name)'""" 
        cur.execute(updateQuery, [newstatus_u, newstatus])
        updated_rows = cur.rowcount
        print ("updated rows: ")
        print(updated_rows)
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


def create_locations(client, src, dest):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    nfs_arn = None
    s3_arn = None

    print("creating nfs location : ")
    print("agent arn "+AgnetARN)
    try:
        response = datasyncclient.create_location_nfs(
            ServerHostname="ghdevhome.ghdna.io",
            Subdirectory=src,
            OnPremConfig={
                'AgentArns': [AgnetARN]
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
        publish_message("Unable to create datasync source location. Exception: "+str(err))
        sys.exit(1)
    
    print("Creating a Task locations :")
    sourceVal = [x.strip() for x in src.split('/') if x]
    print("S3 prefix")
    prefix=sourceVal.pop()
    print(prefix)
    try:
        response = datasyncclient.create_location_s3(
            S3BucketArn="arn:aws:s3:::"+dest,
            Subdirectory=prefix,
            S3Config={"BucketAccessRoleArn": S3RoleArn},
        )
        s3_arn = response["LocationArn"]
    except Exception as err:
        print(f"Unable to create datasync deatination location. Exception: {err}")
        publish_message("Unable to create datasync deatination location. Exception: "+str(err))
        sys.exit(1)
    return {"nfs_arn": nfs_arn, "s3_arn": s3_arn}


def create_task(src, dest):
    print("Creating a Task : source: "+src)
    print("Creating a Task : dest: "+dest)
    try:
        locations = create_locations(datasyncclient, src, dest)
        sourceVal = [x.strip() for x in src.split('/') if x]
        TASKNAME = "GH_BIP_TASK_"+sourceVal.pop()
        print(TASKNAME)
        print("Exclude pattern : ")
        exclude_pattern = get_parameter("/gh-bip/" + REGION + "/exclude-pattern")
        print(exclude_pattern)

        options = {
            "VerifyMode": "ONLY_FILES_TRANSFERRED",
            "Atime": "BEST_EFFORT",
            "Mtime": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "LogLevel": "BASIC",
            "TransferMode": "CHANGED",
        }
        response=datasyncclient.create_task(
		        SourceLocationArn=locations["nfs_arn"],
		        DestinationLocationArn=locations["s3_arn"],
                CloudWatchLogGroupArn="arn:aws:logs:"+REGION+":"+accntid+":log-group:/aws/datasync",
		        Name=TASKNAME,
                Options=options,
                Excludes=[
                    {
                        'FilterType': 'SIMPLE_PATTERN',
                        'Value': exclude_pattern
                    },
                ]
		)
        task_arn=response['TaskArn']
        print("Task ARN")
        print(task_arn)
        updateTaskId(TASKNAME,task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        publish_message("Create Task Failed, Exception: "+str(e))

def describe_task(task_arn):
    response = datasyncclient.describe_task(TaskArn=task_arn)
    return response["Status"]


def start_exec(task_name,task_arn):
    print("Starting Task execution :"+task_name)
    try:
        response=datasyncclient.start_task_execution(TaskArn=task_arn)
        task_execution_arn = response["TaskExecutionArn"]
        print(task_execution_arn)
        updateExecId(task_name, task_execution_arn)
        time.sleep(30) 
        response = datasyncclient.describe_task_execution(TaskExecutionArn=task_execution_arn)
        print(response)
        return response["Status"]
    except Exception as err:
        print(f"Unable to start task : {task_name}, Exception: {err}")
        publish_message("Unable to start task : "+task_name+", Exception: "+str(err))


def any_inprogress_task():
    dbConnection = getDBConnection()
    task_name, src, dest, status = "","","",""
    try:
        cur = dbConnection.cursor()
        not_acceptabe_status_1 = 'completed'
        not_acceptabe_status_2 = 'cancel'
        readQuery = """SELECT task_name, sourcename, destinationname, status, task_id FROM gh_bip_data_copy WHERE lower(status) != '(%not_acceptabe_status_1)' and lower(status) != '(%not_acceptabe_status_2)' ORDER BY "id" DESC""" 
        cur.execute(readQuery, [not_acceptabe_status_1, not_acceptabe_status_2])
        rows = cur.fetchall()
        print("The number of rows returned: ", cur.rowcount)
        if cur.rowcount > 0:
            (task_name, src, dest, status, task_id) = rows[0]
            print(str(status))
    except Exception as err:
        print(f"Unable to read the status from the database. Exception: {err}")
        publish_message("Unable to read the data from the database. Exception: "+str(err))
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return rows

def get_db_status(task_name):
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT status FROM gh_bip_data_copy WHERE task_name = '(%task_name)' ORDER BY "id" DESC LIMIT 1"""
        cur.execute(readQuery, [task_name])
        rows = cur.fetchall()
        print("read status")
        if len(rows) > 0:
            status = rows[0][0]
            print(str(status))
        else:
            status = ""
    except Exception as err:
        print(f"Unable to read the data from the database. Exception: {err}")
        publish_message("Unable to read the data from the database. Exception: "+err)
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
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
    # TODO implement
    print("Inside CronFunction..")
    print(event)
    try:
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
                if task_id and exec_id:
                    task_execution_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id+"/execution/"+exec_id
                    response = datasyncclient.describe_task_execution(TaskExecutionArn=task_execution_arn)
                    print(response)
                    update_db_status(task_name, response["Status"])
                    if response["Status"] == 'ERROR':
                        print("task status : Error")
                        if response['Result']['ErrorCode'] != 'CompleteWithErrorLogsWithNoVerificationError' :
                            publish_message("Error while data copy in task "+task_name+" and Execution Id : "+exec_id)
                else:
                    publish_message("Error while data copy in task "+task_name)
                    print("No task_id or execution id found in database")
            elif status in ['SUCCESS','ERROR']:    
                task_id=getTaskId(task_name)
                task_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id
                status=start_exec(task_name,task_arn)
                update_db_status(task_name, 'EXEC_INPROGRESS')
                task_id=getTaskId(task_name)
                exec_id=getExecutionId(task_name)
                if task_id and exec_id:
                    task_execution_arn="arn:aws:datasync:"+REGION+":"+accntid+":task/"+task_id+"/execution/"+exec_id
                    response = datasyncclient.describe_task_execution(TaskExecutionArn=task_execution_arn)
                    print(response)
                    update_db_status(task_name, response["Status"])
                    if response["Status"] == 'ERROR':
                        if response['Result']['ErrorCode'] != 'CompleteWithErrorLogsWithNoVerificationError' :
                            publish_message("Error while data copy in task "+task_name+" and Execution Id : "+exec_id)
                else:
                    publish_message("Error while data copy in task "+task_name)
                    print("No task_id or execution id found in database")
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        publish_message("Error in copy scheduler: "+str(err))
        raise err
        sys.exit(1)

if __name__ == "__main()__":
    handler(None, None)