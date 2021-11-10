import json
import sys
import os
import json
import boto3
import logging
from datetime import datetime

REGION = os.environ['AWS_REGION']
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
def dbconnect():


def readDBstatus(task_name):
    ## Check db if same source, location any task exists, If 
    ## read all executions status 



def create_locations(client, src, dest):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    nfs_arn = None
    s3_arn = None
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

    sourceVal = [x.strip() for x in src.split('/') if x]
    print("S3 prefix")
    print(sourceVal.pop())
    
    response = client.create_location_s3(
        S3BucketArn="arn:aws:s3:::"+dest,
        Subdirectory=sourceVal.pop(),
        S3Config={"BucketAccessRoleArn": S3RoleArn},
    )
    s3_arn = response["LocationArn"]
    return {"nfs_arn": nfs_arn, "s3_arn": s3_arn}


def create_task():
    try:
        src="/ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/"
        dest="bip-analysis-bucket"
        client = boto3.client("datasync", region_name=os.environ['AWS_REGION'])
        locations = create_locations(client, src, dest)
        sourceVal = [x.strip() for x in src.split('/') if x]
        TASKNAME = "GH_BIP_TASK_"+sourceVal.pop()
        print(TASKNAME)
        response=client.create_task(
		        SourceLocationArn=locations["nfs_arn"],
		        DestinationLocationArn=locations["s3_arn"],
		        Name=TASKNAME,
		)
        task_arn=response['TaskArn']
        print("Task ARN")
        print(task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")




def start_exec():
    ## TO DO
    # start_task_execution
    # response = client.start_task_execution(TaskArn=task_arn)
    #task_execution_arn = response["TaskExecutionArn"]
        #           wait 30 secs
        #           Exestatus= describe_execution(task_execution_arn); 
        #           if Exestatus == 'ERROR'
        #           response = client.start_task_execution(TaskArn=task_arn)
        #           wait 30 secs
        #           Exestatus= describe_execution(task_execution_arn); 
        #           if Exestatus == 'ERROR'
        #               update status in DB and raise exception
    #return exec_id, status

def get_task_status(task_name):


def describe_task(task_name):
    ## TO DO
    #response = client.describe_task(TaskArn=task_arn)
    #task_execution_arn = response["CurrentTaskExecutionArn"]
    #task_status = 'success'
    #return task_status

def describe_execution(task_execution_arn):
    ## TO DO
    
    exec_status = 'success'

    #response = client.describe_task_execution(TaskExecutionArn=task_execution_arn)
    # response["Status"] == "INITIALIZING"
    # response["Status"] == "SUCCESS"

    #return response["Status"]
def get_parameter(name):
    try:
        conn, _ = aws_connect_client("ssm")
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
        conn, _ = aws_connect_client("secretsmanager")
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

def any_inprogress_task(task_name):
    dbConnection = getDBConnection()
    task_name, src, dest, status = "","","",""
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT task_name, src, dest, status FROM gh_bip_data_copy WHERE status != '%s' and status != '%s' ORDER BY "id" DESC LIMIT 1""" % ('COMPLETED','CANCEL')
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
        updateQuery = """update gh_bip_data_copy SET status = '%s'  WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (newstatus, task_name)
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


def lambda_handler(event, context):
    # TODO implement
    print("Inside cronFunction..")
    print(event)
    try:
        (task_name, src, dest, status) = any_inprogress_task()
        if task_name:
            print (task_name, src, dest, status)
            #     '''
            #     Check db if any row with status != 'COMPLETED/CANCEL'
            #     If exists:
            #         get task_name, src and dest, status
            #         if status == 'TASK_CREATION'
            # 	       If task exists and query task status is 'AVAILABLE' 
            # 	        #           exec_id, status = start_exec()
            # 	        			update db status as 'EXEC_INPROGRESS'     
            # 	       else ## If task doesnt exists
            # 	        #       create_task()
            # 	        #       wait 30 secs
            # 	        #       describe_task(task_name)
            # 	        #       if task status is 'AVAILABLE'
            # 	        #       then exec_id, status = start_exec()
            # 	        			 update db status as 'EXEC_INPROGRESS' 
            # 	        elseif status == 'QUEUED'|'LAUNCHING'|'PREPARING'|'TRANSFERRING'|'VERIFYING'|'EXEC_INPROGRESS'
            # 	        #           Exestatus = describe_execution(task_execution_arn); 
            # 	        #           update status in DB
            # 			#           If status == 'ERROR'
            # 	        				cloudwatch Alarm
            # 	        elseif status == 'SUCCESS'|'ERROR'      
            # 	        #           exec_id, status = start_exec()
            # 	        			update db status as 'EXEC_INPROGRESS' 
            # 	        			If status == 'ERROR'
            # 	        				cloudwatch Alarm
            # 	else:
            # 		do nothing
            #     '''
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)


    


