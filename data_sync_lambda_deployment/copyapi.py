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

accntid=getAccountID()

AgnetARN="arn:aws:datasync:"+REGION+":"+accntid+":agent/agent-05cfb5347b04ab37a"

S3RoleArn="arn:aws:iam::"+accntid+":role/s3_data_sync_access"



def create_locations(client, src, dest):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    nfs_arn = None
    s3_arn = None
    dest = "datasync-target-bucket"
    #     response = client.create_location_nfs(
    #         ServerHostname="ghdevhome.ghdna.io",
    #         Subdirectory=src,
    #         OnPremConfig={
    #     		'AgentArns': [
    #         	    AgnetARN,
    #     		]
    # 		},
    # 	    MountOptions={
    # 	        'Version': 'AUTOMATIC'
    # 	    }
    #     )
    #     print("nfs location")
    #     print(response)
    #     nfs_arn = response["LocationArn"]
    ## To be removed
    srcresponse = client.create_location_s3(
        S3BucketArn="arn:aws:s3:::ghbi-bisre-terraform-state-royal-marrsden-testing",
        S3Config={"BucketAccessRoleArn": S3RoleArn},
    )
    src_s3_arn = response["LocationArn"]
    nfs_arn = src_s3_arn
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

def publish_message(error_msg):
    sns_arn = os.environ['snsARN']  # Getting the SNS Topic ARN passed in by the environment variables.
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
    except ClientError as e:
        logger.error("An error occured: %s" % e)




def get_parameter(name):
    try:
        conn = boto3.client("ssm", region_name=REGION)
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
        conn = boto3.client("secretsmanager", region_name=REGION)
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

def any_inprogress_task():
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


def handler(event, context):
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

