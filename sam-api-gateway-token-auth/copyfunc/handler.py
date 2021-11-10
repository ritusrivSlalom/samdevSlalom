import sys
import os
import json
import psycopg2
import boto3
import logging
from datetime import datetime

region_name = os.environ['AWS_REGION']
def return_success(taskname, status, err):
    responsebody = {
            "Task_name": taskname,
            "Status": status,
            "ERROR": ""
        }
    print(responsebody)
    return {
        "statusCode": 200,
       "headers": {
        "content-type": "application/json"
        },
        "body": json.dumps(responsebody),
        "isBase64Encoded": False
    }


def return_error(taskname, status, err):
    responsebody = {
            "Task_name": taskname,
            "Status": status,
            "ERROR": err
        }
    print(responsebody)
    return {
        "statusCode": 401,
       "headers": {
        "content-type": "application/json"
        },
        "body": json.dumps(responsebody),
        "isBase64Encoded": False,
    }


TASKNAME = "GH_BIP_TASK"


# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session
        # session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        session = boto3.Session()
        my_session = boto3.session.Session()
        REGION = my_session.region_name
        # Connect the resource
        conn_client = session.client(service, REGION)
    except Exception as e:
        print('Could not connect to region: %s and resources: %s , Exception: %s\n' % (REGION, service, e))
        conn_client = None
    return conn_client, REGION


def getAccountID():
    conn, _ = aws_connect_client("sts")
    try:
        account_id = conn.get_caller_identity()["Account"]
    except Exception as err:
        print(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id


# Make AWS API call to AWS SSM Params
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


# generate values for insert query
def generateInsertData():
    getDate = datetime.now()
    getTimestamp = getDate.strftime("%m-%d-%Y %H:%M:%S")
    sourceEndPoint = sourceLocation
    destinationEndpoint = destinationLocation
    taskStatus = "TASK_CREATION"
    return gtaskName, getTimestamp, sourceEndPoint, destinationEndpoint, taskStatus


# read the table
def readDB():
    #global readRows
    # Need to read IDs in order to generate the ID pimary key
    #global readIDRows

    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT status FROM gh_bip_data_copy WHERE sourcename = '%s' AND destinationname = '%s' ORDER BY "id" DESC LIMIT 1""" % (
        sourceLocation, destinationLocation)
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("read rows")
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

def get_max_id():
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readIDQuery = """SELECT max(id) FROM gh_bip_data_copy"""
        cur.execute(readIDQuery)
        try:
            maxid = cur.fetchone()[0]
            print("maxid=")
            print(maxid)
        except Exception:
            maxid = 0
    except Exception as err:
        print(f"Unable to read the maxid. Exception: {err}")
        return 0
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return maxid




# Initial - insert data into the database
def instertDB():
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        task_name, created_on, sourcename, destinationname, status = generateInsertData()
        # generate the primary key
        getLPK = get_max_id()
        if getLPK is None:
            getLPK = 0
        print("-----")
        print(getLPK)
        print("-----")
        id = getLPK + 1
        cur.execute("""
        INSERT INTO gh_bip_data_copy (id,task_name,sourcename,destinationname,status,created_on)
        VALUES (%s, %s,%s, %s, %s, %s);
        """, (id, task_name, sourcename, destinationname, status, created_on))
        print('Values inserted to PostgreSQL')
    except Exception as err:
        print(f"Unable to insert the data into the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass


# condition to check the source and destination locations are present in the database.
def conditionToCheckSD(gtaskName):
    taskStatusRow = readDB()
    print("======================")
    print("Status = "+taskStatusRow)
    
    if not taskStatusRow or len(taskStatusRow) == 0:
        print("New source and destination in request...")
        print("Creating new task..."+gtaskName)
        instertDB()
        return return_success(gtaskName, "SUCCESS", "None")
    else:
        if taskStatusRow == 'COMPLETED':
            print("The pervious task is completed. Inserting new row")
            instertDB()
            return return_success(gtaskName, "SUCCESS", "None")
        else:
            print("Task "+gtaskName+" with same source and destination already exists and task status is "+ taskStatusRow)
            return return_error(gtaskName, "FAILED", "Task with same Source and Destination is already exists and Task execution in-progress")
        


# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    global sourceLocation
    global destinationLocation
    global gtaskName

    try:
        print(event)
        print("------------------")
        request_body = json.loads(event.get('body'))
        print(request_body)
        sourceLocation = request_body['src']
        destinationLocation = request_body['dest']
        #sourceLocation = " /ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/"
        #destinationLocation = "bip-analysis-bucket"
        if (len(sourceLocation) == 0 or len(destinationLocation) == 0):
            print("Valid Parameters not defined!")
            return return_error(sourceLocation, "FAILED", "Valid Parameters not defined for src/dest!")
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        return return_error(sourceLocation, "FAILED", "Missing parameters")

    sourceVal = [x.strip() for x in sourceLocation.split('/') if x]
    gtaskName = f"{TASKNAME}_{sourceVal.pop()}"
    try:
        response=conditionToCheckSD(gtaskName)
        return response
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)