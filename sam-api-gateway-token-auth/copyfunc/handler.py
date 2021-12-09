# LDH - backported from PROD copyfunc
import sys
import os
import json
import psycopg2
import boto3
import logging
from datetime import datetime

region_name = os.environ['AWS_REGION']
def return_code(retcode,taskname, status, err):
    responsebody = {
            "Task_name": taskname,
            "Status": status,
            "Error": err
        }
    print(responsebody)
    return {
        "statusCode": retcode,
       "headers": {
        "content-type": "application/json"
        },
        "body": json.dumps(responsebody),
        "isBase64Encoded": False
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
def readDB(task):
    #global readRows
    # Need to read IDs in order to generate the ID pimary key
    #global readIDRows

    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT status FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (task)
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
def insertDB(one_time_copy):
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
        INSERT INTO gh_bip_data_copy (id,task_name,sourcename,destinationname,status,created_on, one_time_copy)
        VALUES (%s, %s,%s, %s, %s, %s, %s); 
        """, (id, task_name, sourcename, destinationname, status, created_on, one_time_copy)) # %s == stringified boolean
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
def conditionToCheckSD(gtaskName, one_time_copy):
    taskStatusRow = readDB(gtaskName)
    print("===+==================")
    print("Status = " + taskStatusRow)
    
    if not taskStatusRow or len(taskStatusRow) == 0:
        print("conditionToCheckSD: Creating new task..." + gtaskName)
        insertDB(one_time_copy)
        return return_code(201, gtaskName, "TASK_CREATION", "")
    else:
        if taskStatusRow == 'COMPLETED':
            print("conditionToCheckSD: The previous task is completed. Inserting new row")
            insertDB(one_time_copy)
            return return_code(201, gtaskName, "TASK_CREATION", "")
        else:
            print("Task " + gtaskName+" with same source and destination already exists and task status is "+ taskStatusRow)
            return return_code(202, gtaskName, taskStatusRow, "Task with same Source and Destination is already exists and Task execution in-progress")
        


# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    global sourceLocation
    global destinationLocation
    global gtaskName

    environment = "production"  #default (not needed as a request param unless dev)
    try:
        print(event)
        print("------------------")
        try:
            print(type(event['body']))  # PROD API request has different format than dev
        except:
            environment = "dev"
            pass
        if ( environment == "dev"):
            request_body = event
            print("-+----------------")
            print("Request body type=" + str(type(request_body)))
            # print(request_body)   # NO - it's a dict now
        else:
            print("Request body type:" + type(event['body']))
            request_body = json.loads(event['body']) # <- PROD API Gway request
            print("JSON request type:" + type(request_body))
            print("JSON request:" + request_body)
        one_time_copy = True if (request_body['one_time_copy'] == "true") else False
        sourceLocation = request_body['src']
        destinationLocation = request_body['dest']
        print("source location : " + sourceLocation)
        print("dest location : " + destinationLocation)
        #sourceLocation = " /ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/"
        #destinationLocation = "bip-analysis-bucket"
        if (len(sourceLocation) == 0 or len(destinationLocation) == 0):
            print("Valid Parameters not defined!")
            return return_code(400, "", "FAILED", "Valid Parameters not defined for src/dest!")
            #return return_error(sourceLocation, "FAILED", "Valid Parameters not defined for src/dest!")
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        #return return_error(sourceLocation, "FAILED", "Missing parameters")
        return return_code(400,"", "FAILED", "Missing input parameters")

    sourceVal = [x.strip() for x in sourceLocation.split('/') if x]
    gtaskName = f"{TASKNAME}_{sourceVal.pop()}"
    try:
        response = conditionToCheckSD(gtaskName, one_time_copy)
        return response
    except Exception as err:
        print(f"Unable to execute the function. Exception : {err}")
        raise err
        sys.exit(1)