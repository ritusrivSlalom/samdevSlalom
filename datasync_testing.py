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

region_name = os.environ['AWS_REGION']

# Enable the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S %Z')
ch.setFormatter(formatter)
logger.addHandler(ch)


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
        logger.error('Could not connect to region: %s and resources: %s , Exception: %s\n' % (REGION, service, e))
        conn_client = None
    return conn_client, REGION


def getAccountID():
    conn, _ = aws_connect_client("sts")
    try:
        account_id = conn.get_caller_identity()["Account"]
    except Exception as err:
        logger.error(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id


# Make AWS API call to AWS SSM Params
def get_parameter(name):
    try:
        conn, _ = aws_connect_client("ssm")
        parameter = conn.get_parameter(Name=name)
    except Exception as err:
        logger.error(f"Unable to get the params from SSM. Exception - {err}")
        sys.exit(1)
    return parameter['Parameter']['Value']


def getDBCredentails():
    logger.info("Getting DB parameters...")
    db_endpoint = get_parameter("/gh-bip/" + region_name + "/db_endpoint")
    db_user = get_parameter("/gh-bip/" + region_name + "/db_username")

    try:
        # Create a Secrets Manager client
        logger.info("getting secrets")
        conn, _ = aws_connect_client("secretsmanager")
        get_secret_value_response = conn.get_secret_value(SecretId='bip_db_pass')

        # print(get_secret_value_response)
        db_password = get_secret_value_response['SecretString']
        if 'SecretString' in get_secret_value_response:
            db_password = json.loads(get_secret_value_response['SecretString'])
    except Exception as err:
        logger.error(f"Unable to get the db credentails from SecretManger. Exception - {err}")
        sys.exit(1)
    return db_endpoint, db_user, db_password['bip_db_pass']


def getDBConnection():
    db_endpoint, db_user, db_password = getDBCredentails()
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='bipanalysisdb', user=db_user,
                                password=db_password)
        # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        conn.autocommit = True
        logger.info("connected ")
    except Exception as err:
        logger.error(f"Unable to connnct the database. Exception {err}")
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


# def createTable():
#     dbConnection = getDBConnection()
#     cur = dbConnection.cursor()
#     create_table1 = '''
#         CREATE TABLE gh_bip_data_copy_test
#             (
#                 id serial PRIMARY KEY,
#                 task_name VARCHAR ( 50 ),
#                 task_id VARCHAR ( 255 ),
#                 execution_id VARCHAR ( 255 ),
#                 sourcename VARCHAR ( 255 ) NOT NULL,
#                 destinationname VARCHAR ( 255 ) NOT NULL,
#                 status VARCHAR ( 50 ),
#                 created_on TIMESTAMP NOT NULL,
#                 updated_on TIMESTAMP NULL
#             );
#     '''
#     cur.execute(create_table1)
#     print("Table is created")

# read the table
def readDB():
    global readRows
    # Need to read IDs in order to generate the ID pimary key
    global readIDRows

    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT id,status FROM gh_bip_data_copy_test WHERE sourcename = '%s' AND destinationname = '%s' ORDER BY "id" DESC LIMIT 1""" % (
        sourceLocation, destinationLocation)

        readIDQuery = """SELECT max(id) FROM gh_bip_data_copy_test"""
        cur.execute(readQuery)
        readRows = cur.fetchall()
        cur.execute(readIDQuery)
        readIDRows = cur.fetchall()
    except Exception as err:
        logger.error(f"Unable to read the data from the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass
    return readRows, readIDRows


# get the last primary key
def getLastPkey():
    getLastPKey = [row[0] for row in readIDRows]
    print(getLastPKey)
    print(readIDRows)
    if not getLastPKey:
        print("The ID row is empty hence set as 0")
        lastVal = 0
    else:
        lastVal = getLastPKey.pop()

    return lastVal


# Initial - insert data into the database
def instertDB():
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        task_name, created_on, sourcename, destinationname, status = generateInsertData()
        # generate the primary key
        getLPK = getLastPkey()
        id = getLPK + 1
        cur.execute("""
        INSERT INTO gh_bip_data_copy_test (id,task_name,sourcename,destinationname,status,created_on)
        VALUES (%s, %s,%s, %s, %s, %s);
        """, (id, task_name, sourcename, destinationname, status, created_on))
        logger.info('Values inserted to PostgreSQL')
    except Exception as err:
        logger.error(f"Unable to insert the data into the database. Exception: {err}")
        sys.exit(1)
    finally:
        try:
            dbConnection.close()
        except:
            pass


# condition to check the source and destination locations are present in the database.
def conditionToCheckSD():
    taskStatusRow, _ = readDB()
    if 'EXECUTION_Completed' in taskStatusRow:
        logger.info("The pervious task is completed. Inserting new row")
        instertDB()
    else:
        logger.error(f"The source and destination is already present and task is : {taskStatusRow}")
        return return_error(gtaskName, "FAILED", "The source and destination is already present/task is not completed")

    if not taskStatusRow:
        logger.info("The source and destination is not present in the table hence Inserting new row")
        instertDB()
        return return_success(gtaskName, "SUCCESS", "None")


# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    global sourceLocation
    global destinationLocation
    global gtaskName

    try:
        sourceLocation = event['queryStringParameters']['src']
        destinationLocation = event['queryStringParameters']['dest']
        if (len(sourceLocation) == 0 or len(destinationLocation) == 0):
            logger.error("Valid Parameters not defined!")
            return return_error(sourceLocation, "FAILED", "Valid Parameters not defined for src/dest!")
        else:
            return return_success(sourceLocation, "SUCCESS", "None")
    except Exception as e:
        logger.error(f"Missing parameters. Exception : {e}")
        return return_error(sourceLocation, "FAILED", "Missing parameters")

    sourceVal = [x.strip() for x in sourceLocation.split('/') if x]
    gtaskName = f"{TASKNAME}_{sourceVal.pop()}"

    try:
        conditionToCheckSD()
    except Exception as err:
        logger.error(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)