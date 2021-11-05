
#
# Lambda function used to write inbound IoT sensor data to RDS PostgeSQL database
#
import sys
import os
import json
import psycopg2
import boto3
import base64
from botocore.exceptions import ClientError
import logging
from datetime import datetime

secret_name = "arn:aws:secretsmanager:eu-west-2:063935053328:secret:bip_db_pass-b2Yqp4"
# region_name = os.environ['AWS_REGION']

# Enable the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S %Z')
ch.setFormatter(formatter)
logger.addHandler(ch)

sampleAPIJSON={"sourceLocation": "/ghdevhome/binfs/210303_A01021_0222_BHY2V2DSXY/",
               "destinationLocation": "bip-analysis-bucket",
               "taskname": "GH_BIP_TASK"}

# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session
        #session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
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
    conn,_ = aws_connect_client("sts")
    try:
        account_id = conn.get_caller_identity()["Account"]
    except Exception as err:
        logger.error(f"Unable to get Account ID. Exception: {err}")
        sys.exit(1)
    return account_id

def getDBConnection():
    # conn = psycopg2.connect(host='bip-db.cluster-c5jsjiocmimr.eu-west-2.rds.amazonaws.com', port=5432, dbname='bipanalysisdb', user='bipadmin', password=secret['bip_db_pass'])

    conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
    conn.autocommit = True
    return conn

# def getTaskStatus():

def generateInsertData():
    sourceVal = [x.strip() for x in sampleAPIJSON["sourceLocation"].split('/') if x]
    taskName = f"{sampleAPIJSON['taskname']}_{sourceVal[2]}"
    print(taskName)
    getDate = datetime.now()
    getTimestamp = getDate.strftime("%m-%d-%Y %H:%M:%S")
    sourceEndPoint = sampleAPIJSON["sourceLocation"]
    destinationEndpoint = sampleAPIJSON["destinationLocation"]
    taskStatus = "TASK_CREATION"
    print(taskName,getTimestamp,sourceEndPoint,destinationEndpoint,taskStatus)
    return taskName,getTimestamp,sourceEndPoint,destinationEndpoint,taskStatus

def createTable():
    dbConnection = getDBConnection()
    cur = dbConnection.cursor()
    create_table1 = '''
        CREATE TABLE gh_bip_data_copy_test
            (
                id serial PRIMARY KEY,
                task_name VARCHAR ( 50 ),
                task_id VARCHAR ( 255 ),
                execution_id VARCHAR ( 255 ),
                sourcename VARCHAR ( 255 ) NOT NULL,
                destinationname VARCHAR ( 255 ) NOT NULL,
                status VARCHAR ( 50 ),
                created_on TIMESTAMP NOT NULL,
                upated_on TIMESTAMP
            );
    '''
    cur.execute(create_table1)
    print("Table is created")

def readDB():
    dbConnection = getDBConnection()
    cur = dbConnection.cursor()
    cur.execute("select * from gh_bip_data_copy_test")
    rows = cur.fetchall()
    for row in rows:
        print(f"{row[0]} {row[1]} {row[2]}")

def instertDB():
    dbConnection = getDBConnection()
    cur = dbConnection.cursor()
    cur.execute("""
    INSERT INTO gh_bip_data_copy_test (id,task_name,task_id,execution_id,sourcename,destinationname,status,created_on,upated_on)
    VALUES (1,'task1','taskid1','executionid1','s3s1','s3d1','inprogress','2021-06-22 19:10:25-07','2021-06-22 19:10:25-07');
    """)
    cur.execute("""
    INSERT INTO gh_bip_data_copy_test (id,task_name,task_id,execution_id,sourcename,destinationname,status,created_on,upated_on)
    VALUES (2,'task2','taskid2','executionid2','s3s2','s3d2','inprogress','2021-06-22 19:10:25-07','2021-06-22 19:10:25-07');
    """)
    print('Values inserted to PostgreSQL')

# Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    # Create a Secrets Manager client
    print("getting secrets")
    conn, region = aws_connect_client("secretsmanager")
    # get_secret_value_response = conn.get_secret_value(SecretId=secret_name)
    # print(get_secret_value_response)
    # secret = get_secret_value_response['SecretString']
    # if 'SecretString' in get_secret_value_response:
    #     secret = json.loads(get_secret_value_response['SecretString'])
    #
    # print("secret2 : " + secret['bip_db_pass'])
    try:
        # conn = psycopg2.connect(host='bip-db.cluster-c5jsjiocmimr.eu-west-2.rds.amazonaws.com', port=5432,
        #                         dbname='bipanalysisdb', user='bipadmin', password=secret['bip_db_pass'])
        #
        # # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        # conn.autocommit = True
        # print("connected ")
        dbConnection = getDBConnection()
        cur = dbConnection.cursor()
        # execute a statement
        # print('PostgreSQL database version:')
        # cur.execute('SELECT version()')
        #
        # # display the PostgreSQL database server version
        # db_version = cur.fetchone()
        # print(db_version)
        print(sampleAPIJSON["sourceLocation"])
        #createTable()
        #instertDB()
        readDB()
        generateInsertData()
        dbConnection.close()
    except Exception as e:
        print("Unable to connect to the database")
        print(str(e))


    # No except statement is used since any exceptions should fail the function so that the
    # failed message is sent to the SQS destination configured for the Lambda function
    finally:
        try:
            dbConnection.close()
        except:
            pass

handler("","")