import sys
import os
import json
import psycopg2
# import psycopg2_win as psycopg2
import boto3
import logging
from datetime import datetime
import uuid


region_name = os.environ['AWS_REGION']

# Enable the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def getAccountID():
    client = boto3.client('sts')
    response = client.get_caller_identity()['Account']
    return response
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
    print("Getting DB connection")
    db_endpoint, db_user, db_password = getDBCredentails()
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='bipanalysisdb', user=db_user,
                                password=db_password)
        # conn = psycopg2.connect(host='localhost', port=5432, dbname='test')
        conn.autocommit = True
        logger.debug("DB connected ")
    except Exception as err:
        logger.error(f"Unable to connnct the database. Exception {err}")
        sys.exit(1)

    return conn


def check_existing_flowcellid(dbConnection, ID):
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT execution_id, LOWER(status) FROM gh_analysis_job_execution WHERE run_id = %s ORDER BY "creation_time" DESC"""
        cur.execute(readQuery, [ID])
        rows = cur.fetchone()
        logger.info(rows)
        logger.debug("The number of rows returned: "+str(cur.rowcount))
        if cur.rowcount > 0:
            (exec_id, status) = rows
            print(exec_id, status)
        else:
            status = ""
            exec_id = ""
    except Exception as err:
        logger.error(f"Unable to read the status from the database. Exception: {err}")
        sys.exit(1)
    return  (exec_id, status)

def add_record(dbConnection, exec_id, status,product_name,schname, BIPversion,run_id,output_dir,image):
    try:
        cur = dbConnection.cursor()
        logger.info(exec_id, status,product_name,schname, BIPversion,run_id,output_dir,image)
        sql = """INSERT INTO gh_analysis_job_execution (execution_id,status,product_name,scheduler_name,bip_version,run_id,output_dir_name,bip_image_name,creation_time)
        VALUES (%s, %s,%s, %s, %s, %s, %s, %s, NOW());"""
        cur.execute(sql, (exec_id, status,product_name,schname, BIPversion,run_id,output_dir,image)) 
        logger.debug('Values inserted to gh_analysis_job_execution')
        logger.debug("The number of rows inserted: ", cur.rowcount)
    except Exception as err:
        logger.error(f"Unable to insert the data into the database. Exception: {err}")
        sys.exit(1)

def check_copy_status(dbConnection, ID):
    task_name = "GH_BIP_TASK_"+ID
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT lower(status) FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (task_name)
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
        status = ""
    return status