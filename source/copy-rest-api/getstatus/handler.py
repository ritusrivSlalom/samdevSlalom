import json

import sys
import os
import json
import psycopg2
import boto3
import logging
from datetime import datetime

region_name = os.environ['AWS_REGION']
def return_success(taskname, status, err):
    return {
        'statusCode': 200,
        'body': json.dumps({
            "Task_name" : taskname,
            "Status" : status,
             "Error": err
            })
    }


def return_error(taskname, status, err):
    return {
        'statusCode': 401,
        'body': json.dumps({
            "Task_name" : taskname,
            "Status" : status,
             "Error": err
            })
    }


TASKNAME = "GH_BIP_TASK"


REGION = os.environ['AWS_REGION']
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

        print(get_secret_value_response)
        db_password = get_secret_value_response['SecretString']
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

def any_inprogress_task(task_name):
    dbConnection = getDBConnection()
    src, dest, status = "","",""
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT task_name, sourcename, destinationname, status FROM gh_bip_data_copy WHERE task_name = '%s' ORDER BY "id" DESC LIMIT 1""" % (task_name)
        cur.execute(readQuery)
        rows = cur.fetchall()
        print("The number of rows returned: ", cur.rowcount)
        for row in rows:
            print(row)
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


def lambda_handler(event, context):

    try:
        print(event)
        print("------------------")
        try: 
            task_name = event['queryStringParameters']['task_name']
            print(task_name)
        except Exception as e:
            print(f"Missing parameters. Exception : {e}")
            return return_error("", "FAILED", "task_name parameter missing!!!")
        if (len(task_name) == 0 ):
            print("Valid Parameters not defined!")
            return return_error("", "FAILED", "task_name parameter empty!!!")
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        return return_error("", "FAILED", "Missing parameters!!!")

    try:
        (task_name, src, dest, status)=any_inprogress_task(task_name)
        if status:
            return return_success(task_name, status, "")
        else:
            return return_error(task_name, "FAILED", "No such task exists for copy!!!")
    except Exception as err:
        print(f"Unable to execute the fucntion. Exception : {err}")
        raise err
        sys.exit(1)