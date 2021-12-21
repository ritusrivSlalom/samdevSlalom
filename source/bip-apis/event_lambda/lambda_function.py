import json
import logging
import boto3
import urllib.parse
import os
import sys
import psycopg2

s3 = boto3.client('s3')
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
    completed_status = 'completed'
    cancel_status = 'cancel'
    try:
        cur = dbConnection.cursor()
        readQuery = """SELECT task_name, src, dest, status FROM gh_bip_data_copy WHERE lower(status) NOT IN (%s, %s) and task_name = %s ORDER BY "id" DESC LIMIT 1"""
        cur.execute(readQuery, [completed_status, cancel_status, task_name])
        rows = cur.fetchall()
        print("The number of parts: ", cur.rowcount)
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


def update_db_status(task_name, newstatus):
    dbConnection = getDBConnection()
    try:
        cur = dbConnection.cursor()
        updateQuery = """update gh_bip_data_copy SET status = %s  WHERE task_name = %s"""
        cur.execute(updateQuery, [newstatus, task_name])
        updated_rows = cur.rowcount
        # Commit the changes to the database
        dbConnection.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except Exception as err:
        print(f"Unable to update the status in the database. Exception: {err}")
        updated_rows = 0
    finally:
        if dbConnection is not None:
            dbConnection.close()
    return updated_rows
    


def handler(event, context):
    regionname =  os.environ['AWS_REGION']
    # Get the object from the event and show its content type
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    #bucket="bip-analysis-bucket-dev"
    #key="CopyTestFolder/210303_A01021_0222_BHY2V2DSXY/CopyComplete.txt"
    
    try:
        print("bucket: " + bucket)
        print("Key : "+ key)
        sourceVal = [x.strip() for x in key.split('/') if x]
        print("S3 prefix")
        prefix = sourceVal.pop(-2)
        TASKNAME = "GH_BIP_TASK_"+prefix
        updated_rows = update_db_status(TASKNAME, 'COMPLETED')
        print(updated_rows)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    
