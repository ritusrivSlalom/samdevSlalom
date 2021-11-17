#
#Lambda function used to write inbound IoT sensor data to RDS PostgeSQL database
#
import sys
import os
import json
import psycopg2
import boto3
import base64
from botocore.exceptions import ClientError


region_name =  os.environ['AWS_REGION']
ssm_client = boto3.client('ssm')

def get_parameter(name):
    parameter = ssm_client.get_parameter(Name=name)
    return parameter['Parameter']['Value']

#Connect to PostgreSQL database and insert sensor data record
def handler(event, context):
    
    print("Getting DB parameters...")
    db_endpoint=get_parameter("/gh-bip/"+region_name+"/db_endpoint")
    db_user=get_parameter("/gh-bip/"+region_name+"/db_username")
    # Create a Secrets Manager client
    print("getting secrets")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(SecretId='bip_db_pass')

    #print(get_secret_value_response)
    secret = get_secret_value_response['SecretString']
    if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
    
    try:
        conn = psycopg2.connect(host=db_endpoint, port=5432, dbname='bipanalysisdb', user=db_user, password=secret['bip_db_pass'])
        conn.autocommit = True
        print("connected ")
        cur = conn.cursor()
        create_table1 = '''
            CREATE TABLE gh_bip_data_copy 
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
        cur.close()
    except Exception as e:
        print("Unable to connect to the database")
        print(str(e))
    
        
    #No except statement is used since any exceptions should fail the function so that the
    #failed message is sent to the SQS destination configured for the Lambda function
    finally:
        try:
            conn.close()
        except:
            pass

#Used when testing from the Linux command line    
if __name__== "__main__":
    handler(None, None)
