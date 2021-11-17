import logging
import jwt
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import boto3
from ipaddress import ip_network, ip_address
import json

##Globals

region_name =  os.environ['AWS_REGION']
secret_name = "gh-key-name"
bucket_name = "gh-pcluster-automation-bucket"
pem_key = "bipkey.pem"
pub_key = "bipkey.pub"

def read_secrets():
    print("getting secrets")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try: 
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            iprange = secret['ip_range']
            auth_key = secret['gh-key']
            return (iprange,auth_key)
        else:
            print("Could not fetch secrets")
            return 1
    except Exception as e:
        print("Unable to fetch secrets")
        print(str(e))
        return 1
print("before secret call")
(iprange,auth_key) = read_secrets()
print("after secret call")
def validate_ip(Callerip, iprange):
    print("Validating IP..."+Callerip)
    print("WhiteList IP Range : "+iprange)
    for ipsub in iprange.split(","):
        net = ip_network(ipsub)
        print("cidr range...")
        print(net)
        print("user ip")
        print(Callerip)
        if (ip_address(Callerip) in net):
            print("Valid IP : "+Callerip)
            return 0
        else:
            print("Not Authorized IP Address")
            ret = 1
    return ret




def authorizer(event, context):
    encoded = event['headers']['Authorization']
    methodArn = event['methodArn']
    # if event['httpMethod'] == 'POST':
    #     source = event['queryStringParameters']['src']
    #     dest = event['queryStringParameters']['dest']
    #     if (len(source) == 0 or len(dest) == 0):
    #         print("Valid Parameters not defined!")
    #         return generateAuthResponse('BipUser', 'Deny', methodArn)
    #     else:
    #         return generateAuthResponse('BipUser', 'Allow', methodArn)
    
    # if event['httpMethod'] == 'GET':
    #     taskname = event['queryStringParameters']['task_name']
    #     if (len(taskname) == 0):
    #         print("Valid Task Name Parameter not defined!")
    #         return generateAuthResponse('BipUser', 'Deny', methodArn)
    #     else:
    #         return generateAuthResponse('BipUser', 'Allow', methodArn)

    Callerip = event['requestContext']['identity']['sourceIp']
    ## Read from S3 s3://gh-pcluster-automation-bucket-dev/mykey.pem
    # try:
    #     (iprange,auth_key) = read_secrets()
    # except Exception as e:
    #     print("Unable to retrieve Secrets!")
    #     print(str(e))
    #     return generateAuthResponse('BipUser', 'Deny', methodArn)
    ## Get required Bucket which holds key to decode
    s3_client = boto3.client('s3')
    # response = s3_client.list_buckets()
    # for bucket in response['Buckets']:
    #   if bucket['Name'].startswith( 'gh-pcluster-automation-bucket' ):
    #       print(bucket['Name'])
    #       bucket_name = bucket['Name']
    # if len(bucket_name) == 0 :
    #     print("Automation Bucket does not exists")
    #     return generateAuthResponse('BipUser', 'Deny', methodArn)

    # s3_clientobj = s3_client.get_object(Bucket=bucket_name, Key=pem_key)
    # private_key  =   s3_clientobj['Body'].read()

    # encoded = jwt.encode({"auth_key": auth_key}, private_key, algorithm="RS256")
    # print("ENCODE : "+encoded)
    try:
        s3_clientobj = s3_client.get_object(Bucket=bucket_name, Key=pub_key)
        public_key  =   s3_clientobj['Body'].read()
    except Exception as e:
        print("Unable to load public key to decrypt the payload")
        print(str(e))
        return generateAuthResponse('BipUser', 'Deny', methodArn)

    try: 
        print("encoded token")
        print(encoded)
        decoded = jwt.decode(encoded, public_key, algorithms=["RS256"])
        # print("DECODE : "+str(decoded))
        # enc = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE2MzY2ODQ5MTAsImlhdCI6MTYzNjY3ODkxMCwiaXNzIjoiZ2gtZ2F0ZXdheSIsImFwaV9rZXkiOiIzYzI4YjQ1Yi0xODFkLTQzYWQtYTU4Yi1mNmVkOTA3OGM4M2UxLTEyMzgifQ.AF9g0-wWdoTxLr-35oFkKBYUwTYmBSJCBp0bxlztW9jMDKXvVdCXqGbk8TTb5KMaLdjW4tcs5qxlO6bmUYb8xEQ9u-0dyRSh_oXXqROrqXQBZJi3dfLN6gapQbXqkrtO8XAYg1YawPE2YRkhby4zGru6OAYZBZc2m1g346TUJc1577fBqiZirhaSqoTAMIFKICO_kzSIF3LL5lQozLb-VKIszCo2D50BIN0_BeStzoORXz4EY3GKRM5ctJxO-SkkQgO-z7YOPYooTHhlptB3pZ7gpnanPAJkShqnXaj7QJ2eyO8xfw81C9gdbCK7LIBzEEF4-m5d0dTzcnj12hPL-g"
        # dec = jwt.decode(enc, public_key, algorithms=["RS256"])
        # print ("new dec ")
        # print(dec)
        request_auth_key = decoded['auth_key']
        try:
            if request_auth_key == auth_key:
                print("API Key matched !!!")
            else:
                print("API Key does not match, Request unthorized !!!")
                return generateAuthResponse('BipUser', 'Deny', methodArn)
            ret=validate_ip(Callerip, iprange)
            print(str(ret))
            if ret == 1:
                print("Caller IP not in allowed WhiteList IP range!")
                return generateAuthResponse('BipUser', 'Deny', methodArn)
            ## All conditions satisfied, Now Allow the call
            return generateAuthResponse('BipUser', 'Allow', methodArn)
        except Exception as e:
            print("Unable to validate whiteList IPs from parameter store!")
            print(str(e))
            return generateAuthResponse('BipUser', 'Deny', methodArn)
    except jwt.ExpiredSignatureError as e:
        print("JWT Token expired!!!")
        print(str(e))
        return generateAuthResponse('BipUser', 'Deny', methodArn)
    except jwt.DecodeError as e:
        print("JWT Token can not be decoded, Invalid token!!!")
        print(str(e))
        return generateAuthResponse('BipUser', 'Deny', methodArn)
    except Exception as e:
        print("Unable to decode the payload")
        print(str(e))
        return generateAuthResponse('BipUser', 'Deny', methodArn)

    

def generateAuthResponse(principalId, effect, methodArn):
    authResponse = {}
    authResponse['principalId'] = principalId
    policyDocument = generatePolicyDocument(effect, methodArn)
    authResponse['policyDocument'] = json.loads(policyDocument)
    return authResponse


def generatePolicyDocument(effect, methodArn):
    policyDocument = '''{
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "execute-api:Invoke",
            "Effect": "''' + effect + '''",
            "Resource": "''' + methodArn + '''"
        }]
    }
    '''
    print(policyDocument)
    return policyDocument
