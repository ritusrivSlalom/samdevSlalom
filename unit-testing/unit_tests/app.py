import json

# import requests
from test_datasync import test_creat_task

def lambda_handler(event, context):
    

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": test_creat_task(),
            # "location": ip.text.replace("\n", "")
        }),
    }
