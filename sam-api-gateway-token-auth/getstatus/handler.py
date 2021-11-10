import json

def lambda_handler(event, context):
    # TODO implement
    print("Inside Get Status Function..")
    print(event)
    return {
        'statusCode': 200,
        'body': json.dumps({
            "Task_name" : "GH_BIP_TASK_210303_A01021_0222_BHY2V2DSXY",
            "Status" : "EXECUTION_Completed"
            })
    }