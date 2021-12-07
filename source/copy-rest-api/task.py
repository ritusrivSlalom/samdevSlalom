import json
print('Loading function')

def start_copy(event, context):
    print(event)
    print("value1 = " + event['queryStringParameters']['SourceLocation'])
    print("value2 = " + event['queryStringParameters']['DestinationLocation'])
    print("value3 = " + event['queryStringParameters']['TaskName'])

    return {

       'statusCode': '200',
       'body': json.dumps("Success from data copy")

   }