import json

def dbconnect():


def readDBstatus(task_name):
    ## Check db if same source, location any task exists, If 
    ## read all executions status 

def create_task():
    ## TO DO
    # 
    #return task_id

def start_exec():
    ## TO DO
    # start_task_execution
    # response = client.start_task_execution(TaskArn=task_arn)
    #task_execution_arn = response["TaskExecutionArn"]
        #           wait 30 secs
        #           Exestatus= describe_execution(task_execution_arn); 
        #           if Exestatus == 'ERROR'
        #           response = client.start_task_execution(TaskArn=task_arn)
        #           wait 30 secs
        #           Exestatus= describe_execution(task_execution_arn); 
        #           if Exestatus == 'ERROR'
        #               update status in DB and raise exception
    #return exec_id, status

def get_task_status(task_name):


def describe_task(task_name):
    ## TO DO
    #response = client.describe_task(TaskArn=task_arn)
    #task_execution_arn = response["CurrentTaskExecutionArn"]
    #task_status = 'success'
    #return task_status

def describe_execution(task_execution_arn):
    ## TO DO
    
    exec_status = 'success'

    #response = client.describe_task_execution(TaskExecutionArn=task_execution_arn)
    # response["Status"] == "INITIALIZING"
    # response["Status"] == "SUCCESS"

    #return response["Status"]

def lambda_handler(event, context):
    # TODO implement
    print("Inside cronFunction..")
    print(event)
    try:
        Check db if any row with status != 'COMPLETED/CANCEL'
         If exists:
               get task_name, src and dest, status
               if status == 'TASK_CREATION'
                       If task exists and query task status is 'AVAILABLE' 
                        #           exec_id, status = start_exec()
                                    update db status as 'EXEC_INPROGRESS'     
                       else ## If task doesnt exists
                        #       create_task()
                        #       wait 30 secs
                        #       describe_task(task_name)
                        #       if task status is 'AVAILABLE'
                        #       then exec_id, status = start_exec()
                                     update db status as 'EXEC_INPROGRESS' 
                elseif status == 'QUEUED'|'LAUNCHING'|'PREPARING'|'TRANSFERRING'|'VERIFYING'|'EXEC_INPROGRESS'
                        #           Exestatus = describe_execution(task_execution_arn); 
                        #           update status in DB
                        #           If status == 'ERROR'
                                        cloudwatch Alarm
                elseif status == 'SUCCESS'|'ERROR'      
                        #           exec_id, status = start_exec()
                                    update db status as 'EXEC_INPROGRESS' 
                                    If status == 'ERROR'
                                        cloudwatch Alarm
                else:
                    do nothing

 else:
 	do nothing
    except Exception as e:
        print("Missing parameters")
        print(str(e))
        return return_error()


    


