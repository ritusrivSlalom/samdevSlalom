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
        ## Check db if any row with status  "TASK_CREATION" 
        # If exists:
        #   get task_name, src and dest
        #    update task status ""TASK_INPROGRESS "
        #  list_tasks with filter name
        #  If task exists and status is 'AVAILABLE'
        #       check if any execution of task  "SUCCESS"|'ERROR'      
        #           exec_id, status = start_exec()
        #        check if DB task execution status in 'QUEUED'|'LAUNCHING'|'PREPARING'|'TRANSFERRING'|'VERIFYING'
        #           Exestatus = describe_execution(task_execution_arn); 
        #            update status in DB
        # If task exists and status is in 'CREATING'|'QUEUED'|'RUNNING'
        #       nothing      
        # else
        #       create_task()
        #       update task status ""TASK_INPROGRESS "
        #       wait 30 secs
        #       describe_task(task_name)
        #        again update status in db
        #       exec_id, status = start_exec()
        #
    except Exception as e:
        print("Missing parameters")
        print(str(e))
        return return_error()


    


