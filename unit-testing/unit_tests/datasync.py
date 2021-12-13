import boto3
import json
from collections import OrderedDict, defaultdict
from moto.core import BaseBackend, BaseModel
from exceptions import (
    InvalidRequestException
)
from _weakref import proxy as _proxy
import sys as _sys


model_data = defaultdict(dict)

class _Link(object):
    __slots__ = 'prev', 'next', 'key', '__weakref__'

class Task():
    def __init__(
        self,
        source_location_arn,
        destination_location_arn,
        name,
        region_name,
        arn_counter=0,
        metadata=None,
    ):
        self.source_location_arn = source_location_arn
        self.destination_location_arn = destination_location_arn
        self.name = name
        self.metadata = metadata
        # For simplicity Tasks are either available or running
        self.status = "AVAILABLE"
        self.current_task_execution_arn = None
        # Generate ARN
        self.arn = "arn:aws:datasync:{0}|task:{1}".format(region_name, name)
        
class TaskExecution():
    TASK_EXECUTION_INTERMEDIATE_STATES = (
        "INITIALIZING",
        "PREPARING",
        "TRANSFERRING",
        "VERIFYING",
    )

    TASK_EXECUTION_FAILURE_STATES = ("ERROR",)
    TASK_EXECUTION_SUCCESS_STATES = ("SUCCESS",)


    def __init__(self, task_arn, arn_counter=0):
        self.task_arn = task_arn
        self.arn = "{0}/execution/exec-{1}".format(task_arn, str(arn_counter).zfill(17))
        self.status = self.TASK_EXECUTION_INTERMEDIATE_STATES[0]

    # Simulate a task execution
    def iterate_status(self):
        if self.status in self.TASK_EXECUTION_FAILURE_STATES:
            return
        if self.status in self.TASK_EXECUTION_SUCCESS_STATES:
            return
        if self.status in self.TASK_EXECUTION_INTERMEDIATE_STATES:
            for i, status in enumerate(self.TASK_EXECUTION_INTERMEDIATE_STATES):
                if status == self.status:
                    if i < len(self.TASK_EXECUTION_INTERMEDIATE_STATES) - 1:
                        self.status = self.TASK_EXECUTION_INTERMEDIATE_STATES[i + 1]
                    else:
                        self.status = self.TASK_EXECUTION_SUCCESS_STATES[0]
                    return
        raise Exception(
            "TaskExecution.iterate_status: Unknown status={0}".format(self.status)
        )

    def cancel(self):
        if self.status not in self.TASK_EXECUTION_INTERMEDIATE_STATES:
            raise InvalidRequestException(
                "Sync task cannot be cancelled in its current status: {0}".format(
                    self.status
                )
            )
        self.status = "ERROR"

class MyDataSyncClient:
    def __init__(self, region_name="eu-west-2"):
        self.arn_counter = 0
        self.region_name = region_name
        self.tasks = OrderedDict()
        self.task_executions = OrderedDict()
        self.client = boto3.client("datasync", region_name=region_name)

    def create_task(self, source_location_arn, destination_location_arn, name, metadata=None):
        
        self.arn_counter = self.arn_counter + 1
        task = Task(
            source_location_arn,
            destination_location_arn,
            name,
            region_name=self.region_name,
            arn_counter=self.arn_counter,
            metadata=metadata,
        )
        self.tasks[task.arn] = task 
        # Sample of tasks[task.arn]: odict_items([('arn:aws:datasync:eu-west-2|task:my_task_name', <datasync.Task object at 0x7fb9ade819b0>)])
        return task.arn

    

    def start_task_execution(self, source_location_arn, destination_location_arn, name, metadata=None):
        
        task_arn = MyDataSyncClient.create_task(self, source_location_arn, destination_location_arn, name, metadata)
        
        if task_arn in self.tasks:
            
            task = self.tasks[task_arn]
        
            if task.status == "AVAILABLE":
                task_execution = TaskExecution(task_arn, arn_counter=self.arn_counter)
                self.task_executions[task_execution.arn] = task_execution
                self.tasks[task_arn].current_task_execution_arn = task_execution.arn
                self.tasks[task_arn].status = "RUNNING"
                return task_execution.arn
        raise InvalidRequestException("Invalid request.")

    def cancel_task_execution(self, source_location_arn, destination_location_arn, name, metadata=None):
        task_execution_arn = MyDataSyncClient.start_task_execution(self, source_location_arn, destination_location_arn, name, metadata)
        
        if task_execution_arn in self.task_executions:
            task_execution = self.task_executions[task_execution_arn]
            task_execution.cancel()
            task_arn = task_execution.task_arn
            self.tasks[task_arn].current_task_execution_arn = None
            self.tasks[task_arn].status = "AVAILABLE"
            return self.tasks[task_arn].status
        raise InvalidRequestException(
            "Sync task {0} is not found.".format(task_execution_arn)
        )    
    
    def update_task(self, task_arn, source_location_arn, destination_location_arn, name, metadata, u_metadata):
        task_arn = MyDataSyncClient.create_task(self, source_location_arn, destination_location_arn, name, metadata)
        if task_arn in self.tasks:
            task = self.tasks[task_arn]
            task.name = name
            task.metadata = u_metadata

            return task
        else:
            raise InvalidRequestException(
                "Sync task {0} is not found.".format(task_arn)
            )

    def delete_task(self, task_arn, source_location_arn, destination_location_arn, name, metadata):
        task_arn = MyDataSyncClient.create_task(self, source_location_arn, destination_location_arn, name, metadata)
        if task_arn in self.tasks:
            del self.tasks[task_arn]
            return ("TaskDeleted", "TaskExist") [task_arn in self.tasks] 
        else:
            raise InvalidRequestException
        

    def create_locations(client, create_smb=False, create_s3=False):

        smb_arn = None
        s3_arn = None
        if create_smb:
            # response = client.create_location_smb(
            #     ServerHostname="host",
            #     Subdirectory="somewhere",
            #     User="",
            #     Password="",
            #     AgentArns=["stuff"],
            # )
            #smb_arn = response["LocationArn"]
            smb_arn = "arn:aws:s3:::gh-pcluster-automation-bucket-dev"
        if create_s3:
            # response = client.create_location_s3(
            #     S3BucketArn="arn:aws:s3:::my_bucket",
            #     Subdirectory="dir",
            #     S3Config={"BucketAccessRoleArn": "role"},
            # )
            #s3_arn = response["LocationArn"]
            s3_arn = "bip-analysis-bucket-dev"
        return {"smb_arn": smb_arn, "s3_arn": s3_arn}

