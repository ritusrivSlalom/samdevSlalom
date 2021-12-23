# Lambda to facilitate creation of a parallel cluster
import json
import sys
import boto3
import base64
import os
import io
import contextlib
import logging
import pcluster
import pcluster.cli as cli

from io import StringIO
import imp
import db
import config

DEBUG = True

if ( DEBUG ):
  print("PATH" + str(sys.path))
  current_path = os.getcwd()
  sys.path.insert(0, current_path + '/utils')
  print("PATH" + str(sys.path))
import platform 
plt = platform.system()

# (Failed) try to import appropriate psyco include based on local vs AWS
print("Platform=" + plt + ", OS nm=" + os.name)
if (plt == "Windows" or os.name == 'nt'):
  print("Aye, there be Windows here")
# import psycopg2_win as psycopg2
#else:
import psycopg2

region = os.environ['AWS_REGION']
DEV_acct = "023839011004"
PROD_acct = "063935053328"

def getAccountID():
    # TODO: Fix this
    # client = boto3.client("sts", region_name=os.environ['AWS_REGION'])
    # try:
    #     account_id = client.get_caller_identity()["Account"]
    # except Exception as err:
    #     print(f"Unable to get Account ID. Exception: {err}")
    #     sys.exit(1)
    account_id = DEV_acct
    return account_id

def publish_message(error_msg):
    snsclient = boto3.client('sns')
    lambda_func_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
    try:
        message = ""
        message += "\nLambda error  summary" + "\n\n"
        message += "##########################################################\n"
        message += "# LogGroup Name:- " + os.environ['AWS_LAMBDA_LOG_GROUP_NAME'] + "\n"
        message += "# LogStream:- " + os.environ['AWS_LAMBDA_LOG_STREAM_NAME'] + "\n"
        message += "# Log Message:- " + "\n"
        message += "# \t\t" + str(error_msg.split("\n")) + "\n"
        message += "##########################################################\n"
        accntid=getAccountID()
        # Sending the notification...
        snsclient.publish(
            TargetArn="arn:aws:sns:" + region + ":" + accntid + ":gh-bip-notify",
            Subject=f'Execution error for Lambda - {lambda_func_name[3]}',
            Message=message
        )
    except Exception as e:
        print(f"Unable to publish message. Exception: {e}")
     
def get_analyses_candidates():
  dbConnection = db.getDBConnection()
  try:
      cur = dbConnection.cursor()
      
      # execution_id             | cluster_id |   status   | product_name | scheduler_name | bip_version |run_id              |                      output_dir_name                       | bip_image_name |       creation_time  | update_time
      completed_status = "COMPLETED"
      cancel_status = "CANCELED"
      processing_status = "PROCESSING"
      readQuery = """SELECT execution_id, cluster_id, status, product_name, scheduler_name, bip_version, run_id, output_dir_name, bip_image_name, creation_time, update_time FROM gh_analysis_job_execution ORDER BY "cluster_id" DESC"""
      # readQuery = """SELECT execution_id, cluster_id, status, product_name, scheduler_name, bip_version, run_id, output_dir_name, bip_image_name, creation_time, update_time FROM gh_analysis_job_execution WHERE UPPER(status) NOT IN (%s, %s, %s) ORDER BY "cluster_id" DESC"""
      # cur.execute(readQuery, [processing_status, completed_status, cancel_status])
      cur.execute(readQuery)
      rows = cur.fetchall()
      print("The number of analyses_candidate rows returned: ", cur.rowcount)
      print(rows)
  except Exception as err:
      print(f"Unable to obtain analyses_candidates from the database. Exception: {err}")
      publish_message("Unable to read the data from the database. Exception: "+str(err))
      sys.exit(1)
  return rows

# TODO: These are hardcoded DEV environ params - add dynamics
# NOTE: Do NOT chain together the replace's, otherwise you'll be sorry (Time wasted: 1hr)
def replace_tokens(config_info, region, run_id, vpc_id = "vpc-026a0da3cc0a3b2bd", 
  private_subnet = "subnet-0625e61c2ae4243ff", vpc_sg_id = "sg-05adef62d9a0283c5"):
  region_token = "__REGION__"
  vpc_id_token = "__VPC__"
  private_subnet_token = "__PRV_SN_ID__"
  vpc_sg_id_token = "__SG_ID__"
  run_id_token = "__RUN_ID__"
  cfg_info = config_info.replace("__REGION__", region)
  cfg_info = cfg_info.replace(vpc_id_token, vpc_id)
  cfg_info = cfg_info.replace(private_subnet_token, private_subnet)
  cfg_info = cfg_info.replace(vpc_sg_id_token, vpc_sg_id)
  cfg_info = cfg_info.replace(run_id_token, run_id)
  # print(f"Replaced content={cfg_info}")
  return cfg_info

"""
What needs to be checked/developed?

Receive API params from API request - job_id (correlates to cluster name, product name, )

See if a current PllCLuster can be recycled/reconfigured for next flow cell job

Get (S3) scripts, objects and (SSM) Params

Configure PllCluster based on config info - ASG, instance sizes, 

Check and report PllCluster status
"""
def lambda_handler(event, context):
    #command to execute with ParallelCluster
    # product_name = "g360"
    # scheduler_name = "sge"
    config_body = None

    if ( DEBUG ):
      # print(event)
      # print("PATH" + str(sys.path))
      # print(os.listdir("/opt/python/lib/python3.6/site-packages"))  <- ERRNO 2
      print("----------------")
      # print(os.listdir("/var/lang/lib/python3.6/site-packages"))
      
      try:
          imp.find_module('pcluster')
          found = True
          if ( hasattr(pcluster,'cli')):
            print ("pcluster,'cli' exists")
          else:
            print ("pcluster,'cli' NOT FOUND")
            exit(1)
          if ( hasattr(pcluster.cli,'main')): # <- NONE of the methods are found - ??
            print ("pcluster.cli,'main' exists")
          else:
            print ("pcluster.cli,'main' NOT FOUND")
            exit(1)
      except ImportError:
          found = False
          print("can't find import module pcluster")
          exit(1)
    # END DEBUG
    command = event["queryStringParameters"]["command"]
    #the cluster name
    cluster_name = ""
    try:
      cluster_name = event["queryStringParameters"]["cluster_name"]
    except:
      cluster_name = "Generic_cluster"
    
    try:
      exec_id = event["queryStringParameters"]["execution_id"]
    except:
      return {
           'statusCode': 200,
           'body': 'missing execution ID - try again'
        }
    
    # If request has a Base64-encoded body, then use it over Param Store config
    try:
      config_body = event['body'] # TODO: Check to ensure it's been Base64 encoded
    except:
      config_body = None
    #
    # START DB escapades
    #
    print("Finding candidates...")
    candidates = get_analyses_candidates()
    #
    # END DB escapades
    #
    # NOTE: run_id == flow_cell_run_id (not a job run id)
    execution_candidate = None  
    for (execution_id, cluster_id, status, product_name, scheduler_name, bip_version, run_id, output_dir_name, bip_image_name, creation_time, update_time) in candidates:
        print ("IN_PROGRESS:", execution_id, cluster_id, status, product_name, scheduler_name, bip_version, run_id, output_dir_name, bip_image_name, creation_time, update_time)
        if ( status == config.PROCESSING ):
          execution_candidate = (product_name, scheduler_name, run_id, output_dir_name, bip_image_name)  # Just grab last one for now

    print(f"Creating pll cluster for exec_id{execution_id}: /gh-bip/{region}/GH_analysis/{product_name}/{scheduler_name}/pcconfig")
    # Retrieve pcluster configuration file and its capacity
    try:
        if ( config_body == None):  # If config info not supplied by requester (Postman, etc)
          pllcluster_config = f"/gh-bip/{region}/GH_analysis/{product_name}/{scheduler_name}/pcconfig"
          print(f"Getting pllconfig params from {pllcluster_config}")
          file_content = db.get_parameter(pllcluster_config)
        else:
          file_content = base64.b64decode(config_body)
        pllcluster_capacity = f"/gh-bip/{region}/GH_analysis/{product_name}/capacity"
        pllcluster_capacity = db.get_parameter(pllcluster_capacity)
        # TODO: Split off file operations in separate try block
        path_config = '/tmp/config'
        config_file = open(path_config,'w')
        if ( config_body != None):
          file_content = file_content.decode('utf-8')
        file_content = replace_tokens(file_content, region, run_id )
        config_file.write(file_content)
        config_file.close()
        print(f"Capacity:{pllcluster_capacity},\nConfig Info:\n{file_content}")
    except Exception as err:
        print(f"Unable to get capacity and/or cluster config info. Exception: {err}")
        return {
           'statusCode': 200,
           'body': 'Please specify the pcluster configuration body and capacity\n'
        }
    os.environ['HOME'] = '/tmp'
    sys.argv = ["pcluster"]
    sys.argv.append(command)
    
    #append the additional parameters
    # print(event["headers"])
    try:
      additional_parameters = event["headers"]["additional_parameters"]
      add_params = additional_parameters.split()
      sys.argv = sys.argv + add_params
    except:
      print("no_params")

    """
    if (DEBUG):
      print("Exiting for now.")
      return {
          'statusCode': 200,
          'body': 'Exiting just before actual PllCluster creation\n'
      }
    """
    sys.argv.append('--config')
    sys.argv.append('/tmp/config')
    if command in ['create', 'delete', 'update', 'start', 'stop', 'status']:
      sys.argv.append(cluster_name)
      
    #execute the pcluster command
    output = ''
    try:
      pcluster_logger = logging.getLogger("pcluster")
      pcluster_logger.propagate = False
      stdout = io.StringIO()
      stderr = io.StringIO()
      # NOTE: Remove this DEBUG conditional when DB & Param Stores working
      with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        cli.main()  # ERR+ "module 'pcluster.cli' has no attribute 'main': AttributeError" if pcluster > 2.11.3 (backwards incompatibility)
      pllcluster_info = f"PllCluster Capacity: {pllcluster_capacity}\nPllCLusterCfg=\n{file_content}"
      print(pllcluster_info)
      print("stdout:\n{}".format(stdout.getvalue()))
      print("stderr:\n{}".format(stderr.getvalue()))
      output = stdout.getvalue() + '\n' + stderr.getvalue() + '\n'
    except SystemExit as e:
      print("stdout:\n{}".format(stdout.getvalue()))
      print("stderr:\n{}".format(stderr.getvalue()))
      print("exception: {}".format(e))
      output = stdout.getvalue() + '\n' + stderr.getvalue() + '\n' + str(e) + '\n'
        
    return {
        'statusCode': 200,
        'body': output
    }
