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


# Lambda to facilitate creation of a parallel cluster
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
    product_name = "g360"
    scheduler_name = "sge"

    print(event)
    if ( DEBUG ):
      print("PATH" + str(sys.path))
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
      cluster_name = event["queryStringParameters"]["execution_id"]
    except:
      print("missing execution ID - try again")
      exit(0)

    # Retrieve pcluster configuration file and its capacity
    try:
        if ( DEBUG != False):
          pllcluster_config = f"/gh-bip/{region}/GH_analysis/{product_name}/{scheduler_name}/pcconfig"
          file_content = db.get_parameter(pllcluster_config)
        else:
          file_content = base64.b64decode(event['body'])
        path_config = '/tmp/config'
        config_file = open(path_config,'w')
        config_file.write(file_content.decode('utf-8'))
        config_file.close()
        pllcluster_capacity = f"/gh-bip/{region}/GH_analysis/{product_name}/capacity"
    except:
        return {
           'statusCode': 200,
           'body': 'Please specify the pcluster configuration file\n'
        }
    os.environ['HOME'] = '/tmp'
    sys.argv = ["pcluster"]
    sys.argv.append(command)
    
    #append the additional parameters
    print(event["headers"])
    try:
      additional_parameters = event["headers"]["additional_parameters"]
      add_params = additional_parameters.split()
      sys.argv = sys.argv + add_params
    except:
      print("no_params")
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
      if ( DEBUG == False): # This step alrdy validated
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
