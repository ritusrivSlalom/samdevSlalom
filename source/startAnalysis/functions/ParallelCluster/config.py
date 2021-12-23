#!/usr/bin/env python
# Dev acct = 023839011004 (state machine not foudn here yet)
# PROD acct = 063935053328
state_machine_arn = "arn:aws:states:eu-west-2:023839011004:stateMachine:AnalysisStateMachine-5mIm1CcPPsTf"
bucket_name = "bip-analysis-bucket"
sns_topic = "gh-bip-notify"

# Parallel Cluster states
PC_START = "PC_START"   # First initial state
PC_FAIL = "PC_FAILED"   # Get failure info & populate in job info tabl's err_details column
PC_STARTED = "PC_STARTED"
PC_AVAILABLE = "PC_AVAILABLE"     # Wait for 30 mins before taking down
PC_DELETE = "PC_DELETE"
PC_DELETED = "PC_DELETED"

# Parallel Cluster execution states
PROCESSING = "PROCESSING"   # Job request going on
FAIL = "FAILED"     # Put fail reason in 
SUCCESS = "SUCCESS"