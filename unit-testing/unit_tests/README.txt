You need to install the following libraries to run the Unit Test by MOTO:

boto3: AWS SDK for Python
moto: Mock AWS Services
Pytest: Test framework for Python


You can find the default setting in configtest.py. 
If you need to add Unit testing for a new Service, you need to add that Service to configtest.py.
You have code for two Services (DataSync, s3) in configtest.py.

Make a new file Python file. Set the name of this file like the same as the new Service name you like to add. (Like datasync.py)
Make a Class and enter all methods inside this Class. You can find a PDF file (docs-getmoto-org-en-stable) that 
helps to find out which functions are available in MOTO at this time.

Create another file (test_datasync.py). Import the Class you just made it.
You can add all test case functions here and call all functions you already set up in your Class.

How run your test:

1. Run Test Unit without details: ...$pytest test_datasync.py
2. Run Test Unit with details: ...$pytest test_datasync.py -v