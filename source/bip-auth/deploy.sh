aws-azure-login --profile gh-aws-slalom --no-prompt
export AWS_PROFILE=gh-aws-slalom
export AWS_DEFAULT_REGION="eu-west-2"
venv/bin/sam  deploy --stack-name auth-deploy -t template.yml --s3-bucket gh-pcluster-automation-bucket-dev --s3-prefix source/auth --capabilities CAPABILITY_IAM
