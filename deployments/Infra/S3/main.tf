
terraform {
  backend "s3" {}
}
data "aws_region" "current" {
}

data "aws_caller_identity" "current" {
}

locals {
  region       = data.aws_region.current.name
  account_id   = data.aws_caller_identity.current.account_id
  function_arn = "arn:aws:lambda:${local.region}:${local.account_id}:function:event_lambda"

}
resource "aws_s3_bucket" "bip-bucket" {

  bucket = "bip-analysis-bucket"
  acl    = "private"

  versioning {
    enabled = true
  }

}


resource "aws_s3_bucket" "gh-pcluster-data" {

  bucket = "gh-pcluster-automation-bucket"
  acl    = "private"

  versioning {
    enabled = true
  }
  provisioner "local-exec" {
     command = "aws s3 cp bipkey.pub ${aws_s3_bucket.gh-pcluster-data.id}"
  }

}

##################
# Adding S3 bucket as trigger to lambda to stop datasync schedule
##################
resource "aws_s3_bucket_notification" "aws-lambda-trigger" {
  bucket = aws_s3_bucket.bip-bucket.id
  lambda_function {
    lambda_function_arn = local.function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "CopyComplete.txt"
  }
}

resource "aws_lambda_permission" "test" {
statement_id  = "AllowS3Invoke"
action        = "lambda:InvokeFunction"
function_name = "event_lambda_v2"
principal = "s3.amazonaws.com"
source_arn = "arn:aws:s3:::${aws_s3_bucket.bip-bucket.id}"
}

