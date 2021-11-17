
data "aws_region" "current" {
}

data "aws_caller_identity" "current" {
}
data "aws_ssm_parameter" "vpc_id" {
  name = "/gh-bip/${data.aws_region.current.name}/vpc_id"
}
data "aws_ssm_parameter" "subnets" {
  name = "/gh-bip/${data.aws_region.current.name}/prv_subnet_ids"
}

# Generates an archive from content, a file, or a directory of files.

data "archive_file" "default" {
  type        = "zip"
  source_dir  = "${path.module}/create-table-lambda/"
  output_path = "${path.module}/functionzip/python.zip"
}

data "archive_file" "eventfile" {
  type        = "zip"
  source_dir  = "${path.module}/event_lambda/"
  output_path = "${path.module}/functionzip/eventfunction.zip"
}

locals {
  vpcid = data.aws_ssm_parameter.vpc_id.value
  subnet_list = split(", ", data.aws_ssm_parameter.subnets.value)
}
data "aws_security_group" "default" {
  name   = "default"
  vpc_id = local.vpcid
}
module "lambda" {
  source                         = "../../modules/lambda"
  description                    = "function to create DB table"
  filename                       = "${path.module}/functionzip/python.zip"
  function_name                  = "pg_create_table_function_v2"
  handler                        = "lambda_function.handler"
  runtime                        = "python3.6"
  timeout                        = "300"
  vpc_config                      =  {
                                          subnet_ids         = [local.subnet_list[0], local.subnet_list[1]] 
                                          security_group_ids = [data.aws_security_group.default.id]
                                      }
}

module "event-lambda" {
  source                         = "../../modules/lambda"
  description                    = "function to stop data sync"
  filename                       = "${path.module}/functionzip/eventfunction.zip"
  function_name                  = "event_lambda_v2"
  handler                        = "lambda_function.handler"
  runtime                        = "python3.6"
  timeout                        = "300"
  vpc_config                      =  {
                                          subnet_ids         = [local.subnet_list[0], local.subnet_list[1]] 
                                          security_group_ids = [data.aws_security_group.default.id]
                                      }

}

resource "aws_iam_policy" "datasync-policy" {
  name        = "datasync-policy1"
  description = "A datasync-policy"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "datasync:*",
                "ec2:CreateNetworkInterface",
                "ec2:CreateNetworkInterfacePermission",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:ModifyNetworkInterfaceAttribute",
                "fsx:DescribeFileSystems",
                "elasticfilesystem:DescribeFileSystems",
                "elasticfilesystem:DescribeMountTargets",
                "iam:GetRole",
                "iam:ListRoles",
                "logs:CreateLogGroup",
                "logs:DescribeLogGroups",
                "logs:DescribeResourcePolicies",
                "s3:ListAllMyBuckets",
                "s3:ListBucket"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": [
                        "datasync.amazonaws.com"
                    ]
                }
            }
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "policy-attach" {
  role       = "event_lambda-${data.aws_region.current.name}"
  policy_arn = aws_iam_policy.datasync-policy.arn
  
}

resource "aws_iam_role_policy_attachment" "role-policy-attachment" {
  for_each = toset([
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole", 
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/SecretsManagerReadWrite",
    "arn:aws:iam::aws:policy/AmazonSSMFullAccess"
  ])

  role       =  "event_lambda-${data.aws_region.current.name}"
  policy_arn = each.value
}
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${module.event-lambda.function_name}"
  retention_in_days = "60"
}

resource "aws_lambda_permission" "cloudwatch_logs" {
  action        = "lambda:InvokeFunction"
  function_name = module.event-lambda.function_name
  principal     = "logs.${data.aws_region.current.name}.amazonaws.com"
  source_arn    = aws_cloudwatch_log_group.lambda.arn
}

/*
module "event-cloudwatch" {
  source = "./modules/event/cloudwatch-event"
  enable = lookup(var.event, "type", "") == "cloudwatch-event" ? true : false

  lambda_function_arn = module.lambda.arn
  description         = lookup(var.event, "description", "")
  event_pattern       = lookup(var.event, "event_pattern", "")
  is_enabled          = lookup(var.event, "is_enabled", true)
  name                = lookup(var.event, "name", null)
  name_prefix         = lookup(var.event, "name_prefix", null)
  schedule_expression = lookup(var.event, "schedule_expression", "")
}


module "event-s3" {
  source = "./modules/event/s3"
  enable = lookup(var.event, "type", "") == "s3" ? true : false

  lambda_function_arn = module.lambda.arn
  s3_bucket_arn       = lookup(var.event, "s3_bucket_arn", "")
  s3_bucket_id        = lookup(var.event, "s3_bucket_id", "")
}

module "event-sns" {
  source = "./modules/event/sns"
  enable = lookup(var.event, "type", "") == "sns" ? true : false

  endpoint      = module.lambda.arn
  function_name = module.lambda.function_name
  topic_arn     = lookup(var.event, "topic_arn", "")
}

module "event-sqs" {
  source = "./modules/event/sqs"
  enable = lookup(var.event, "type", "") == "sqs" ? true : false

  batch_size                   = lookup(var.event, "batch_size", 10)
  event_source_mapping_enabled = lookup(var.event, "event_source_mapping_enabled", true)
  function_name                = module.lambda.function_name
  event_source_arn             = lookup(var.event, "event_source_arn", "")
  iam_role_name                = module.lambda.role_name
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${module.lambda.function_name}"
  retention_in_days = var.log_retention_in_days
}

resource "aws_lambda_permission" "cloudwatch_logs" {
  count         = var.logfilter_destination_arn != "" ? 1 : 0
  action        = "lambda:InvokeFunction"
  function_name = var.logfilter_destination_arn
  principal     = "logs.${data.aws_region.current.name}.amazonaws.com"
  source_arn    = aws_cloudwatch_log_group.lambda.arn
}

resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_logs_to_es" {
  depends_on      = [aws_lambda_permission.cloudwatch_logs]
  count           = var.logfilter_destination_arn != "" ? 1 : 0
  name            = "elasticsearch-stream-filter"
  log_group_name  = aws_cloudwatch_log_group.lambda.name
  filter_pattern  = ""
  destination_arn = var.logfilter_destination_arn
  distribution    = "ByLogStream"
}

data "aws_iam_policy_document" "ssm_policy_document" {
  count = length(var.ssm_parameter_names)

  statement {
    actions = [
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]

    resources = [
      "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${element(var.ssm_parameter_names, count.index)}",
    ]
  }
}

resource "aws_iam_policy" "ssm_policy" {
  count       = length(var.ssm_parameter_names)
  name        = "${module.lambda.function_name}-ssm-${count.index}-${data.aws_region.current.name}"
  description = "Provides minimum Parameter Store permissions for ${module.lambda.function_name}."
  policy      = data.aws_iam_policy_document.ssm_policy_document[count.index].json
}

resource "aws_iam_role_policy_attachment" "ssm_policy_attachment" {
  count      = length(var.ssm_parameter_names)
  role       = module.lambda.role_name
  policy_arn = aws_iam_policy.ssm_policy[count.index].arn
}
*/