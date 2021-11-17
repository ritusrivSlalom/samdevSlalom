data "aws_caller_identity" "current" {}

terraform {
  backend "s3" {}
}
#########################################
data "aws_iam_policy_document" "datasync_assume_role" {
  statement {
    actions = ["sts:AssumeRole",]
    principals {
      identifiers = ["datasync.amazonaws.com"]
      type        = "Service"
    }
  }
}
data "aws_iam_policy_document" "bucket_access" {
  statement {
    actions = ["s3:*",]
    resources = [
      "*"
    ]
  }
}
resource "aws_iam_role" "datasync-s3-access-role" {
  name               = "s3_data_sync_access"
  assume_role_policy = "${data.aws_iam_policy_document.datasync_assume_role.json}"
}

resource "aws_iam_role_policy" "datasync-s3-access-policy" {
  name   = "datasync-s3-access-policy"
  role   = "${aws_iam_role.datasync-s3-access-role.name}"
  policy = "${data.aws_iam_policy_document.bucket_access.json}"
}

#########################################
# IAM policy
#########################################
module "iam_policy" {
  count = 0
  source = "../../modules/iam/iam-policy"

  name        = "parallelcluster-custom-policy"
  path        = "/"
  description = "Parallel cluster policy"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ssm:DescribeAssociation",
                "ssm:GetDeployablePatchSnapshotForInstance",
                "ssm:GetDocument",
                "ssm:DescribeDocument",
                "ssm:GetManifest",
                "ssm:GetParameter",
                "ssm:GetParameters",
                "ssm:ListAssociations",
                "ssm:ListInstanceAssociations",
                "ssm:PutInventory",
                "ssm:PutComplianceItems",
                "ssm:PutConfigurePackageResult",
                "ssm:UpdateAssociationStatus",
                "ssm:UpdateInstanceAssociationStatus",
                "ssm:UpdateInstanceInformation"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssmmessages:CreateControlChannel",
                "ssmmessages:CreateDataChannel",
                "ssmmessages:OpenControlChannel",
                "ssmmessages:OpenDataChannel"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2messages:AcknowledgeMessage",
                "ec2messages:DeleteMessage",
                "ec2messages:FailMessage",
                "ec2messages:GetEndpoint",
                "ec2messages:GetMessages",
                "ec2messages:SendReply"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::gh-pcluster-data",
                "arn:aws:s3:::gh-pcluster-data/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ecr:GetAuthorizationToken",
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:GetRepositoryPolicy",
                "ecr:DescribeRepositories",
                "ecr:ListImages",
                "ecr:DescribeImages",
                "ecr:BatchGetImage",
                "ecr:GetLifecyclePolicy",
                "ecr:GetLifecyclePolicyPreview",
                "ecr:ListTagsForResource",
                "ecr:DescribeImageScanFindings"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "ecr:*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateServiceLinkedRole",
                "iam:AttachRolePolicy",
                "iam:PutRolePolicy"
            ],
            "Resource": "arn:aws:iam::*:role/aws-service-role/s3.datasource.lustre.fsx.amazonaws.com/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "fsx:CreateDataRepositoryTask"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}