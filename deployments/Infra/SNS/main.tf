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
  sns_name =  "gh-bip-notify"
  ## "List of email addresses as string(space separated)"
  sns_subscription_email_address_list = "rsrivastava@guardanthealth.com kgangineni@guardanthealth.com"

}
resource "aws_sns_topic" "bip_sns_topic" {
  name = local.sns_name
  provisioner "local-exec" {
    command = "sh sns_subscription.sh"
    environment = {
      sns_arn = self.arn
      sns_emails = local.sns_subscription_email_address_list
    }
  }
}

resource "aws_sns_topic_policy" "my_sns_topic_policy" {
  arn = aws_sns_topic.bip_sns_topic.arn
  policy = data.aws_iam_policy_document.custom_sns_policy_document.json
}

data "aws_iam_policy_document" "custom_sns_policy_document" {
  policy_id = "__default_policy_ID"

  statement {
    actions = [
      "SNS:Subscribe",
      "SNS:SetTopicAttributes",
      "SNS:RemovePermission",
      "SNS:Receive",
      "SNS:Publish",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:DeleteTopic",
      "SNS:AddPermission",
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"

      values = [
        local.account_id,
      ]
    }

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      aws_sns_topic.bip_sns_topic.arn,
    ]

    sid = "__default_statement_ID"
  }
}
