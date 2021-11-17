# Creating the IP Set
terraform {
  backend "s3" {}
}

resource "aws_wafv2_ip_set" "ipset" {
  name               = "Firstipset"
  description        = "First IP set"
  scope              = "REGIONAL"
  ip_address_version = "IPV4"
  addresses          = var.waf_iplist ## e.g. ["1.2.3.4/32", "5.6.7.8/32"]

}

# Creating the Web ACL component in AWS WAF

resource "aws_wafv2_web_acl" "waf_acl" {
  name        = var.web_acl_name
  scope       = "REGIONAL"

  default_action {
    allow {}
  }

 rule {
    name     = "CommonRuleSet"
    priority = 1

    override_action {
      count {}
    }
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"

        excluded_rule {
          name = "SizeRestrictions_QUERYSTRING"
        }

        excluded_rule {
          name = "NoUserAgent_HEADER"
        }

        scope_down_statement {
          geo_match_statement {
            country_codes = ["US", "NL"]
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "bip-rule-metric-name"
      sampled_requests_enabled   = false
    }
  }
  rule {
      name = "AWS-AWSManagedRulesLinuxRuleSet"
      priority = 0
      override_action {
        count {}
      }
      statement {
        managed_rule_group_statement {
          name = "AWSManagedRulesLinuxRuleSet"
          vendor_name = "AWS"
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled  = false 
        metric_name                 = "bip-rule-metric-name"
        sampled_requests_enabled    = false
      }
    }
    rule {
      name = "AWS-AWSManagedRulesSQLiRuleSet"
      priority = 2
      override_action {
        count {}
      }
      statement {
        managed_rule_group_statement {
          name = "AWSManagedRulesSQLiRuleSet"
          vendor_name = "AWS"
        }
      }
      visibility_config {
        cloudwatch_metrics_enabled = false
        metric_name                = "bip-rule-metric-name"
        sampled_requests_enabled   = false
      }
    }
  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "bip-web-acl-metric-name"
    sampled_requests_enabled   = false
  }
}
## arn should be passes as API ARN
/**
resource "aws_wafv2_web_acl_association" "gh-bip-api-association" {
  resource_arn = var.api_arn
  web_acl_arn  = aws_wafv2_web_acl.waf_acl.arn
}
**/