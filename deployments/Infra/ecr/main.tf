
terraform {
  backend "s3" {
  }
}

data "aws_region" "current" {
}

data "aws_caller_identity" "current" {
}

locals {
  region       = data.aws_region.current.name
}

resource "aws_ecr_repository" "cloudybiprepo" {
  name                 = "gh-bip-repo"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}