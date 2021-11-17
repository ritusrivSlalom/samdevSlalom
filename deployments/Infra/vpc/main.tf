
terraform {
  backend "s3" {}
}
data "aws_region" "current" {}
locals {
  name   = "gh-bip-vpc"
  tags = {
    Owner       = "GH"
    Environment = "gh-bip"
  }
}


################################################################################
# VPC Module
################################################################################

module "vpc" {
  source = "../../modules/vpc"

  name = local.name
  vpc_cidr_block = "10.130.0.0/16"

  availability_zones  = ["${data.aws_region.current.name}a", "${data.aws_region.current.name}b", "${data.aws_region.current.name}c"]
  private_subnets_cidr     = ["10.130.8.0/22", "10.130.12.0/22","10.130.16.0/22"]
  public_subnets_cidr      = ["10.130.0.0/22", "10.130.4.0/22"]
  default_security_group_name = "gh-bip-sg"
  public_subnet_tags = local.tags
  private_subnet_tags = local.tags

}
data "aws_security_group" "default" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "group-name"
    values = ["default"]
  }
}

resource "aws_ssm_parameter" "vpc_id" {
  name  = "/gh-bip/${data.aws_region.current.name}/vpc_id"
  type  = "String"
  value = module.vpc.vpc_id
}


resource "aws_ssm_parameter" "prv_subnet_ids" {
  name  = "/gh-bip/${data.aws_region.current.name}/prv_subnet_ids"
  type  = "StringList"
  value = join(", ", module.vpc.private_subnets)
}

resource "aws_ssm_parameter" "prv_public_ids" {
  name  = "/gh-bip/${data.aws_region.current.name}/public_subnet_ids"
  type  = "StringList"
  value = join(", ", module.vpc.public_subnets)
}

resource "aws_ssm_parameter" "prv_public_ids_1" {
  name  = "/gh-bip/${data.aws_region.current.name}/public_subnet_id1"
  type  = "String"
  value = module.vpc.public_subnets[0]
}

resource "aws_ssm_parameter" "prv_public_ids_2" {
  name  = "/gh-bip/${data.aws_region.current.name}/public_subnet_id2"
  type  = "String"
  value = module.vpc.public_subnets[1]
}

resource "aws_ssm_parameter" "default_sg_id" {
  name  = "/gh-bip/${data.aws_region.current.name}/default_securitygroupid"
  type  = "String"
  value = data.aws_security_group.default.id
}