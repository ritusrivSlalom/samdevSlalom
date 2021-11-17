data "aws_security_group" "default" {
  name   = "default"
  vpc_id = module.vpc.vpc_id
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "2.70.0"

  name = var.name

  cidr            = var.vpc_cidr_block
  azs             = var.availability_zones
  private_subnets = var.private_subnets_cidr
  public_subnets  = var.public_subnets_cidr

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  # VPC endpoint for S3
  //  enable_s3_endpoint = true

  # VPC Endpoint for EC2
  enable_ec2_endpoint              = true
  ec2_endpoint_private_dns_enabled = true
  ec2_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  ec2_endpoint_subnet_ids          = module.vpc.public_subnets


  # VPC endpoint for ECR
  enable_ecr_api_endpoint              = true
  ecr_api_endpoint_private_dns_enabled = true
  ecr_api_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  ecr_api_endpoint_subnet_ids          = module.vpc.public_subnets



  # VPC endpoint for reaching docker hub
  enable_ecr_dkr_endpoint              = true
  ecr_dkr_endpoint_private_dns_enabled = true
  ecr_dkr_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  ecr_dkr_endpoint_subnet_ids          = module.vpc.public_subnets

  enable_secretsmanager_endpoint              = true
  secretsmanager_endpoint_private_dns_enabled = true
  secretsmanager_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  secretsmanager_endpoint_subnet_ids          = module.vpc.public_subnets

  enable_ssm_endpoint              = true
  ssm_endpoint_private_dns_enabled = true
  ssm_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  ssm_endpoint_subnet_ids          = module.vpc.public_subnets

  enable_lambda_endpoint              = true
  lambda_endpoint_private_dns_enabled = true
  lambda_endpoint_security_group_ids  = [data.aws_security_group.default.id]
  lambda_endpoint_subnet_ids          = module.vpc.public_subnets

  # Default security group - ingress/egress rules cleared to deny all
  default_security_group_name   = var.default_security_group_name
  manage_default_security_group = true
  default_security_group_ingress = [
    {
      description = "VPC Internal"
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = var.vpc_cidr_block
    },
    {
      description = "SSH GH onsite"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = "50.233.156.0/24"
    },
    {
      description = "Allow traffic originating in the VPC - useful for VPC endpoints"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = "10.134.0.0/16"
    }

  ]
  default_security_group_egress = [
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = "0.0.0.0/0"
    }
  ]

  # VPC Flow Logs (Cloudwatch log group and IAM role will be created)
  enable_flow_log                      = true
  create_flow_log_cloudwatch_log_group = true
  create_flow_log_cloudwatch_iam_role  = true
  flow_log_max_aggregation_interval    = 60

  # Adding subnet tags
  private_subnet_tags = var.private_subnet_tags
  public_subnet_tags  = var.public_subnet_tags


}