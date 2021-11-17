terraform {
  backend "s3" {}
}
data "aws_region" "current" {}
data "aws_ssm_parameter" "vpc_id" {
  name = "/gh-bip/${data.aws_region.current.name}/vpc_id"
}
data "aws_ssm_parameter" "subnets" {
  name = "/gh-bip/${data.aws_region.current.name}/prv_subnet_ids"
}

data "aws_secretsmanager_secret" "secrets" {
  name = "bip_db_pass"
}

data "aws_secretsmanager_secret_version" "current" {
  secret_id = data.aws_secretsmanager_secret.secrets.id
}
locals {
  vpcid = data.aws_ssm_parameter.vpc_id.value
  subnet_list = split(", ", data.aws_ssm_parameter.subnets.value)
}
data "aws_security_group" "default" {
  vpc_id = local.vpcid

  filter {
    name   = "group-name"
    values = ["default"]
  }
}
# resource "aws_subnet" "rds_subnet" {
#   vpc_id     = local.vpcid
#   cidr_block = "10.134.8.0/24"
#   availability_zone = "${data.aws_region.current.name}a"
# }

# resource "aws_subnet" "rds_subnet1" {
#   vpc_id     = local.vpcid
#   cidr_block = "10.134.7.0/24"
#   availability_zone = "${data.aws_region.current.name}b"
# }
# resource "aws_route_table_association" "a" {
#   subnet_id      = aws_subnet.rds_subnet.id
#   route_table_id = aws_route_table.dbroutetable.id

# }
# resource "aws_route_table_association" "b" {
#   subnet_id      = aws_subnet.rds_subnet1.id
#   route_table_id = aws_route_table.dbroutetable.id

# }
# resource "aws_route_table" "dbroutetable" {
#   vpc_id = local.vpcid
# }
module "db" {
  source  = "../../modules/rds-aurora"

  name                                = "bip-db"
  engine                              = "aurora-postgresql"
  serverless_enabled                  = false
  engine_version                      = "11.9"
  database_name                       = "bipanalysisdb"
  instance_type                       = "db.t3.medium"
  apply_immediately                   = true
  skip_final_snapshot                 = true
  publicly_accessible                 = false
  iam_database_authentication_enabled = false
  db_subnet_group_name                = "bip-db-sng"
  source_region                       = data.aws_region.current.name

  vpc_id                              = data.aws_ssm_parameter.vpc_id.value
  #subnets                             = ["${aws_subnet.rds_subnet.id}","${aws_subnet.rds_subnet1.id}"]
  subnets                             = [local.subnet_list[0], local.subnet_list[1]]
  allowed_security_groups             = [ data.aws_security_group.default.id ]


  username                            = "bipadmin"
  password                            = jsondecode(data.aws_secretsmanager_secret_version.current.secret_string)["bip_db_pass"]
  replica_count                       = 1
  create_security_group               = true
  storage_encrypted                   = true
  monitoring_interval                 = 10

  db_parameter_group_name             = "default"
  db_cluster_parameter_group_name     = "default"
  

  tags = {
    Environment = "bip"
    Name = "GH_PC_terraform"
  }
}

resource "aws_ssm_parameter" "database_name" {
  name  = "/gh-bip/${data.aws_region.current.name}/db_name"
  type  = "String"
  value = module.db.rds_cluster_database_name
}


resource "aws_ssm_parameter" "db_username" {
  name  = "/gh-bip/${data.aws_region.current.name}/db_username"
  type        = "String"
  value       = "bipadmin"
}

resource "aws_ssm_parameter" "db_ro_endpoint" {
  name  = "/gh-bip/${data.aws_region.current.name}/db_ro_endpoint"
  type        = "String"
  value       = module.db.rds_cluster_reader_endpoint
}
resource "aws_ssm_parameter" "db_endpoint" {
  name  = "/gh-bip/${data.aws_region.current.name}/db_endpoint"
  type        = "String"
  value       = module.db.rds_cluster_endpoint
}