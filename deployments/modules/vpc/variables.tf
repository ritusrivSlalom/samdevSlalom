variable "name" {
  type        = string
  description = "Name of the vpc"
}

variable "vpc_cidr_block" {
  type        = string
  description = "CIDR block for VPC"
}

variable "availability_zones" {
  type        = list
  description = "VPC availability zones"
}

variable "private_subnets_cidr" {
  type        = list
  description = "Private subnet cidr"
}

variable "public_subnets_cidr" {
  type        = list
  description = "Public subnet cidr"
}

variable "public_subnet_tags" {
  type = map
}

variable "private_subnet_tags" {
  type = map
}

variable "default_security_group_name" {
  type        = string
  description = "Default VPC security group name"
}