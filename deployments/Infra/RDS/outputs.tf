
# aws_rds_cluster
output "postgresql_rds_cluster_id" {
  description = "The ID of the cluster"
  value       = module.db.rds_cluster_id
}

output "postgresql_rds_cluster_resource_id" {
  description = "The Resource ID of the cluster"
  value       = module.db.rds_cluster_resource_id
}

output "postgresql_rds_cluster_endpoint" {
  description = "The cluster endpoint"
  value       = module.db.rds_cluster_endpoint
}

output "postgresql_rds_cluster_reader_endpoint" {
  description = "The cluster reader endpoint"
  value       = module.db.rds_cluster_reader_endpoint
}

output "postgresql_rds_cluster_database_name" {
  description = "Name for an automatically created database on cluster creation"
  value       = module.db.rds_cluster_database_name
}