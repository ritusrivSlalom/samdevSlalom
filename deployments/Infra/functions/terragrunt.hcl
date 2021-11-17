include "root" {
  path = find_in_parent_folders()
}
dependency "rds" {
  config_path = "../RDS"
}
