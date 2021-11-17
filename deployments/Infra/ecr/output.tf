output "ecr-arn" {
  value       = aws_ecr_repository.cloudybiprepo.arn
  description = "Registry arn."
}

output "ecr-url" {
  value       = aws_ecr_repository.cloudybiprepo.repository_url
  description = "Registry URL."
}