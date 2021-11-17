remote_state {
  backend = "s3"
  config = {
    bucket = "ghbi-bisre-terraform-state-royal-marrsden"
    key    = "bisre/${path_relative_to_include()}/terraform.tfstate"
    region = "eu-west-2"
  }
}
