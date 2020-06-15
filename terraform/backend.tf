terraform {
  backend "s3" {
    bucket = "constellationlabs-tf"
    key = "snapshot-streaming"
    region = "us-west-1"
  }
}
