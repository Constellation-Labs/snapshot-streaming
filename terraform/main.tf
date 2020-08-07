locals {
  workspace = terraform.workspace
}

module "gap-filling" {
  source = "./modules/gap-filling"
  ami = data.aws_ami.amzn2-ami.id
  bucket-names = var.bucket-names
  cl-subnet-id = var.cl-subnet-id
  elasticsearch-url = var.elasticsearch-url
  ending-height = var.starting-height - var.snapshot-interval
  env = var.env
  iam_instance_profile = aws_iam_instance_profile.ec2-snapshot-streaming-profile.name
  instance-count = 1
  instance-type = var.instance-type
  vpc_security_group_ids = [aws_security_group.security-group.id, aws_security_group.security-group-access-to-vpc.id]
}