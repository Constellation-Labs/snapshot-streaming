locals {
  workspace = terraform.workspace
}

module "gap-filling" {
  source = "./modules/gap-filling"
  ami = data.aws_ami.amzn2-ami.id
  instance-type = var.instance-type
  vpc_security_group_ids = [aws_security_group.security-group.id, aws_security_group.security-group-access-to-vpc.id]
  cl-subnet-id = var.cl-subnet-id
  iam_instance_profile = aws_iam_instance_profile.ec2-snapshot-streaming-profile.name
  env = var.env
  node-urls = var.node-urls
  opensearch-url = var.opensearch-url
  ordinals-gaps = var.ordinals-gaps
}