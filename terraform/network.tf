resource "aws_security_group" "security-group" {
  name = "cl-snapshot-streaming_security_group-${var.env}-${local.instance_id}"
  vpc_id = var.cl-vpc-id

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "cl-snapshot-streaming_security_group-${local.instance_id}"
    Env = var.env
    Workspace = terraform.workspace
  }
}

resource "aws_security_group" "security-group-access-to-vpc" {
  name = "cl-snapshot-streaming_security_group-access-${var.env}-${local.instance_id}"
  vpc_id = var.cl-vpc-id

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "cl-snapshot-streaming_security-group-access-${local.instance_id}"
    Env = var.env
    Workspace = terraform.workspace
  }
}
