resource "aws_network_interface" "cl_snapshot-streaming-network_interface" {
  subnet_id = var.cl-subnet-id
  private_ips = ["20.0.0.12"]

  tags = {
    Name = "cl-network_interface-${var.env}"
    Env = var.env
    Workspace = local.workspace
  }
}

resource "aws_security_group" "security-group" {
  name = "cl-snapshot-streaming_security_group-${var.env}"
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
    Name = "cl-snapshot-streaming_security_group"
    Env = var.env
    Workspace = terraform.workspace
  }
}

resource "aws_security_group" "security-group-access-to-vpc" {
  name = "cl-snapshot-streaming_security_group-access-${var.env}"
  vpc_id = var.cl-vpc-id

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "cl-snapshot-streaming_security-group-access"
    Env = var.env
    Workspace = terraform.workspace
  }
}
