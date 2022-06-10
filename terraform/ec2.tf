data "aws_ami" "amzn2-ami" {
  most_recent = true

  filter {
    name = "name"
    values = ["amzn2-ami-hvm-2.0.*"]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["amazon"]
}

resource "aws_instance" "snapshot-streaming" {
  count = 1
  associate_public_ip_address = true
  ami = data.aws_ami.amzn2-ami.id
  instance_type = var.instance-type
  vpc_security_group_ids = [aws_security_group.security-group.id, aws_security_group.security-group-access-to-vpc.id]

  user_data = file("ssh_keys.sh")

  subnet_id = var.cl-subnet-id

  iam_instance_profile = aws_iam_instance_profile.ec2-snapshot-streaming-profile.name

  tags = {
    Name = "cl-snapshot-streaming-${var.env}-${count.index}"
    Env = var.env
    Workspace = terraform.workspace
  }

  connection {
    type = "ssh"
    user = "ec2-user"
    host = self.public_ip
    timeout = "240s"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo yum -y update",
      "sudo yum -y install jq",
      "sudo yum -y install java-1.8.0-openjdk-headless",
      "sudo yum -y install polkit-devel",
      "mkdir /home/ec2-user/snapshot-streaming"
    ]
  }

  provisioner "file" {
    source = "templates/start"
    destination = "/home/ec2-user/snapshot-streaming/start"
  }

  provisioner "file" {
    source = "snapshot-streaming.jar"
    destination = "/home/ec2-user/snapshot-streaming/snapshot-streaming.jar"
  }

  provisioner "file" {
    content = templatefile("templates/application.conf", {
      node-urls = var.node-urls
      opensearch-url = var.opensearch-url
    })
    destination = "/home/ec2-user/snapshot-streaming/application.conf"
  }

  provisioner "file" {
    content = templatefile("templates/nextOrdinal.tftpl", {
      starting-ordinal = var.starting-ordinal
      ordinals-gaps = var.ordinals-gaps
    })
    destination = "/home/ec2-user/snapshot-streaming/nextOrdinal.json"
  }

  provisioner "file" {
    source = "templates/snapshot-streaming.service"
    destination = "/tmp/snapshot-streaming.service"
  }

   provisioner "file" {
    content = templatefile("templates/clean_indices", {
      opensearch-url = var.opensearch-url
    })
    destination = "/home/ec2-user/snapshot-streaming/clean_indices"
  }

  provisioner "file" {
    source = "templates/restart"
    destination = "/home/ec2-user/snapshot-streaming/restart"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo chmod 774 /home/ec2-user/snapshot-streaming/start",
      "sudo chmod 774 /home/ec2-user/snapshot-streaming/restart",
      "sudo chmod 774 /home/ec2-user/snapshot-streaming/clean_indices",
      "sudo mv /tmp/snapshot-streaming.service /etc/systemd/system/multi-user.target.wants/snapshot-streaming.service",
      "sudo chmod 774 /etc/systemd/system/multi-user.target.wants/snapshot-streaming.service",
      "sudo systemctl daemon-reload",
      "sudo systemctl enable snapshot-streaming.service",
      "sudo systemctl start snapshot-streaming.service"
    ]
  }



  depends_on = [
    aws_iam_instance_profile.ec2-snapshot-streaming-profile,
  ]


}

resource "aws_iam_role" "ec2-snapshot-streaming-role" {
  name = "cl-ec2-snapshot-streaming-role-${var.env}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF

  tags = {
    Name = "cl-ec2-snapshot-streaming-role"
    Env = var.env
    Workspace = terraform.workspace
  }
}

resource "aws_iam_instance_profile" "ec2-snapshot-streaming-profile" {
  name = "cl-ec2-snapshot-streaming-profile-${var.env}"
  role = aws_iam_role.ec2-snapshot-streaming-role.name
}

