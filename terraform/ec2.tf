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
      bucket-name = var.bucket-name,
      elasticsearch-url = var.elasticsearch-url
    })
    destination = "/home/ec2-user/snapshot-streaming/application.conf"
  }

  provisioner "file" {
    source = "templates/snapshot-streaming.service"
    destination = "/tmp/snapshot-streaming.service"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo chmod 774 /home/ec2-user/snapshot-streaming/start",
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


resource "aws_iam_policy" "s3-access-to-ec2-snapshot-streaming" {
  name = "cl-s3-access-to-ec2-snapshot-streaming-${var.env}"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
    ]
}
EOF
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

resource "aws_iam_role_policy_attachment" "s3-iam-role-snapshot-streaming" {
  role = aws_iam_role.ec2-snapshot-streaming-role.name
  policy_arn = aws_iam_policy.s3-access-to-ec2-snapshot-streaming.arn
}

resource "aws_iam_instance_profile" "ec2-snapshot-streaming-profile" {
  name = "cl-ec2-snapshot-streaming-profile-${var.env}"
  role = aws_iam_role.ec2-snapshot-streaming-role.name
}

