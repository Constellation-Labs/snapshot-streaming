# Module for filling potential gaps in heights. It runs `n` recovery instances, each one sending different
# interval of data to elasticsearch.

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

resource "aws_instance" "snapshot-streaming-gap-filling" {
  count = var.instance-count
  associate_public_ip_address = true
  ami = data.aws_ami.amzn2-ami.id
  instance_type = var.instance-type
  vpc_security_group_ids = var.vpc_security_group_ids

  user_data = file("ssh_keys.sh")

  subnet_id = var.cl-subnet-id

  iam_instance_profile = var.iam_instance_profile

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
      bucket-names = var.bucket-names,
      elasticsearch-url = var.elasticsearch-url
      skip-height-on-failure = true
      starting-height = (var.starting-height + (count.index) * (2 * floor(var.ending-height / var.instance-count / var.snapshot-interval) + var.snapshot-interval))
      ending-height = count.index + 1 == var.instance-count ? var.ending-height : (var.starting-height + (count.index) * (2 * floor(var.ending-height / var.instance-count / var.snapshot-interval) + var.snapshot-interval)) + (2 * floor(var.ending-height / var.instance-count / var.snapshot-interval))
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
//
//  depends_on = [
//    aws_iam_instance_profile.ec2-snapshot-streaming-profile,
//  ]


}

