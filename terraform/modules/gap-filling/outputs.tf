output "instance_ips" {
  value = aws_instance.snapshot-streaming-gap-filling.*.public_ip
}