output "instance_ip" {
  value = aws_instance.snapshot-streaming-gap-filling.*.public_ip
}
