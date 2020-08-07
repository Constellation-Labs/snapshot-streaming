output "instance_ip" {
  value = aws_instance.snapshot-streaming.*.public_ip
}

output "gap_filling_instance_ips" {
  value = module.gap-filling.instance_ips
}