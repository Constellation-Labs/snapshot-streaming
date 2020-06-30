output "instance_ips" {
  value = aws_instance.snapshot-streaming.*.public_ip
}

output "instance_ips_grafana" {
  // TODO: hardcoded application port
  value = "[${join(",", aws_instance.snapshot-streaming.*.public_ip)}]"
}