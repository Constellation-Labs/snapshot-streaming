data "aws_s3_bucket" "cluster_snapshots" {
  bucket = var.bucket-name
}
