variable "aws_region" {
  type = string
  default = "us-west-1"
}

variable "env" {
  type = string
  default = "dev"
}

variable "cl-vpc-id" {
  type = string
}

variable "cl-vpc-cidr-block" {
  type = string
}

variable "cl-subnet-id" {
  type = string
}

variable "cl-network-interface-id" {
  type = string
}

variable "elasticsearch-url" {
  type = string
}

variable "bucket-names" {
  type = list(string)
}

variable "instance-type" {
  type = string
  default = "t2.medium"
}

variable "starting-height" {
  type = number
}

variable "snapshot-interval" {
  type = number
  default = 2
}