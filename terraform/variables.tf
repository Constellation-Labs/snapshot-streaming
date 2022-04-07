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

variable "instance-type" {
  type = string
  default = "t2.medium"
}

variable "node-urls" {
  type = list(string)
}

variable "opensearch-url" {
  type = string
}

variable "starting-ordinal" {
  type = number
  default = 0
}

variable "ordinals-gaps" {
  type = list(number)
  default = []
}
