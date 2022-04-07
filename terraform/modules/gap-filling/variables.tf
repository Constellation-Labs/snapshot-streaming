variable "ami" {
  type = string
}

variable "instance-type" {
  type = string
}

variable "vpc_security_group_ids" {
  type = list(string)
}

variable "cl-subnet-id" {
  type = string
}

variable "iam_instance_profile" {
  type = string
}

variable "env" {
  type = string
}

variable "node-urls" {
  type = list(string)
}

variable "opensearch-url" {
  type = string
}

variable "ordinals-gaps" {
  type = list(number)
  default = []
}
