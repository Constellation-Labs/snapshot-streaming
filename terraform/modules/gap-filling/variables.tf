variable "instance-count" {
  type = number
  description = "How many gap filling instances should be spinned up. Interval to fill the gaps will be divided by number of instances to parallelize"
}

variable "starting-height" {
  type = number
  default = 2
  description = "At which height gap filling should start"
}

variable "ending-height" {
  type = number
  description = "At which height gap filling should stop"
}

variable "snapshot-interval" {
  type = number
  default = 2
}

variable "ami" {
  type = string
}

variable "instance-type" {
  type = string
}

variable "cl-subnet-id" {
  type = string
}

variable "vpc_security_group_ids" {
  type = list(string)
}

variable "iam_instance_profile" {
  type = string
}

variable "env" {
  type = string
}


variable "bucket-names" {
  type = set(string)
}

variable "elasticsearch-url" {
  type = string
}