terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
}

locals {
  deploy_method = "terraform"
}

variable repo_name {
  type = string
}

variable environment {
  type = string
}

variable aws_region {
  type = string
}

resource "aws_ecr_repository" "repository" {
  name                 = "${var.environment}-${var.repo_name}"
  image_tag_mutability = "IMMUTABLE"

  tags = {
      environment = var.environment
      deploy_method = local.deploy_method
  }

}
