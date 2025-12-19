# Terraform template for AWS S3 bucket, EMR cluster, and IAM roles (simplified)
provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "iceberg_data" {
  bucket = "my-iceberg-data-bucket"
  force_destroy = true
}

resource "aws_emr_cluster" "iceberg_cluster" {
  name          = "iceberg-emr-cluster"
  release_label = "emr-6.10.0"
  applications  = ["Spark"]
  ec2_attributes {
    subnet_id = "subnet-xxxxxx"
    emr_managed_master_security_group = "sg-xxxxxx"
    emr_managed_slave_security_group  = "sg-xxxxxx"
  }
  master_instance_type = "m5.xlarge"
  core_instance_type   = "m5.xlarge"
  core_instance_count  = 2
  service_role         = aws_iam_role.emr_service.arn
  job_flow_role        = aws_iam_role.emr_ec2.arn
  log_uri              = "s3://my-iceberg-data-bucket/logs/"
}

resource "aws_iam_role" "emr_service" {
  name = "EMR_DefaultRole"
  assume_role_policy = data.aws_iam_policy_document.emr_service.json
}

resource "aws_iam_role" "emr_ec2" {
  name = "EMR_EC2_DefaultRole"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2.json
}

data "aws_iam_policy_document" "emr_service" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "emr_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}
