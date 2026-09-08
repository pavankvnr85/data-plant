terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the lakehouse"
  type        = string
}

# Fill these in from the Unity Catalog "storage credential" setup screen
variable "databricks_account_id" {
  description = "Databricks AWS account ID to trust (from Unity Catalog setup UI)"
  type        = string
  default     = ""
}

variable "databricks_external_id" {
  description = "External ID Databricks gives you for the trust policy"
  type        = string
  default     = ""
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_role" "unity_catalog" {
  name = "data-plant-unity-catalog-role"

  # TODO: replace databricks_account_id/external_id with real values from
  # the Unity Catalog "create storage credential" screen before applying.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${var.databricks_account_id}:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = var.databricks_external_id
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "unity_catalog_s3" {
  name = "data-plant-unity-catalog-s3-access"
  role = aws_iam_role.unity_catalog.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*"
      ]
    }]
  })
}

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "unity_catalog_role_arn" {
  value = aws_iam_role.unity_catalog.arn
}
