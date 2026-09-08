# AWS setup

Manual steps first (fastest way to unblock Databricks), Terraform to follow
once you know the shape you want — don't fight Terraform IAM policy syntax
before you've confirmed Unity Catalog can actually reach your bucket.

## Manual steps

1. **S3 bucket** — create one bucket, e.g. `data-plant-<yourname>-lakehouse`, same region you'll use everywhere else. Block all public access (default).
2. **IAM role for Unity Catalog** — Databricks needs a role it can assume to read/write the bucket. Follow Databricks' current instructions here, since the exact trust policy JSON changes with their account ID requirements:
   https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials
   In short: create an IAM role with a trust policy allowing the Databricks AWS account to assume it (with an external ID Databricks gives you), and a permissions policy scoped to your bucket (`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on the bucket and `arn:...:bucket/*`).
3. **Billing alarm** — CloudWatch billing alarm at $5 and $20. Do this before anything else runs.

## Terraform (optional, once manual works)

`main.tf` here is a starting skeleton — fill in the trust policy condition
values Databricks gives you during Unity Catalog setup (account ID, external
ID) before applying.

```
terraform init
terraform plan
terraform apply
```

Keep `terraform.tfstate` out of git (already in `.gitignore`) — for a solo
project local state is fine; don't over-engineer remote state for a resume
project.
