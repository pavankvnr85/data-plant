# Databricks setup

1. Sign up at https://www.databricks.com/try-databricks, choose the option
   linked to your existing AWS account (not Express signup) so the workspace
   deploys against your AWS account and can use Unity Catalog with your S3
   bucket.
2. In the account console, create a **storage credential** using the IAM
   role ARN from `infra/aws` (`unity_catalog_role_arn` output). This is
   where Databricks gives you the exact trust policy account ID and
   external ID — go back and fill those into `infra/aws/main.tf` if you're
   using Terraform, or just paste them into the IAM role trust policy in
   the console if you did it manually.
3. Create an **external location** pointing at `s3://<your-bucket>/`.
4. Create a **Unity Catalog metastore** for your region if one doesn't
   exist, and assign your workspace to it.
5. Create a **catalog** named `data_plant`, with schemas `bronze`,
   `silver`, `gold`, `metadata`.
6. Create a small **cluster** (single node, smallest instance type is
   plenty for this project's data volumes) or use **serverless SQL** for
   the warehouse-style queries — serverless is billed differently and
   often cheaper for intermittent use, which matters once the trial ends.
7. Sanity check: run this in a notebook or SQL editor.

```sql
CREATE TABLE data_plant.bronze.sanity_check (id INT, note STRING);
INSERT INTO data_plant.bronze.sanity_check VALUES (1, 'unity catalog can write to s3');
SELECT * FROM data_plant.bronze.sanity_check;
DESCRIBE HISTORY data_plant.bronze.sanity_check;
```

If `DESCRIBE HISTORY` returns a version row, Delta/Iceberg time travel is
working and you're ready for Phase 1.

## After the trial ends

Databricks usage outside the $400 credit is billed per DBU; the underlying
AWS resources (S3 storage, any EC2 the cluster used) are billed by AWS
regardless. Before the 14 days are up, decide whether to:
- add a payment method and keep going at low/serverless usage, or
- tear down the cluster and keep only the S3 data + Unity Catalog metadata (mostly free), resuming with a new trial or a different compute engine (OSS Spark on EMR/local) later.

Either way, terminate idle clusters — this is the single biggest cost
leak in a personal Databricks project.
