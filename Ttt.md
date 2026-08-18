1. Triggers & Orchestration
 Amazon EventBridge: Acts as the catalyst and starting trigger for the pipeline. In this design, it uses a cron schedule to automatically start the orchestration process at a specific time.
 AWS Step Functions: A serverless orchestration service that coordinates the components of the distributed data pipeline. It controls the execution sequence, ensures the first job finishes before the second starts, and handles error catching and retries.
 Amazon SNS (Simple Notification Service): Handles failure alerts. If Step Functions detects an error or timeout in any of the underlying jobs, it routes a message to SNS to alert the operations team.
2. Security & Governance
 AWS Secrets Manager: Securely stores and rotates the sensitive API credentials (like OAuth tokens or API keys). It provides these secrets to the ingestion job at runtime so they are never hardcoded in the scripts.
 AWS IAM (Identity and Access Management): Provides the execution roles that grant least-privilege permissions. It ensures Step Functions is allowed to trigger jobs, and that Glue is allowed to read secrets and write to specific S3 buckets.
3. Compute & Data Integration
 AWS Glue Job 1 (API Ingestion / Python Shell): The initial extraction compute layer. It uses the credentials from Secrets Manager to connect to external Source Systems (APIs), handles pagination, and extracts the raw JSON data.
 AWS Glue Job 2 (PySpark / Iceberg Transform): The heavy ETL engine. It reads the raw data, cleanses it, flattens structures, and writes the optimized data as Apache Iceberg tables.
 AWS Glue Crawler: An automated discovery tool that scans the new partitions and files in the Silver layer to infer the schema and update the metadata.
4. Storage & Metadata (The Data Lakehouse)
 Amazon S3 (Bronze Layer): The central data lake store that acts as the landing zone for the untransformed, raw data exactly as it was extracted from the source APIs.
 Amazon S3 (Silver Layer): Stores the cleansed and curated data in an optimized, columnar format (Apache Iceberg).
 AWS Glue Data Catalog: The central metadata repository that holds the table definitions, schemas, and partition locations, acting as the bridge between S3 storage and Athena.
 Amazon S3 (Athena Query Results): A required administrative bucket where Athena automatically stores the ⁠.csv⁠ outputs and execution logs of all queries run against the lake.
5. Consumption (Serving Layer)
 Amazon Athena: A serverless SQL query engine used to run interactive analytical queries directly against the data in S3. In this architecture, it also hosts the logical Gold Layer by defining customized SQL views (⁠CREATE VIEW⁠) on top of the Silver tables for business intelligence (BI) tools and data consumers.
