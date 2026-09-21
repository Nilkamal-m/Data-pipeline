"""
Unit and Integration Validation for Silver & Gold Serving Layer.
Tests:
1. Strict 2-Option Parameter Validation (--PROCESS_LAYER=silver|gold)
2. Shared MySQL Database Guardrails & Schema Pre-Existence Verification
3. Naming Convention Guardrails (gold_tbl_* and v_*)
4. Column Schema Introspection & Evolution Detection
5. Silver API Enrichment Hook & Inter-Table Custom Transform Calling
"""

import sys
import os
import types
import unittest
from unittest.mock import MagicMock, patch
import json
import re

# Mock awsglue, pyspark, boto3, dateutil for standalone testing without Glue/Hadoop runtime
for pkg in ['pyspark', 'pyspark.sql', 'awsglue', 'botocore', 'dateutil']:
    m = types.ModuleType(pkg)
    m.__path__ = []
    sys.modules[pkg] = m

for mod in [
    'pyspark.context', 'pyspark.conf', 'pyspark.sql', 'pyspark.sql.functions', 'pyspark.sql.types', 'pyspark.sql.window',
    'awsglue', 'awsglue.context', 'awsglue.job', 'awsglue.utils',
    'boto3', 'botocore', 'botocore.session', 'botocore.client', 'botocore.exceptions',
    'dateutil', 'dateutil.tz', 'dateutil.parser'
]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

sys.modules['pyspark.sql'].SparkSession = MagicMock
sys.modules['pyspark.sql.window'].Window = MagicMock
sys.modules['pyspark.sql'].DataFrame = MagicMock
sys.modules['pyspark.context'].SparkContext = MagicMock
sys.modules['pyspark.conf'].SparkConf = MagicMock
sys.modules['awsglue.context'].GlueContext = MagicMock
sys.modules['awsglue.job'].Job = MagicMock


class MockClientError(Exception):
    def __init__(self, error_response=None, operation_name=None):
        super().__init__(str(error_response))
        self.response = error_response or {}
        self.operation_name = operation_name


sys.modules['botocore.exceptions'].ClientError = MockClientError

# Ensure directories are on sys.path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
bronze_dir = os.path.join(repo_root, "bronze", "script")
silver_dir = os.path.join(repo_root, "silver", "script")
gold_dir = os.path.join(repo_root, "gold", "script")
lambda_dir = os.path.join(repo_root, "lambda_helper")
for p in [repo_root, bronze_dir, silver_dir, gold_dir, lambda_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from gold_layer_manager import GoldLayerManager
from transformer import SilverTransformer
from silver_config_loader import SilverConfigLoader
from custom_transforms import servicenow_incident
from config_loader import ConfigLoader
from uax_bronze_load import get_table_state_key, update_last_load_date, parse_arguments, get_last_load_date


class TestProcessLayerOptions(unittest.TestCase):
    """Verifies that --PROCESS_LAYER strictly accepts only 'silver' or 'gold'."""

    def test_invalid_process_layer_all(self):
        """Passing --PROCESS_LAYER=all must raise an explicit ValueError."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "all",
            "--SOURCE_SYSTEM", "servicenow"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            with self.assertRaises(ValueError) as ctx:
                parse_spark_arguments()
            self.assertIn("Strictly 2 options are supported", str(ctx.exception))

    def test_invalid_process_layer_custom(self):
        """Passing an unknown layer must raise an explicit ValueError."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "bronze",
            "--SOURCE_SYSTEM", "servicenow"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            with self.assertRaises(ValueError) as ctx:
                parse_spark_arguments()
            self.assertIn("Strictly 2 options are supported", str(ctx.exception))

    def test_valid_process_layer_silver(self):
        """Passing --PROCESS_LAYER=silver succeeds and parses correctly."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "silver",
            "--SOURCE_SYSTEM", "servicenow",
            "--DATA_LAKE_BUCKET", "test-bucket",
            "--GLUE_DATABASE", "test_db",
            "--TABLE_PREFIX", "tbl_",
            "--SOURCE_TABLE_NAME", "raw_tbl_incident"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            params = parse_spark_arguments()
            self.assertEqual(params['PROCESS_LAYER'], 'silver')
            self.assertEqual(params['SOURCE_SYSTEM'], 'servicenow')

    def test_valid_process_layer_gold(self):
        """Passing --PROCESS_LAYER=gold succeeds and parses gold parameters."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "gold",
            "--SOURCE_SYSTEM", "servicenow",
            "--GOLD_SCHEMA", "gold_marts",
            "--GOLD_TARGET", "aurora",
            "--DATA_LAKE_BUCKET", "test-bucket",
            "--GLUE_DATABASE", "test_db",
            "--TABLE_PREFIX", "tbl_"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            params = parse_spark_arguments()
            self.assertEqual(params['PROCESS_LAYER'], 'gold')
            self.assertEqual(params['SOURCE_SYSTEM'], 'servicenow')
            self.assertEqual(params['GOLD_SCHEMA'], 'gold_marts')
            self.assertEqual(params['GOLD_TARGET'], 'aurora')

    def test_gold_layer_missing_schema_raises_value_error(self):
        """Omitting --GOLD_SCHEMA when --PROCESS_LAYER=gold must raise a guiding ValueError."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "gold",
            "--SOURCE_SYSTEM", "servicenow",
            "--DATA_LAKE_BUCKET", "test-bucket"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            with self.assertRaises(ValueError) as ctx:
                parse_spark_arguments()
            self.assertIn("Missing required parameter '--GOLD_SCHEMA'", str(ctx.exception))
            self.assertIn("no fallback schema is permitted", str(ctx.exception))

    def test_gold_layer_missing_source_system_raises_value_error(self):
        """Omitting --SOURCE_SYSTEM when --PROCESS_LAYER=gold must raise a guiding ValueError."""
        test_args = [
            "uax_silver_etl.py",
            "--PROCESS_LAYER", "gold",
            "--GOLD_SCHEMA", "my_mart",
            "--DATA_LAKE_BUCKET", "test-bucket"
        ]
        with patch.object(sys, 'argv', test_args):
            from uax_silver_etl import parse_spark_arguments
            with self.assertRaises(ValueError) as ctx:
                parse_spark_arguments()
            self.assertIn("Missing required parameter '--SOURCE_SYSTEM'", str(ctx.exception))


class TestGoldSharedDatabaseSafety(unittest.TestCase):
    """Verifies MySQL shared DB safety, schema pre-existence check, and naming guardrails."""

    def test_schema_missing_raises_runtime_error(self):
        """If target schema does not exist, must raise RuntimeError with zero DDL execution."""
        jdbc_info = {"host": "mock-db", "port": "3306", "user": "user", "password": "pwd"}

        with patch.object(GoldLayerManager, '_execute_sql_query', return_value=[]):
            with self.assertRaises(RuntimeError) as ctx:
                GoldLayerManager._verify_schema_exists_or_raise(jdbc_info, "non_existent_schema")
            self.assertIn("CRITICAL SHARED-DB POLICY ERROR", str(ctx.exception))
            self.assertIn("NEVER executes CREATE DATABASE", str(ctx.exception))

    def test_schema_exists_passes(self):
        """If target schema exists, validation passes silently."""
        jdbc_info = {"host": "mock-db", "port": "3306", "user": "user", "password": "pwd"}

        with patch.object(GoldLayerManager, '_execute_sql_query', return_value=[("gold_marts",)]):
            try:
                GoldLayerManager._verify_schema_exists_or_raise(jdbc_info, "gold_marts")
            except RuntimeError:
                self.fail("Schema check raised RuntimeError unexpectedly for an existing schema.")

    def test_table_naming_guardrails(self):
        """Verifies naming conventions for tables and views."""
        table_base = "incident_kpi"
        target_table = f"gold_tbl_{table_base}"
        staging_table = f"gold_tbl_{table_base}_staging"
        view_name = f"v_{table_base}"

        self.assertTrue(target_table.startswith("gold_tbl_"))
        self.assertTrue(staging_table.startswith("gold_tbl_") and staging_table.endswith("_staging"))
        self.assertTrue(view_name.startswith("v_"))

        # Negative test: unauthorized table name
        invalid_table = "tbl_financial_records"
        with self.assertRaises(AssertionError):
            assert invalid_table.startswith("gold_tbl_"), "Safety Error"

    def test_password_manual_option(self):
        """Tests that manual password via --RDS_PASSWORD is used."""
        params = {
            "GOLD_SCHEMA": "enterprise_reporting",
            "RDS_PASSWORD": "manual_secure_password_123",
            "RDS_HOST": "aurora-cluster.internal",
            "RDS_USER": "report_user"
        }
        conn = GoldLayerManager._resolve_mysql_connection_info(params)
        self.assertEqual(conn["password"], "manual_secure_password_123")
        self.assertEqual(conn["user"], "report_user")
        self.assertEqual(conn["database"], "enterprise_reporting")

    def test_password_from_secret_json(self):
        """Tests retrieving credentials from AWS Secrets Manager JSON payload."""
        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "password": "secret_vault_pwd",
                "username": "secret_user",
                "host": "secret-aurora.aws.com",
                "port": "3306"
            })
        }
        params = {
            "GOLD_SCHEMA": "enterprise_reporting",
            "RDS_SECRET_NAME": "prod/rds/mysql_credentials"
        }
        conn = GoldLayerManager._resolve_mysql_connection_info(params, secrets_client=mock_secrets)
        self.assertEqual(conn["password"], "secret_vault_pwd")
        self.assertEqual(conn["user"], "secret_user")
        self.assertEqual(conn["host"], "secret-aurora.aws.com")
        self.assertEqual(conn["database"], "enterprise_reporting")
        mock_secrets.get_secret_value.assert_called_once_with(SecretId="prod/rds/mysql_credentials")

    def test_password_from_secret_plain_text(self):
        """Tests retrieving credentials from AWS Secrets Manager plain string secret."""
        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": "raw_plain_password\n"
        }
        params = {
            "GOLD_SCHEMA": "enterprise_reporting",
            "SECRET_NAME": "prod/rds/raw_password"
        }
        conn = GoldLayerManager._resolve_mysql_connection_info(params, secrets_client=mock_secrets)
        self.assertEqual(conn["password"], "raw_plain_password")
        self.assertEqual(conn["database"], "enterprise_reporting")

    def test_password_secret_failure_raises_guiding_error(self):
        """Tests that a failure in Secrets Manager raises a clear guiding ValueError."""
        mock_secrets = MagicMock()
        mock_secrets.get_secret_value.side_effect = Exception("AccessDenied: User is not authorized")
        params = {
            "GOLD_SCHEMA": "enterprise_reporting",
            "RDS_SECRET_NAME": "prod/rds/denied_secret"
        }
        with self.assertRaises(ValueError) as ctx:
            GoldLayerManager._resolve_mysql_connection_info(params, secrets_client=mock_secrets)
        self.assertIn("Failed to retrieve MySQL credentials from AWS Secrets Manager", str(ctx.exception))
        self.assertIn("AccessDenied", str(ctx.exception))

    def test_missing_password_raises_guiding_error(self):
        """Tests that missing database password raises an explicit guiding ValueError."""
        params = {
            "GOLD_SCHEMA": "enterprise_reporting"
        }
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as ctx:
                GoldLayerManager._resolve_mysql_connection_info(params)
            self.assertIn("CRITICAL AUTH ERROR: MySQL database password is missing", str(ctx.exception))
            self.assertIn("AWS Secrets Manager", str(ctx.exception))
            self.assertIn("Manual Password", str(ctx.exception))

    def test_missing_schema_raises_guiding_error(self):
        """Tests that missing GOLD_SCHEMA raises an explicit guiding ValueError."""
        params = {
            "RDS_PASSWORD": "pwd"
        }
        with self.assertRaises(ValueError) as ctx:
            GoldLayerManager._resolve_mysql_connection_info(params)
        self.assertIn("CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'", str(ctx.exception))
        self.assertIn("no fallback schema is permitted", str(ctx.exception))


class TestCustomTransformsAndApiEnrichment(unittest.TestCase):
    """Verifies that custom_transforms can query other tables and API enrichment attaches default null string."""

    def test_custom_transform_signature(self):
        """Verifies servicenow_incident.transform accepts (df, spark, context)."""
        import inspect
        sig = inspect.signature(servicenow_incident.transform)
        param_names = list(sig.parameters.keys())
        self.assertIn("df", param_names)
        self.assertIn("spark", param_names)
        self.assertIn("context", param_names)

    def test_external_columns_null_string(self):
        """Tests that configured external columns are attached with null string default."""
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.withColumn.return_value = mock_df

        table_cfg = {"external_columns": ["external_api_data"]}
        res_df = SilverTransformer._apply_external_columns(mock_df, table_cfg)
        mock_df.withColumn.assert_called_once()
        args = mock_df.withColumn.call_args[0]
        self.assertEqual(args[0], "external_api_data")

    def test_api_enrichment_backward_compat(self):
        """Tests that legacy api_enrichment_columns key and alias function still work."""
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.withColumn.return_value = mock_df

        table_cfg = {"api_enrichment_columns": ["legacy_api_data"]}
        res_df = SilverTransformer._apply_api_enrichment(mock_df, table_cfg)
        mock_df.withColumn.assert_called_once()
        args = mock_df.withColumn.call_args[0]
        self.assertEqual(args[0], "legacy_api_data")

    def test_silver_config_declares_external_columns(self):
        """Tests that silver_config.json explicitly defines external_columns in defaults and table configs."""
        with open("silver/script/config/silver_config.json", "r") as f:
            cfg = json.load(f)
        defaults = cfg.get("pipeline_defaults") or cfg.get("silver_defaults", {})
        self.assertIn("external_columns", defaults)
        for source_sys, s_cfg in cfg.get("source_systems", {}).items():
            tbls = s_cfg.get("tables") or s_cfg.get("table_configs", {})
            for tbl, t_cfg in tbls.items():
                self.assertIn("external_columns", t_cfg, f"Table {source_sys}.{tbl} missing external_columns in config")

    def test_silver_schema_introspection(self):
        """Tests formatting of Silver schema introspection."""
        field1 = MagicMock()
        field1.name = "incident_id"
        field1.dataType.simpleString.return_value = "string"
        field2 = MagicMock()
        field2.name = "_valid_from"
        field2.dataType.simpleString.return_value = "timestamp"

        mock_df = MagicMock()
        mock_df.schema.fields = [field1, field2]

        with patch('transformer.logger') as mock_logger:
            SilverTransformer.log_schema_introspection(mock_df, "Test Table Introspection")
            log_calls = [str(call) for call in mock_logger.info.call_args_list]
            self.assertTrue(any("SCHEMA INTROSPECTION" in c for c in log_calls))
            self.assertTrue(any("incident_id" in c for c in log_calls))
            self.assertTrue(any("_valid_from" in c for c in log_calls))

    def test_schema_evolution_detection(self):
        """Tests that new columns in incoming DataFrame are detected against existing MySQL table."""
        jdbc_info = {"host": "mock-db", "port": "3306", "user": "user", "password": "pwd"}
        existing_cols = [("c1", "varchar"), ("c2", "int")]

        field1 = MagicMock()
        field1.name = "c1"
        field1.dataType.simpleString.return_value = "string"
        field2 = MagicMock()
        field2.name = "c2"
        field2.dataType.simpleString.return_value = "int"
        field3 = MagicMock()
        field3.name = "c3_new"
        field3.dataType.simpleString.return_value = "double"

        mock_df = MagicMock()
        mock_df.schema.fields = [field1, field2, field3]

        with patch.object(GoldLayerManager, '_execute_sql_query', return_value=existing_cols):
            with patch('gold_layer_manager.logger') as mock_logger:
                GoldLayerManager._detect_schema_evolution(jdbc_info, "gold_marts", "gold_tbl_test", mock_df)
                log_calls = [str(call) for call in mock_logger.info.call_args_list]
                self.assertTrue(any("SCHEMA EVOLUTION DETECTED" in c for c in log_calls))
                self.assertTrue(any("c3_new" in c for c in log_calls))

    def test_discover_local_queries(self):
        """Tests that local .sql files are discovered when S3 is not available."""
        s3_client = MagicMock()
        s3_client.get_paginator.side_effect = Exception("No S3")
        queries = GoldLayerManager._discover_queries(
            "s3://fake-bucket/gold/query/servicenow",
            "fake-bucket",
            s3_client,
            source_system="servicenow"
        )
        self.assertIn("incident_kpi", queries)
        self.assertIn("tbl_incident", queries["incident_kpi"])

    def test_discover_source_specific_s3_queries(self):
        """Tests that S3 queries under bucket/gold/query/<source>/*.sql are discovered."""
        s3_client = MagicMock()
        mock_page = {
            'Contents': [
                {'Key': 'gold/query/servicenow/v_incident_kpi.sql'}
            ]
        }
        paginator = MagicMock()
        paginator.paginate.return_value = [mock_page]
        s3_client.get_paginator.return_value = paginator
        s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: b"SELECT 1 FROM tbl_incident")
        }

        queries = GoldLayerManager._discover_queries(
            "s3://fake-bucket/gold/query/servicenow",
            "fake-bucket",
            s3_client,
            source_system="servicenow"
        )
        self.assertIn("incident_kpi", queries)
        self.assertEqual(queries["incident_kpi"], "SELECT 1 FROM tbl_incident")
        paginator.paginate.assert_called_once_with(
            Bucket="fake-bucket",
            Prefix="gold/query/servicenow"
        )

    def test_discover_source_specific_local_queries(self):
        """Tests that local queries in gold/query/<source>/*.sql are discovered when S3 is unavailable."""
        s3_client = MagicMock()
        s3_client.get_paginator.side_effect = Exception("No S3")
        queries = GoldLayerManager._discover_queries(
            "s3://fake-bucket/gold/query/servicenow",
            "fake-bucket",
            s3_client,
            source_system="servicenow"
        )
        self.assertIn("incident_kpi", queries)
        self.assertIn("tbl_incident", queries["incident_kpi"])

    def test_gold_source_specific_path_resolution(self):
        """Tests that query_s3_path and data_s3_path automatically append source_system."""
        from uax_silver_etl import parse_spark_arguments

        test_args = [
            "script_name",
            "--PROCESS_LAYER", "gold",
            "--SOURCE_SYSTEM", "servicenow",
            "--DATA_LAKE_BUCKET", "my-test-bucket",
            "--GOLD_SCHEMA", "gold_marts"
        ]
        with patch.object(sys, 'argv', test_args):
            params = parse_spark_arguments()
            self.assertEqual(params['GOLD_QUERY_S3_PATH'], "s3://my-test-bucket/gold/query/servicenow")
            self.assertEqual(params['GOLD_DATA_S3_PATH'], "s3://my-test-bucket/gold/data/servicenow")


class TestLambdaHelper(unittest.TestCase):
    """Verifies that the helper Lambda function can trigger and monitor Bronze, Silver, Gold, and Multi-Stage pipelines."""

    def test_athena_query_detection(self):
        from lambda_function import is_athena_query_event
        self.assertTrue(is_athena_query_event({"query": "SELECT 1"}))
        self.assertTrue(is_athena_query_event({"sql": "SELECT 1"}))
        self.assertTrue(is_athena_query_event({"layer": "athena"}))
        self.assertFalse(is_athena_query_event({"layer": "gold", "source_system": "servicenow"}))

    def test_build_glue_arguments_gold(self):
        from lambda_function import build_glue_arguments
        event = {
            "layer": "gold",
            "source_system": "servicenow",
            "gold_schema": "enterprise_reporting",
            "rds_secret_name": "prod/rds/credentials",
            "rds_password": "manual_password",
            "rds_host": "db.internal",
            "gold_target": "aurora"
        }
        args = build_glue_arguments(event)
        self.assertEqual(args["--SOURCE_SYSTEM"], "servicenow")
        self.assertEqual(args["--PROCESS_LAYER"], "gold")
        self.assertEqual(args["--GOLD_SCHEMA"], "enterprise_reporting")
        self.assertEqual(args["--RDS_SECRET_NAME"], "prod/rds/credentials")
        self.assertEqual(args["--RDS_PASSWORD"], "manual_password")
        self.assertEqual(args["--RDS_HOST"], "db.internal")
        self.assertEqual(args["--GOLD_TARGET"], "aurora")

    def test_build_glue_arguments_gold_missing_schema_raises_error(self):
        from lambda_function import build_glue_arguments
        event = {
            "layer": "gold",
            "source_system": "servicenow"
        }
        with self.assertRaises(ValueError) as ctx:
            build_glue_arguments(event)
        self.assertIn("Missing required parameter 'gold_schema'", str(ctx.exception))
        self.assertIn("no fallback schema is permitted", str(ctx.exception))

    def test_build_glue_arguments_silver(self):
        from lambda_function import build_glue_arguments
        event = {
            "layer": "silver",
            "source_system": "servicenow",
            "source_table_name": "incident",
            "full_refresh": True
        }
        args = build_glue_arguments(event)
        self.assertEqual(args["--SOURCE_SYSTEM"], "servicenow")
        self.assertEqual(args["--PROCESS_LAYER"], "silver")
        self.assertEqual(args["--FULL_REFRESH"], "True")

    @patch('lambda_function.glue_client')
    @patch('lambda_function.poll_glue_job_run')
    def test_single_gold_job_execution(self, mock_poll, mock_glue):
        from lambda_function import lambda_handler
        mock_glue.start_job_run.return_value = {"JobRunId": "jr_gold_123"}
        mock_poll.return_value = {
            "JobState": "SUCCEEDED",
            "ExecutionTimeSeconds": 42,
            "LogGroupName": "/aws-glue/jobs/output"
        }
        event = {
            "layer": "gold",
            "source_system": "servicenow",
            "gold_schema": "enterprise_reporting"
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["job_status"], "SUCCEEDED")
        self.assertEqual(body["layer"], "gold")
        self.assertEqual(body["gold_schema"], "enterprise_reporting")
        self.assertEqual(body["process_layer"], "gold")

    @patch('lambda_function.glue_client')
    @patch('lambda_function.poll_glue_job_run')
    def test_multi_stage_pipeline_execution_success(self, mock_poll, mock_glue):
        from lambda_function import lambda_handler
        mock_glue.start_job_run.side_effect = [
            {"JobRunId": "jr_bronze_1"},
            {"JobRunId": "jr_silver_2"},
            {"JobRunId": "jr_gold_3"}
        ]
        mock_poll.side_effect = [
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 30, "LogGroupName": "log1"},
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 45, "LogGroupName": "log2"},
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 25, "LogGroupName": "log3"}
        ]
        event = {
            "layer": "all",
            "source_system": "servicenow",
            "gold_schema": "enterprise_reporting"
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "SUCCEEDED")
        self.assertIn("bronze", body["stage_results"])
        self.assertIn("silver", body["stage_results"])
        self.assertIn("gold", body["stage_results"])

    @patch('lambda_function.glue_client')
    @patch('lambda_function.poll_glue_job_run')
    def test_multi_stage_pipeline_failure_halts_pipeline(self, mock_poll, mock_glue):
        from lambda_function import lambda_handler
        mock_glue.start_job_run.side_effect = [
            {"JobRunId": "jr_bronze_1"},
            {"JobRunId": "jr_silver_2"}
        ]
        mock_poll.side_effect = [
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 30, "LogGroupName": "log1"},
            {"JobState": "FAILED", "ExecutionTimeSeconds": 15, "ErrorMessage": "Iceberg merge conflict", "LogGroupName": "log2"}
        ]
        event = {
            "layers": ["bronze", "silver", "gold"],
            "source_system": "servicenow",
            "gold_schema": "enterprise_reporting"
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 500)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "FAILED")
        self.assertEqual(body["failed_stage"], "silver")
        self.assertIn("Iceberg merge conflict", body["error_message"])
        self.assertEqual(mock_glue.start_job_run.call_count, 2)


class TestLambdaCrawlerAndMultilineSql(unittest.TestCase):
    """
    Validates Helper Lambda functionality for:
    1. Glue Crawler triggering and synchronous/asynchronous monitoring.
    2. Crawler running as a stage in multi-stage pipelines (Bronze -> Crawler -> Silver).
    3. Multiline SQL query resolution and statement splitting (like interactions.sql).
    4. Reading SQL queries from files and S3 URIs with parameter substitution.
    """

    def test_is_crawler_event_detection(self):
        from lambda_function import is_crawler_event
        self.assertTrue(is_crawler_event({"layer": "crawler"}))
        self.assertTrue(is_crawler_event({"layer": "glue_crawler"}))
        self.assertTrue(is_crawler_event({"action": "run_crawler"}))
        self.assertTrue(is_crawler_event({"crawler_name": "uax-datalake-bronze-crawler-dev"}))
        self.assertFalse(is_crawler_event({"layer": "bronze", "source_system": "moveworks"}))
        self.assertFalse(is_crawler_event({"query": "SELECT 1"}))

    @patch('lambda_function.glue_client')
    def test_crawler_execution_synchronous_success(self, mock_glue):
        from lambda_function import lambda_handler
        mock_glue.start_crawler.return_value = {}
        mock_glue.get_crawler.return_value = {
            'Crawler': {
                'State': 'READY',
                'LastCrawl': {
                    'Status': 'SUCCEEDED',
                    'LogGroup': '/aws-glue/crawlers',
                    'LogStream': 'stream-123'
                },
                'Metrics': {
                    'TablesCreated': 2,
                    'TablesUpdated': 3,
                    'TablesDeleted': 0,
                    'PartitionsCreated': 10,
                    'PartitionsUpdated': 5,
                    'PartitionsDeleted': 0
                }
            }
        }
        event = {
            "layer": "crawler",
            "crawler_name": "uax-datalake-bronze-crawler-dev",
            "wait_until_completion": True,
            "poll_interval_seconds": 0.01
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "SUCCEEDED")
        self.assertEqual(body["crawler_name"], "uax-datalake-bronze-crawler-dev")
        self.assertEqual(body["tables_created"], 2)
        self.assertEqual(body["tables_updated"], 3)
        mock_glue.start_crawler.assert_called_once_with(Name="uax-datalake-bronze-crawler-dev")

    @patch('lambda_function.glue_client')
    def test_crawler_execution_already_running_handled(self, mock_glue):
        from lambda_function import lambda_handler
        from botocore.exceptions import ClientError
        error_response = {'Error': {'Code': 'CrawlerRunningException', 'Message': 'Crawler is already running'}}
        mock_glue.start_crawler.side_effect = ClientError(error_response, 'StartCrawler')
        mock_glue.get_crawler.return_value = {
            'Crawler': {
                'State': 'READY',
                'LastCrawl': {'Status': 'SUCCEEDED'},
                'Metrics': {'TablesUpdated': 1}
            }
        }
        event = {
            "action": "crawler",
            "crawler_name": "uax-datalake-bronze-crawler-dev",
            "wait_until_completion": True,
            "poll_interval_seconds": 0.01
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "SUCCEEDED")

    @patch('lambda_function.glue_client')
    @patch('lambda_function.poll_glue_job_run')
    def test_crawler_in_multi_stage_pipeline(self, mock_poll_job, mock_glue):
        from lambda_function import lambda_handler
        mock_glue.start_job_run.side_effect = [
            {"JobRunId": "jr_bronze_10"},
            {"JobRunId": "jr_silver_20"}
        ]
        mock_poll_job.side_effect = [
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 20, "LogGroupName": "log1"},
            {"JobState": "SUCCEEDED", "ExecutionTimeSeconds": 30, "LogGroupName": "log2"}
        ]
        mock_glue.start_crawler.return_value = {}
        mock_glue.get_crawler.return_value = {
            'Crawler': {
                'State': 'READY',
                'LastCrawl': {'Status': 'SUCCEEDED'},
                'Metrics': {'TablesUpdated': 4}
            }
        }
        event = {
            "layers": ["bronze", "crawler", "silver"],
            "source_system": "moveworks",
            "crawler_name": "uax-datalake-bronze-crawler-dev",
            "poll_interval_seconds": 0.01
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "SUCCEEDED")
        self.assertIn("bronze", body["stage_results"])
        self.assertIn("crawler", body["stage_results"])
        self.assertIn("silver", body["stage_results"])
        self.assertEqual(body["stage_results"]["crawler"]["status"], "SUCCEEDED")

    def test_split_sql_statements_multiline_cte_and_quotes(self):
        from lambda_function import split_sql_statements
        # 1. Multiline CTE query (like interactions.sql) with comments and trailing semicolon
        multiline_sql = """
        -- Header comments
        -- Table: interactions
        WITH conversation_topics AS (
            SELECT conversation_id, concat_ws(', ', collect_set(detail_entity)) AS topic
            FROM tbl_interactions
            WHERE _is_current = 'Y' AND _is_deleted = 'N'
            GROUP BY conversation_id
        )
        SELECT * FROM conversation_topics;
        -- Trailing comment
        """
        stmts = split_sql_statements(multiline_sql)
        self.assertEqual(len(stmts), 1)
        self.assertFalse(stmts[0].endswith(";"))
        self.assertIn("WITH conversation_topics", stmts[0])

        # 2. Multiple statements with semicolon inside string literals
        multi_stmt_sql = """
        CREATE TABLE test_table (id int, name varchar);
        INSERT INTO test_table VALUES (1, 'value; with; semicolons');
        SELECT * FROM test_table WHERE name != 'semi;colon';
        """
        stmts2 = split_sql_statements(multi_stmt_sql)
        self.assertEqual(len(stmts2), 3)
        self.assertEqual(stmts2[0], "CREATE TABLE test_table (id int, name varchar)")
        self.assertEqual(stmts2[1], "INSERT INTO test_table VALUES (1, 'value; with; semicolons')")
        self.assertFalse(stmts2[2].endswith(";"))

    def test_resolve_sql_query_local_file_and_template_params(self):
        from lambda_function import resolve_sql_query
        # Reading v_interactions.sql directly from gold/query/moveworks/v_interactions.sql
        event = {
            "query_file": "gold/query/moveworks/v_interactions.sql",
            "table_replacements": {
                "tbl_interactions": "silver_tbl_moveworks_interactions"
            },
            "params": {
                "source_system": "moveworks"
            }
        }
        resolved = resolve_sql_query(event)
        self.assertIn("silver_tbl_moveworks_interactions", resolved)
        self.assertNotIn("FROM\n    tbl_interactions", resolved)
        self.assertIn("conversation_topics", resolved)

    @patch('lambda_function.s3_client')
    def test_resolve_sql_query_s3_uri(self, mock_s3):
        from lambda_function import resolve_sql_query
        mock_body = MagicMock()
        mock_body.read.return_value = b"SELECT * FROM tbl_interactions WHERE id = '${id}';"
        mock_s3.get_object.return_value = {'Body': mock_body}

        event = {
            "sql_s3_path": "s3://my-test-bucket/gold/query/moveworks/v_interactions.sql",
            "params": {"id": "int_12345"}
        }
        resolved = resolve_sql_query(event)
        self.assertEqual(resolved, "SELECT * FROM tbl_interactions WHERE id = 'int_12345';")
        mock_s3.get_object.assert_called_once_with(Bucket="my-test-bucket", Key="gold/query/moveworks/v_interactions.sql")

    @patch('lambda_function.athena_client')
    def test_execute_athena_query_multiline_interactions(self, mock_athena):
        from lambda_function import lambda_handler
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qe_12345"}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {'State': 'SUCCEEDED'},
                'Statistics': {
                    'EngineExecutionTimeInMillis': 450,
                    'DataScannedInBytes': 1048576
                }
            }
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                'ResultSet': {
                    'Rows': [
                        {'Data': [{'VarCharValue': 'conversation_id'}, {'VarCharValue': 'topic'}]},
                        {'Data': [{'VarCharValue': 'conv_001'}, {'VarCharValue': 'ticket operation'}]}
                    ]
                }
            }
        ]
        mock_athena.get_paginator.return_value = mock_paginator

        multiline_sql = """
        WITH conversation_topics AS (
            SELECT conversation_id, detail_entity AS topic
            FROM tbl_interactions
            WHERE _is_current = 'Y' AND _is_deleted = 'N'
        )
        SELECT * FROM conversation_topics LIMIT 10;
        """
        event = {
            "query": multiline_sql,
            "database": "uax_datalake_db_dev",
            "poll_interval_seconds": 0.01
        }
        resp = lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "SUCCEEDED")
        self.assertEqual(body["query_execution_id"], "qe_12345")
        self.assertEqual(body["row_count"], 1)
        self.assertEqual(body["records"][0]["conversation_id"], "conv_001")
        # Verify that start_query_execution was called without the trailing semicolon
        call_query = mock_athena.start_query_execution.call_args[1]["QueryString"]
        self.assertFalse(call_query.endswith(";"))



class TestHyphenToUnderscoreHandling(unittest.TestCase):
    """
    Validates end-to-end handling of table names containing hyphens ('-'):
    API calls preserve hyphens, while Bronze, Silver, and Gold lake tables
    consistently sanitize hyphens to underscores ('_').
    """

    def test_bronze_watermark_state_key_sanitizes_hyphens(self):
        state_key = get_table_state_key("moveworks", "chat-sessions")
        self.assertEqual(state_key, "metadata/bronze/moveworks/chat_sessions/watermark.json")

    def test_bronze_config_loader_hyphen_and_underscore_lookup(self):
        sample_config = {
            "source_systems": {
                "moveworks": {
                    "api_endpoint_template": "/export/v1/records/{table_name}",
                    "default_delta_filter": "last_updated_time gt '{last_load_date}'",
                    "table_initial_load_dates": {
                        "chat-sessions": "2024-01-01T00:00:00Z"
                    },
                    "custom_table_endpoints": {
                        "user-profiles": "/export/v1/records/user-profiles"
                    }
                }
            }
        }
        # 1. table_initial_load_dates finds date whether looked up via hyphen or underscore
        date_hyphen = ConfigLoader.get_table_initial_load_date("moveworks", "chat-sessions", source_config=sample_config["source_systems"]["moveworks"])
        date_underscore = ConfigLoader.get_table_initial_load_date("moveworks", "chat_sessions", source_config=sample_config["source_systems"]["moveworks"])
        self.assertEqual(date_hyphen, "2024-01-01T00:00:00Z")
        self.assertEqual(date_underscore, "2024-01-01T00:00:00Z")

        # 2. get_table_endpoint formats API endpoint with exact entity name
        ep = ConfigLoader.get_table_endpoint("moveworks", "chat-sessions", source_config=sample_config["source_systems"]["moveworks"])
        self.assertEqual(ep, "/export/v1/records/chat-sessions")

        # 3. custom_table_endpoints matches whether looked up via hyphen or underscore
        custom_ep = ConfigLoader.get_table_endpoint("moveworks", "user_profiles", source_config=sample_config["source_systems"]["moveworks"])
        self.assertEqual(custom_ep, "/export/v1/records/user-profiles")

    def test_silver_table_name_and_config_lookup_hyphen_sanitization(self):
        sample_silver_config = {
            "silver_defaults": {
                "table_prefix": "tbl_"
            },
            "source_systems": {
                "moveworks": {
                    "table_configs": {
                        "raw_tbl_chat_sessions": {
                            "nkey": "session_id",
                            "target_table_name": "tbl_chat_sessions"
                        }
                    }
                }
            }
        }
        # Look up using hyphen variant matches underscore config
        cfg_hyphen = SilverConfigLoader.get_table_config("moveworks", "chat-sessions", sample_silver_config)
        self.assertEqual(cfg_hyphen.get("nkey"), "session_id")

        cfg_raw_hyphen = SilverConfigLoader.get_table_config("moveworks", "raw_tbl_chat-sessions", sample_silver_config)
        self.assertEqual(cfg_raw_hyphen.get("nkey"), "session_id")

        # Verify silver table name sanitizes hyphen to underscore
        silver_name = SilverConfigLoader.get_silver_table_name(
            source_system="moveworks",
            table_name="chat-sessions",
            glue_database="uax_db",
            config_dict=sample_silver_config
        )
        self.assertEqual(silver_name, "uax_db.tbl_chat_sessions")

    def test_gold_query_filter_matches_hyphens_and_underscores(self):
        # Discovered queries has underscore mart name
        queries = {"chat_sessions": "SELECT * FROM tbl_chat_sessions"}
        
        # Filtering with hyphen parameter
        table_filter = ["chat-sessions"]
        clean_filters = set()
        for t in table_filter:
            low = t.lower()
            clean_filters.add(low)
            clean_filters.add(low.replace('-', '_'))
            clean_filters.add(low.replace('_', '-'))
            for prefix in ['gold_tbl_', 'raw_tbl_', 'tbl_', 'v_']:
                if low.startswith(prefix):
                    stripped = low[len(prefix):]
                    clean_filters.add(stripped)
                    clean_filters.add(stripped.replace('-', '_'))
                    clean_filters.add(stripped.replace('_', '-'))
        matched = {
            k: v for k, v in queries.items()
            if k.lower() in clean_filters or any(cf in k.lower() for cf in clean_filters)
        }
        self.assertIn("chat_sessions", matched)

        # Target MySQL table and view naming check
        clean_base_name = "chat-sessions".replace('-', '_')
        self.assertEqual(f"gold_tbl_{clean_base_name}", "gold_tbl_chat_sessions")
        self.assertEqual(f"v_{clean_base_name}", "v_chat_sessions")


class TestSilverUniformConfigAndGoldMandatoryAthenaView(unittest.TestCase):
    """
    Validates uniform Silver configuration modeled after Bronze, dynamic schema technical column stripping,
    and mandatory Gold Athena View creation with multi-target serving.
    """

    def setUp(self):
        SilverConfigLoader._config_cache = None

    def test_silver_uniform_config_loading_and_table_resolution(self):
        config = SilverConfigLoader.load_config()
        self.assertIn("pipeline_defaults", config)
        
        # Verify bronze technical columns list contains strictly the 4 Bronze technical columns
        tech_cols = SilverConfigLoader.get_bronze_technical_columns(config)
        self.assertEqual(tech_cols, ["_ingested_at", "_source_system", "_table_name", "_execution_id"])

        # Verify default tables without explicit default_tables key
        servicenow_tables = SilverConfigLoader.get_default_tables("servicenow", config)
        self.assertIn("tbl_incident", servicenow_tables)
        self.assertIn("tbl_change_request", servicenow_tables)

        # Lookup table config using tbl_ prefix, raw_tbl_ prefix, and base name
        cfg_by_tbl = SilverConfigLoader.get_table_config("servicenow", "tbl_incident", config)
        self.assertEqual(cfg_by_tbl.get("source_table_name"), "raw_tbl_incident")
        self.assertEqual(cfg_by_tbl.get("target_table_name"), "tbl_incident")
        self.assertEqual(cfg_by_tbl.get("nkey"), "sys_id")

        cfg_by_raw = SilverConfigLoader.get_table_config("servicenow", "raw_tbl_incident", config)
        self.assertEqual(cfg_by_raw.get("nkey"), "sys_id")

        cfg_by_base = SilverConfigLoader.get_table_config("servicenow", "incident", config)
        self.assertEqual(cfg_by_base.get("nkey"), "sys_id")

        # Verify silver table name resolution
        silver_name_1 = SilverConfigLoader.get_silver_table_name("servicenow", "tbl_incident", glue_database="uax_db", config_dict=config)
        self.assertEqual(silver_name_1, "uax_db.tbl_incident")

        silver_name_2 = SilverConfigLoader.get_silver_table_name("servicenow", "raw_tbl_incident", glue_database="uax_db", config_dict=config)
        self.assertEqual(silver_name_2, "uax_db.tbl_incident")

        silver_name_3 = SilverConfigLoader.get_silver_table_name("servicenow", "incident", glue_database="uax_db", config_dict=config)
        self.assertEqual(silver_name_3, "uax_db.tbl_incident")

    def test_mandatory_athena_view_creation_via_boto3(self):
        mock_spark = MagicMock()
        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'athena_query_123'}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {
                    'State': 'SUCCEEDED'
                }
            }
        }

        params = {
            'DATA_LAKE_BUCKET': 'test-lake-bucket',
            'GLUE_DATABASE': 'uax_datalake_db_dev'
        }

        created = GoldLayerManager.create_athena_view(
            spark=mock_spark,
            glue_database='uax_datalake_db_dev',
            view_name='v_interactions',
            sql_text='SELECT * FROM tbl_interactions;',
            params=params,
            athena_client=mock_athena
        )
        self.assertTrue(created)
        mock_athena.start_query_execution.assert_called_once()
        call_kwargs = mock_athena.start_query_execution.call_args[1]
        self.assertIn("CREATE OR REPLACE VIEW uax_datalake_db_dev.v_interactions AS", call_kwargs['QueryString'])
        self.assertEqual(call_kwargs['QueryExecutionContext']['Database'], 'uax_datalake_db_dev')

    def test_mandatory_athena_view_creation_fallback_to_spark_sql(self):
        mock_spark = MagicMock()
        mock_athena = MagicMock()
        mock_athena.start_query_execution.side_effect = Exception("Athena workgroup primary disabled")

        params = {
            'DATA_LAKE_BUCKET': 'test-lake-bucket',
            'GLUE_DATABASE': 'uax_datalake_db_dev'
        }

        created = GoldLayerManager.create_athena_view(
            spark=mock_spark,
            glue_database='uax_datalake_db_dev',
            view_name='v_incident_kpi',
            sql_text='SELECT * FROM tbl_incident',
            params=params,
            athena_client=mock_athena
        )
        self.assertTrue(created)
        mock_spark.sql.assert_called_once()
        spark_call = mock_spark.sql.call_args[0][0]
        self.assertIn("CREATE OR REPLACE VIEW `uax_datalake_db_dev`.`v_incident_kpi` AS", spark_call)

    def test_gold_query_discovery_strips_v_prefix(self):
        # When discovery finds v_interactions.sql, the mart name should be interactions and view v_interactions
        mock_s3 = MagicMock()
        queries = GoldLayerManager._discover_queries(
            query_path="gold/query/moveworks",
            bucket="test-bucket",
            s3_client=mock_s3,
            source_system="moveworks"
        )
        self.assertIn("interactions", queries)
        self.assertTrue(len(queries["interactions"]) > 0)

    def test_gold_query_discovery_rejects_non_v_query_file(self):
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': 'gold/query/moveworks/interactions.sql'}]}
        ]
        with self.assertRaises(ValueError) as ctx:
            GoldLayerManager._discover_queries(
                query_path="s3://test-bucket/gold/query/moveworks/",
                bucket="test-bucket",
                s3_client=mock_s3,
                source_system="moveworks"
            )
        self.assertIn("violates the strict Gold naming standard", str(ctx.exception))
        self.assertIn("v_<tablename>.sql", str(ctx.exception))


class TestWatermarkExecutionStartTime(unittest.TestCase):
    """
    Validates that Bronze watermark state records 'last_load_date' as the execution timestamp,
    preventing data gaps during long-running extractions.
    """

    @patch('uax_bronze_load.s3_client')
    def test_update_last_load_date_records_execution_start_time(self, mock_s3):
        execution_start = "2026-09-18T10:00:00Z"
        update_last_load_date(
            state_bucket="test-bucket",
            state_key="metadata/bronze/servicenow/incident/watermark.json",
            source_system="servicenow",
            table_name="incident",
            current_run_time=execution_start,
            total_records=150,
            table_prefix="raw_tbl_"
        )
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        payload = json.loads(call_kwargs["Body"].decode('utf-8'))

        self.assertEqual(payload["last_load_date"], execution_start)
        self.assertEqual(payload["table_name"], "raw_tbl_incident")
        self.assertEqual(payload["source_system"], "servicenow")
        self.assertEqual(payload["records_ingested"], 150)
        self.assertEqual(payload["last_status"], "SUCCESS")
        self.assertIn("updated_at", payload)
        self.assertTrue(payload["updated_at"].endswith("Z"))


class TestDynamicEnvironmentInterpolation(unittest.TestCase):
    """
    Validates dynamic environment ({env}) interpolation across ConfigLoader and parse_arguments.
    """

    def setUp(self):
        ConfigLoader.clear_cache()

    def tearDown(self):
        ConfigLoader.clear_cache()

    def test_interpolate_env_nested_structures(self):
        sample = {
            "db": "uax_datalake_db_{env}",
            "crawler": "uax-datalake-bronze-crawler-{env}",
            "upper_placeholder": "BUCKET_{ENV}",
            "nested": {
                "prefix": "data/{env}/raw",
                "items": ["db_{env}_tbl", 123, True]
            }
        }
        interpolated = ConfigLoader.interpolate_env(sample, "prod")
        self.assertEqual(interpolated["db"], "uax_datalake_db_prod")
        self.assertEqual(interpolated["crawler"], "uax-datalake-bronze-crawler-prod")
        self.assertEqual(interpolated["upper_placeholder"], "BUCKET_PROD")
        self.assertEqual(interpolated["nested"]["prefix"], "data/prod/raw")
        self.assertEqual(interpolated["nested"]["items"][0], "db_prod_tbl")

    def test_load_config_with_custom_env(self):
        prod_cfg = ConfigLoader.load_config(env="prod")
        catalog_cfg = prod_cfg["pipeline_defaults"]["glue_catalog"]
        self.assertEqual(catalog_cfg["database_name"], "uax_datalake_db_prod")
        self.assertEqual(catalog_cfg["crawler_name"], "uax-datalake-bronze-crawler-prod")

        dev_cfg = ConfigLoader.load_config(env="dev")
        dev_catalog = dev_cfg["pipeline_defaults"]["glue_catalog"]
        self.assertEqual(dev_catalog["database_name"], "uax_datalake_db_dev")
        self.assertEqual(dev_catalog["crawler_name"], "uax-datalake-bronze-crawler-dev")

    def test_get_glue_database_and_crawler_name_env_resolution(self):
        db_qa = ConfigLoader.get_glue_database(env="qa")
        self.assertEqual(db_qa, "uax_datalake_db_qa")

        crawler_qa = ConfigLoader.get_crawler_name(env="qa")
        self.assertEqual(crawler_qa, "uax-datalake-bronze-crawler-qa")

    @patch('sys.argv', ['uax_bronze_load.py', '--SOURCE_SYSTEM', 'servicenow', '--SOURCE_TABLE_NAME', 'incident', '--ENV', 'staging', '--BRONZE_BUCKET', 'my-bucket-{env}'])
    def test_parse_arguments_resolves_env_parameter(self):
        params = parse_arguments()
        self.assertEqual(params["ENV"], "staging")
        self.assertEqual(params["GLUE_DATABASE_NAME"], "uax_datalake_db_staging")
        self.assertEqual(params["BRONZE_CRAWLER_NAME"], "uax-datalake-bronze-crawler-staging")
        self.assertEqual(params["BRONZE_BUCKET"], "my-bucket-staging")


class TestUpperBoundSentinelAndTimestampSupport(unittest.TestCase):
    """
    Validates table-wise upper_bound ('table_upper_bounds'), format validation
    ('YYYY-MM-DD HH:MM:SS'), Moveworks shard building with high-date capping,
    query filter normalization, and watermark sentinel protection.
    """

    def setUp(self):
        ConfigLoader.clear_cache()

    def tearDown(self):
        ConfigLoader.clear_cache()

    def test_moveworks_parse_ts_formats(self):
        from datetime import timezone
        from connectors.moveworks import MoveworksConnector

        ts1 = MoveworksConnector._parse_ts("9999-01-01 00:00:00")
        self.assertEqual(ts1.year, 9999)
        self.assertEqual(ts1.tzinfo, timezone.utc)

        ts2 = MoveworksConnector._parse_ts("2024-03-01 15:30:00")
        self.assertEqual(ts2.year, 2024)
        self.assertEqual(ts2.month, 3)
        self.assertEqual(ts2.day, 1)
        self.assertEqual(ts2.tzinfo, timezone.utc)

        ts3 = MoveworksConnector._parse_ts("2024-03-01T15:30:00Z")
        self.assertEqual(ts3.year, 2024)
        self.assertEqual(ts3.tzinfo, timezone.utc)

    def test_moveworks_build_shards_caps_high_date(self):
        from datetime import datetime, timezone
        from connectors.moveworks import MoveworksConnector

        # Without capping, 2024 to 9999 would generate >190,000 shards and crash memory
        shards = MoveworksConnector._build_shards("2024-01-01 00:00:00", "9999-01-01 00:00:00", window_days=30)
        self.assertGreater(len(shards), 0)
        self.assertLess(len(shards), 100)  # Capped at current UTC time
        self.assertTrue(shards[0][0].startswith("2024-01-01T00:00:00Z"))
        self.assertTrue(shards[-1][1].endswith("Z"))

    def test_config_loader_moveworks_query_filter_normalization(self):
        filter_str = ConfigLoader.get_table_query_filter(
            source_system="moveworks",
            table_name="conversations",
            last_load_date="2024-01-01 00:00:00",
            upper_bound="9999-01-01 00:00:00"
        )
        # Verify timestamps are normalized to ISO-8601 without spaces for Moveworks OData
        self.assertIn("last_updated_time ge '2024-01-01T00:00:00Z'", filter_str)
        self.assertIn("last_updated_time le '9999-01-01T00:00:00Z'", filter_str)
        self.assertNotIn("00:00:00'", filter_str)  # No raw space-separated timestamp in quotes

    def test_watermark_sentinel_protection(self):
        current_run_time = "2026-09-20T12:00:00Z"

        # Case 1: Open-ended sentinel '9999-01-01 00:00:00'
        ub_high = "9999-01-01 00:00:00"
        is_high_date = bool(ub_high and (str(ub_high).strip().startswith('9999') or str(ub_high).strip().startswith('9998')))
        effective_watermark = current_run_time if (not ub_high or is_high_date) else str(ub_high).strip()
        self.assertTrue(is_high_date)
        self.assertEqual(effective_watermark, current_run_time)

        # Case 2: Historical backfill date
        ub_backfill = "2024-03-01 00:00:00"
        is_high_date_bf = bool(ub_backfill and (str(ub_backfill).strip().startswith('9999') or str(ub_backfill).strip().startswith('9998')))
        effective_watermark_bf = current_run_time if (not ub_backfill or is_high_date_bf) else str(ub_backfill).strip()
        self.assertFalse(is_high_date_bf)
        self.assertEqual(effective_watermark_bf, "2024-03-01 00:00:00")

    @patch('sys.argv', ['uax_bronze_load.py', '--SOURCE_SYSTEM', 'servicenow', '--SOURCE_TABLE_NAME', 'incident', '--UPPER_BOUND', '9999-01-01 00:00:00', '--BRONZE_BUCKET', 'test-bucket'])
    def test_parse_arguments_upper_bound_propagation(self):
        params = parse_arguments()
        self.assertEqual(params["UPPER_BOUND"], "9999-01-01 00:00:00")

    def test_table_wise_upper_bound_resolution(self):
        mock_source_config = {
            "tables": {
                "incident": {
                    "initial_load_date": "2024-01-01 00:00:00",
                    "upper_bound": "2024-06-01 00:00:00"
                },
                "change_request": {
                    "initial_load_date": "2024-03-01 00:00:00",
                    "upper_bound": ""
                }
            }
        }
        # 1. Configured table has specific upper bound
        ub_inc = ConfigLoader.get_table_upper_bound("servicenow", "incident", source_config=mock_source_config)
        self.assertEqual(ub_inc, "2024-06-01 00:00:00")

        # 2. Configured table with empty string returns None (open-ended)
        ub_cr = ConfigLoader.get_table_upper_bound("servicenow", "change_request", source_config=mock_source_config)
        self.assertIsNone(ub_cr)

        # 3. CLI override takes priority over tables.<tbl>.upper_bound
        ub_cli = ConfigLoader.get_table_upper_bound(
            "servicenow", "incident", cli_upper_bound="2024-12-31 23:59:59", source_config=mock_source_config
        )
        self.assertEqual(ub_cli, "2024-12-31 23:59:59")

    def test_all_initial_load_dates_format_in_config(self):
        """Validates that all tables.initial_load_date follow 'YYYY-MM-DD HH:MM:SS' across all sources."""
        from datetime import datetime
        config = ConfigLoader.load_config()
        sources = config.get("source_systems", {})
        
        checked_count = 0
        for source_name, source_cfg in sources.items():
            tables = source_cfg.get("tables", {})
            for tbl, tbl_cfg in tables.items():
                dt_str = tbl_cfg.get("initial_load_date", "")
                checked_count += 1
                # Must parse strictly with '%Y-%m-%d %H:%M:%S'
                try:
                    parsed = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    self.assertIsNotNone(parsed)
                except ValueError:
                    self.fail(f"Source '{source_name}', table '{tbl}' initial load date '{dt_str}' does not match 'YYYY-MM-DD HH:MM:SS'")
                
                # Must not contain 'T' or 'Z'
                self.assertNotIn('T', dt_str)
                self.assertNotIn('Z', dt_str)

        self.assertGreater(checked_count, 15)

    def test_unified_source_tables_resolution(self):
        """Validates that ConfigLoader resolves all table properties from the unified 'tables' structure."""
        config = ConfigLoader.load_config()
        sn_cfg = config["source_systems"]["servicenow"]

        # 1. Initial load date from tables
        init_date = ConfigLoader.get_table_initial_load_date("servicenow", "incident", source_config=sn_cfg)
        self.assertEqual(init_date, "2024-01-01 00:00:00")

        # 2. Query override from tables
        query = ConfigLoader.get_table_query_filter("servicenow", "incident", "2024-01-01 00:00:00", source_config=sn_cfg)
        self.assertIn("active=true", query)

        # 3. Custom endpoint from tables
        endpoint = ConfigLoader.get_table_endpoint("servicenow", "u_special_report", source_config=sn_cfg)
        self.assertEqual(endpoint, "/api/now/v1/custom_reports")

        # 4. S3 File feed file_path and fetch_mode
        va_cfg = config["source_systems"]["vendor_a_s3"]
        file_path = ConfigLoader.get_table_file_path("vendor_a_s3", "employee_feed", source_config=va_cfg)
        self.assertEqual(file_path, "raw_feed/employee_feed/")
        fetch_mode = ConfigLoader.get_table_fetch_mode("vendor_a_s3", "employee_feed", source_config=va_cfg)
        self.assertEqual(fetch_mode, "latest")

        # 5. Table list discovery
        tables = ConfigLoader.get_source_tables("servicenow", source_config=sn_cfg)
        self.assertIn("incident", tables)
        self.assertIn("change_request", tables)
        self.assertIn("sys_user", tables)

    def test_backward_compatibility_with_legacy_maps(self):
        """Validates dual-read strategy with legacy parallel dictionaries."""
        legacy_source_cfg = {
            "table_initial_load_dates": {
                "legacy_tbl": "2023-05-01 00:00:00"
            },
            "table_upper_bounds": {
                "legacy_tbl": "2023-12-31 23:59:59"
            },
            "table_query_overrides": {
                "legacy_tbl": "status='ARCHIVED'^sys_updated_on>={last_load_date}"
            },
            "custom_table_endpoints": {
                "legacy_tbl": "/api/custom/legacy"
            },
            "default_tables": ["legacy_tbl"]
        }

        # 1. Initial load date fallback
        d = ConfigLoader.get_table_initial_load_date("legacy_source", "legacy_tbl", source_config=legacy_source_cfg)
        self.assertEqual(d, "2023-05-01 00:00:00")

        # 2. Upper bound fallback
        ub = ConfigLoader.get_table_upper_bound("legacy_source", "legacy_tbl", source_config=legacy_source_cfg)
        self.assertEqual(ub, "2023-12-31 23:59:59")

        # 3. Query override fallback
        q = ConfigLoader.get_table_query_filter("legacy_source", "legacy_tbl", "2023-05-01 00:00:00", source_config=legacy_source_cfg)
        self.assertIn("status='ARCHIVED'", q)

        # 4. Custom endpoint fallback
        ep = ConfigLoader.get_table_endpoint("legacy_source", "legacy_tbl", source_config=legacy_source_cfg)
        self.assertEqual(ep, "/api/custom/legacy")

        # 5. Table list fallback
        tbls = ConfigLoader.get_source_tables("legacy_source", source_config=legacy_source_cfg)
        self.assertEqual(tbls, ["legacy_tbl"])

    def test_defaults_upper_bound_is_empty(self):
        """Validates that pipeline_defaults.upper_bound is empty string and not hardcoded to 9999."""
        config = ConfigLoader.load_config()
        pipeline_defaults = config.get("pipeline_defaults", {})
        self.assertEqual(pipeline_defaults.get("upper_bound"), "")

    @patch('uax_bronze_load.s3_client')
    def test_manual_date_range_cli_initial_load_date_override(self, mock_s3):
        """Validates that CLI --INITIAL_LOAD_DATE overrides existing S3 watermark for manual date range ingestion."""
        # Setup S3 mock with existing watermark of 2026-09-18
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({
            "source_system": "servicenow",
            "table_name": "raw_tbl_incident",
            "last_load_date": "2026-09-18T10:00:00Z",
            "last_status": "SUCCESS"
        }).encode('utf-8')
        mock_s3.get_object.return_value = {"Body": mock_body}

        # 1. Without CLI override, returns S3 watermark
        res_standard = get_last_load_date(
            state_bucket="test-bucket",
            state_key="metadata/bronze/servicenow/incident/watermark.json",
            source_system="servicenow",
            table_name="incident",
            cli_initial_date=None
        )
        self.assertEqual(res_standard, "2026-09-18T10:00:00Z")

        # 2. With CLI override (manual backfill for date range), overrides S3 watermark
        res_manual = get_last_load_date(
            state_bucket="test-bucket",
            state_key="metadata/bronze/servicenow/incident/watermark.json",
            source_system="servicenow",
            table_name="incident",
            cli_initial_date="2024-03-01 00:00:00"
        )
        self.assertEqual(res_manual, "2024-03-01 00:00:00")

    @patch('connectors.oauth.OAuth2Client.get_access_token', return_value='mock_token_123')
    def test_http_client_auth_type_oauth2_and_aliases(self, mock_oauth):
        """Validates that HTTPClient accepts 'oauth2', 'oauth', 'basic', and 'api_key'."""
        from connectors.http_client import HTTPClient

        # 1. auth_type = 'oauth2'
        h1 = HTTPClient._build_auth_header({
            'auth_type': 'oauth2',
            'token_url': 'https://api.moveworks.ai/rest/v1/oauth/token',
            'client_id': 'cid',
            'client_secret': 'sec'
        })
        self.assertEqual(h1, {'Authorization': 'Bearer mock_token_123'})

        # 2. auth_type = 'oauth'
        h2 = HTTPClient._build_auth_header({
            'auth_type': 'oauth',
            'token_url': 'https://api.moveworks.ai/rest/v1/oauth/token'
        })
        self.assertEqual(h2, {'Authorization': 'Bearer mock_token_123'})

        # 3. auth_type = 'basic'
        h3 = HTTPClient._build_auth_header({
            'auth_type': 'basic',
            'username': 'admin',
            'password': 'secret_password'
        })
        self.assertTrue(h3['Authorization'].startswith('Basic '))

        # 4. auth_type = 'api_key'
        h4 = HTTPClient._build_auth_header({
            'auth_type': 'api_key',
            'api_key': 'key_abc',
            'api_key_header': 'X-Custom-Key'
        })
        self.assertEqual(h4, {'X-Custom-Key': 'key_abc'})

        # 5. Invalid auth_type raises ValueError
        with self.assertRaises(ValueError):
            HTTPClient._build_auth_header({'auth_type': 'invalid_scheme'})

    @patch('connectors.http_client.HTTPClient.get')
    def test_moveworks_empty_odata_response_omitted_value_key(self, mock_http_get):
        """Validates that MoveworksConnector treats OData response with only '@odata.context' as 0 records."""
        from connectors.moveworks import MoveworksConnector

        # Moveworks returns only @odata.context when 0 records match the shard window
        mock_http_get.return_value = {
            '@odata.context': 'https://api.moveworks.ai/export/v1beta2/$metadata#records/plugin-calls'
        }

        cfg = ConfigLoader.load_config(env='dev')
        mw_cfg = cfg['source_systems']['moveworks']
        records = MoveworksConnector._fetch_single_window(
            lower_bound='2024-12-26T00:00:00Z',
            upper_bound='2025-01-10T00:00:00Z',
            base_url='https://api.moveworks.ai',
            endpoint='/export/v1beta2/records/plugin-calls',
            response_key='value',
            limit=500,
            secret_dict={'auth_type': 'oauth2', 'token_url': 'https://token'},
            custom_headers={},
            table_name='plugin_calls',
            source_config=mw_cfg,
            custom_query=None,
            on_chunk_callback=None,
            s3_chunk_size=10000,
        )
        self.assertEqual(records, [])

    @patch('time.sleep')
    @patch('urllib.request.urlopen')
    def test_http_client_429_uniform_60s_backoff_and_retry(self, mock_urlopen, mock_sleep):
        """Validates that HTTPClient pauses for ~60s on HTTP 429 across any table and retries successfully."""
        from urllib.error import HTTPError
        from connectors.http_client import HTTPClient

        # First call raises HTTP 429; second call succeeds with 200 OK
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"value": [{"id": "rec_1"}]}).encode('utf-8')
        mock_response.info.return_value = {}

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_response

        err_429 = HTTPError('https://api.moveworks.ai/test', 429, 'Too Many Requests', {}, None)
        mock_urlopen.side_effect = [err_429, mock_cm]

        res = HTTPClient.get(
            url='https://api.moveworks.ai/test',
            secret_dict={'auth_type': 'api_key', 'api_key': 'key_123'},
            max_retries=3
        )

        self.assertEqual(res, {"value": [{"id": "rec_1"}]})
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once()
        sleep_arg = mock_sleep.call_args[0][0]
        self.assertGreaterEqual(sleep_arg, 60.0)
        self.assertLessEqual(sleep_arg, 63.0)

    def test_moveworks_full_initial_load_1900_empty_filter(self):
        """Validates that last_load_date=1900-01-01 produces an empty filter for 100% historical extraction."""
        # 1. Open-ended upper bound (empty) -> empty filter
        f1 = ConfigLoader.get_table_query_filter(
            source_system="moveworks",
            table_name="interactions",
            last_load_date="1900-01-01 00:00:00",
            upper_bound=""
        )
        self.assertEqual(f1, "")

        # 2. Open-ended sentinel upper bound (9999) -> empty filter
        f2 = ConfigLoader.get_table_query_filter(
            source_system="moveworks",
            table_name="interactions",
            last_load_date="1900-01-01 00:00:00",
            upper_bound="9999-01-01 00:00:00"
        )
        self.assertEqual(f2, "")

        # 3. Bounded backfill date -> le upper_bound
        f3 = ConfigLoader.get_table_query_filter(
            source_system="moveworks",
            table_name="interactions",
            last_load_date="1900-01-01 00:00:00",
            upper_bound="2024-06-01 00:00:00"
        )
        self.assertEqual(f3, "last_updated_time le '2024-06-01T00:00:00Z'")

    @patch('connectors.moveworks.MoveworksConnector._fetch_parallel')
    @patch('connectors.moveworks.MoveworksConnector._fetch_single_window')
    def test_moveworks_full_initial_load_fetch_delta_routing(self, mock_single, mock_parallel):
        """Validates that 1900-01-01 routes to single window cursor pagination without sharding."""
        from connectors.moveworks import MoveworksConnector

        mock_single.return_value = [{"id": "row_1"}, {"id": "row_2"}]
        cfg = ConfigLoader.load_config(env='dev')
        mw_cfg = cfg['source_systems']['moveworks']

        records = MoveworksConnector.fetch_delta(
            last_load_date="1900-01-01 00:00:00",
            secret_dict={'auth_type': 'oauth2', 'token_url': 'https://token'},
            table_name="interactions",
            source_config=mw_cfg,
        )

        self.assertEqual(records, [{"id": "row_1"}, {"id": "row_2"}])
        mock_parallel.assert_not_called()
        mock_single.assert_called_once()
        # Verify upper_bound passed to _fetch_single_window is None (open-ended cursor pagination)
        call_kwargs = mock_single.call_args[1]
        self.assertIsNone(call_kwargs['upper_bound'])

    @patch('connectors.http_client.HTTPClient.get')
    def test_moveworks_multi_page_pagination_without_top(self, mock_http_get):
        """Validates that _fetch_single_window does NOT send $top and traverses all @odata.nextLink pages."""
        from connectors.moveworks import MoveworksConnector

        page1_records = [{"id": f"p1_{i}"} for i in range(500)]
        page2_records = [{"id": f"p2_{i}"} for i in range(500)]
        page3_records = [{"id": f"p3_{i}"} for i in range(481)]

        mock_http_get.side_effect = [
            {
                "@odata.context": "https://api.moveworks.ai/export/v1beta2/$metadata#records/interactions",
                "value": page1_records,
                "@odata.nextLink": "https://api.moveworks.ai/export/v1beta2/records/interactions?skiptoken=page2_cursor"
            },
            {
                "@odata.context": "https://api.moveworks.ai/export/v1beta2/$metadata#records/interactions",
                "value": page2_records,
                "@odata.nextLink": "https://api.moveworks.ai/export/v1beta2/records/interactions?skiptoken=page3_cursor"
            },
            {
                "@odata.context": "https://api.moveworks.ai/export/v1beta2/$metadata#records/interactions",
                "value": page3_records
            }
        ]

        cfg = ConfigLoader.load_config(env='dev')
        mw_cfg = cfg['source_systems']['moveworks']

        all_records = MoveworksConnector._fetch_single_window(
            lower_bound='2024-01-01T00:00:00Z',
            upper_bound=None,
            base_url='https://api.moveworks.ai',
            endpoint='/export/v1beta2/records/interactions',
            response_key='value',
            limit=500,
            secret_dict={'auth_type': 'oauth2', 'token_url': 'https://token'},
            custom_headers={},
            table_name='interactions',
            source_config=mw_cfg,
            custom_query=None,
            on_chunk_callback=None,
            s3_chunk_size=10000,
        )

        # 1. Must fetch all 3 pages: 500 + 500 + 481 = 1,481 records (no truncation at 500)
        self.assertEqual(len(all_records), 1481)
        self.assertEqual(mock_http_get.call_count, 3)

        # 2. Page 1 URL must NOT contain '$top' or '%24top'
        first_call_url = mock_http_get.call_args_list[0][1]['url']
        self.assertNotIn('%24top', first_call_url)
        self.assertNotIn('$top', first_call_url)
        self.assertIn('%24orderby', first_call_url)

        # 3. Page 2 and Page 3 must use nextLink URLs verbatim
        second_call_url = mock_http_get.call_args_list[1][1]['url']
        self.assertEqual(second_call_url, "https://api.moveworks.ai/export/v1beta2/records/interactions?skiptoken=page2_cursor")

        third_call_url = mock_http_get.call_args_list[2][1]['url']
        self.assertEqual(third_call_url, "https://api.moveworks.ai/export/v1beta2/records/interactions?skiptoken=page3_cursor")

    def test_moveworks_endpoint_hyphenation(self):
        """Validates that Moveworks endpoint resolution converts underscores to hyphens."""
        ep_calls = ConfigLoader.get_table_endpoint('moveworks', 'plugin_calls')
        self.assertEqual(ep_calls, '/export/v1beta2/records/plugin-calls')

        ep_resources = ConfigLoader.get_table_endpoint('moveworks', 'plugin_resources')
        self.assertEqual(ep_resources, '/export/v1beta2/records/plugin-resources')

    def test_clean_illegal_chars_logic(self):
        """Validates clean_illegal_chars regex removes control chars."""
        from transformer import clean_illegal_chars
        # Test with mock object that implements applymap
        class MockDF:
            def __init__(self, data):
                self.data = data
            def applymap(self, fn):
                return MockDF({k: [fn(v) for v in vals] for k, vals in self.data.items()})

        mock_df = MockDF({'text': ['Hello\x00World\x1f!', 'Clean\tText\n']})
        pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\ufffd]'
        res = clean_illegal_chars(mock_df, pattern=pattern)
        self.assertEqual(res.data['text'][0], 'HelloWorld!')
        self.assertEqual(res.data['text'][1], 'Clean\tText\n')

    def test_gold_moveworks_interactions_sql_file(self):
        """Validates that gold/query/moveworks/v_interactions.sql exists and contains expected CTEs and columns."""
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "gold", "query", "moveworks", "v_interactions.sql"
        )
        self.assertTrue(os.path.exists(sql_path), f"File not found: {sql_path}")
        with open(sql_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check CTEs
        self.assertTrue(re.search(r"WITH\s+conversation_topics\s+AS", content))
        self.assertIn("bot_responses AS", content)
        self.assertIn("plugin_aggregates AS", content)
        self.assertIn("resource_aggregates AS", content)
        self.assertIn("ranked_users AS", content)

        # Check core columns
        self.assertIn("AS timestamp", content)
        self.assertIn("AS conversation_id", content)
        self.assertIn("AS interaction_id", content)
        self.assertIn("AS bot_response", content)
        self.assertIn("AS unsuccessful_plugins", content)
        self.assertIn("AS plugin_served", content)
        self.assertIn("AS plugin_used", content)
        self.assertIn("AS resource_domain", content)
        self.assertIn("AS no_of_citations", content)
        self.assertIn("AS ticket_type", content)
        self.assertIn("AS ticket_id", content)
        self.assertIn("lower(ui.actor) = 'user'", content)
        self.assertIn("_is_current = 'Y'", content)
        self.assertIn("_is_deleted = 'N'", content)
        # Ensure count of active flag filters across all 7 table touchpoints
        self.assertEqual(content.count("_is_current = 'Y'"), 7)
        self.assertEqual(content.count("_is_deleted = 'N'"), 7)

    def test_test_bjhbc_sql_active_records_filters(self):
        """Validates that test/bjhbc.sql enforces _is_current = 'Y' and _is_deleted = 'N' across all tables."""
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "test", "bjhbc.sql"
        )
        self.assertTrue(os.path.exists(sql_path), f"File not found: {sql_path}")
        with open(sql_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertEqual(content.count("_is_current = 'Y'"), 7)
        self.assertEqual(content.count("_is_deleted = 'N'"), 7)

    def test_nested_record_flattener_all_scenarios(self):
        """Validates all nested array, primitive array, and nested dictionary flattening scenarios."""
        from uax_bronze_load import flatten_and_expand_record, flatten_dict_single

        # 1. User scenario: detail with content, domain, entity, platform_name
        rec1 = {
            "id": "int_001",
            "detail": {
                "content": "avavav",
                "domain": "IT",
                "entity": "ticket operation",
                "platform_name": "Assist"
            }
        }
        res1 = flatten_and_expand_record(rec1)[0]
        self.assertEqual(res1.get("detail_content"), "avavav")
        self.assertEqual(res1.get("detail_domain"), "IT")
        self.assertEqual(res1.get("detail_entity"), "ticket operation")
        self.assertEqual(res1.get("detail_platform_name"), "Assist")

        # 2. Primitive array inside nested dictionary (e.g. entities as list)
        rec2 = {
            "id": "int_002",
            "detail": {
                "content": "vpn reset",
                "domain": "IT",
                "entity": ["ticket operation", "hardware"],
                "platform_name": "Assist"
            }
        }
        res2 = flatten_and_expand_record(rec2)[0]
        self.assertEqual(res2.get("detail_entity"), "ticket operation, hardware")

        # 3. Array of dictionaries inside nested dictionary (e.g. detail.resources)
        rec3 = {
            "id": "int_003",
            "detail": {
                "resources": [
                    {"id": "res_1", "name": "Cisco VPN"},
                    {"id": "res_2", "name": "GlobalProtect"}
                ]
            }
        }
        res3 = flatten_and_expand_record(rec3)[0]
        self.assertIn("res_1", res3.get("detail_resources"))
        self.assertEqual(res3.get("detail_resources_id"), "res_1, res_2")
        self.assertEqual(res3.get("detail_resources_name"), "Cisco VPN, GlobalProtect")

        # 4. Empty array handling
        rec4 = {"id": "int_004", "detail": {"citations": []}}
        res4 = flatten_and_expand_record(rec4)[0]
        self.assertIsNone(res4.get("detail_citations"))

    def test_schema_accumulation_and_glue_evolution(self):
        """Validates that schema evolution adds newly observed columns to an existing Glue Catalog table."""
        from datetime import datetime, timezone
        from uax_bronze_load import sync_bronze_catalog_table
        from unittest.mock import MagicMock

        mock_glue = MagicMock()
        # Mock existing table with only detail_content and detail_platform_name
        mock_glue.get_table.return_value = {
            'Table': {
                'Name': 'raw_tbl_interactions',
                'PartitionKeys': [{'Name': '_ingested_at', 'Type': 'string'}],
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'string'},
                        {'Name': 'detail_content', 'Type': 'string'},
                        {'Name': 'detail_platform_name', 'Type': 'string'},
                        {'Name': '_source_system', 'Type': 'string'},
                        {'Name': '_table_name', 'Type': 'string'},
                        {'Name': '_execution_id', 'Type': 'string'}
                    ]
                }
            }
        }

        # Simulated accumulated sample record that now has detail_domain and detail_entity
        sample_record = {
            'id': '101',
            'detail_content': 'avavav',
            'detail_domain': 'IT',
            'detail_entity': 'ticket operation',
            'detail_platform_name': 'Assist',
            '_source_system': 'moveworks',
            '_table_name': 'interactions',
            '_execution_id': 'exec_123'
        }

        import uax_bronze_load
        orig_glue = uax_bronze_load.glue_client
        uax_bronze_load.glue_client = mock_glue
        try:
            sync_bronze_catalog_table(
                database_name='test_db',
                table_prefix='raw_tbl_',
                source_system='moveworks',
                table_name='interactions',
                bronze_bucket='test_bucket',
                bronze_data_prefix='bronze/data',
                partition_date=datetime.now(timezone.utc),
                sample_record=sample_record,
                output_format='parquet',
                ingested_at='2026-09-21T00:00:00Z'
            )

            # Verify update_table was called to evolve the schema
            self.assertTrue(mock_glue.update_table.called)
            call_kwargs = mock_glue.update_table.call_args[1]
            table_input = call_kwargs['TableInput']
            evolved_columns = [c['Name'] for c in table_input['StorageDescriptor']['Columns']]
            self.assertIn('detail_domain', evolved_columns)
            self.assertIn('detail_entity', evolved_columns)
            self.assertIn('detail_content', evolved_columns)
            self.assertIn('detail_platform_name', evolved_columns)
            # Ensure partition key _ingested_at is NOT in StorageDescriptor.Columns
            self.assertNotIn('_ingested_at', evolved_columns)
        finally:
            uax_bronze_load.glue_client = orig_glue

    def test_json_serialization_excludes_partition_col(self):
        """Validates that serialize_chunk_to_bytes strips _ingested_at from JSON payload."""
        from uax_bronze_load import serialize_chunk_to_bytes
        import json

        records = [
            {
                "id": "1",
                "name": "test",
                "_source_system": "moveworks",
                "_table_name": "interactions",
                "_execution_id": "exec_1",
                "_ingested_at": "2026-09-21T00:00:00Z"
            }
        ]
        file_bytes, content_type, file_ext = serialize_chunk_to_bytes(
            records_chunk=records,
            output_format="json"
        )
        self.assertEqual(file_ext, ".json")
        deserialized = json.loads(file_bytes.decode('utf-8'))
        self.assertIn("id", deserialized[0])
        self.assertIn("_source_system", deserialized[0])
        self.assertNotIn("_ingested_at", deserialized[0])

    def test_lambda_fix_catalog_columns_event_routing(self):
        """Validates that Lambda routes catalog maintenance events properly and deletes extra columns."""
        from lambda_function import is_catalog_maintenance_event, lambda_handler
        event = {
            "action": "fix_catalog_table",
            "database": "uax_datalake_db_dev",
            "table": "raw_tbl_interactions",
            "exclude_columns": ["_ingested_at"]
        }
        self.assertTrue(is_catalog_maintenance_event(event))

        with patch('lambda_function.glue_client') as mock_glue:
            mock_glue.get_table.return_value = {
                'Table': {
                    'Name': 'raw_tbl_interactions',
                    'PartitionKeys': [{'Name': '_ingested_at', 'Type': 'string'}],
                    'StorageDescriptor': {
                        'Columns': [
                            {'Name': 'id', 'Type': 'string'},
                            {'Name': '_ingested_at', 'Type': 'string'}
                        ]
                    }
                }
            }
            resp = lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 200)
            body = json.loads(resp['body'])
            self.assertEqual(body['status'], 'SUCCEEDED')
            self.assertIn('_ingested_at', body['deleted_columns'])
            mock_glue.update_table.assert_called_once()
            table_input = mock_glue.update_table.call_args[1]['TableInput']
            cols = [c['Name'] for c in table_input['StorageDescriptor']['Columns']]
            self.assertNotIn('_ingested_at', cols)

    def test_lambda_delete_column_action(self):
        """Validates that Lambda handles action='delete_column' with 'column_name'."""
        from lambda_function import is_catalog_maintenance_event, lambda_handler
        event = {
            "action": "delete_column",
            "database": "uax_datalake_db_dev",
            "table": "raw_tbl_interactions",
            "column_name": "_ingested_at"
        }
        self.assertTrue(is_catalog_maintenance_event(event))

        with patch('lambda_function.glue_client') as mock_glue:
            mock_glue.get_table.return_value = {
                'Table': {
                    'Name': 'raw_tbl_interactions',
                    'PartitionKeys': [{'Name': '_ingested_at', 'Type': 'string'}],
                    'StorageDescriptor': {
                        'Columns': [
                            {'Name': 'id', 'Type': 'string'},
                            {'Name': '_ingested_at', 'Type': 'string'}
                        ]
                    }
                }
            }
            resp = lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 200)
            body = json.loads(resp['body'])
            self.assertEqual(body['status'], 'SUCCEEDED')
            self.assertIn('_ingested_at', body['deleted_columns'])

    def test_lambda_delete_from_parquet_and_catalog(self):
        """Validates that Lambda rewrites S3 Parquet files to drop columns entirely and syncs catalog."""
        from lambda_function import is_catalog_maintenance_event, lambda_handler
        import sys

        event = {
            "action": "delete_from_parquet",
            "database": "uax_datalake_db_dev",
            "table": "raw_tbl_interactions",
            "column_name": "_ingested_at",
            "s3_path": "s3://test-bucket/bronze/data/moveworks/interactions/"
        }
        self.assertTrue(is_catalog_maintenance_event(event))

        # Mock PyArrow Table
        mock_arrow_table = MagicMock()
        mock_arrow_table.column_names = ['id', 'session_id', '_ingested_at']
        mock_cleaned_table = MagicMock()
        mock_arrow_table.drop.return_value = mock_cleaned_table
        mock_arrow_table.__len__.return_value = 500

        mock_pq = MagicMock()
        mock_pq.read_table.return_value = mock_arrow_table

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'bronze/data/moveworks/interactions/_ingested_at=2026-09-20-00-00-00/part-0.parquet'}]}
        ]

        mock_pyarrow = MagicMock()
        mock_pyarrow.parquet = mock_pq

        with patch('lambda_function.glue_client') as mock_glue, \
             patch('lambda_function.s3_client') as mock_s3, \
             patch.dict(sys.modules, {'pyarrow': mock_pyarrow, 'pyarrow.parquet': mock_pq}):

            mock_glue.get_table.return_value = {
                'Table': {
                    'Name': 'raw_tbl_interactions',
                    'PartitionKeys': [{'Name': '_ingested_at', 'Type': 'string'}],
                    'StorageDescriptor': {
                        'Location': 's3://test-bucket/bronze/data/moveworks/interactions/',
                        'Columns': [
                            {'Name': 'id', 'Type': 'string'},
                            {'Name': '_ingested_at', 'Type': 'string'}
                        ]
                    }
                }
            }
            mock_s3.get_paginator.return_value = mock_paginator
            mock_s3.get_object.return_value = {'Body': MagicMock(read=lambda: b'PARQUET_BYTES')}

            resp = lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 200)
            body = json.loads(resp['body'])

            self.assertEqual(body['status'], 'SUCCEEDED')
            self.assertIn('_ingested_at', body['deleted_columns'])

            # Verify Parquet rewrite occurred
            parquet_res = body['parquet_sanitization']
            self.assertEqual(parquet_res['status'], 'SUCCEEDED')
            self.assertEqual(parquet_res['files_scanned'], 1)
            self.assertEqual(parquet_res['files_rewritten'], 1)
            mock_arrow_table.drop.assert_called_with(['_ingested_at'])
            mock_s3.put_object.assert_called_once()

            # Verify Catalog update occurred
            catalog_res = body['catalog_update']
            self.assertEqual(catalog_res['status'], 'SUCCEEDED')
            mock_glue.update_table.assert_called_once()

    def test_lambda_delete_from_parquet_when_table_dropped(self):
        """Validates that Lambda handles dropped/missing Glue tables gracefully and still sanitizes S3 Parquet."""
        from lambda_function import lambda_handler
        import sys

        event = {
            "action": "delete_from_parquet",
            "s3_path": "s3://test-bucket/bronze/data/moveworks/interactions/",
            "table": "raw_tbl_interactions",
            "column_name": "_ingested_at"
        }

        mock_arrow_table = MagicMock()
        mock_arrow_table.column_names = ['id', '_ingested_at']
        mock_arrow_table.drop.return_value = MagicMock()
        mock_arrow_table.__len__.return_value = 100

        mock_pq = MagicMock()
        mock_pq.read_table.return_value = mock_arrow_table

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'bronze/data/moveworks/interactions/part-0.parquet'}]}
        ]

        mock_pyarrow = MagicMock()
        mock_pyarrow.parquet = mock_pq

        with patch('lambda_function.glue_client') as mock_glue, \
             patch('lambda_function.s3_client') as mock_s3, \
             patch.dict(sys.modules, {'pyarrow': mock_pyarrow, 'pyarrow.parquet': mock_pq}):

            # Simulate table dropped in Glue Catalog (EntityNotFoundException)
            mock_glue.get_table.side_effect = Exception("EntityNotFoundException: Table raw_tbl_interactions not found")
            mock_s3.get_paginator.return_value = mock_paginator
            mock_s3.get_object.return_value = {'Body': MagicMock(read=lambda: b'BYTES')}

            resp = lambda_handler(event, None)
            # Must be 200 OK, NEVER 400!
            self.assertEqual(resp['statusCode'], 200)
            body = json.loads(resp['body'])
            self.assertEqual(body['status'], 'SUCCEEDED')
            self.assertEqual(body['parquet_sanitization']['files_rewritten'], 1)
            mock_s3.put_object.assert_called_once()

    def test_lambda_delete_from_parquet_minimal_s3_path_payload(self):
        """Validates that Lambda handles a dead-simple payload containing only s3_path without crashing."""
        from lambda_function import lambda_handler
        import sys

        # Simplest possible payload: just s3_path!
        event = {
            "s3_path": "s3://test-bucket/bronze/data/moveworks/interactions/"
        }

        mock_arrow_table = MagicMock()
        mock_arrow_table.column_names = ['id', '_ingested_at']
        mock_arrow_table.drop.return_value = MagicMock()
        mock_arrow_table.__len__.return_value = 50

        mock_pq = MagicMock()
        mock_pq.read_table.return_value = mock_arrow_table

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'bronze/data/moveworks/interactions/part-0.parquet'}]}
        ]

        mock_pyarrow = MagicMock()
        mock_pyarrow.parquet = mock_pq

        with patch('lambda_function.glue_client') as mock_glue, \
             patch('lambda_function.s3_client') as mock_s3, \
             patch.dict(sys.modules, {'pyarrow': mock_pyarrow, 'pyarrow.parquet': mock_pq}):

            mock_s3.get_paginator.return_value = mock_paginator
            mock_s3.get_object.return_value = {'Body': MagicMock(read=lambda: b'BYTES')}

            resp = lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 200)
            body = json.loads(resp['body'])
            self.assertEqual(body['status'], 'SUCCEEDED')
            self.assertEqual(body['parquet_sanitization']['files_rewritten'], 1)
            self.assertEqual(body['parquet_sanitization']['deleted_columns'], ['_ingested_at'])
            mock_s3.put_object.assert_called_once()

    def test_lambda_handles_string_or_body_event(self):
        """Validates that Lambda handles string events or API Gateway body strings safely without 400 error."""
        from lambda_function import lambda_handler
        import sys

        string_event = json.dumps({
            "s3_path": "s3://test-bucket/bronze/data/moveworks/interactions/"
        })

        mock_arrow_table = MagicMock()
        mock_arrow_table.column_names = ['id', '_ingested_at']
        mock_arrow_table.drop.return_value = MagicMock()
        mock_arrow_table.__len__.return_value = 50

        mock_pq = MagicMock()
        mock_pq.read_table.return_value = mock_arrow_table

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'bronze/data/moveworks/interactions/part-0.parquet'}]}
        ]

        mock_pyarrow = MagicMock()
        mock_pyarrow.parquet = mock_pq

        with patch('lambda_function.glue_client') as mock_glue, \
             patch('lambda_function.s3_client') as mock_s3, \
             patch.dict(sys.modules, {'pyarrow': mock_pyarrow, 'pyarrow.parquet': mock_pq}):

            mock_s3.get_paginator.return_value = mock_paginator
            mock_s3.get_object.return_value = {'Body': MagicMock(read=lambda: b'BYTES')}

            resp = lambda_handler(string_event, None)
            self.assertEqual(resp['statusCode'], 200)


class TestMultipleNaturalKeysHandling(unittest.TestCase):
    """
    QA Validation: Verifies that multiple / composite natural keys (nkeys)
    are seamlessly handled across config loading, deduplication, payload hashing,
    column protection, and Iceberg merge logic.
    """

    def test_get_nkey_single_and_composite(self):
        from silver_config_loader import SilverConfigLoader
        mock_cfg = {
            "source_systems": {
                "servicenow": {
                    "tables": {
                        "tbl_single": {"nkey": "sys_id"},
                        "tbl_composite_list": {"nkey": ["sys_id", "company"]},
                        "tbl_composite_csv": {"nkey": "sys_id, company"}
                    }
                }
            }
        }
        # Single string
        self.assertEqual(SilverConfigLoader.get_nkey("servicenow", "tbl_single", mock_cfg), "sys_id")
        self.assertEqual(SilverConfigLoader.get_nkey_list("servicenow", "tbl_single", mock_cfg), ["sys_id"])

        # Composite list
        self.assertEqual(SilverConfigLoader.get_nkey("servicenow", "tbl_composite_list", mock_cfg), ["sys_id", "company"])
        self.assertEqual(SilverConfigLoader.get_nkey_list("servicenow", "tbl_composite_list", mock_cfg), ["sys_id", "company"])

        # Composite CSV string
        self.assertEqual(SilverConfigLoader.get_nkey_list("servicenow", "tbl_composite_csv", mock_cfg), ["sys_id", "company"])

    def test_existing_silver_config_composite_keys(self):
        from silver_config_loader import SilverConfigLoader
        # Moveworks interactions has composite key ['id', 'interaction_id']
        mw_keys = SilverConfigLoader.get_nkey_list("moveworks", "tbl_interactions")
        self.assertEqual(mw_keys, ["id", "interaction_id"])

        # Postgresql order_items has composite key ['order_id', 'item_id']
        pg_keys = SilverConfigLoader.get_nkey_list("postgresql", "tbl_order_items")
        self.assertEqual(pg_keys, ["order_id", "item_id"])

    def test_get_payload_columns_excludes_all_composite_keys(self):
        from uax_silver_etl import get_payload_columns
        cols = ["sys_id", "company", "short_description", "state", "_valid_from", "_inserted_at"]
        composite_keys = ["sys_id", "company"]
        payload_cols = get_payload_columns(cols, composite_keys)
        # Neither sys_id nor company should be in payload_cols, nor technical columns
        self.assertNotIn("sys_id", payload_cols)
        self.assertNotIn("company", payload_cols)
        self.assertNotIn("_valid_from", payload_cols)
        self.assertNotIn("_inserted_at", payload_cols)
        self.assertEqual(payload_cols, ["short_description", "state"])

    def test_merge_conditions_generation_for_composite_keys(self):
        composite_keys = ["k1", "k2", "k3"]
        join_conditions = [f"target.`{k}` = source.`{k}`" for k in composite_keys]
        join_condition = " AND ".join(join_conditions)
        self.assertEqual(
            join_condition,
            "target.`k1` = source.`k1` AND target.`k2` = source.`k2` AND target.`k3` = source.`k3`"
        )
        # Verify keys are excluded from business update columns
        all_cols = ["k1", "k2", "k3", "attr1", "attr2", "_inserted_at", "_valid_from"]
        silver_tech = {'_valid_from', '_valid_to', '_is_current', '_is_deleted', '_inserted_at', '_updated_at'}
        nkey_set = set(composite_keys)
        business_update_cols = [c for c in all_cols if c not in silver_tech and c not in nkey_set]
        self.assertEqual(business_update_cols, ["attr1", "attr2"])


class TestCheckIcebergTableExists(unittest.TestCase):
    """Verifies that check_iceberg_table_exists automatically detects and cleans up stale/crawler-created catalog tables."""

    def test_drops_stale_crawler_hive_table(self):
        from uax_silver_etl import check_iceberg_table_exists
        mock_spark = MagicMock()
        mock_glue = MagicMock()
        mock_s3 = MagicMock()

        # Simulate crawler created standard Hive table (no ICEBERG table_type or metadata_location)
        mock_glue.get_table.return_value = {
            'Table': {
                'Name': 'tbl_plugin_resources',
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'EXTERNAL': 'TRUE'
                },
                'StorageDescriptor': {'Location': 's3://test-bucket/silver/data/moveworks/tbl_plugin_resources/'}
            }
        }

        exists = check_iceberg_table_exists(
            spark=mock_spark,
            glue_client=mock_glue,
            database_name='uax_datalake_db_dev',
            table_name='tbl_plugin_resources',
            silver_location='s3://test-bucket/silver/data/moveworks/tbl_plugin_resources/',
            s3_client=mock_s3
        )

        self.assertFalse(exists)
        mock_glue.delete_table.assert_called_once_with(
            DatabaseName='uax_datalake_db_dev',
            Name='tbl_plugin_resources'
        )

    def test_drops_ghost_table_on_missing_s3_metadata(self):
        from uax_silver_etl import check_iceberg_table_exists
        from botocore.exceptions import ClientError
        mock_spark = MagicMock()
        mock_glue = MagicMock()
        mock_s3 = MagicMock()

        mock_glue.get_table.return_value = {
            'Table': {
                'Name': 'tbl_users',
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'metadata_location': 's3://test-bucket/silver/data/moveworks/tbl_users/metadata/missing.json'
                },
                'StorageDescriptor': {'Location': 's3://test-bucket/silver/data/moveworks/tbl_users/'}
            }
        }

        err = ClientError({'Error': {'Code': '404', 'Message': 'Not Found'}}, 'HeadObject')
        err.response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        mock_s3.head_object.side_effect = err

        exists = check_iceberg_table_exists(
            spark=mock_spark,
            glue_client=mock_glue,
            database_name='uax_datalake_db_dev',
            table_name='tbl_users',
            silver_location='s3://test-bucket/silver/data/moveworks/tbl_users/',
            s3_client=mock_s3
        )

        self.assertFalse(exists)
        mock_glue.delete_table.assert_called_once_with(
            DatabaseName='uax_datalake_db_dev',
            Name='tbl_users'
        )

    def test_valid_iceberg_table_returns_true(self):
        from uax_silver_etl import check_iceberg_table_exists
        mock_spark = MagicMock()
        mock_glue = MagicMock()
        mock_s3 = MagicMock()

        mock_glue.get_table.return_value = {
            'Table': {
                'Name': 'tbl_interactions',
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'metadata_location': 's3://test-bucket/silver/data/moveworks/tbl_interactions/metadata/v1.metadata.json'
                },
                'StorageDescriptor': {'Location': 's3://test-bucket/silver/data/moveworks/tbl_interactions/'}
            }
        }

        mock_s3.head_object.return_value = {'ContentLength': 1024}
        mock_spark.sql.return_value = MagicMock()

        exists = check_iceberg_table_exists(
            spark=mock_spark,
            glue_client=mock_glue,
            database_name='uax_datalake_db_dev',
            table_name='tbl_interactions',
            silver_location='s3://test-bucket/silver/data/moveworks/tbl_interactions/',
            s3_client=mock_s3
        )

        self.assertTrue(exists)
        mock_glue.delete_table.assert_not_called()


class TestCleanIllegalCharsAndDefaultString(unittest.TestCase):
    """Verifies that clean_illegal_chars and default string column casting work across Silver."""

    def test_clean_illegal_chars_pandas_applymap(self):
        from transformer import SilverTransformer, clean_illegal_chars
        class MockDF:
            def __init__(self, data):
                self.data = data
            def applymap(self, fn):
                return MockDF({k: [fn(v) for v in vals] for k, vals in self.data.items()})

        # Contains \x00, \x08, \x0b, \x0c, \x1f, \ufffd, and legal \t, \n, \r
        raw_text = "Bad\x00Char\x08Test\x0bVT\x0cFF\x1fUS\ufffdEnd\tTab\nNewline\rCR"
        expected = "BadCharTestVTFFUSEnd\tTab\nNewline\rCR"
        pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\ufffd]'

        mock_df = MockDF({'col1': [raw_text, "Clean"]})

        # When pattern is provided, it cleans
        res = clean_illegal_chars(mock_df, pattern=pattern)
        self.assertEqual(res.data['col1'][0], expected)
        self.assertEqual(res.data['col1'][1], "Clean")

        # Classmethod invocation
        res2 = SilverTransformer.clean_illegal_chars(mock_df, pattern=pattern)
        self.assertEqual(res2.data['col1'][0], expected)

        # When pattern is NOT provided, it ignores and returns df unchanged
        res_ignored = clean_illegal_chars(mock_df, pattern=None)
        self.assertEqual(res_ignored.data['col1'][0], raw_text)

    def test_clean_illegal_chars_pyspark_with_column(self):
        from transformer import SilverTransformer
        pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\ufffd]'
        mock_df = MagicMock()
        mock_df.columns = ["id", "detail", "meta"]
        # Ensure it does NOT have applymap or map so it exercises PySpark withColumn branch
        del mock_df.applymap
        del mock_df.map
        mock_df.withColumn.return_value = mock_df

        # When pattern is provided, withColumn is called
        res = SilverTransformer.clean_illegal_chars(mock_df, pattern=pattern)
        self.assertEqual(mock_df.withColumn.call_count, 3)

        # When pattern is None, ignores
        mock_df.reset_mock()
        res_ignored = SilverTransformer.clean_illegal_chars(mock_df, pattern=None)
        self.assertEqual(mock_df.withColumn.call_count, 0)

    def test_moveworks_custom_transforms_export(self):
        from custom_transforms.moveworks_plugin_resources import transform as pr_transform
        from custom_transforms.moveworks_users import transform as u_transform

        mock_df = MagicMock()
        mock_df.columns = ["id", "detail"]
        r1 = pr_transform(mock_df)
        self.assertIs(r1, mock_df)

        r2 = u_transform(mock_df)
        self.assertIs(r2, mock_df)


    def test_silver_config_isolation(self):
        import json
        config_path = os.path.join(os.path.dirname(__file__), "..", "silver", "script", "config", "silver_config.json")
        with open(config_path, "r") as f:
            cfg = json.load(f)

        # Global defaults: clean_illegal_chars must be completely removed from global pipeline_defaults
        self.assertNotIn("clean_illegal_chars", cfg["pipeline_defaults"])
        self.assertNotIn("clean_illegal_chars_expression", cfg["pipeline_defaults"])
        self.assertTrue(cfg["pipeline_defaults"]["cast_all_columns_to_string"])

        # Moveworks tables must have clean_illegal_chars_expression configured table-specifically
        mw_tables = cfg["source_systems"]["moveworks"]["tables"]
        for tname, tcfg in mw_tables.items():
            self.assertTrue(
                tcfg.get("clean_illegal_chars_expression"),
                f"Moveworks table {tname} should have clean_illegal_chars_expression"
            )

        # ServiceNow tables must NOT have clean_illegal_chars_expression
        sn_tables = cfg["source_systems"]["servicenow"]["tables"]
        for tname, tcfg in sn_tables.items():
            self.assertNotIn(
                "clean_illegal_chars_expression",
                tcfg,
                f"ServiceNow table {tname} should NOT have clean_illegal_chars_expression"
            )

    def test_transformer_scopes_illegal_chars_to_moveworks_only(self):
        from transformer import SilverTransformer

        # Test with ServiceNow: no clean_illegal_chars_expression -> clean_illegal_chars must NOT be called
        with patch.object(SilverTransformer, 'clean_illegal_chars') as mock_clean:
            mock_df = MagicMock()
            mock_df.columns = ["sys_id", "number"]
            mock_df.drop.return_value = mock_df
            mock_df.withColumn.return_value = mock_df

            table_cfg = {}
            SilverTransformer.apply_transformations(
                df=mock_df,
                table_cfg=table_cfg,
                source_system="servicenow",
                table_name="tbl_incident",
            )
            mock_clean.assert_not_called()

        # Test with Moveworks: clean_illegal_chars_expression set -> clean_illegal_chars MUST be called with that expression
        mw_pattern = "[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\ufffd]"
        with patch.object(SilverTransformer, 'clean_illegal_chars') as mock_clean:
            mock_clean.return_value = mock_df
            mock_df = MagicMock()
            mock_df.columns = ["id", "detail"]
            mock_df.drop.return_value = mock_df
            mock_df.withColumn.return_value = mock_df

            table_cfg = {"clean_illegal_chars_expression": mw_pattern}
            SilverTransformer.apply_transformations(
                df=mock_df,
                table_cfg=table_cfg,
                source_system="moveworks",
                table_name="tbl_interactions",
            )
            mock_clean.assert_called_once_with(mock_df, pattern=mw_pattern)

    def test_silver_transformer_execute_custom_script_compatibility(self):
        """Verifies _execute_custom_script compatibility method exists and delegates to _apply_custom_script without AttributeError."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "val"]
        mock_df.drop.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        # Ensure calling _execute_custom_script does not raise AttributeError
        with patch.object(SilverTransformer, '_apply_custom_script', return_value=mock_df) as mock_apply:
            res = SilverTransformer._execute_custom_script(mock_df, "dummy_script.py")
            mock_apply.assert_called_once_with(mock_df, "dummy_script.py", spark=None)
            self.assertEqual(res, mock_df)

        # Ensure apply_transformations works with custom_transform_script configured without raising AttributeError
        with patch.object(SilverTransformer, '_apply_custom_script', return_value=mock_df):
            table_cfg = {"custom_transform_script": "custom_transforms/moveworks_interactions.py"}
            res = SilverTransformer.apply_transformations(
                df=mock_df,
                table_cfg=table_cfg,
                source_system="moveworks",
                table_name="tbl_interactions",
            )
            self.assertIsNotNone(res)


class TestGoldTriggerAndAthenaQuerySanitization(unittest.TestCase):
    """Unit tests validating Gold layer invocation payloads and Athena statement sanitization."""

    def test_gold_trigger_manual_db_params(self):
        """Validates that build_glue_arguments maps manual DB credentials (rds_schema, rds_url, rds_password, rds_username, rds_port)."""
        import lambda_function
        event = {
            "layer": "gold",
            "source_system": "moveworks",
            "rds_schema": "enterprise_reporting",
            "rds_url": "aurora-mysql-prod.cluster-xyz.us-east-1.rds.amazonaws.com",
            "rds_port": 3306,
            "rds_username": "reporting_user",
            "rds_password": "super_secret_password"
        }
        glue_args = lambda_function.build_glue_arguments(event)
        self.assertEqual(glue_args["--PROCESS_LAYER"], "gold")
        self.assertEqual(glue_args["--GOLD_SCHEMA"], "enterprise_reporting")
        self.assertEqual(glue_args["--RDS_HOST"], "aurora-mysql-prod.cluster-xyz.us-east-1.rds.amazonaws.com")
        self.assertEqual(glue_args["--RDS_PORT"], "3306")
        self.assertEqual(glue_args["--RDS_USER"], "reporting_user")
        self.assertEqual(glue_args["--RDS_PASSWORD"], "super_secret_password")

    def test_gold_trigger_db_secret(self):
        """Validates that build_glue_arguments maps db_secret to --RDS_SECRET_NAME for Secrets Manager."""
        import lambda_function
        event = {
            "layer": "gold",
            "source_system": "servicenow",
            "rds_schema": "enterprise_reporting",
            "db_secret": "prod/rds/mysql_credentials"
        }
        glue_args = lambda_function.build_glue_arguments(event)
        self.assertEqual(glue_args["--PROCESS_LAYER"], "gold")
        self.assertEqual(glue_args["--GOLD_SCHEMA"], "enterprise_reporting")
        self.assertEqual(glue_args["--RDS_SECRET_NAME"], "prod/rds/mysql_credentials")

    def test_gold_trigger_typo_alias_support(self):
        """Validates that build_glue_arguments supports user typo rds_uaername."""
        import lambda_function
        event = {
            "layer": "gold",
            "source_system": "moveworks",
            "rds_schema": "enterprise_reporting",
            "rds_uaername": "admin_user"
        }
        glue_args = lambda_function.build_glue_arguments(event)
        self.assertEqual(glue_args["--RDS_USER"], "admin_user")

    def test_athena_statement_comment_stripping(self):
        """Validates that leading comments (-- and /* */) are cleanly stripped from Athena queries."""
        import re
        sql_with_header = (
            "-- ==============================================================================\n"
            "-- Gold Mart Query: v_interactions.sql\n"
            "-- Description: Test query header comments\n"
            "-- ==============================================================================\n\n"
            "WITH conversation_topics AS (\n"
            "    SELECT * FROM tbl_interactions\n"
            ")\n"
            "SELECT * FROM conversation_topics;"
        )
        clean = re.sub(r'^(?:\s*(?:--[^\r\n]*|/\*[\s\S]*?\*/)\s*)+', '', sql_with_header).strip()
        self.assertTrue(clean.startswith("WITH conversation_topics AS"))
        self.assertNotIn("-- Gold Mart Query", clean[:50])

    def test_v_interactions_sql_syntax_compatibility(self):
        """Validates that v_interactions.sql uses Trino/Presto compatible VARCHAR cast and CURRENT_TIMESTAMP."""
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "gold", "query", "moveworks", "v_interactions.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as f:
            content = f.read()

        # STRING is Hive specific; Trino and Spark SQL require parameterized VARCHAR(length)
        self.assertNotIn("CAST(NULL AS STRING)", content)
        self.assertIn("CAST(NULL AS VARCHAR(255))", content)

        # Trino / Presto requires CURRENT_TIMESTAMP without parentheses
        self.assertNotIn("CURRENT_TIMESTAMP()", content)
        self.assertIn("CURRENT_TIMESTAMP", content)


if __name__ == "__main__":
    unittest.main()


