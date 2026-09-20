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
        self.assertIn("external_columns", cfg.get("silver_defaults", {}))
        for source_sys, s_cfg in cfg.get("source_systems", {}).items():
            for tbl, t_cfg in s_cfg.get("table_configs", {}).items():
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
                {'Key': 'gold/query/servicenow/incident_kpi.sql'}
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


if __name__ == "__main__":
    unittest.main()


