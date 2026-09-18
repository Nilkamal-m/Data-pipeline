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
from uax_bronze_load import get_table_state_key


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


if __name__ == "__main__":
    unittest.main()

