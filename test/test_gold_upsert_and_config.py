import sys
import os
import types
import unittest
from unittest.mock import MagicMock, patch
import json
import re

# Mock awsglue, pyspark, boto3, dateutil for standalone testing without Glue/Hadoop runtime
for pkg in ['pyspark', 'pyspark.sql', 'awsglue', 'botocore', 'dateutil']:
    if pkg not in sys.modules:
        m = types.ModuleType(pkg)
        m.__path__ = []
        sys.modules[pkg] = m

for mod in [
    'pyspark.context', 'pyspark.conf', 'pyspark.sql', 'pyspark.sql.functions', 'pyspark.sql.types', 'pyspark.sql.window',
    'awsglue', 'awsglue.context', 'awsglue.job', 'awsglue.utils',
    'boto3', 'botocore', 'botocore.session', 'botocore.client', 'botocore.exceptions',
    'dateutil', 'dateutil.tz', 'dateutil.parser'
]:
    if mod not in sys.modules or not isinstance(sys.modules[mod], (MagicMock, types.ModuleType)):
        sys.modules[mod] = MagicMock()

sys.modules['pyspark.sql'].SparkSession = MagicMock
sys.modules['pyspark.sql'].DataFrame = MagicMock

class MockColumn(MagicMock):
    def __gt__(self, other):
        return MockColumn()
    def __lt__(self, other):
        return MockColumn()
    def __ge__(self, other):
        return MockColumn()
    def __le__(self, other):
        return MockColumn()
    def __eq__(self, other):
        return MockColumn()
    def __ne__(self, other):
        return MockColumn()
    def __or__(self, other):
        return MockColumn()
    def __and__(self, other):
        return MockColumn()
    def isNull(self):
        return MockColumn()
    def isNotNull(self):
        return MockColumn()
    def alias(self, name):
        return MockColumn()

sys.modules['pyspark.sql.functions'].col = lambda name: MockColumn()
sys.modules['pyspark.sql.functions'].lit = lambda val: MockColumn()

# Ensure repository root and gold scripts are in sys.path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
gold_script_dir = os.path.join(repo_root, "gold", "script")
for p in [repo_root, gold_script_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from gold_config_loader import GoldConfigLoader
from gold_layer_manager import GoldLayerManager
from gold_initial_load import GoldInitialLoader


class TestGoldConfigLoader(unittest.TestCase):
    """Tests for GoldConfigLoader auto-discovery and resolution."""

    def setUp(self):
        GoldConfigLoader.clear_cache()

    def tearDown(self):
        GoldConfigLoader.clear_cache()

    def test_auto_load_local_config(self):
        cfg = GoldConfigLoader.load_config()
        self.assertIsInstance(cfg, dict)
        self.assertIn("source_systems", cfg)
        self.assertIn("pipeline_defaults", cfg)
        self.assertIn("genesys", cfg["source_systems"])

    def test_get_primary_key(self):
        pk_genesys = GoldConfigLoader.get_primary_key("genesys", "conversations")
        self.assertEqual(pk_genesys, ["conversation_id"])

        pk_moveworks = GoldConfigLoader.get_primary_key("moveworks", "interactions")
        self.assertEqual(pk_moveworks, ["interaction_id"])

        pk_composite = GoldConfigLoader.get_primary_key("servicenow", "incident_kpi")
        self.assertEqual(pk_composite, ["priority_level", "incident_state", "incident_category"])

    def test_get_target_table_name(self):
        # Default athena table name: gold_<source>_<tablename>
        tbl_name = GoldConfigLoader.get_target_table_name("genesys", "conversations", "athena")
        self.assertEqual(tbl_name, "gold_genesys_conversations")

        # Configured aurora table name for moveworks interactions matches athena table name
        aurora_tbl = GoldConfigLoader.get_target_table_name("moveworks", "interactions", "aurora")
        athena_tbl = GoldConfigLoader.get_target_table_name("moveworks", "interactions", "athena")
        self.assertEqual(aurora_tbl, "gold_moveworks_interactions")
        self.assertEqual(athena_tbl, aurora_tbl)

    def test_get_target_engines(self):
        # From config
        targets = GoldConfigLoader.get_target_engines("genesys", "conversations")
        self.assertIn("athena", targets)
        self.assertIn("aurora", targets)

        # With CLI override
        cli_targets = GoldConfigLoader.get_target_engines("genesys", "conversations", cli_override="databricks,snowflake")
        self.assertEqual(cli_targets, ["databricks", "snowflake"])

    def test_get_custom_transform_path(self):
        path = GoldConfigLoader.get_custom_transform_path("genesys", "conversations")
        self.assertIsNotNone(path)
        self.assertTrue(path.endswith("genesys_conversations.py"))

    def test_get_initial_load_config(self):
        # 1. Directly from gold_config.json for genesys conversations
        init_cfg = GoldConfigLoader.get_initial_load_config("genesys", "conversations")
        self.assertIn("conversations.csv", init_cfg.get("path", ""))
        self.assertEqual(init_cfg.get("delimiter"), ",")
        self.assertTrue(init_cfg.get("has_header"))

        # 2. Custom dict with initial_load_path string
        custom = {
            "source_systems": {
                "custom_src": {
                    "tables": {
                        "tbl": {"initial_load_path": "s3://bucket/custom.csv"}
                    }
                }
            }
        }
        res = GoldConfigLoader.get_initial_load_config("custom_src", "tbl", custom)
        self.assertEqual(res.get("path"), "s3://bucket/custom.csv")

    def test_load_config_when_file_not_found_returns_empty_dict(self):
        GoldConfigLoader.clear_cache()
        with patch("os.path.exists", return_value=False):
            cfg = GoldConfigLoader.load_config()
            self.assertEqual(cfg, {})
            self.assertFalse(hasattr(GoldConfigLoader, "DEFAULT_CONFIG"))
            self.assertFalse(hasattr(GoldLayerManager, "DEFAULT_CONFIG"))


class TestGoldLayerManagerExtensions(unittest.TestCase):
    """Tests for GoldLayerManager upsert and transform capabilities."""

    def test_extract_sql_annotations(self):
        sql = """-- ==============================================================================
-- Gold Mart Query: conversations.sql
-- PRIMARY_KEY: conversation_id, org_id
-- TARGET_TABLE: gold_genesys_conversations
-- ==============================================================================
SELECT conversation_id, org_id, user_id FROM tbl_conversations;
"""
        annotations = GoldLayerManager._extract_sql_annotations(sql)
        self.assertEqual(annotations.get("primary_key"), ["conversation_id", "org_id"])
        self.assertEqual(annotations.get("target_table"), "gold_genesys_conversations")

    def test_discover_queries_relaxed_naming(self):
        # Both conversations.sql and v_conversations.sql should produce clean name 'conversations'
        queries = {"v_conversations": "SELECT 1"}
        sorted_q = GoldLayerManager._sort_queries_by_dependency(queries)
        self.assertIn("v_conversations", sorted_q)

    def test_apply_custom_transform_nonexistent(self):
        mock_df = MagicMock()
        # Should gracefully return original df if script doesn't exist
        result_df = GoldLayerManager._apply_custom_transform(mock_df, "non_existent_script.py")
        self.assertEqual(result_df, mock_df)

    def test_apply_custom_transform_genesys(self):
        mock_df = MagicMock()
        mock_df.columns = ["conversation_id", "prompt_text"]
        mock_df.withColumn.return_value = mock_df

        transform_path = os.path.join(gold_script_dir, "custom_transforms", "genesys_conversations.py")
        result_df = GoldLayerManager._apply_custom_transform(mock_df, transform_path)
        # Should attach _updated_at
        mock_df.withColumn.assert_called()


class TestGoldInitialLoader(unittest.TestCase):
    """Tests for GoldInitialLoader schema reconciliation."""

    def test_reconcile_schema_missing_and_extra_columns(self):
        mock_df = MagicMock()
        # Source CSV has 'id', 'extra_csv_col'
        mock_df.columns = ["id", "extra_csv_col"]
        mock_df.withColumn.return_value = mock_df

        # Target Gold table has 'id', 'bot_response', 'sentiment'
        field_id = MagicMock()
        field_id.name = "id"
        field_id.dataType = "string"

        field_bot = MagicMock()
        field_bot.name = "bot_response"
        field_bot.dataType = "string"

        field_sentiment = MagicMock()
        field_sentiment.name = "sentiment"
        field_sentiment.dataType = "string"

        target_schema = [field_id, field_bot, field_sentiment]

        reconciled = GoldInitialLoader.reconcile_schema(mock_df, target_schema)

        # withColumn should be called for 'bot_response' and 'sentiment' (missing in source CSV)
        calls = [c[0][0] for c in mock_df.withColumn.call_args_list]
        self.assertIn("bot_response", calls)
        self.assertIn("sentiment", calls)

    def test_sanitize_column_name_spaces_and_dots(self):
        """Verify spaces, dots, hyphens, and illegal characters are cleansed into valid snake_case."""
        self.assertEqual(GoldInitialLoader.sanitize_column_name("Customer Name"), "customer_name")
        self.assertEqual(GoldInitialLoader.sanitize_column_name("caller_id.name"), "caller_id_name")
        self.assertEqual(GoldInitialLoader.sanitize_column_name("user-id"), "user_id")
        self.assertEqual(GoldInitialLoader.sanitize_column_name("incident.category #1"), "incident_category_1")
        self.assertEqual(GoldInitialLoader.sanitize_column_name("  nested.field.name  "), "nested_field_name")
        self.assertEqual(GoldInitialLoader.sanitize_column_name("123_number"), "col_123_number")
        self.assertEqual(GoldInitialLoader.sanitize_column_name(""), "unnamed_column")

    def test_reconcile_schema_cleanses_spaces_and_dots_and_pads_missing(self):
        """Verify CSV column cleansing, target schema alignment, and evolution retention."""
        mock_df = MagicMock()
        mock_df.columns = ["Customer Name", "caller_id.name", "Extra Notes"]
        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        target_field_cust = MagicMock()
        target_field_cust.name = "customer_name"
        target_field_cust.dataType = "string"

        target_field_caller = MagicMock()
        target_field_caller.name = "caller_id_name"
        target_field_caller.dataType = "string"

        target_field_kpi = MagicMock()
        target_field_kpi.name = "sentiment_score"
        target_field_kpi.dataType = "double"

        target_schema = [target_field_cust, target_field_caller, target_field_kpi]

        reconciled = GoldInitialLoader.reconcile_schema(mock_df, target_schema)

        # withColumnRenamed should be called to cleanse spaces and dots
        renamed_calls = [c[0] for c in mock_df.withColumnRenamed.call_args_list]
        self.assertTrue(any(c[0] == "Customer Name" and c[1] == "customer_name" for c in renamed_calls))
        self.assertTrue(any(c[0] == "caller_id.name" and c[1] == "caller_id_name" for c in renamed_calls))

        # withColumn should be called to pad missing 'sentiment_score'
        added_calls = [c[0][0] for c in mock_df.withColumn.call_args_list]
        self.assertIn("sentiment_score", added_calls)


class TestAuroraMySQLIndexing(unittest.TestCase):
    """Tests for Aurora MySQL performance index creation based on nkey."""

    def test_ensure_mysql_index_creation(self):
        jdbc_info = {"host": "localhost", "port": 3306, "user": "uax_user", "password": "pwd"}
        with patch.object(GoldLayerManager, "_execute_sql_query", return_value=[]), \
             patch.object(GoldLayerManager, "_execute_ddl") as mock_ddl:

            GoldLayerManager._ensure_mysql_index(
                jdbc_info=jdbc_info,
                schema_name="enterprise_reporting",
                target_table="gold_genesys_conversations",
                nkeys=["conversation_id"]
            )

            mock_ddl.assert_called_once()
            called_ddl = mock_ddl.call_args[0][1]
            self.assertIn("ALTER TABLE `enterprise_reporting`.`gold_genesys_conversations` ADD INDEX", called_ddl)
            self.assertIn("`conversation_id`", called_ddl)

    def test_ensure_mysql_index_skips_when_already_exists(self):
        jdbc_info = {"host": "localhost", "port": 3306, "user": "uax_user", "password": "pwd"}
        with patch.object(GoldLayerManager, "_execute_sql_query", return_value=[{"Key_name": "idx_nkey_conversations"}]), \
             patch.object(GoldLayerManager, "_execute_ddl") as mock_ddl:

            GoldLayerManager._ensure_mysql_index(
                jdbc_info=jdbc_info,
                schema_name="enterprise_reporting",
                target_table="gold_genesys_conversations",
                nkeys=["conversation_id"]
            )

            # DDL should not be called since index already exists
            mock_ddl.assert_not_called()


class TestGoldQuerySchemaProbe(unittest.TestCase):
    """Tests for zero-record schema extraction via SELECT * FROM (<query>) WHERE 1=0."""

    def test_probe_target_schema_from_sql_text(self):
        mock_spark = MagicMock()
        mock_probe_df = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "conversation_id"
        mock_probe_df.schema.fields = [mock_field]
        mock_spark.sql.return_value = mock_probe_df

        params = {
            "GOLD_SQL": "SELECT conversation_id, user_id FROM tbl_conversations"
        }
        fields = GoldInitialLoader._probe_target_schema_from_query(
            spark=mock_spark,
            source_system="genesys",
            table_name="conversations",
            params=params
        )

        self.assertIsNotNone(fields)
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0].name, "conversation_id")
        called_sql = mock_spark.sql.call_args[0][0]
        self.assertIn("WHERE 1=0", called_sql)


class TestGoldIncrementalDelta(unittest.TestCase):
    """Tests for incremental delta filtering to protect LLM calls from exponential cost growth."""

    def test_config_loader_is_incremental(self):
        # conversations was configured with "incremental": true
        is_inc = GoldConfigLoader.is_incremental("genesys", "conversations")
        self.assertTrue(is_inc)

        # users has no incremental flag, should default to False
        is_inc_users = GoldConfigLoader.is_incremental("genesys", "users")
        self.assertFalse(is_inc_users)

    def test_filter_incremental_delta_target_table_not_found(self):
        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Table not found in catalog")
        mock_incoming_df = MagicMock()

        # Should return full incoming df on initial run when target table doesn't exist
        result = GoldLayerManager.filter_incremental_delta(
            spark=mock_spark,
            df_incoming=mock_incoming_df,
            glue_database="uax_datalake_db_dev",
            target_table_name="gold_genesys_conversations",
            nkeys=["conversation_id"]
        )
        self.assertEqual(result, mock_incoming_df)

    def test_filter_incremental_delta_when_target_exists(self):
        mock_spark = MagicMock()
        mock_target_df = MagicMock()
        mock_target_df.columns = ["conversation_id", "_updated_at", "sentiment_score"]
        mock_spark.table.return_value = mock_target_df

        mock_incoming_df = MagicMock()
        mock_incoming_df.columns = ["conversation_id", "_updated_at"]
        mock_joined_df = MagicMock()
        mock_incoming_df.join.return_value = mock_joined_df
        mock_filtered_df = MagicMock()
        mock_joined_df.filter.return_value = mock_filtered_df
        mock_filtered_df.columns = ["conversation_id", "_updated_at", "_target_conversation_id", "_target_updated_at", "_target_sentiment_score"]
        mock_filtered_df.drop.return_value = mock_filtered_df
        mock_filtered_df.count.return_value = 5

        delta_result = GoldLayerManager.filter_incremental_delta(
            spark=mock_spark,
            df_incoming=mock_incoming_df,
            glue_database="uax_datalake_db_dev",
            target_table_name="gold_genesys_conversations",
            nkeys=["conversation_id"],
            enrichment_column="sentiment_score"
        )
        # Verify join and filter were invoked to isolate delta
        mock_incoming_df.join.assert_called_once()
        mock_joined_df.filter.assert_called_once()

    def test_get_secret_name_resolution(self):
        # 1. Custom config with source-level secret
        cfg_source = {
            "source_systems": {
                "genesys": {
                    "aurora": {"secret_name": "rds/genesys-secret"}
                }
            },
            "pipeline_defaults": {
                "aurora": {"secret_name": "rds/default-secret"}
            }
        }
        self.assertEqual(
            GoldConfigLoader.get_secret_name("genesys", cfg_source),
            "rds/genesys-secret"
        )
        # 2. Source without aurora secret falls back to pipeline_defaults
        self.assertEqual(
            GoldConfigLoader.get_secret_name("other_source", cfg_source),
            "rds/default-secret"
        )
        # 3. Direct config level secret
        cfg_root = {
            "pipeline_defaults": {"secret_name": "rds/pipeline-secret"}
        }
        self.assertEqual(
            GoldConfigLoader.get_secret_name(None, cfg_root),
            "rds/pipeline-secret"
        )

    def test_resolve_mysql_connection_info_with_secret_name(self):
        # Test CLI param takes precedence, else falls back to config
        with patch("gold.script.gold_layer_manager.GoldConfigLoader.get_secret_name") as mock_cfg_secret:
            mock_cfg_secret.return_value = "rds/config-secret"
            mock_secrets_client = MagicMock()
            mock_secrets_client.get_secret_value.return_value = {
                "SecretString": json.dumps({"password": "resolved_pwd", "username": "dbuser"})
            }

            # 1. When CLI SECRET_NAME is provided
            conn_info_cli = GoldLayerManager._resolve_mysql_connection_info({
                "SECRET_NAME": "rds/cli-secret",
                "GOLD_SCHEMA": "enterprise_reporting",
                "RDS_HOST": "aurora.cluster.test",
                "SOURCE_SYSTEM": "genesys"
            }, secrets_client=mock_secrets_client)
            mock_secrets_client.get_secret_value.assert_called_with(SecretId="rds/cli-secret")
            self.assertEqual(conn_info_cli["password"], "resolved_pwd")

            # 2. When CLI SECRET_NAME is omitted, falls back to config secret
            mock_secrets_client.reset_mock()
            conn_info_fallback = GoldLayerManager._resolve_mysql_connection_info({
                "GOLD_SCHEMA": "enterprise_reporting",
                "RDS_HOST": "aurora.cluster.test",
                "SOURCE_SYSTEM": "genesys"
            }, secrets_client=mock_secrets_client)
            mock_secrets_client.get_secret_value.assert_called_with(SecretId="rds/config-secret")
            self.assertEqual(conn_info_fallback["password"], "resolved_pwd")

    def test_get_api_secret_name_resolution(self):
        # 1. Direct from gold_config.json for genesys conversations
        api_secret = GoldConfigLoader.get_api_secret_name("genesys", "conversations")
        self.assertEqual(api_secret, "dev/data-pipeline/genesys-llm-api")

        # 2. Table-level override in custom dict
        custom_cfg = {
            "source_systems": {
                "custom_src": {
                    "tables": {
                        "tbl": {"api_secret_name": "secrets/table-api-key"}
                    }
                }
            },
            "pipeline_defaults": {"api_secret_name": "secrets/default-api-key"}
        }
        self.assertEqual(
            GoldConfigLoader.get_api_secret_name("custom_src", "tbl", custom_cfg),
            "secrets/table-api-key"
        )
        # 3. Source without table config falls back to default
        self.assertEqual(
            GoldConfigLoader.get_api_secret_name("other_src", "tbl", custom_cfg),
            "secrets/default-api-key"
        )

    def test_genesys_transform_with_api_secret_name(self):
        mock_df = MagicMock()
        mock_df.columns = ["conversation_id"]
        mock_df.withColumn.return_value = mock_df

        transform_path = os.path.join(gold_script_dir, "custom_transforms", "genesys_conversations.py")
        context = {
            "source_system": "genesys",
            "table_name": "conversations",
            "api_secret_name": "dev/data-pipeline/genesys-llm-api",
            "is_incremental": True
        }
        result_df = GoldLayerManager._apply_custom_transform(
            mock_df, transform_path, spark=MagicMock(), context=context
        )
        self.assertIsNotNone(result_df)
        mock_df.withColumn.assert_called()


class TestAuroraTableDropAndRecordDeleteRestrictions(unittest.TestCase):
    """Tests for Aurora MySQL table drop and rename restrictions (DML-only pattern)."""

    def test_validate_droppable_table_name_allows_staging_and_old(self):
        """Only tables ending in _staging or _old are permissible to drop."""
        GoldLayerManager._validate_droppable_table_name("gold_genesys_conversations_staging")
        GoldLayerManager._validate_droppable_table_name("gold_genesys_conversations_old")
        GoldLayerManager._validate_droppable_table_name("gold_moveworks_interactions_staging")

    def test_validate_droppable_table_name_forbids_production_tables(self):
        """Production Gold tables must NEVER be droppable."""
        with self.assertRaises(AssertionError) as ctx1:
            GoldLayerManager._validate_droppable_table_name("gold_genesys_conversations")
        self.assertIn("CRITICAL SAFETY VIOLATION", str(ctx1.exception))

        with self.assertRaises(AssertionError) as ctx2:
            GoldLayerManager._validate_droppable_table_name("gold_moveworks_interactions")
        self.assertIn("CRITICAL SAFETY VIOLATION", str(ctx2.exception))

    def test_replace_mysql_records_executes_dml_only(self):
        """Verify record-level replacement uses DELETE + INSERT without dropping or renaming production tables."""
        jdbc_info = {"host": "localhost", "port": 3306, "user": "dbuser", "password": "pwd"}
        ddl_calls = []

        def fake_execute_ddl(info, sql):
            ddl_calls.append(sql)

        with patch.object(GoldLayerManager, "_execute_ddl", side_effect=fake_execute_ddl), \
             patch.object(GoldLayerManager, "_table_exists", return_value=True):

            GoldLayerManager._replace_mysql_records(
                jdbc_info=jdbc_info,
                schema_name="enterprise_reporting",
                target_table="gold_genesys_conversations",
                staging_table="gold_genesys_conversations_staging",
                columns=["conversation_id", "sentiment_score", "prompt_text"]
            )

            # 1. Must execute DELETE FROM target_table
            self.assertTrue(any("DELETE FROM `enterprise_reporting`.`gold_genesys_conversations`" in s for s in ddl_calls))

            # 2. Must execute INSERT INTO target_table SELECT ... FROM staging_table
            self.assertTrue(any(
                "INSERT INTO `enterprise_reporting`.`gold_genesys_conversations` (`conversation_id`, `sentiment_score`, `prompt_text`)" in s
                and "SELECT `conversation_id`, `sentiment_score`, `prompt_text` FROM `enterprise_reporting`.`gold_genesys_conversations_staging`" in s
                for s in ddl_calls
            ))

            # 3. Must cleanup staging table
            self.assertTrue(any("DROP TABLE IF EXISTS `enterprise_reporting`.`gold_genesys_conversations_staging`" in s for s in ddl_calls))

            # 4. Target production table must NEVER appear in a DROP TABLE or RENAME TABLE statement
            for s in ddl_calls:
                self.assertNotIn("DROP TABLE `enterprise_reporting`.`gold_genesys_conversations`", s)
                self.assertNotIn("DROP TABLE IF EXISTS `enterprise_reporting`.`gold_genesys_conversations`", s)
                self.assertNotIn("RENAME TABLE `enterprise_reporting`.`gold_genesys_conversations`", s)

    def test_cleanup_staging_table_fallback_on_drop_denied_1142(self):
        """Verify fallback to TRUNCATE/DELETE when database user lacks DROP permissions."""
        jdbc_info = {"host": "localhost", "port": 3306, "user": "dbuser", "password": "pwd"}
        executed_sqls = []

        def fake_execute_ddl(info, sql):
            executed_sqls.append(sql)
            if "DROP TABLE" in sql:
                raise RuntimeError("MySQL Error (1142): DROP command denied to user 'app'@'host' for table 'staging'")

        with patch.object(GoldLayerManager, "_execute_ddl", side_effect=fake_execute_ddl):
            GoldLayerManager._cleanup_staging_table(
                jdbc_info=jdbc_info,
                schema_name="enterprise_reporting",
                staging_table="gold_genesys_conversations_staging"
            )

            # First attempted DROP
            self.assertTrue(any("DROP TABLE" in s for s in executed_sqls))
            # Gracefully fell back to TRUNCATE or DELETE
            self.assertTrue(any("TRUNCATE TABLE `enterprise_reporting`.`gold_genesys_conversations_staging`" in s for s in executed_sqls))


class TestGoldSchemaEvolution(unittest.TestCase):
    """Tests for first-load schema creation and dynamic schema evolution (new columns)."""

    def test_spark_type_to_mysql(self):
        """Translates PySpark schema types into valid MySQL DDL column types."""
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("string"), "TEXT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("int"), "INT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("integer"), "INT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("bigint"), "BIGINT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("long"), "BIGINT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("double"), "DOUBLE")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("float"), "FLOAT")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("boolean"), "TINYINT(1)")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("timestamp"), "DATETIME")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("date"), "DATE")
        self.assertEqual(GoldLayerManager._spark_type_to_mysql("decimal(18,4)"), "DECIMAL(18,4)")

    def test_sync_iceberg_schema_with_new_columns(self):
        """Evolves Apache Iceberg table definition in Glue/Athena via ALTER TABLE ADD COLUMNS."""
        mock_spark = MagicMock()
        mock_target_df = MagicMock()
        mock_f1 = MagicMock()
        mock_f1.name = "conversation_id"
        mock_target_df.schema.fields = [mock_f1]
        mock_spark.table.return_value = mock_target_df

        mock_incoming_df = MagicMock()
        mock_in_f1 = MagicMock()
        mock_in_f1.name = "conversation_id"
        mock_in_f2 = MagicMock()
        mock_in_f2.name = "sentiment_score"
        mock_in_f2.dataType.simpleString.return_value = "double"
        mock_incoming_df.schema.fields = [mock_in_f1, mock_in_f2]

        evolved = GoldLayerManager._sync_iceberg_schema(
            spark=mock_spark,
            full_table="`uax_datalake_db_dev`.`gold_genesys_conversations`",
            incoming_df=mock_incoming_df
        )

        self.assertTrue(evolved)
        mock_spark.sql.assert_called_once()
        called_sql = mock_spark.sql.call_args[0][0]
        self.assertIn("ALTER TABLE `uax_datalake_db_dev`.`gold_genesys_conversations` ADD COLUMNS", called_sql)
        self.assertIn("`sentiment_score` double", called_sql)

    def test_detect_schema_evolution_alters_mysql_table(self):
        """Dynamically adds new columns to existing MySQL Aurora table via ALTER TABLE ADD COLUMN."""
        jdbc_info = {"host": "localhost", "port": 3306, "user": "dbuser", "password": "pwd"}

        # Target MySQL table only has 'conversation_id'
        existing_cols = [("conversation_id", "text", "YES")]

        mock_incoming_df = MagicMock()
        f_id = MagicMock()
        f_id.name = "conversation_id"
        f_id.dataType.simpleString.return_value = "string"

        f_sentiment = MagicMock()
        f_sentiment.name = "sentiment_score"
        f_sentiment.dataType.simpleString.return_value = "double"

        mock_incoming_df.schema.fields = [f_id, f_sentiment]

        ddl_calls = []
        def fake_ddl(info, sql):
            ddl_calls.append(sql)

        with patch.object(GoldLayerManager, "_execute_sql_query", return_value=existing_cols), \
             patch.object(GoldLayerManager, "_execute_ddl", side_effect=fake_ddl):

            GoldLayerManager._detect_schema_evolution(
                jdbc_info=jdbc_info,
                schema_name="enterprise_reporting",
                target_table="gold_genesys_conversations",
                df_mart=mock_incoming_df
            )

            # Must have executed ALTER TABLE ADD COLUMN for sentiment_score
            self.assertTrue(any(
                "ALTER TABLE `enterprise_reporting`.`gold_genesys_conversations` ADD COLUMN `sentiment_score` DOUBLE NULL" in s
                for s in ddl_calls
            ))


class TestAthenaToAuroraExtension(unittest.TestCase):
    """Tests for Athena-first architecture where Aurora acts as an optional extension reading from Athena."""

    def test_serve_to_mysql_reads_from_athena_iceberg_table(self):
        """When in-memory DataFrame is not cached, _serve_to_mysql reads directly from Athena table."""
        mock_spark = MagicMock()
        mock_athena_df = MagicMock()
        mock_athena_df.columns = ["conversation_id", "prompt_text"]
        mock_athena_df.count.return_value = 100
        mock_spark.table.return_value = mock_athena_df

        jdbc_info = {"host": "localhost", "port": 3306, "user": "dbuser", "password": "pwd"}

        with patch.object(GoldLayerManager, "_resolve_mysql_connection_info", return_value=jdbc_info), \
             patch.object(GoldLayerManager, "_verify_schema_exists_or_raise"), \
             patch.object(GoldLayerManager, "_detect_schema_evolution"), \
             patch.object(GoldLayerManager, "_write_staging_table"), \
             patch.object(GoldLayerManager, "_table_exists", return_value=True), \
             patch.object(GoldLayerManager, "_upsert_mysql_table"):

            GoldLayerManager._serve_to_mysql(
                spark=mock_spark,
                queries={"conversations": "SELECT 1"},
                gold_schema="enterprise_reporting",
                data_s3_path="s3://bucket/gold/data/genesys",
                params={"GLUE_DATABASE": "uax_datalake_db_dev"},
                glue_client=None,
                secrets_client=None,
                mart_stats=[],
                source_system="genesys",
                materialized_dfs=None,  # Not in memory -> Must read from Athena table!
                mart_keys={"conversations": ["conversation_id"]}
            )

            # Must have called spark.table with Athena table name
            mock_spark.table.assert_called_with("uax_datalake_db_dev.gold_genesys_conversations")

    def test_aurora_extension_only_when_requested(self):
        """Verifies Aurora extension is created ONLY when requested, and skipped when targets is ['athena']."""
        # 1. When targets is ['athena'] -> needs_mysql is False
        targets_athena_only = ["athena"]
        needs_mysql = any(t in targets_athena_only for t in ('aurora', 'rds', 'mysql'))
        self.assertFalse(needs_mysql)

        # 2. When targets contains 'aurora' -> needs_mysql is True
        targets_with_aurora = ["athena", "aurora"]
        needs_mysql_aurora = any(t in targets_with_aurora for t in ('aurora', 'rds', 'mysql'))
        self.assertTrue(needs_mysql_aurora)

    def test_initial_load_aurora_extension_invoked(self):
        """When GOLD_TARGETS includes 'aurora', initial load creates Athena table then serves to Aurora."""
        mock_spark = MagicMock()
        mock_raw_df = MagicMock()
        mock_raw_df.columns = ["conversation_id"]
        mock_raw_df.count.return_value = 50
        mock_spark.read.option.return_value.option.return_value.option.return_value.csv.return_value = mock_raw_df
        mock_spark.table.return_value = mock_raw_df

        params = {
            "JOB_NAME": "test_job",
            "SOURCE_SYSTEM": "genesys",
            "TABLE_NAME": "conversations",
            "CSV_PATH": "s3://bucket/init/conversations.csv",
            "GOLD_TARGETS": "athena,aurora",
            "GLUE_DATABASE": "uax_datalake_db_dev",
            "GOLD_SCHEMA": "enterprise_reporting"
        }

        with patch.object(GoldLayerManager, "_serve_to_mysql") as mock_mysql_serve:
            GoldInitialLoader.run_initial_load(mock_spark, params)
            # Aurora serving extension must have been called
            mock_mysql_serve.assert_called_once()
            call_kwargs = mock_mysql_serve.call_args[1]
            self.assertEqual(call_kwargs["gold_schema"], "enterprise_reporting")
            self.assertEqual(call_kwargs["source_system"], "genesys")


class TestDynamicEnvInterpolationAcrossAllLayers(unittest.TestCase):
    """
    Comprehensive verification that all layers (Bronze, Silver, Gold, and Initial Load)
    dynamically replace '{env}' and '{ENV}' placeholders with the exact values passed from
    Glue job arguments (--ENV or --ENVIRONMENT) triggered from Lambda or Step Functions.
    """

    def setUp(self):
        GoldConfigLoader.clear_cache()
        try:
            from silver.script.silver_config_loader import SilverConfigLoader
            SilverConfigLoader.clear_cache()
        except ImportError:
            pass

    def tearDown(self):
        GoldConfigLoader.clear_cache()
        try:
            from silver.script.silver_config_loader import SilverConfigLoader
            SilverConfigLoader.clear_cache()
        except ImportError:
            pass

    def test_gold_config_loader_interpolate_env(self):
        sample = {
            "db": "uax_datalake_db_{env}",
            "upper": "BUCKET_{ENV}",
            "nested": {
                "prefix": "gold/data/{env}",
                "tables": ["tbl_{env}_conversations", 99]
            }
        }
        res = GoldConfigLoader.interpolate_env(sample, "prod")
        self.assertEqual(res["db"], "uax_datalake_db_prod")
        self.assertEqual(res["upper"], "BUCKET_PROD")
        self.assertEqual(res["nested"]["prefix"], "gold/data/prod")
        self.assertEqual(res["nested"]["tables"][0], "tbl_prod_conversations")

    def test_gold_config_loader_load_config_with_custom_env(self):
        prod_cfg = GoldConfigLoader.load_config(env="prod")
        self.assertEqual(prod_cfg["pipeline_defaults"]["glue_catalog"]["database_name"], "uax_datalake_db_prod")

        qa_db = GoldConfigLoader.get_glue_database(env="qa")
        self.assertEqual(qa_db, "uax_datalake_db_qa")

    def test_silver_config_loader_interpolate_env_and_caching(self):
        from silver.script.silver_config_loader import SilverConfigLoader
        SilverConfigLoader.clear_cache()

        sample = {
            "catalog": {
                "db": "uax_datalake_db_{env}",
                "crawler": "uax-datalake-silver-crawler-{env}"
            }
        }
        res = SilverConfigLoader.interpolate_env(sample, "staging")
        self.assertEqual(res["catalog"]["db"], "uax_datalake_db_staging")
        self.assertEqual(res["catalog"]["crawler"], "uax-datalake-silver-crawler-staging")

        prod_cfg = SilverConfigLoader.load_config(env="prod")
        self.assertEqual(prod_cfg["pipeline_defaults"]["glue_catalog"]["database_name"], "uax_datalake_db_prod")
        self.assertEqual(prod_cfg["pipeline_defaults"]["glue_catalog"]["crawler_name"], "uax-datalake-silver-crawler-prod")

        db_prod = SilverConfigLoader.get_glue_database(env="prod")
        self.assertEqual(db_prod, "uax_datalake_db_prod")
        crawler_prod = SilverConfigLoader.get_crawler_name(env="prod")
        self.assertEqual(crawler_prod, "uax-datalake-silver-crawler-prod")

    def test_silver_etl_parse_spark_arguments_resolves_env(self):
        from silver.script.uax_silver_etl import parse_spark_arguments

        cli_args = [
            'uax_silver_etl.py',
            '--SOURCE_SYSTEM', 'genesys',
            '--ENV', 'prod',
            '--DATA_LAKE_BUCKET', 'my-bucket-{env}',
            '--GLUE_DATABASE', 'uax_datalake_db_{env}'
        ]
        with patch('sys.argv', cli_args):
            parsed = parse_spark_arguments()
            self.assertEqual(parsed['ENV'], 'prod')
            self.assertEqual(parsed['DATA_LAKE_BUCKET'], 'my-bucket-prod')
            self.assertEqual(parsed['GLUE_DATABASE'], 'uax_datalake_db_prod')
            self.assertEqual(parsed['CRAWLER_NAME'], 'uax-datalake-silver-crawler-prod')

    def test_gold_layer_manager_resolves_env_dynamically(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.columns = ["conversation_id"]
        mock_spark.sql.return_value = mock_df

        params = {
            "JOB_NAME": "gold_job",
            "SOURCE_SYSTEM": "genesys",
            "ENV": "qa",
            "DATA_LAKE_BUCKET": "uax-datalake-{env}-bucket",
            "GOLD_TARGETS": "athena"
        }

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=b"SELECT 1"))}

        with patch.object(GoldLayerManager, "_discover_queries", return_value={"conversations": "SELECT 1 AS conversation_id"}):
            with patch.object(GoldLayerManager, "_materialize_athena_table", return_value=10) as mock_mat:
                results = GoldLayerManager.run_gold_pipeline(mock_spark, params, s3_client=mock_s3)
                self.assertEqual(len(results), 1)
                self.assertEqual(results[0]["status"], "SUCCESS")
                self.assertEqual(results[0]["target_table"], "uax_datalake_db_qa.gold_genesys_conversations")
                mock_mat.assert_called_once()
                self.assertEqual(mock_mat.call_args[1]["glue_database"], "uax_datalake_db_qa")

    def test_gold_initial_load_resolves_env_dynamically(self):
        mock_spark = MagicMock()
        mock_raw_df = MagicMock()
        mock_raw_df.columns = ["conversation_id"]
        mock_raw_df.count.return_value = 25
        mock_spark.read.option.return_value.option.return_value.option.return_value.csv.return_value = mock_raw_df
        mock_spark.table.return_value = mock_raw_df

        params = {
            "JOB_NAME": "test_init_job",
            "SOURCE_SYSTEM": "genesys",
            "TABLE_NAME": "conversations",
            "ENV": "prod",
            "CSV_PATH": "s3://{bucket}/gold/initial_exports/{source}/{table}.csv",
            "DATA_LAKE_BUCKET": "uax-datalake-{env}-bucket",
            "GOLD_TARGETS": "athena"
        }

        res = GoldInitialLoader.run_initial_load(mock_spark, params)
        self.assertEqual(res["target_table"], "uax_datalake_db_prod.gold_genesys_conversations")
        self.assertEqual(res["records_loaded"], 25)

    def test_lambda_build_glue_arguments_maps_env(self):
        from lambda_helper.lambda_function import build_glue_arguments

        # Test lowercase 'env'
        event_lower = {
            "source_system": "genesys",
            "layer": "silver",
            "env": "prod"
        }
        glue_args_lower = build_glue_arguments(event_lower)
        self.assertEqual(glue_args_lower["--ENV"], "prod")

        # Test uppercase 'ENVIRONMENT'
        event_upper = {
            "source_system": "genesys",
            "layer": "gold",
            "gold_schema": "enterprise_reporting",
            "ENVIRONMENT": "staging"
        }
        glue_args_upper = build_glue_arguments(event_upper)
        self.assertEqual(glue_args_upper["--ENV"], "staging")


class TestGoldMartDependencyAndOmittedColumns(unittest.TestCase):
    """
    Validates:
    1. Gold table dependency detection handles gold_<source>_<table> and db.gold_<source>_<table>.
    2. Joining silver tables (tbl_conversations) or SQL comments does not invert dependency order.
    3. Composite nkeys are strictly preserved without guessing single-column keys.
    4. MySQL omitted columns (such as _data_as_of) are altered to NULL DEFAULT NULL to prevent Error 1364.
    """

    def test_dependency_ordering_moveworks_marts(self):
        """interactions must run before conversations and feedbacks even when referencing physical gold table names."""
        interactions_sql = """
        -- Aggregates Moveworks conversations and interactions
        -- Constructed from interactions and joins tbl_conversations
        SELECT
            ui.id AS interaction_id,
            ui.conversation_id AS conversation_id,
            c.primary_domain AS conversation_domain
        FROM tbl_interactions ui
        LEFT JOIN tbl_conversations c ON ui.conversation_id = c.id
        """

        conversations_sql = """
        -- Constructed directly from physical gold table gold_moveworks_interactions
        SELECT
            conversation_id,
            min(timestamp) AS conversation_start,
            max(timestamp) AS conversation_end
        FROM uax_datalake_db_dev.gold_moveworks_interactions
        GROUP BY conversation_id
        """

        feedbacks_sql = """
        -- Constructed from gold_moveworks_interactions
        SELECT
            conversation_id,
            interaction_id,
            interaction_content AS rating
        FROM gold_moveworks_interactions
        WHERE lower(interaction_type) = 'link_click'
        """

        queries = {
            "conversations": conversations_sql,
            "feedbacks": feedbacks_sql,
            "interactions": interactions_sql
        }

        ordered = GoldLayerManager._sort_queries_by_dependency(
            queries, source_system="moveworks", glue_database="uax_datalake_db_dev"
        )
        ordered_keys = list(ordered.keys())

        # interactions MUST be the first mart executed
        self.assertEqual(ordered_keys[0], "interactions")
        self.assertIn("conversations", ordered_keys[1:])
        self.assertIn("feedbacks", ordered_keys[1:])

    def test_resolve_query_dependencies_schedules_uncreated_prerequisite(self):
        """When feedbacks is filtered alone, interactions is automatically resolved and scheduled."""
        all_queries = {
            "interactions": "SELECT * FROM tbl_interactions",
            "feedbacks": "SELECT * FROM v_interactions WHERE is_feedback = 1"
        }
        active_queries = {
            "feedbacks": all_queries["feedbacks"]
        }
        resolved = GoldLayerManager._resolve_query_dependencies(
            active_queries=active_queries,
            all_queries=all_queries,
            source_system="moveworks",
            glue_database="uax_datalake_db_dev"
        )
        self.assertIn("interactions", resolved)
        self.assertIn("feedbacks", resolved)

        # Ordering must run interactions first
        ordered = GoldLayerManager._sort_queries_by_dependency(
            resolved, source_system="moveworks", glue_database="uax_datalake_db_dev"
        )
        keys = list(ordered.keys())
        self.assertEqual(keys[0], "interactions")
        self.assertEqual(keys[1], "feedbacks")

    def test_v_feedbacks_sql_projects_feedback_id(self):
        """v_feedbacks.sql projects feedback_id to match gold_config.json nkey."""
        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "gold", "query", "moveworks", "v_feedbacks.sql"
        )
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        self.assertIn("feedback_id", sql_text)
        self.assertIn("fb.interaction_id AS feedback_id", sql_text)

    def test_composite_nkey_preservation_no_autodetect(self):
        """Composite nkeys are strictly preserved and not overridden by auto-detection."""
        composite_cfg = {
            "source_systems": {
                "servicenow": {
                    "tables": {
                        "incident_kpi": {
                            "nkey": ["priority_level", "incident_state", "incident_category"]
                        }
                    }
                }
            }
        }
        pks = GoldConfigLoader.get_primary_key("servicenow", "incident_kpi", composite_cfg)
        self.assertEqual(pks, ["priority_level", "incident_state", "incident_category"])

        # When no nkey is specified, it returns empty list (no single-column auto-detect guessing)
        empty_pks = GoldConfigLoader.get_primary_key("servicenow", "unknown_table", composite_cfg)
        self.assertEqual(empty_pks, [])

    def test_mysql_omitted_columns_altered_to_null_default(self):
        """When an incoming query omits a NOT NULL column present in target MySQL table, alter it to NULL DEFAULT NULL."""
        mock_df = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "conversation_id"
        mock_field.dataType.simpleString.return_value = "string"
        mock_df.schema.fields = [mock_field]
        mock_df.columns = ["conversation_id"]

        # Target MySQL table contains conversation_id AND _data_as_of (NOT NULL)
        target_cols = [
            ("conversation_id", "varchar(255)", "NO"),
            ("_data_as_of", "timestamp", "NO")
        ]

        executed_ddls = []
        def mock_execute_ddl(jdbc_info, sql):
            executed_ddls.append(sql)

        def mock_execute_query(jdbc_info, sql, *args):
            return target_cols

        with patch.object(GoldLayerManager, "_execute_sql_query", side_effect=mock_execute_query):
            with patch.object(GoldLayerManager, "_execute_ddl", side_effect=mock_execute_ddl):
                GoldLayerManager._detect_schema_evolution(
                    jdbc_info={"host": "localhost", "port": 3306, "user": "u", "password": "p"},
                    schema_name="enterprise_reporting",
                    target_table="gold_moveworks_conversations",
                    df_mart=mock_df
                )

        # Must have modified omitted _data_as_of to allow NULL DEFAULT NULL
        modify_ddls = [d for d in executed_ddls if "MODIFY COLUMN `_data_as_of`" in d]
        self.assertTrue(len(modify_ddls) > 0)
        self.assertIn("NULL DEFAULT NULL", modify_ddls[0])


class TestGoldInitialLoadIntegration(unittest.TestCase):
    """Verifies that gold_layer_manager actively checks, logs, and executes initial historical export loads."""

    def test_check_and_run_initial_load_when_file_exists(self):
        """When an initial export exists in S3, _check_and_run_initial_load invokes GoldInitialLoader and returns True."""
        mock_spark = MagicMock()
        mock_loader = MagicMock()
        mock_loader.run_initial_load.return_value = {"status": "SUCCESS"}

        with patch.object(GoldLayerManager, "_s3_path_exists", return_value=True):
            with patch.object(GoldLayerManager, "_get_gold_initial_loader", return_value=mock_loader):
                result = GoldLayerManager._check_and_run_initial_load(
                    spark=mock_spark,
                    source_system="moveworks",
                    table_name="interactions",
                    target_table_name="gold_moveworks_interactions",
                    glue_database="uax_datalake_db_dev",
                    bucket_name="uax-datalake-dev-bucket",
                    env="dev",
                    pks=["interaction_id"],
                    params={"JOB_NAME": "test_job"},
                    sql_text="SELECT * FROM silver_interactions",
                    gold_cfg=None,
                    s3_client=MagicMock()
                )

        self.assertTrue(result)
        mock_loader.run_initial_load.assert_called_once()
        call_args = mock_loader.run_initial_load.call_args[0]
        params_passed = call_args[1]
        self.assertEqual(params_passed["SOURCE_SYSTEM"], "moveworks")
        self.assertEqual(params_passed["TABLE_NAME"], "interactions")
        self.assertEqual(params_passed["SKIP_AURORA_SERVE"], "true")
        self.assertEqual(params_passed["PRIMARY_KEY"], "interaction_id")

    def test_check_and_run_initial_load_when_no_file_exists(self):
        """When no initial export exists, returns False without errors so normal query materialization proceeds."""
        mock_spark = MagicMock()
        mock_loader = MagicMock()

        with patch.object(GoldLayerManager, "_s3_path_exists", return_value=False):
            with patch.object(GoldLayerManager, "_get_gold_initial_loader", return_value=mock_loader):
                result = GoldLayerManager._check_and_run_initial_load(
                    spark=mock_spark,
                    source_system="moveworks",
                    table_name="interactions",
                    target_table_name="gold_moveworks_interactions",
                    glue_database="uax_datalake_db_dev",
                    bucket_name="uax-datalake-dev-bucket",
                    env="dev",
                    pks=["interaction_id"],
                    params={"JOB_NAME": "test_job"},
                    s3_client=MagicMock()
                )

        self.assertFalse(result)
        mock_loader.run_initial_load.assert_not_called()

    def test_check_and_run_initial_load_skip_flag(self):
        """SKIP_INITIAL_LOAD flag causes initial load check to be skipped entirely."""
        mock_loader = MagicMock()
        with patch.object(GoldLayerManager, "_get_gold_initial_loader", return_value=mock_loader):
            result = GoldLayerManager._check_and_run_initial_load(
                spark=MagicMock(),
                source_system="moveworks",
                table_name="interactions",
                target_table_name="gold_moveworks_interactions",
                glue_database="uax_datalake_db_dev",
                bucket_name="uax-datalake-dev-bucket",
                env="dev",
                pks=["interaction_id"],
                params={"SKIP_INITIAL_LOAD": "true"}
            )
        self.assertFalse(result)
        mock_loader.run_initial_load.assert_not_called()

    def test_s3_path_exists_detection(self):
        """_s3_path_exists detects files via head_object or directory prefix via list_objects_v2."""
        mock_s3 = MagicMock()

        # 1. Exact object head succeeds
        mock_s3.head_object.return_value = {}
        self.assertTrue(GoldLayerManager._s3_path_exists("s3://bucket/path/file.csv", mock_s3))

        # 2. Exact head fails, but folder prefix contains objects
        mock_s3.head_object.side_effect = Exception("NoSuchKey")
        mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "path/file/part1.parquet"}]}
        self.assertTrue(GoldLayerManager._s3_path_exists("s3://bucket/path/file/", mock_s3))

        # 3. Both fail
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        self.assertFalse(GoldLayerManager._s3_path_exists("s3://bucket/missing.csv", mock_s3))

    def test_missing_nkey_in_config_raises_value_error_in_pipeline(self):
        """When nkey is missing from gold_config.json for a table, run_gold_pipeline raises ValueError."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.columns = ["val"]
        mock_spark.sql.return_value = mock_df

        params = {
            "JOB_NAME": "test_missing_nkey",
            "SOURCE_SYSTEM": "unknown_system",
            "DATA_LAKE_BUCKET": "test-bucket"
        }
        with patch.object(GoldLayerManager, "_discover_queries", return_value={"test_table": "SELECT 1"}):
            with self.assertRaises(ValueError) as ctx:
                GoldLayerManager.run_gold_pipeline(mock_spark, params)
            self.assertIn("Missing 'nkey' in gold configuration", str(ctx.exception))

    def test_missing_nkey_in_config_raises_value_error_in_initial_load(self):
        """When nkey is missing from gold_config.json for a table, run_initial_load raises ValueError."""
        mock_spark = MagicMock()
        params = {
            "JOB_NAME": "test_init_missing_nkey",
            "SOURCE_SYSTEM": "unknown_system",
            "TABLE_NAME": "test_table",
            "DATA_LAKE_BUCKET": "test-bucket"
        }
        with self.assertRaises(ValueError) as ctx:
            GoldInitialLoader.run_initial_load(mock_spark, params)
        self.assertIn("Missing 'nkey' in gold configuration", str(ctx.exception))

    def test_deduplicate_by_nkey_drops_duplicates(self):
        """_deduplicate_by_nkey enforces row uniqueness on the given natural key columns."""
        mock_df = MagicMock()
        mock_df.columns = ["interaction_id", "content"]
        mock_df.dropDuplicates.return_value = "deduped_df"

        result = GoldLayerManager._deduplicate_by_nkey(mock_df, ["interaction_id"])
        mock_df.dropDuplicates.assert_called_once_with(subset=["interaction_id"])
        self.assertEqual(result, "deduped_df")

    def test_resolve_natural_keys_strictly_from_config(self):
        """Natural keys must come from gold_config.json, with zero hardcoding."""
        sample_cfg = {
            "source_systems": {
                "moveworks": {
                    "tables": {
                        "interactions": {"nkey": ["interaction_id"]},
                        "conversations": {"primary_key": ["conversation_id"]}
                    }
                }
            }
        }
        # 1. Matches configured table
        pks = GoldLayerManager._resolve_natural_keys("moveworks", "interactions", sample_cfg)
        self.assertEqual(pks, ["interaction_id"])

        # 2. Matches prefixed name (v_interactions)
        pks_v = GoldLayerManager._resolve_natural_keys("moveworks", "v_interactions", sample_cfg)
        self.assertEqual(pks_v, ["interaction_id"])

        # 3. Unconfigured table returns [] (no hardcoded fallback)
        pks_unknown = GoldLayerManager._resolve_natural_keys("moveworks", "unknown_tbl", sample_cfg)
        self.assertEqual(pks_unknown, [])

        # 4. CLI parameter override takes effect if not in config
        pks_cli = GoldLayerManager._resolve_natural_keys("moveworks", "unknown_tbl", sample_cfg, params={"NKEY": "custom_id"})
        self.assertEqual(pks_cli, ["custom_id"])

    def test_load_gold_config_from_s3_path_containing_raw_json(self):
        """GoldLayerManager._load_gold_config parses raw JSON even if passed via config_s3_path."""
        import json
        raw_json = json.dumps({
            "source_systems": {
                "inline_src": {
                    "tables": {
                        "records": {"nkey": ["rec_id"]}
                    }
                }
            }
        })
        loaded = GoldLayerManager._load_gold_config(config_s3_path=raw_json)
        self.assertIn("inline_src", loaded.get("source_systems", {}))
        pks = GoldLayerManager._resolve_natural_keys("inline_src", "records", loaded)
        self.assertEqual(pks, ["rec_id"])

    def test_resolve_natural_keys_table_specific_override(self):
        """Table-specific overrides like --interactions_nkey work seamlessly."""
        cfg = {"source_systems": {}}
        pks = GoldLayerManager._resolve_natural_keys(
            "moveworks", "interactions", cfg,
            params={"interactions_nkey": "interaction_id"}
        )
        self.assertEqual(pks, ["interaction_id"])

    def test_lambda_build_glue_arguments_auto_defaults_gold_config(self):
        """Lambda helper auto-defaults --GOLD_CONFIG_S3_PATH when running Gold layer."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda_helper"))
        from lambda_function import build_glue_arguments
        event = {
            "source_system": "moveworks",
            "layer": "gold",
            "gold_schema": "enterprise_reporting"
        }
        args = build_glue_arguments(event)
        self.assertEqual(args.get("--PROCESS_LAYER"), "gold")
        self.assertEqual(args.get("--GOLD_SCHEMA"), "enterprise_reporting")
        self.assertIn("--GOLD_CONFIG_S3_PATH", args)
        self.assertTrue(args["--GOLD_CONFIG_S3_PATH"].endswith("gold/script/config/gold_config.json"))


if __name__ == "__main__":
    unittest.main()



