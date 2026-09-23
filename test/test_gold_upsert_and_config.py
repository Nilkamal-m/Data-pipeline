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

        # Configured aurora table name for moveworks interactions
        aurora_tbl = GoldConfigLoader.get_target_table_name("moveworks", "interactions", "aurora")
        self.assertEqual(aurora_tbl, "gold_tbl_interactions")

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


if __name__ == "__main__":
    unittest.main()
