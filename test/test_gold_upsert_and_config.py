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


if __name__ == "__main__":
    unittest.main()
