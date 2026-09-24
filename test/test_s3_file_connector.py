"""
Unit tests for S3FileConnector verifying:
1. File discovery and AWS S3 LastModified sorting.
2. Ingestion of files matching pattern <tablename_mmddyyyyhh:mm:ss>.csv.
3. fetch_mode='latest' (loads only the single most recently modified file).
4. fetch_mode='all' (loads all files modified after watermark).
5. CLI parameter override handling for --FETCH_MODE.
"""

import os
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Mock dateutil if not present in the local environment
if 'dateutil' not in sys.modules:
    d = types.ModuleType('dateutil')
    d.tz = MagicMock()
    d.parser = MagicMock()
    sys.modules['dateutil'] = d
    sys.modules['dateutil.tz'] = d.tz
    sys.modules['dateutil.parser'] = d.parser

# Ensure repository root and bronze script directory are in sys.path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
bronze_script_dir = os.path.join(repo_root, "bronze", "script")
for p in [repo_root, bronze_script_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

from bronze.script.connectors.s3_file import S3FileConnector


class TestS3FileConnector(unittest.TestCase):

    def setUp(self):
        self.secret_dict = {}
        self.table_name = "conversations"
        self.source_config = {
            "source_bucket": "test-data-bucket",
            "file_prefix": "raw_feed/genesys/",
            "file_format": "csv",
            "delimiter": ",",
            "has_header": True,
            "encoding": "utf-8",
            "fetch_mode": "all"
        }

        # Mock CSV contents
        self.csv_file1 = (
            b"id,conversation_start,conversation_end,division_ids,media_types,originating_direction,customer_name,sentiment_score\n"
            b"conv_001,2026-09-24T09:00:00Z,2026-09-24T09:12:30Z,div_01,voice,inbound,Alice Smith,0.85\n"
            b"conv_002,2026-09-24T09:15:00Z,2026-09-24T09:28:10Z,div_01,chat,inbound,Bob Jones,0.62\n"
        )
        self.csv_file2 = (
            b"id,conversation_start,conversation_end,division_ids,media_types,originating_direction,customer_name,sentiment_score\n"
            b"conv_004,2026-09-24T10:05:00Z,2026-09-24T10:18:22Z,div_01,voice,inbound,Diana Prince,0.95\n"
        )

        # Mock S3 objects list with AWS LastModified datetimes
        self.s3_objects = [
            {
                "Key": "raw_feed/genesys/conversations_0924202610:00:00.csv",
                "LastModified": datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc),
            },
            {
                "Key": "raw_feed/genesys/conversations_0924202611:00:00.csv",
                "LastModified": datetime(2026, 9, 24, 11, 0, 0, tzinfo=timezone.utc),
            },
            # Another table in the same folder that should NOT be selected for conversations
            {
                "Key": "raw_feed/genesys/users_0924202610:00:00.csv",
                "LastModified": datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc),
            }
        ]

    def _setup_mock_s3(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        # Mock paginator for list_objects_v2
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": self.s3_objects}]
        mock_s3.get_paginator.return_value = mock_paginator

        def mock_get_object(Bucket, Key):
            if "conversations_0924202610:00:00.csv" in Key:
                return {"Body": MagicMock(read=lambda: self.csv_file1)}
            elif "conversations_0924202611:00:00.csv" in Key:
                return {"Body": MagicMock(read=lambda: self.csv_file2)}
            raise ValueError(f"Unexpected Key: {Key}")

        mock_s3.get_object.side_effect = mock_get_object
        return mock_s3

    @patch("bronze.script.connectors.s3_file.boto3.client")
    def test_fetch_mode_latest(self, mock_boto):
        mock_s3 = self._setup_mock_s3(mock_boto)

        config = dict(self.source_config)
        config["fetch_mode"] = "latest"

        ingested_records = []
        def chunk_callback(chunk, part):
            ingested_records.extend(chunk)

        S3FileConnector.fetch_delta(
            last_load_date="1900-01-01 00:00:00",
            secret_dict=self.secret_dict,
            table_name=self.table_name,
            source_config=config,
            on_chunk_callback=chunk_callback
        )

        # Only the latest file (11:00:00) should be read
        self.assertEqual(len(ingested_records), 1)
        self.assertEqual(ingested_records[0]["id"], "conv_004")
        self.assertEqual(ingested_records[0]["customer_name"], "Diana Prince")

    @patch("bronze.script.connectors.s3_file.boto3.client")
    def test_fetch_mode_all(self, mock_boto):
        mock_s3 = self._setup_mock_s3(mock_boto)

        config = dict(self.source_config)
        config["fetch_mode"] = "all"

        ingested_records = []
        def chunk_callback(chunk, part):
            ingested_records.extend(chunk)

        S3FileConnector.fetch_delta(
            last_load_date="2026-09-24 09:30:00",
            secret_dict=self.secret_dict,
            table_name=self.table_name,
            source_config=config,
            on_chunk_callback=chunk_callback
        )

        # Both files modified at 10:00 and 11:00 should be read (total 2 + 1 = 3 records)
        self.assertEqual(len(ingested_records), 3)
        self.assertEqual(ingested_records[0]["id"], "conv_001")
        self.assertEqual(ingested_records[1]["id"], "conv_002")
        self.assertEqual(ingested_records[2]["id"], "conv_004")

    @patch("bronze.script.connectors.s3_file.boto3.client")
    def test_cli_fetch_mode_override(self, mock_boto):
        mock_s3 = self._setup_mock_s3(mock_boto)

        config = dict(self.source_config)
        config["fetch_mode"] = "all"  # config says all, but CLI overrides to latest

        ingested_records = []
        def chunk_callback(chunk, part):
            ingested_records.extend(chunk)

        # Simulate CLI --FETCH_MODE latest
        test_argv = ["script.py", "--FETCH_MODE", "latest"]
        with patch.object(sys, "argv", test_argv):
            S3FileConnector.fetch_delta(
                last_load_date="1900-01-01 00:00:00",
                secret_dict=self.secret_dict,
                table_name=self.table_name,
                source_config=config,
                on_chunk_callback=chunk_callback
            )

        # Verified that CLI override took effect and only latest file was ingested
        self.assertEqual(len(ingested_records), 1)
        self.assertEqual(ingested_records[0]["id"], "conv_004")

    @patch("bronze.script.connectors.s3_file.boto3.client")
    def test_wildcard_file_path(self, mock_boto):
        mock_s3 = self._setup_mock_s3(mock_boto)

        # Configure file_path directly with wildcard pattern: raw_feed/genesys/conversations_*.csv/
        config = {
            "source_bucket": "test-data-bucket",
            "file_format": "csv",
            "tables": {
                "conversations": {
                    "file_path": "raw_feed/genesys/conversations_*.csv/",
                    "fetch_mode": "latest"
                }
            }
        }

        ingested_records = []
        def chunk_callback(chunk, part):
            ingested_records.extend(chunk)

        S3FileConnector.fetch_delta(
            last_load_date="1900-01-01 00:00:00",
            secret_dict=self.secret_dict,
            table_name=self.table_name,
            source_config=config,
            on_chunk_callback=chunk_callback
        )

        # Prefix is correctly resolved to "raw_feed/genesys/" and pattern to "conversations_*.csv"
        # Filtering matches conversations and ignores users_*.csv, picking latest
        self.assertEqual(len(ingested_records), 1)
        self.assertEqual(ingested_records[0]["id"], "conv_004")

    @patch("bronze.script.connectors.s3_file.boto3.client")
    def test_table_name_prefix_mismatch_with_wildcard(self, mock_boto):
        """
        Verify that when table_name is 'genesys_users', file_path is 'landing/genesys/users_*.csv',
        and S3 files are 'landing/genesys/users_09242026100000.csv', records are successfully extracted.
        """
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        s3_files = [
            {
                "Key": "landing/genesys/users_09242026100000.csv",
                "LastModified": datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc),
            }
        ]
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": s3_files}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"user_id,username\nu1,john\n")
        }

        config = {
            "source_bucket": "uax-datalake-{env}-bucket",
            "env": "dev",
            "file_format": "csv",
            "tables": {
                "genesys_users": {
                    "file_path": "landing/genesys/users_*.csv",
                    "fetch_mode": "all",
                    "initial_load_date": "2024-01-01 00:00:00"
                }
            }
        }

        ingested = []
        S3FileConnector.fetch_delta(
            last_load_date="2024-01-01 00:00:00",
            secret_dict={},
            table_name="genesys_users",
            source_config=config,
            on_chunk_callback=lambda chunk, part: ingested.extend(chunk)
        )

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0]["user_id"], "u1")
        self.assertEqual(ingested[0]["username"], "john")
        # Verify {env} was interpolated
        mock_paginator.paginate.assert_called_with(
            Bucket="uax-datalake-dev-bucket",
            Prefix="landing/genesys/"
        )


if __name__ == "__main__":
    unittest.main()

