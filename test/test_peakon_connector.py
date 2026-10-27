"""
Unit Tests for Peakon REST API Connector (PeakonConnector).
"""

import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bronze', 'script')))

from connectors import CONNECTOR_MAP, get_connector, PeakonConnector


class TestPeakonConnector(unittest.TestCase):

    def setUp(self):
        PeakonConnector._jwt_cache.clear()

    def test_connector_registration(self):
        """Verify peakon is registered in CONNECTOR_MAP and discoverable via get_connector()."""
        self.assertIn('peakon', CONNECTOR_MAP)
        self.assertIs(CONNECTOR_MAP['peakon'], PeakonConnector)
        self.assertIs(get_connector('peakon'), PeakonConnector)

    def test_direct_jwt_detection(self):
        """Verify tokens already formatted as JWT (ey... with 2 dots) are used directly without exchange."""
        jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozG48"
        token = PeakonConnector._get_bearer_token("https://api.peakon.com", jwt_token)
        self.assertEqual(token, jwt_token)

    @patch('urllib.request.urlopen')
    def test_custom_app_token_exchange(self, mock_urlopen):
        """Verify custom app access token is exchanged via POST /api/v1/auth/application."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "data": {
                "type": "bearer_tokens",
                "id": "jwt-exchanged-token-12345"
            }
        }).encode('utf-8')
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        raw_token = "custom_app_secret_abc123"
        token = PeakonConnector._get_bearer_token("https://api.peakon.com", raw_token)
        self.assertEqual(token, "jwt-exchanged-token-12345")
        self.assertIn(raw_token, PeakonConnector._jwt_cache)

    @patch.object(PeakonConnector, '_execute_request_with_retry')
    def test_fetch_delta_with_pagination_and_filtering(self, mock_request):
        """Verify cursor pagination via links.next and delta date filtering on responseAnsweredAt."""
        # Page 1 contains 2 answers: 1 newer than watermark, 1 older
        page1_resp = {
            "data": [
                {
                    "id": "ans_001",
                    "type": "answers_export",
                    "attributes": {
                        "answerId": 101,
                        "answerScore": 9,
                        "responseAnsweredAt": "2024-06-15T12:00:00.000Z",
                        "questionText": "How happy are you?"
                    }
                },
                {
                    "id": "ans_002",
                    "type": "answers_export",
                    "attributes": {
                        "answerId": 102,
                        "answerScore": 5,
                        "responseAnsweredAt": "2024-01-10T08:00:00.000Z",  # older than cutoff
                        "questionText": "How is workload?"
                    }
                }
            ],
            "links": {
                "next": "https://api.peakon.com/api/v1/answers/export?page=2"
            }
        }

        # Page 2 contains 1 newer answer, links.next is None
        page2_resp = {
            "data": [
                {
                    "id": "ans_003",
                    "type": "answers_export",
                    "attributes": {
                        "answerId": 103,
                        "answerScore": 8,
                        "responseAnsweredAt": "2024-07-01T15:30:00.000Z",
                        "questionText": "Do you feel supported?"
                    }
                }
            ],
            "links": {
                "next": None
            }
        }

        mock_request.side_effect = [page1_resp, page2_resp]

        secret_dict = {
            "peakon_api_token": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig",
            "api_base_url": "https://api.peakon.com"
        }
        source_config = {
            "base_url": "https://api.peakon.com",
            "tables": {
                "answers": {
                    "custom_endpoint": "/api/v1/answers/export"
                }
            }
        }

        chunks = []
        def chunk_callback(records, part_num):
            chunks.append((part_num, list(records)))

        # Cutoff date: 2024-06-01 (ans_002 is before, ans_001 and ans_003 are after)
        PeakonConnector.fetch_delta(
            last_load_date="2024-06-01 00:00:00",
            secret_dict=secret_dict,
            table_name="answers",
            source_config=source_config,
            on_chunk_callback=chunk_callback,
            s3_chunk_size=10
        )

        self.assertEqual(len(chunks), 1)
        part_num, recs = chunks[0]
        self.assertEqual(part_num, 1)
        self.assertEqual(len(recs), 2)
        # Verify unwrapped attributes
        self.assertEqual(recs[0]["id"], "ans_001")
        self.assertEqual(recs[0]["answerScore"], 9)
        self.assertEqual(recs[1]["id"], "ans_003")
        self.assertEqual(recs[1]["answerScore"], 8)


if __name__ == '__main__':
    unittest.main()
