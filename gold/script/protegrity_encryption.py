"""
Protegrity Enterprise Encryption & Tokenization Module.
Shared between Bronze Layer (uax_bronze_load.py) and Gold Layer (gold_initial_load.py).

Features:
1. Multi-Column & Multi-Row Batching:
   - Slices datasets into configurable micro-batches (default: 500 records) to stay well under AWS Lambda 6MB limits.
   - Packages all configured PII columns into a single synchronous Lambda invocation per batch.
2. Jitter Exponential Backoff:
   - Implements randomized exponential jitter backoff matching protegrity-test.py up to max_attempts (5).
3. Dual Processing Engine:
   - encrypt_record_chunk(): For Bronze in-memory Python dictionaries (List[Dict[str, Any]]).
   - encrypt_spark_dataframe(): For Gold PySpark DataFrames (using mapInPandas across Spark workers).
4. Dynamic Secret Resolution:
   - Resolves Protegrity user identity secret from AWS Secrets Manager or direct configuration.
"""

import json
import logging
import random
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Global singleton cache for ProtegrityEncryptionManager
_MANAGER_CACHE: Dict[str, Any] = {}


class ProtegrityEncryptionManager:
    """
    Manages communication with the Protegrity Cloud Protector Lambda service.
    """

    def __init__(self, global_config: Optional[Dict[str, Any]] = None, env: str = "dev"):
        """
        Initializes the encryption manager from pipeline_defaults.

        Args:
            global_config: The pipeline_defaults dictionary or full configuration dictionary.
            env: Target environment name ('dev', 'stage', 'prod').
        """
        self.env = env.strip().lower() if env else "dev"
        cfg = global_config or {}

        # Look for encryption block in pipeline_defaults or top-level
        self.enc_defaults = cfg.get("encryption", {}) if "encryption" in cfg else cfg

        self.enabled = bool(self.enc_defaults.get("enabled", False))
        
        # Resolve Lambda ARN (validated lazily when encryption is actually triggered for table columns)
        raw_arn = self.enc_defaults.get("lambda_arn", "")
        self.lambda_arn = str(raw_arn).replace("{ENV}", self.env.upper()).replace("{env}", self.env.lower()).strip() if raw_arn else ""
        
        # Batch size (default: 500 records per invocation)
        self.batch_size = int(self.enc_defaults.get("batch_size", 500))
        self.default_data_element = str(self.enc_defaults.get("default_data_element", "AES256")).strip()
        self.default_encoding = str(self.enc_defaults.get("default_encoding", "utf8")).strip()
        self.max_retries = int(self.enc_defaults.get("max_retries", 5))

        # Resolve AWS Region from Lambda ARN
        self.region = self._extract_region_from_arn(self.lambda_arn)
        self._lambda_client = None
        self._user_secret = None

    @staticmethod
    def _extract_region_from_arn(arn: str) -> str:
        """Extracts the AWS region from a Lambda ARN."""
        parts = arn.split(":")
        return parts[3] if len(parts) >= 4 else "us-east-2"

    @property
    def lambda_client(self):
        """Lazy-loaded Boto3 Lambda client."""
        if self._lambda_client is None:
            self._lambda_client = boto3.client("lambda", region_name=self.region)
        return self._lambda_client

    def _resolve_user_secret(self) -> str:
        """
        Resolves the Protegrity user identity secret key:
        1. Checks for 'encryption_key' directly in config.
        2. Retrieves 'user_secret_key' from AWS Secrets Manager ('secret_name').
        """
        if self._user_secret is not None:
            return self._user_secret

        # 1. Direct configuration
        direct_key = self.enc_defaults.get("encryption_key") or self.enc_defaults.get("user_secret")
        if direct_key:
            self._user_secret = str(direct_key).strip()
            return self._user_secret

        # 2. AWS Secrets Manager
        secret_name = self.enc_defaults.get("secret_name", f"uax/protegrity-credentials-{self.env}")
        secret_name = secret_name.replace("{ENV}", self.env.upper()).replace("{env}", self.env.lower())
        user_key = self.enc_defaults.get("user_secret_key", "protegrity_user_secret")

        try:
            sm_client = boto3.client("secretsmanager", region_name=self.region)
            secret_val = sm_client.get_secret_value(SecretId=secret_name)
            raw = secret_val.get("SecretString", "{}")
            parsed = json.loads(raw) if raw.startswith("{") else {user_key: raw}
            resolved = parsed.get(user_key, raw)
            self._user_secret = str(resolved).strip()
            return self._user_secret
        except Exception as err:
            logger.warning(
                f"[Protegrity] Could not resolve secret '{user_key}' from Secrets Manager '{secret_name}': {err}"
            )
            self._user_secret = ""
            return ""

    def normalize_columns_config(
        self,
        columns_cfg: Union[Dict[str, Any], List[str], str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Normalizes column configurations into a uniform dictionary mapping:
        { "col_name": { "data_element": "...", "encoding": "utf8" } }
        """
        normalized: Dict[str, Dict[str, str]] = {}
        if not columns_cfg:
            return normalized

        if isinstance(columns_cfg, str):
            columns_cfg = [c.strip() for c in columns_cfg.split(",") if c.strip()]

        if isinstance(columns_cfg, list):
            for col in columns_cfg:
                c_name = str(col).strip().lower()
                if c_name:
                    normalized[c_name] = {
                        "data_element": self.default_data_element,
                        "encoding": self.default_encoding
                    }
        elif isinstance(columns_cfg, dict):
            for col, val in columns_cfg.items():
                c_name = str(col).strip().lower()
                if not c_name:
                    continue
                if isinstance(val, str):
                    normalized[c_name] = {
                        "data_element": val.strip(),
                        "encoding": self.default_encoding
                    }
                elif isinstance(val, dict):
                    normalized[c_name] = {
                        "data_element": str(val.get("data_element", self.default_data_element)).strip(),
                        "encoding": str(val.get("encoding", self.default_encoding)).strip()
                    }
        return normalized

    def _invoke_protector_lambda(
        self,
        action: str,
        arguments: List[Dict[str, Any]],
        client: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Invokes the Protegrity Protector Lambda service with exponential jitter retry.

        Args:
            action: 'protect' (encrypt) or 'unprotect' (decrypt).
            arguments: List of column argument dictionaries for the Protegrity API.
            client: Optional pre-instantiated boto3 lambda client.

        Returns:
            Dict containing the parsed Protegrity response body (including 'results').
        """
        active_client = client or self.lambda_client
        user_secret = self._resolve_user_secret()

        inner_body = {
            "query_id": str(uuid.uuid4()),
            "user": user_secret,
            "arguments": arguments
        }
        envelope = {
            "httpMethod": "POST",
            "path": f"/v2/{action}",
            "body": json.dumps(inner_body)
        }

        for attempt in range(self.max_retries):
            try:
                resp = active_client.invoke(
                    FunctionName=self.lambda_arn,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(envelope).encode("utf-8")
                )

                status_code = resp.get("StatusCode", 0)
                if status_code != 200:
                    func_err = resp.get("FunctionError", "Unknown")
                    raise RuntimeError(f"Lambda returned HTTP status {status_code}, error: {func_err}")

                raw_payload = json.loads(resp["Payload"].read().decode("utf-8"))
                
                # Check API Gateway / Protegrity status code
                gw_status = raw_payload.get("statusCode", 200)
                if gw_status == 200:
                    body_val = raw_payload.get("body", "{}")
                    return json.loads(body_val) if isinstance(body_val, str) else body_val

                raise RuntimeError(
                    f"Protegrity API returned error code {gw_status}: {raw_payload}"
                )

            except ClientError as ce:
                err_code = ce.response.get("Error", {}).get("Code", "")
                if err_code in ("TooManyRequestsException", "ThrottlingException") and attempt < self.max_retries - 1:
                    pass  # Retry with backoff
                else:
                    raise
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"[Protegrity] Failed after {self.max_retries} attempts: {e}")
                    raise

            # Exponential backoff with randomized jitter
            backoff_sec = min(10.0, (2 ** attempt)) * (random.randint(0, 1000) / 1000.0)
            logger.warning(
                f"[Protegrity] Throttling encountered. Backing off for {backoff_sec:.2f}s "
                f"(attempt {attempt + 1}/{self.max_retries})..."
            )
            time.sleep(backoff_sec)

        raise TimeoutError(f"Protegrity Lambda call failed after {self.max_retries} attempts.")

    def _validate_lambda_arn(self) -> None:
        """Validates that lambda_arn is configured when encryption is actively invoked."""
        if not self.lambda_arn or not str(self.lambda_arn).strip():
            raise ValueError(
                "CRITICAL CONFIGURATION ERROR: Protegrity encryption is triggered for table columns, "
                "but 'lambda_arn' is not configured.\n"
                "Please configure 'lambda_arn' in your pipeline configuration under 'pipeline_defaults.encryption'.\n\n"
                "Configuration Example (in bronze_config.json / gold_config.json):\n"
                "{\n"
                '  "pipeline_defaults": {\n'
                '    "encryption": {\n'
                '      "enabled": true,\n'
                '      "lambda_arn": "arn:aws:lambda:<region>:<account_id>:function:<protegrity-protector-lambda-name>-{ENV}",\n'
                '      "batch_size": 500,\n'
                '      "secret_name": "uax/protegrity-credentials-{env}",\n'
                '      "user_secret_key": "protegrity_user_secret"\n'
                "    }\n"
                "  }\n"
                "}\n"
            )

    # =========================================================================
    # BRONZE LAYER ENTRYPOINT: List of Dictionaries
    # =========================================================================
    def encrypt_record_chunk(
        self,
        records: List[Dict[str, Any]],
        table_columns_config: Union[Dict[str, Any], List[str], str],
        action: str = "protect"
    ) -> List[Dict[str, Any]]:
        """
        Encrypts in-memory chunk of records in-place, slicing into micro-batches of batch_size (500).

        Args:
            records: List of record dictionaries in current batch chunk.
            table_columns_config: Column configuration defining target columns and data elements.
            action: 'protect' (encrypt) or 'unprotect' (decrypt).

        Returns:
            List[Dict[str, Any]]: Records chunk with sensitive fields replaced with tokens in-place.
        """
        # Immediate short-circuit: if encryption_columns is not present or empty, return immediately
        if not table_columns_config:
            return records

        target_cols = self.normalize_columns_config(table_columns_config)
        if not target_cols:
            return records

        if not self.enabled or not records:
            return records

        self._validate_lambda_arn()

        total_records = len(records)

        # Loop through dataset in slices of batch_size (e.g. 500 records)
        for start_idx in range(0, total_records, self.batch_size):
            end_idx = min(start_idx + self.batch_size, total_records)
            slice_records = records[start_idx:end_idx]

            arguments = []
            tracking_map: Dict[str, List[Tuple[int, str]]] = {}

            # Build argument for each configured column
            for col_name, meta in target_cols.items():
                data_values = []
                row_positions = []

                for row_offset, rec in enumerate(slice_records):
                    if not isinstance(rec, dict):
                        continue
                    # Case-insensitive column key matching
                    found_key = next((k for k in rec.keys() if k.lower() == col_name), None)
                    if found_key:
                        val = rec.get(found_key)
                        if val is not None and str(val).strip() != "":
                            data_values.append(str(val))
                            row_positions.append((start_idx + row_offset, found_key))

                if data_values:
                    arg_id = f"protect_{col_name}"
                    arguments.append({
                        "id": arg_id,
                        "data_element": meta["data_element"],
                        "encoding": meta["encoding"],
                        "data": data_values
                    })
                    tracking_map[arg_id] = row_positions

            # If no sensitive values found in this 500-record slice, continue
            if not arguments:
                continue

            # Invoke Protegrity Lambda for this 500-record slice
            resp = self._invoke_protector_lambda(action=action, arguments=arguments)

            # Replace original plaintext values with returned tokens
            results_list = resp.get("results", [])
            for res_block in results_list:
                arg_id = res_block.get("id")
                tokens = res_block.get("results", [])
                positions = tracking_map.get(arg_id, [])

                if len(tokens) != len(positions):
                    raise ValueError(
                        f"[Protegrity] Token count mismatch for '{arg_id}': "
                        f"Expected {len(positions)} tokens, received {len(tokens)}."
                    )

                for (target_idx, target_key), token in zip(positions, tokens):
                    records[target_idx][target_key] = token

        return records

    # =========================================================================
    # GOLD LAYER ENTRYPOINT: PySpark DataFrame
    # =========================================================================
    def encrypt_spark_dataframe(
        self,
        spark_df,
        table_columns_config: Union[Dict[str, Any], List[str], str],
        action: str = "protect"
    ):
        """
        Encrypts a PySpark DataFrame in parallel across partitions using mapInPandas.
        Avoids driver memory bottlenecks during large historical initial loads.

        Args:
            spark_df: Input PySpark DataFrame.
            table_columns_config: Column configuration defining target columns and data elements.
            action: 'protect' (encrypt) or 'unprotect' (decrypt).

        Returns:
            PySpark DataFrame with sensitive columns encrypted.
        """
        # Immediate short-circuit: if encryption_columns is not present or empty, return immediately
        if not table_columns_config or spark_df is None:
            return spark_df

        target_cols = self.normalize_columns_config(table_columns_config)
        if not target_cols:
            return spark_df

        if not self.enabled:
            return spark_df

        self._validate_lambda_arn()

        # Verify which target columns are present in the DataFrame schema
        df_cols_lower = {c.lower(): c for c in spark_df.columns}
        active_cols = {col_k: meta for col_k, meta in target_cols.items() if col_k in df_cols_lower}

        if not active_cols:
            logger.info("[Protegrity] None of the configured encryption columns are present in DataFrame. Skipping.")
            return spark_df

        # State dictionary broadcasted to Spark worker tasks
        worker_state = {
            "lambda_arn": self.lambda_arn,
            "region": self.region,
            "user_secret": self._resolve_user_secret(),
            "batch_size": self.batch_size,
            "max_retries": self.max_retries,
            "target_cols": active_cols,
            "action": action
        }

        def process_partition(iterator):
            """Executes on Spark worker per partition."""
            client = boto3.client("lambda", region_name=worker_state["region"])
            b_size = worker_state["batch_size"]
            lambda_arn = worker_state["lambda_arn"]
            user_secret = worker_state["user_secret"]
            active_target_cols = worker_state["target_cols"]
            act = worker_state["action"]

            for pdf in iterator:
                records = pdf.to_dict(orient="records")
                total_rows = len(records)

                # Process partition in slices of 500 records
                for start_idx in range(0, total_rows, b_size):
                    end_idx = min(start_idx + b_size, total_rows)
                    slice_records = records[start_idx:end_idx]

                    arguments = []
                    pos_map: Dict[str, List[Tuple[int, str]]] = {}

                    for col_name, meta in active_target_cols.items():
                        vals, pos = [], []
                        for offset, r in enumerate(slice_records):
                            # Case-insensitive column match
                            actual_key = next((k for k in r.keys() if k.lower() == col_name), None)
                            if actual_key and r[actual_key] is not None and str(r[actual_key]).strip() != "":
                                vals.append(str(r[actual_key]))
                                pos.append((start_idx + offset, actual_key))

                        if vals:
                            aid = f"protect_{col_name}"
                            arguments.append({
                                "id": aid,
                                "data_element": meta["data_element"],
                                "encoding": meta["encoding"],
                                "data": vals
                            })
                            pos_map[aid] = pos

                    if not arguments:
                        continue

                    # Invoke Lambda for this 500-row slice
                    envelope = {
                        "httpMethod": "POST",
                        "path": f"/v2/{act}",
                        "body": json.dumps({
                            "query_id": str(uuid.uuid4()),
                            "user": user_secret,
                            "arguments": arguments
                        })
                    }

                    resp = client.invoke(
                        FunctionName=lambda_arn,
                        InvocationType="RequestResponse",
                        Payload=json.dumps(envelope).encode("utf-8")
                    )
                    raw_p = json.loads(resp["Payload"].read().decode("utf-8"))
                    body_val = raw_p.get("body", "{}")
                    body_obj = json.loads(body_val) if isinstance(body_val, str) else body_val

                    for res_block in body_obj.get("results", []):
                        aid = res_block.get("id")
                        tokens = res_block.get("results", [])
                        for (target_idx, target_col), tkn in zip(pos_map.get(aid, []), tokens):
                            records[target_idx][target_col] = tkn

                import pandas as pd
                yield pd.DataFrame(records)

        return spark_df.mapInPandas(process_partition, schema=spark_df.schema)


def get_encryption_manager(
    global_config: Optional[Dict[str, Any]] = None,
    env: str = "dev"
) -> ProtegrityEncryptionManager:
    """
    Returns a cached or new instance of ProtegrityEncryptionManager.
    """
    env_clean = env.strip().lower() if env else "dev"
    if env_clean not in _MANAGER_CACHE:
        _MANAGER_CACHE[env_clean] = ProtegrityEncryptionManager(global_config=global_config, env=env_clean)
    return _MANAGER_CACHE[env_clean]
