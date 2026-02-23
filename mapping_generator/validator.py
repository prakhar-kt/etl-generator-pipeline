"""Validate generated YAML mapping files against expected schemas."""

import re
from pathlib import Path

import yaml


class ValidationError:
    def __init__(self, message: str, severity: str = "error"):
        self.message = message
        self.severity = severity  # "error" or "warning"

    def __repr__(self):
        return f"[{self.severity.upper()}] {self.message}"


class MappingValidator:
    """Validate generated YAML mapping files match expected Smart DTC patterns."""

    def validate(self, yaml_content: str, layer: str) -> list[ValidationError]:
        errors = []

        # Check valid YAML
        try:
            data = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            return [ValidationError(f"Invalid YAML: {e}")]

        if not isinstance(data, dict):
            return [ValidationError("YAML root must be a dictionary")]

        if layer in ("CDL", "Cleansed_RAW"):
            errors.extend(self._validate_dataflow(data))
        elif layer in ("BL", "BR"):
            errors.extend(self._validate_sql_mapping(data, layer))

        return errors

    def validate_file(self, file_path: str | Path, layer: str) -> list[ValidationError]:
        file_path = Path(file_path)
        if not file_path.exists():
            return [ValidationError(f"File not found: {file_path}")]
        content = file_path.read_text(encoding="utf-8")
        return self.validate(content, layer)

    def _validate_dataflow(self, data: dict) -> list[ValidationError]:
        """Validate CDL/Cleansed_RAW dataflow-style YAML."""
        errors = []

        if "dataflow" not in data:
            errors.append(ValidationError("Missing required top-level key: 'dataflow'"))
            return errors

        if not isinstance(data["dataflow"], list):
            errors.append(ValidationError("'dataflow' must be a list"))
            return errors

        for i, table in enumerate(data["dataflow"]):
            prefix = f"dataflow[{i}]"
            for key in ("source_dataset", "source_table", "target_dataset", "target_table", "fields"):
                if key not in table:
                    errors.append(ValidationError(f"{prefix}: missing required key '{key}'"))

            if "fields" in table and isinstance(table["fields"], list):
                for j, field in enumerate(table["fields"]):
                    for fkey in ("source_col", "target_col", "type"):
                        if fkey not in field:
                            errors.append(ValidationError(
                                f"{prefix}.fields[{j}]: missing required key '{fkey}'"
                            ))

                    # Validate type is a known BigQuery type
                    if "type" in field:
                        valid_types = {
                            "STRING", "INT64", "INTEGER", "NUMERIC", "FLOAT64", "FLOAT",
                            "BOOL", "BOOLEAN", "DATE", "DATETIME", "TIMESTAMP", "TIME",
                            "BYTES", "GEOGRAPHY", "JSON", "BIGNUMERIC",
                        }
                        if field["type"].upper() not in valid_types:
                            errors.append(ValidationError(
                                f"{prefix}.fields[{j}]: unknown type '{field['type']}'",
                                severity="warning",
                            ))

            # Check for recommended keys
            for opt_key in ("primary_keys", "foreign_keys"):
                if opt_key not in table:
                    errors.append(ValidationError(
                        f"{prefix}: missing recommended key '{opt_key}'",
                        severity="warning",
                    ))

        return errors

    def _validate_sql_mapping(self, data: dict, layer: str) -> list[ValidationError]:
        """Validate BL/BR SQL-style YAML."""
        errors = []

        # Check metadata
        if "metadata" not in data:
            errors.append(ValidationError("Missing required top-level key: 'metadata'"))
        else:
            meta = data["metadata"]
            for key in ("stage_name", "source_table_names", "target_table_name"):
                if key not in meta:
                    errors.append(ValidationError(f"metadata: missing required key '{key}'"))

            # Validate stage_name matches layer
            expected_stages = {
                "BL": ["CDL to BL", "BL to BL", "CDL FACTS to BL"],
                "BR": ["BL to BR"],
            }
            if "stage_name" in meta and meta["stage_name"] not in expected_stages.get(layer, []):
                errors.append(ValidationError(
                    f"metadata.stage_name '{meta['stage_name']}' unexpected for {layer} layer",
                    severity="warning",
                ))

        # Check create_table
        if "create_table" not in data:
            errors.append(ValidationError("Missing required top-level key: 'create_table'"))
        else:
            ct = data["create_table"]
            if "CREATE TABLE" not in ct.upper() and "CREATE OR REPLACE" not in ct.upper():
                errors.append(ValidationError("create_table: does not contain CREATE TABLE statement"))
            if "{{ target_project }}" not in ct and "{{target_project}}" not in ct:
                errors.append(ValidationError(
                    "create_table: missing Jinja2 template variable '{{ target_project }}'",
                    severity="warning",
                ))
            # Check for ADMIN columns
            if "ADMIN_LOAD_DATE" not in ct:
                errors.append(ValidationError(
                    "create_table: missing ADMIN_LOAD_DATE column",
                    severity="warning",
                ))

        # Check get_max_date
        if "get_max_date" not in data:
            errors.append(ValidationError(
                "Missing recommended key: 'get_max_date'",
                severity="warning",
            ))

        # Check for merge or other statement
        has_merge = "merge_statement" in data
        has_other = "other_statement" in data
        if not has_merge and not has_other:
            errors.append(ValidationError(
                "Missing required key: 'merge_statement' or 'other_statement'"
            ))

        # Validate SQL content
        if has_merge:
            sql = data["merge_statement"]
            if "MERGE" not in sql.upper():
                errors.append(ValidationError("merge_statement: does not contain MERGE keyword"))
            if "WHEN MATCHED" not in sql.upper() and "WHEN NOT MATCHED" not in sql.upper():
                errors.append(ValidationError(
                    "merge_statement: missing WHEN MATCHED/NOT MATCHED clauses",
                    severity="warning",
                ))

        return errors
