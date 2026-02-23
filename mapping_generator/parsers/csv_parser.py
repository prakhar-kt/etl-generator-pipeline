"""Parse CSV/Excel requirement files into structured mapping requirements."""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class FieldMapping:
    source_col: str
    target_col: str
    data_type: str
    is_key: bool = False
    transform: Optional[str] = None
    foreign_key_ref: Optional[str] = None
    value: Optional[str] = None  # static value when source_col is null


@dataclass
class TableMapping:
    source_dataset: str
    source_table: str
    target_dataset: str
    target_table: str
    fields: list[FieldMapping] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)
    source_incremental_fields: list[str] = field(default_factory=list)
    target_incremental_fields: list[str] = field(default_factory=list)
    partition_fields: list[str] = field(default_factory=list)
    join_logic: str = ""
    business_rules: str = ""


@dataclass
class ParsedRequirements:
    """Structured output from parsing a requirements file."""
    source_name: str  # e.g. "PRP1", "SHOPIFY_MC"
    layer: str  # e.g. "CDL", "BL", "BR"
    table_mappings: list[TableMapping] = field(default_factory=list)
    raw_text: str = ""  # original text for Claude context
    metadata: dict = field(default_factory=dict)


class CSVParser:
    """Parse CSV/Excel files containing mapping requirements.

    Expected CSV columns (flexible - will attempt to match):
        source_dataset, source_table, target_dataset, target_table,
        source_col, target_col, type, is_key, transform, foreign_key_ref
    """

    COLUMN_ALIASES = {
        "source_dataset": ["source_dataset", "src_dataset", "source_schema", "src_schema"],
        "source_table": ["source_table", "src_table", "source_tbl"],
        "target_dataset": ["target_dataset", "tgt_dataset", "target_schema", "tgt_schema",
                           "data set", "data_set", "dataset"],
        "target_table": ["target_table", "tgt_table", "target_tbl", "table", "tbl"],
        "source_col": ["source_col", "source_column", "src_col", "src_column", "source_field"],
        "target_col": ["target_col", "target_column", "tgt_col", "tgt_column", "target_field",
                        "field name", "field_name"],
        "type": ["type", "data_type", "datatype", "col_type", "column_type",
                 "field type", "field_type"],
        "is_key": ["is_key", "key", "primary_key", "pk", "is_pk"],
        "transform": ["transform", "transformation", "logic", "expression", "business_logic"],
        "foreign_key_ref": ["foreign_key_ref", "fk_ref", "fk", "foreign_key", "references"],
        "value": ["value", "default_value", "static_value", "constant"],
    }

    # Columns to exclude when detecting wide-format source table columns
    WIDE_FORMAT_EXCLUDE = {
        "#", "id", "field name confirmed?", "schema",
        "business area", "data domain", "data concept", "data classification",
        "pii?", "encryption required?", "masking rules", "description",
        "test steps", "expected result/outcome", "community", "term",
        "business definition", "cognos", "tableau", "thoughtspot",
        "comments / notes",
        # Logic & transformation helper columns (not source tables)
        "pseudo logic", "business logic", "default value",
        # Data quality & governance helper columns
        "functional dq requirements", "outcome/action", "r+",
    }

    # Prefixes that identify dimension/lookup tables (used for JOINs, not primary sources)
    _DIM_TABLE_PREFIXES = ("bl_dim_", "dim_", "bl_fact_preferred_", "bl_fact_toy_")

    def parse(self, file_path: str | Path) -> ParsedRequirements:
        file_path = Path(file_path)
        if file_path.suffix in (".xlsx", ".xls"):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        df.columns = [c.strip().lower() for c in df.columns]
        col_map = self._resolve_columns(df.columns.tolist())

        # Detect multi-header CSV: if no useful columns found, try using row 1 as header
        if not col_map or (len(col_map) <= 1 and "is_key" not in col_map):
            df = self._reload_with_subheader(file_path)
            if df is not None:
                df.columns = [c.strip().lower() for c in df.columns]
                col_map = self._resolve_columns(df.columns.tolist())

        # Detect wide-format: source table columns present but no source_col column
        source_table_cols = self._detect_source_table_columns(
            df.columns.tolist(), col_map
        )
        if source_table_cols and "source_col" not in col_map:
            return self._parse_wide_format(df, col_map, source_table_cols, file_path)

        return self._parse_standard_format(df, col_map)

    def _parse_standard_format(
        self, df: pd.DataFrame, col_map: dict[str, str]
    ) -> ParsedRequirements:
        """Parse standard narrow-format CSV with source_col/target_col columns."""
        # Group rows by (source_dataset, source_table, target_dataset, target_table)
        table_groups: dict[tuple, list] = {}
        for _, row in df.iterrows():
            key = (
                str(row.get(col_map.get("source_dataset", ""), "")).strip(),
                str(row.get(col_map.get("source_table", ""), "")).strip(),
                str(row.get(col_map.get("target_dataset", ""), "")).strip(),
                str(row.get(col_map.get("target_table", ""), "")).strip(),
            )
            table_groups.setdefault(key, []).append(row)

        table_mappings = []
        for (src_ds, src_tbl, tgt_ds, tgt_tbl), rows in table_groups.items():
            fields = []
            primary_keys = []
            foreign_keys = []

            for row in rows:
                src_col = str(row.get(col_map.get("source_col", ""), "")).strip()
                tgt_col = str(row.get(col_map.get("target_col", ""), "")).strip()
                dtype = str(row.get(col_map.get("type", ""), "STRING")).strip().upper()
                is_key = self._parse_bool(row.get(col_map.get("is_key", ""), False))
                transform = self._get_optional(row, col_map.get("transform", ""))
                fk_ref = self._get_optional(row, col_map.get("foreign_key_ref", ""))
                value = self._get_optional(row, col_map.get("value", ""))

                if not tgt_col:
                    continue

                fm = FieldMapping(
                    source_col=src_col if src_col and src_col.lower() != "nan" else "null",
                    target_col=tgt_col,
                    data_type=dtype if dtype and dtype != "NAN" else "STRING",
                    is_key=is_key,
                    transform=transform,
                    foreign_key_ref=fk_ref,
                    value=value,
                )
                fields.append(fm)

                if is_key:
                    primary_keys.append(tgt_col)
                if fk_ref:
                    foreign_keys.append({"column": tgt_col, "references": fk_ref})

            tm = TableMapping(
                source_dataset=src_ds,
                source_table=src_tbl,
                target_dataset=tgt_ds,
                target_table=tgt_tbl,
                fields=fields,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys,
            )
            table_mappings.append(tm)

        # Infer source name and layer from first table mapping
        source_name = ""
        layer = ""
        if table_mappings:
            first = table_mappings[0]
            tgt_ds = first.target_dataset.upper()
            if "CDL" in tgt_ds:
                layer = "CDL"
            elif "BUSINESS_LOGIC" in tgt_ds or "BL" in tgt_ds:
                layer = "BL"
            elif "SUPPLY_CHAIN" in tgt_ds or "BR" in tgt_ds:
                layer = "BR"
            elif "CLEANSED" in tgt_ds or "RAW" in tgt_ds:
                layer = "Cleansed_RAW"
            # Try to extract source name from source_dataset or source_table
            src_ds = first.source_dataset.lower()
            for part in src_ds.replace("src_", "").split("_"):
                if part:
                    source_name = src_ds.replace("src_", "").upper()
                    break

        return ParsedRequirements(
            source_name=source_name,
            layer=layer,
            table_mappings=table_mappings,
            raw_text=df.to_string(),
        )

    def _parse_wide_format(
        self,
        df: pd.DataFrame,
        col_map: dict[str, str],
        source_table_cols: list[str],
        file_path: Path,
    ) -> ParsedRequirements:
        """Parse wide-format CSV where each source table is a column.

        Wide format has columns like: Field Name, Field Type, Data Set, Table,
        then one column per source table containing transformation logic.
        """
        tgt_col_name = col_map.get("target_col", "")
        type_col_name = col_map.get("type", "")
        tgt_ds_col = col_map.get("target_dataset", "")
        tgt_tbl_col = col_map.get("target_table", "")
        schema_col = "schema" if "schema" in df.columns.tolist() else ""

        # Collect field mappings with per-source-table transform info
        fields = []
        primary_keys = []
        tgt_ds = ""
        tgt_tbl = ""
        source_dataset = ""

        for _, row in df.iterrows():
            tgt_col = str(row.get(tgt_col_name, "")).strip()
            if not tgt_col or tgt_col.lower() == "nan":
                continue

            dtype = str(row.get(type_col_name, "STRING")).strip().upper()
            if not dtype or dtype == "NAN":
                dtype = "STRING"

            # Infer is_key from KEY_ prefix
            is_key = tgt_col.startswith("KEY_")
            if is_key:
                primary_keys.append(tgt_col)

            # Collect transformation logic from each source table column
            transforms = {}
            for src_tbl_col in source_table_cols:
                val = str(row.get(src_tbl_col, "")).strip()
                if val and val.lower() not in ("nan", "none", ""):
                    transforms[src_tbl_col] = val

            # Build a combined transform description for the LLM
            transform = None
            if transforms:
                # Check if all sources use the same value (direct mapping)
                unique_vals = set(transforms.values())
                if len(unique_vals) == 1 and list(unique_vals)[0] == tgt_col:
                    # Direct mapping from all sources — source_col = target_col
                    source_col = tgt_col
                else:
                    source_col = tgt_col
                    # Build per-source transform description
                    parts = []
                    for src_tbl, logic in transforms.items():
                        if logic != tgt_col:
                            parts.append(f"{src_tbl}: {logic}")
                    if parts:
                        transform = "; ".join(parts)
            else:
                source_col = "null"

            fm = FieldMapping(
                source_col=source_col,
                target_col=tgt_col,
                data_type=dtype,
                is_key=is_key,
                transform=transform,
            )
            fields.append(fm)

            # Capture target dataset/table from first valid row
            if not tgt_ds and tgt_ds_col:
                tgt_ds = str(row.get(tgt_ds_col, "")).strip()
                if tgt_ds.lower() == "nan":
                    tgt_ds = ""
            if not tgt_tbl and tgt_tbl_col:
                tgt_tbl = str(row.get(tgt_tbl_col, "")).strip()
                if tgt_tbl.lower() == "nan":
                    tgt_tbl = ""
            if not source_dataset and schema_col:
                sd = str(row.get(schema_col, "")).strip()
                if sd and sd.lower() != "nan":
                    source_dataset = sd

        # Build source_dataset from schema + dataset if available
        if source_dataset and tgt_ds:
            full_source_dataset = f"{source_dataset}.{tgt_ds}"
        elif tgt_ds:
            full_source_dataset = tgt_ds
        else:
            full_source_dataset = ""

        # If target_table is still empty, derive from input filename
        if not tgt_tbl:
            tgt_tbl = file_path.stem

        source_tables_str = ",".join(source_table_cols)

        # Extract join logic and filter conditions from special rows
        # (rows where the target_col is empty or contains "FILTERS:", "SELECT", etc.)
        join_logic_parts = []
        filter_parts = []
        for _, row in df.iterrows():
            tgt_col_val = str(row.get(tgt_col_name, "")).strip()
            # Check for FILTERS: row
            if tgt_col_val.upper().startswith("FILTERS:") or tgt_col_val == "":
                for src_tbl_col in source_table_cols:
                    val = str(row.get(src_tbl_col, "")).strip()
                    if val and val.lower() not in ("nan", "none", ""):
                        src_lower = src_tbl_col.lower()
                        is_dim = any(src_lower.startswith(p) for p in self._DIM_TABLE_PREFIXES)
                        if is_dim and ("join" in val.lower() or "on" in val.lower()):
                            join_logic_parts.append(f"{src_tbl_col}: {val}")
                        elif "!=" in val or "filter" in tgt_col_val.lower() or "<" in val or ">" in val:
                            filter_parts.append(val)

        join_logic = "\n".join(join_logic_parts)
        business_rules = "\n".join(filter_parts) if filter_parts else ""

        tm = TableMapping(
            source_dataset=full_source_dataset,
            source_table=source_tables_str,
            target_dataset=tgt_ds or "Business_Logic",
            target_table=tgt_tbl,
            fields=fields,
            primary_keys=primary_keys,
            join_logic=join_logic,
            business_rules=business_rules,
        )

        # Infer layer
        layer = ""
        tgt_ds_upper = (tgt_ds or "").upper()
        if "CDL" in tgt_ds_upper:
            layer = "CDL"
        elif "BUSINESS_LOGIC" in tgt_ds_upper or "BL" in tgt_ds_upper:
            layer = "BL"
        elif "SUPPLY_CHAIN" in tgt_ds_upper or "BR" in tgt_ds_upper:
            layer = "BR"
        elif "CLEANSED" in tgt_ds_upper or "RAW" in tgt_ds_upper:
            layer = "Cleansed_RAW"

        return ParsedRequirements(
            source_name="",
            layer=layer,
            table_mappings=[tm],
            raw_text=df.to_string(),
            metadata={
                "source_table_columns": source_table_cols,
                "input_filename": file_path.stem,
            },
        )

    def _reload_with_subheader(self, file_path: Path) -> pd.DataFrame | None:
        """Reload CSV using row 1 as header (for multi-header CSVs).

        Some requirements CSVs have section headers in row 0 (e.g., "DATABASE FIELD",
        "LOGIC", "CLASSIFICATION & SECURITY") with actual column names in row 1.
        This method tries using row 1 as the header and skips description rows.
        """
        try:
            if file_path.suffix in (".xlsx", ".xls"):
                df = pd.read_excel(file_path, header=1)
            else:
                df = pd.read_csv(file_path, header=1)

            df.columns = [c.strip().lower() for c in df.columns]
            col_map = self._resolve_columns(df.columns.tolist())

            # Only use this if we found more useful columns than before
            if len(col_map) >= 2:
                # Skip description rows: drop rows where the target_col column
                # contains long descriptive text (likely column descriptions)
                tgt_col_name = col_map.get("target_col", "")
                if tgt_col_name:
                    df = df[
                        df[tgt_col_name].apply(
                            lambda x: isinstance(x, str)
                            and len(x) < 100
                            and x.strip().lower() != "nan"
                            and not x.startswith("The name of")
                            and not x.startswith("the name of")
                        )
                    ].reset_index(drop=True)
                return df
        except Exception:
            pass
        return None

    def _detect_source_table_columns(
        self, actual_cols: list[str], col_map: dict[str, str]
    ) -> list[str]:
        """Detect columns that represent source tables in wide-format CSVs.

        Returns a list of column names that are likely source table names
        (not matched to any alias and not in the exclude list).
        """
        matched_cols = set(col_map.values())
        source_cols = []
        for col in actual_cols:
            if col in matched_cols:
                continue
            if col in self.WIDE_FORMAT_EXCLUDE:
                continue
            # Skip empty or unnamed columns
            if not col or col.startswith("unnamed"):
                continue
            source_cols.append(col)
        return source_cols

    def _resolve_columns(self, actual_cols: list[str]) -> dict[str, str]:
        """Map canonical column names to actual CSV column names."""
        col_map = {}
        for canonical, aliases in self.COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in actual_cols:
                    col_map[canonical] = alias
                    break
        return col_map

    def _parse_bool(self, val) -> bool:
        if isinstance(val, bool):
            return val
        s = str(val).strip().lower()
        return s in ("true", "1", "yes", "y", "t")

    def _get_optional(self, row, col: str) -> str | None:
        if not col:
            return None
        val = str(row.get(col, "")).strip()
        if val and val.lower() not in ("nan", "none", ""):
            return val
        return None
