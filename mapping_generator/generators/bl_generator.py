"""Generator for BL (Business Logic) layer YAML mappings with CREATE TABLE + MERGE SQL."""

import json

import yaml

from ..config import ADMIN_COLUMNS, ADMIN_MERGE_INSERT_VALUES, ADMIN_MERGE_UPDATE_SET
from ..parsers.csv_parser import ParsedRequirements
from .base import BaseGenerator


class BLGenerator(BaseGenerator):
    """Generate BL (Business Logic) YAML mapping files.

    BL mappings contain:
      - metadata: data_sources, stage_name, source/target table names
      - create_table: BigQuery CREATE TABLE DDL with Jinja2 templates
      - get_max_date: incremental load query
      - merge_statement: MERGE INTO with USING subquery, WHEN MATCHED/NOT MATCHED
        OR other_statement: DELETE+INSERT pattern for full refresh tables
    """

    layer = "BL"

    # Known dimension / lookup table prefixes — these are JOIN tables, not primary sources
    _DIM_PREFIXES = ("bl_dim_", "dim_", "bl_fact_preferred_", "bl_fact_toy_")
    # Known CDL source prefixes — presence means this is a CDL-to-BL pattern
    _CDL_PREFIXES = ("cdl_",)

    def _classify_source_tables(self, source_tables: list[str]) -> dict:
        """Classify source tables into primary CDL sources vs dimension/lookup tables.

        Returns dict with keys:
          primary_sources: list of main source tables (CDL tables)
          join_tables: list of dimension/lookup tables used for JOINs
          is_bl_to_bl: True only if multiple primary BL source tables are aggregated
        """
        primary_sources = []
        join_tables = []

        for tbl in source_tables:
            tbl_lower = tbl.strip().lower()
            if any(tbl_lower.startswith(p) for p in self._DIM_PREFIXES):
                join_tables.append(tbl.strip())
            elif any(tbl_lower.startswith(p) for p in self._CDL_PREFIXES):
                primary_sources.append(tbl.strip())
            else:
                # Tables starting with bl_ or fact_ that aren't dims are BL sources
                if tbl_lower.startswith(("bl_fact_", "bl_agg_", "fact_")) and not any(
                    tbl_lower.startswith(p) for p in self._CDL_PREFIXES
                ):
                    primary_sources.append(tbl.strip())
                else:
                    # Default: treat unknown tables as primary sources
                    primary_sources.append(tbl.strip())

        # BL-to-BL only when there are multiple primary BL sources being aggregated
        # A single CDL source + dimension JOIN tables is CDL-to-BL
        has_cdl_source = any(
            s.lower().startswith(self._CDL_PREFIXES) for s in primary_sources
        )
        is_bl_to_bl = (
            len(primary_sources) > 1
            and not has_cdl_source
        )

        return {
            "primary_sources": primary_sources,
            "join_tables": join_tables,
            "is_bl_to_bl": is_bl_to_bl,
            "has_cdl_source": has_cdl_source,
        }

    def generate(self, requirements: ParsedRequirements) -> dict[str, str]:
        examples = self.load_examples("BL", max_examples=2)
        examples_text = self.format_examples_prompt(examples)
        field_mappings_text = self.format_field_mappings_text(requirements)

        admin_cols_text = "\n".join(
            f"    {name:<40}{dtype}" for name, dtype in ADMIN_COLUMNS
        )

        system_prompt = f"""You are a BigQuery data engineering expert generating YAML mapping files
for the Smart DTC ETL pipeline. You generate BL (Business Logic) layer YAML files that contain
BigQuery SQL for transforming data into business-ready tables.

There are two patterns:
- **CDL to BL**: Standard pattern sourcing from CDL layer, possibly with dimension table JOINs.
  stage_name = "CDL to BL". Uses merge_statement (incremental MERGE).
  The primary CDL source table uses `{{{{ source_projects[0] }}}}` (data-discovery project).
  Dimension/lookup tables (bl_dim_*, dim_*) use `{{{{ source_projects[2] }}}}` (BL project).
- **BL to BL**: Aggregation pattern combining multiple BL source tables (no CDL sources).
  stage_name = "BL to BL". Uses other_statement (DELETE+INSERT full refresh).
  All source tables use `{{{{ source_projects[2] }}}}`.

IMPORTANT: A CDL source table joined with dimension tables (e.g., cdl_* + bl_dim_company)
is STILL "CDL to BL", NOT "BL to BL". Only classify as BL-to-BL when there are multiple
primary BL fact/aggregate tables being combined.

The YAML file must have these exact top-level keys:
1. `metadata:` with sub-keys: data_sources, stage_name, source_table_names, target_table_name
   - data_sources: the source dataset name (e.g., "Demands", "Supply_Chain")
   - source_table_names: fully qualified table names with project placeholders
2. `create_table: |` - BigQuery CREATE TABLE IF NOT EXISTS DDL using Jinja2 template variables
   - Use `{{{{ target_project }}}}` for project reference
   - Target dataset is typically `Business_Logic`
   - ALWAYS use `PARTITION BY TIMESTAMP_TRUNC(ADMIN_LOAD_DATE, MONTH)` for partitioning
   - CLUSTER BY should include all composite key columns (from is_key fields)
   - ALWAYS include these admin columns at the end:
{admin_cols_text}
3. `get_max_date: |` - Query to get the max incremental timestamp
   - For "CDL to BL": use `MAX(LOAD_DATE)` (the source data's load timestamp)
   - For "BL to BL": use `MAX(ADMIN_LOAD_DATE)`
4. Either `merge_statement: |` (for CDL to BL) or `other_statement: |` (for BL to BL)
   - For MERGE: Use MERGE INTO target USING (subquery) AS SOURCE ON ADMIN_COMPOSITEKEY_HASH
   - For other_statement: DELETE FROM target WHERE 1=1; then INSERT INTO with CTEs
   - Include FARM_FINGERPRINT for ADMIN_COMPOSITEKEY_HASH and ADMIN_ROW_HASH

## CRITICAL SQL generation rules:
- ALWAYS apply REPLACE, SAFE_CAST, and other transforms EXACTLY as specified in the field mappings
- ALWAYS include a deduplication step using ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY LAST_MODIFY_DATE DESC) WHERE RN = 1
- ALWAYS use SELECT DISTINCT in base CTEs to eliminate duplicate source rows
- ADMIN_COMPOSITEKEY_HASH must include ALL composite key columns (all fields marked as KEY plus date range fields)
- ADMIN_ROW_HASH must include ALL non-admin data columns
- ADMIN_ISERROR should check ONLY key/required fields (typically COMPANY_CODE, main entity codes) for NULL
- ADMIN_SOURCE_SYSTEM should use the source name provided in requirements
- Do NOT invent or hallucinate tables, JOINs, or CTEs not described in the requirements
- Only include columns and JOINs that are explicitly in the field mappings or business rules

The merge_statement admin fields for INSERT should end with:
{ADMIN_MERGE_INSERT_VALUES}

The WHEN MATCHED UPDATE should end with:
{ADMIN_MERGE_UPDATE_SET}

Output ONLY valid YAML content. Use `|` for multiline SQL blocks. No markdown fences."""

        user_prompt = f"""Generate a BL mapping YAML file based on these requirements.

## Existing BL Examples (match this style EXACTLY):
{examples_text}

## Requirements:
Source: {requirements.source_name}
Target Layer: Business Logic (BL)
{field_mappings_text}

## Additional context from requirements:
{requirements.raw_text[:3000] if requirements.raw_text else 'None'}

Generate a complete YAML file. The SQL in merge_statement should:
- Use CTEs for source data preparation
- Apply FARM_FINGERPRINT for all KEY_ columns
- Use COALESCE for null-safe key generation
- Include proper JOIN logic between source tables
- Use ADMIN_COMPOSITEKEY_HASH = FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...))) for composite key
- Follow the exact pattern from the examples above."""

        files = {}
        for tm in requirements.table_mappings:
            # Generate per-table
            table_prompt = self._build_table_prompt(tm, requirements, examples_text, system_prompt)
            yaml_content = self.call_claude(system_prompt, table_prompt)
            yaml_content = self._clean_yaml(yaml_content)

            try:
                yaml.safe_load(yaml_content)
            except yaml.YAMLError:
                yaml_content = self._generate_fallback(tm, requirements)

            source = requirements.source_name
            target_table = tm.target_table or requirements.metadata.get(
                "input_filename", "unknown"
            )
            filename = f"{source}_{target_table}.yml"
            files[filename] = yaml_content

        return files

    def _build_table_prompt(self, tm, requirements, examples_text, system_prompt) -> str:
        fields_text = []
        for f in tm.fields:
            line = f"  {f.source_col} -> {f.target_col} ({f.data_type})"
            if f.is_key:
                line += " [KEY]"
            if f.transform:
                line += f" [TRANSFORM: {f.transform}]"
            if f.value:
                line += f" [STATIC VALUE: {f.value}]"
            fields_text.append(line)

        join_logic = getattr(tm, "join_logic", "") or ""
        business_rules = getattr(tm, "business_rules", "") or ""

        # Classify source tables into primary sources vs dimension/lookup tables
        source_tables = [s.strip() for s in tm.source_table.split(",") if s.strip()]
        classification = self._classify_source_tables(source_tables)

        primary_sources = classification["primary_sources"]
        join_tables = classification["join_tables"]
        is_bl_to_bl = classification["is_bl_to_bl"]
        has_cdl_source = classification["has_cdl_source"]

        # Also check business rules for explicit BL-to-BL indicators (union, combine)
        if not is_bl_to_bl and business_rules:
            has_aggregation_keywords = any(
                kw in business_rules.lower()
                for kw in ["union", "combine", "aggregate multiple bl"]
            )
            if has_aggregation_keywords and not has_cdl_source:
                is_bl_to_bl = True

        use_other_statement = is_bl_to_bl

        # Build composite key columns from is_key fields
        key_columns = [f.target_col for f in tm.fields if f.is_key]
        # If no explicit keys, derive from common patterns
        if not key_columns:
            key_columns = [
                f.target_col for f in tm.fields
                if f.target_col.upper().startswith("KEY_")
                or f.target_col.upper() in (
                    "COMPANY_CODE", "LOCATION_CODE", "PARENT_TOY_NO",
                    "COMPONENT_TOY_NO",
                )
            ]

        # Build cluster-by from key columns
        cluster_columns = key_columns[:4] if key_columns else []

        aggregation_instructions = ""
        if use_other_statement:
            aggregation_instructions = f"""
IMPORTANT: This is a BL to BL table that requires full refresh logic.
- stage_name in metadata should be "BL to BL" (not "CDL to BL")
- Use `other_statement` (DELETE + INSERT pattern) instead of `merge_statement`
- The other_statement should:
  1. DELETE FROM the target table WHERE 1=1 (full refresh)
  2. INSERT INTO with explicit column list
  3. Use WITH clause CTEs to implement the business logic described below
  4. SELECT from the final CTE
- Source tables: {', '.join(source_tables)}
- Source dataset: {tm.source_dataset}
- Use `{{{{ source_projects[2] }}}}` for ALL source project references (not target_project)
- data_sources in metadata should be "{tm.source_dataset}"
- source_table_names in metadata should list all source tables comma-separated
- get_max_date should use MAX(ADMIN_LOAD_DATE)
"""
        else:
            # CDL to BL instructions
            cdl_source = primary_sources[0] if primary_sources else source_tables[0]
            join_table_list = ", ".join(join_tables) if join_tables else "none"
            aggregation_instructions = f"""
IMPORTANT: This is a CDL to BL table using incremental MERGE.
- stage_name in metadata should be "CDL to BL"
- Use `merge_statement` (MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED)
- Primary CDL source table: `{{{{ source_projects[0] }}}}.{tm.source_dataset or 'Demands'}.{cdl_source}`
  (source_projects[0] = data-discovery project for CDL tables)
- Dimension/lookup JOIN tables use `{{{{ source_projects[2] }}}}.Business_Logic.<table_name>`
  (source_projects[2] = BL project for dimension tables)
- JOIN tables for enrichment: {join_table_list}
- get_max_date should use MAX(LOAD_DATE) — the source data's load timestamp, NOT ADMIN_LOAD_DATE
- source_table_names in metadata should be fully qualified: e.g., "data-discovery-mattel.Demands.{cdl_source}, s-mart-mattel.Business_Logic.{join_tables[0] if join_tables else ''}"
"""

        # Build explicit transform instructions
        transform_instructions = []
        for f in tm.fields:
            if f.transform:
                transform_instructions.append(
                    f"- {f.target_col}: Apply EXACTLY this transform: {f.transform}"
                )
        transform_section = ""
        if transform_instructions:
            transform_section = f"""
## Explicit Transforms (MUST be applied exactly as specified):
{chr(10).join(transform_instructions)}
"""

        # Build business rules section with prominence when rules are available
        business_rules_section = ""
        if business_rules:
            business_rules_section = f"""
## Business Rules (CRITICAL - implement these as SQL logic):
{business_rules}

You MUST implement the above business rules as CTEs in the SQL query. These rules describe
the core transformation logic for this table. Translate each rule into specific SQL operations
(JOINs, WHERE clauses, window functions, etc.) within the WITH clause CTEs.
Do NOT add extra JOINs or CTEs beyond what these rules describe.
"""

        # Build reference schema section for tables not in existing mappings
        missing_schemas = requirements.metadata.get("missing_table_schemas", {})
        schema_section = ""
        if missing_schemas:
            parts = []
            for tbl_name, schema_data in missing_schemas.items():
                parts.append(f"### {tbl_name}\n```json\n{json.dumps(schema_data, indent=2, default=str)}\n```")
            schema_section = "\n## Reference Table Schemas (user-provided for tables not in existing mappings):\n" + "\n".join(parts) + "\n\nUse the column names and types from these schemas when building JOINs or referencing these tables in the SQL.\n"

        return f"""Generate a BL mapping YAML for this specific table:

## Existing BL Examples (match this style EXACTLY):
{examples_text}

## Table Requirements:
Source: {requirements.source_name}
Source Tables: {tm.source_dataset}.{tm.source_table}
Target Table: {tm.target_dataset}.{tm.target_table}
Primary Keys: {', '.join(tm.primary_keys) if tm.primary_keys else 'derive from KEY_ columns'}
Composite Key Columns (for ADMIN_COMPOSITEKEY_HASH): {', '.join(key_columns) if key_columns else 'derive from key fields'}
Cluster By Columns: {', '.join(cluster_columns) if cluster_columns else 'derive from key fields'}
{aggregation_instructions}
{transform_section}
Fields:
{chr(10).join(fields_text)}

Join Logic: {join_logic or 'Single source table or derive from field references'}
{business_rules_section or f'Business Rules: Standard transformation logic'}
{schema_section}
## SQL Structure Requirements:
- PARTITION BY must be: TIMESTAMP_TRUNC(ADMIN_LOAD_DATE, MONTH)
- CLUSTER BY must include: {', '.join(cluster_columns) if cluster_columns else 'key columns'}
- Include a deduplication CTE using ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY LAST_MODIFY_DATE DESC) and filter WHERE RN = 1
- Use SELECT DISTINCT in the base CTE
- ADMIN_ISERROR should check for NULL only on: {', '.join(key_columns[:3]) if key_columns else 'primary key columns'}
- ADMIN_SOURCE_SYSTEM should be '{requirements.source_name} DP' or as specified in requirements
- Do NOT include tables or JOINs not mentioned in the requirements above

Additional context:
{requirements.raw_text[:3000] if requirements.raw_text else 'None'}

Generate the complete YAML with metadata, create_table, get_max_date, and {'other_statement (DELETE+INSERT)' if use_other_statement else 'merge_statement'}."""

    def _clean_yaml(self, content: str) -> str:
        lines = content.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()

    def _generate_fallback(self, tm, requirements) -> str:
        """Generate a minimal valid BL YAML as fallback."""
        # Build column list for CREATE TABLE
        col_lines = []
        for f in tm.fields:
            col_lines.append(f"    {f.target_col:<40}{f.data_type}")
        admin_lines = [f"    {name:<40}{dtype}" for name, dtype in ADMIN_COLUMNS]
        all_cols = ",\n".join(col_lines + admin_lines)

        target_table = tm.target_table
        dataset = tm.target_dataset or "Business_Logic"

        # Classify sources to determine CDL-to-BL vs BL-to-BL
        source_tables_list = [s.strip() for s in tm.source_table.split(",") if s.strip()]
        classification = self._classify_source_tables(source_tables_list)
        is_bl_to_bl = classification["is_bl_to_bl"]

        stage_name = "BL to BL" if is_bl_to_bl else "CDL to BL"
        max_date_col = "ADMIN_LOAD_DATE" if is_bl_to_bl else "LOAD_DATE"
        source_project = "source_projects[2]" if is_bl_to_bl else "source_projects[0]"
        source_tables = f"{tm.source_dataset}.{tm.source_table}"

        # Build cluster-by from key columns
        key_columns = [f.target_col for f in tm.fields if f.is_key]
        cluster_clause = f"\n  CLUSTER BY {', '.join(key_columns[:4])};" if key_columns else ";"

        yaml_content = f"""metadata:
  data_sources: "{tm.source_dataset}"
  stage_name: "{stage_name}"
  source_table_names: "{source_tables}"
  target_table_name: "{target_table}"

create_table: |
  CREATE TABLE IF NOT EXISTS `{{{{ target_project }}}}.{dataset}.{target_table}`(
{all_cols}
  )
  PARTITION BY TIMESTAMP_TRUNC(ADMIN_LOAD_DATE, MONTH){cluster_clause}

get_max_date: |
  SELECT COALESCE(MAX({max_date_col}), TIMESTAMP('1970-01-01 00:00:00'))
  FROM `{{{{ target_project }}}}.{dataset}.{target_table}`;

merge_statement: |
  -- TODO: Complete MERGE statement with business logic
  -- Generated as fallback - review and customize the SQL below
  MERGE INTO `{{{{ target_project }}}}.{dataset}.{target_table}` AS TARGET
  USING (
    SELECT *
    FROM `{{{{ {source_project} }}}}.{source_tables}`
  ) AS SOURCE
  ON TARGET.ADMIN_COMPOSITEKEY_HASH = SOURCE.ADMIN_COMPOSITEKEY_HASH
  WHEN NOT MATCHED THEN
    INSERT ROW;
"""
        return yaml_content
