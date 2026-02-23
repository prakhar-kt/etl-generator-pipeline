"""Generator for BR (Business Reporting) layer YAML mappings."""

import yaml

from ..config import ADMIN_COLUMNS
from ..parsers.csv_parser import ParsedRequirements
from .base import BaseGenerator


class BRGenerator(BaseGenerator):
    """Generate BR (Business Reporting) YAML mapping files.

    BR mappings are similar to BL but:
      - stage_name is "BL to BR"
      - Target datasets are reporting schemas (e.g. Supply_Chain, Sales)
      - SQL typically involves complex aggregations and cross-table joins
      - May use CREATE OR REPLACE TABLE instead of CREATE TABLE IF NOT EXISTS
    """

    layer = "BR"

    def generate(self, requirements: ParsedRequirements) -> dict[str, str]:
        examples = self.load_examples("BR", max_examples=2)
        examples_text = self.format_examples_prompt(examples)
        field_mappings_text = self.format_field_mappings_text(requirements)

        admin_cols_text = "\n".join(
            f"    {name:<40}{dtype}" for name, dtype in ADMIN_COLUMNS
        )

        system_prompt = f"""You are a BigQuery data engineering expert generating YAML mapping files
for the Smart DTC ETL pipeline. You generate BR (Business Reporting) layer YAML files that contain
BigQuery SQL for transforming BL (Business Logic) data into reporting-ready tables.

The YAML file must have these exact top-level keys:
1. `metadata:` with sub-keys: data_sources, stage_name (always "BL to BR"), source_table_names, target_table_name
2. `create_table: |` - BigQuery CREATE OR REPLACE TABLE DDL
   - Use `{{{{ target_project }}}}` for project reference
   - Target dataset is a reporting schema (e.g. Supply_Chain, Sales)
   - Include all business columns plus these admin columns:
{admin_cols_text}
3. `get_max_date: |` - Query for incremental loading
4. Either `merge_statement: |` or `other_statement: |` with the transformation SQL
   - BR typically involves complex JOINs across multiple BL tables
   - Includes aggregations, CASE statements, and currency conversions
   - Source references use `{{{{ source_projects[N] }}}}` for cross-project joins

Output ONLY valid YAML content. Use `|` for multiline SQL blocks. No markdown fences."""

        user_prompt = f"""Generate a BR mapping YAML file based on these requirements.

## Existing BR Examples (match this style EXACTLY):
{examples_text}

## Requirements:
Source: {requirements.source_name}
Target Layer: Business Reporting (BR)
{field_mappings_text}

## Additional context from requirements:
{requirements.raw_text[:3000] if requirements.raw_text else 'None'}

Generate a complete YAML file with the full SQL transformation logic.
The SQL should include proper CTEs, JOINs, aggregations, and ADMIN field generation."""

        files = {}
        for tm in requirements.table_mappings:
            table_prompt = self._build_table_prompt(tm, requirements, examples_text)
            yaml_content = self.call_claude(system_prompt, table_prompt)
            yaml_content = self._clean_yaml(yaml_content)

            try:
                yaml.safe_load(yaml_content)
            except yaml.YAMLError:
                yaml_content = self._generate_fallback(tm, requirements)

            source = requirements.source_name
            filename = f"{source}_{tm.target_table}.yml"
            files[filename] = yaml_content

        return files

    def _build_table_prompt(self, tm, requirements, examples_text) -> str:
        fields_text = []
        for f in tm.fields:
            line = f"  {f.source_col} -> {f.target_col} ({f.data_type})"
            if f.is_key:
                line += " [KEY]"
            if f.transform:
                line += f" [TRANSFORM: {f.transform}]"
            fields_text.append(line)

        join_logic = getattr(tm, "join_logic", "") or ""
        business_rules = getattr(tm, "business_rules", "") or ""

        return f"""Generate a BR mapping YAML for this specific table:

## Existing BR Examples (match this style EXACTLY):
{examples_text}

## Table Requirements:
Source: {requirements.source_name}
Source Tables: {tm.source_dataset}.{tm.source_table}
Target Table: {tm.target_dataset}.{tm.target_table}
Primary Keys: {', '.join(tm.primary_keys) if tm.primary_keys else 'derive from KEY_ columns'}

Fields:
{chr(10).join(fields_text)}

Join Logic: {join_logic or 'Derive from source table relationships'}
Business Rules: {business_rules or 'Standard reporting aggregation'}

Generate the complete YAML with metadata, create_table, get_max_date, and merge_statement or other_statement."""

    def _clean_yaml(self, content: str) -> str:
        lines = content.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()

    def _generate_fallback(self, tm, requirements) -> str:
        col_lines = []
        for f in tm.fields:
            col_lines.append(f"  {f.target_col:<40}{f.data_type}")
        admin_lines = [f"  {name:<40}{dtype}" for name, dtype in ADMIN_COLUMNS]
        all_cols = ",\n".join(col_lines + admin_lines)

        source_tables = f"{tm.source_dataset}.{tm.source_table}"
        target_table = tm.target_table
        dataset = tm.target_dataset or "Supply_Chain"

        return f"""metadata:
  data_sources: "Business_Logic"
  stage_name: "BL to BR"
  source_table_names: "{source_tables}"
  target_table_name: "{target_table}"

create_table: |
  CREATE OR REPLACE TABLE `{{{{ target_project }}}}.{dataset}.{target_table}`
  (
{all_cols}
  );

get_max_date: |
  SELECT COALESCE(MAX(ADMIN_LOAD_DATE), TIMESTAMP('1970-01-01 00:00:00'))
  FROM `{{{{ target_project }}}}.{dataset}.{target_table}`;

other_statement: |
  -- TODO: Complete transformation SQL
  -- Generated as fallback - review and customize
  DELETE FROM `{{{{ target_project }}}}.{dataset}.{target_table}` WHERE 1=1;

  INSERT INTO `{{{{ target_project }}}}.{dataset}.{target_table}`
  SELECT *
  FROM `{{{{ source_projects[0] }}}}.{source_tables}`;
"""
