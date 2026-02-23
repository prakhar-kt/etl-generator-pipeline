"""Generator for Cleansed_RAW layer YAML mappings."""

import yaml

from ..parsers.csv_parser import ParsedRequirements
from .base import BaseGenerator


class CleansedRawGenerator(BaseGenerator):
    """Generate Cleansed_RAW dataflow YAML files.

    Very similar to CDL but targets Cleansed datasets (e.g. Shopify_Cleansed)
    and may include storefront-specific static values.
    """

    layer = "Cleansed_RAW"

    def generate(self, requirements: ParsedRequirements) -> dict[str, str]:
        examples = self.load_examples("Cleansed_RAW", max_examples=2)
        examples_text = self.format_examples_prompt(examples)
        field_mappings_text = self.format_field_mappings_text(requirements)

        system_prompt = """You are a BigQuery data engineering expert generating YAML mapping files
for the Smart DTC ETL pipeline. You generate Cleansed_RAW mapping YAML files that define
source-to-target field mappings from raw/ETL tables to cleansed tables.

The YAML must follow this exact structure:
- Top-level key: `dataflow` (a list of table mappings)
- Each table mapping has: source_dataset, source_table, target_dataset, target_table, fields
- Each field has: source_col, target_col, type
- For static/computed columns: source_col is 'null', include 'value' key
- Table-level keys: source_incremental_fields, target_incremental_fields, partition_fields, primary_keys, foreign_keys

Output ONLY valid YAML content, no markdown fences or explanations."""

        user_prompt = f"""Generate a Cleansed_RAW mapping YAML file based on these requirements.

## Existing Examples (match this style exactly):
{examples_text}

## Requirements:
Source: {requirements.source_name}
{field_mappings_text}

## Additional context:
{requirements.raw_text[:3000] if requirements.raw_text else 'None'}

Generate the complete YAML file matching the exact structure and formatting of the examples."""

        yaml_content = self.call_claude(system_prompt, user_prompt)
        yaml_content = self._clean_yaml(yaml_content)

        try:
            yaml.safe_load(yaml_content)
        except yaml.YAMLError:
            yaml_content = self._generate_programmatic(requirements)

        source = requirements.source_name
        has_facts = any("fact" in tm.target_table.lower() for tm in requirements.table_mappings)
        has_dims = any("dim" in tm.target_table.lower() for tm in requirements.table_mappings)

        files = {}
        if has_facts:
            files[f"{source}_FACTS_Mapping_RAW.yml"] = yaml_content
        if has_dims:
            files[f"{source}_DIMENSIONS_Mapping_RAW.yml"] = yaml_content
        if not files:
            files[f"{source}_FACTS_Mapping_RAW.yml"] = yaml_content

        return files

    def _clean_yaml(self, content: str) -> str:
        lines = content.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()

    def _generate_programmatic(self, requirements: ParsedRequirements) -> str:
        dataflow = []
        for tm in requirements.table_mappings:
            fields = []
            for f in tm.fields:
                entry = {"source_col": f.source_col, "target_col": f.target_col, "type": f.data_type}
                if f.value:
                    entry["value"] = f.value
                fields.append(entry)

            table_entry = {
                "source_dataset": tm.source_dataset,
                "source_table": tm.source_table,
                "target_dataset": tm.target_dataset,
                "target_table": tm.target_table,
                "fields": fields,
                "source_incremental_fields": tm.source_incremental_fields or ["LOAD_DATE"],
                "target_incremental_fields": tm.target_incremental_fields or ["RAW_LOAD_DATE"],
                "partition_fields": tm.partition_fields or [],
                "primary_keys": tm.primary_keys or [],
                "foreign_keys": tm.foreign_keys or [],
            }
            dataflow.append(table_entry)

        return yaml.dump({"dataflow": dataflow}, default_flow_style=False, sort_keys=False)
