"""Generator for CDL layer (RAW to CDL) dataflow YAML mappings."""

import yaml

from ..parsers.csv_parser import ParsedRequirements
from .base import BaseGenerator


class CDLGenerator(BaseGenerator):
    """Generate CDL dataflow YAML files.

    CDL mappings use the 'dataflow' style:
      - List of source->target table mappings
      - Each with field-level column mappings (source_col, target_col, type)
      - Includes primary_keys, foreign_keys, incremental fields, partition fields
    """

    layer = "CDL"

    def generate(self, requirements: ParsedRequirements) -> dict[str, str]:
        examples = self.load_examples("CDL", max_examples=2)
        examples_text = self.format_examples_prompt(examples)
        field_mappings_text = self.format_field_mappings_text(requirements)

        system_prompt = """You are a BigQuery data engineering expert generating YAML mapping files
for the Smart DTC ETL pipeline. You generate CDL (Cleansed Data Layer) mapping YAML files
that define source-to-target field mappings from RAW to CDL.

The YAML must follow this exact structure:
- Top-level key: `dataflow` (a list of table mappings)
- Each table mapping has: source_dataset, source_table, target_dataset, target_table, fields
- Each field has: source_col, target_col, type
- For computed/static columns: source_col is 'null' and a 'value' key is added
- Table-level keys: source_incremental_fields, target_incremental_fields, partition_fields, primary_keys, foreign_keys

Output ONLY valid YAML content, no markdown fences or explanations."""

        user_prompt = f"""Generate a CDL mapping YAML file based on these requirements.

## Existing Examples (match this style exactly):
{examples_text}

## Requirements:
Source: {requirements.source_name}
{field_mappings_text}

## Additional context from the requirements document:
{requirements.raw_text[:3000] if requirements.raw_text else 'None'}

Generate the complete YAML file. Use the EXACT same structure and formatting as the examples.
Key columns should be prefixed with KEY_ in the target if they serve as primary/foreign keys.
Include appropriate source_incremental_fields (typically LOAD_DATE or LAST_UPDATE_DATE),
target_incremental_fields (typically RAW_LOAD_DATE), and primary_keys."""

        yaml_content = self.call_claude(system_prompt, user_prompt)

        # Clean up response - remove markdown fences if present
        yaml_content = self._clean_yaml(yaml_content)

        # Validate it parses as YAML
        try:
            yaml.safe_load(yaml_content)
        except yaml.YAMLError:
            # If Claude's output isn't valid YAML, fall back to programmatic generation
            yaml_content = self._generate_programmatic(requirements)

        # Determine output filename
        source = requirements.source_name
        # Check if FACTS or DIMENSIONS based on target_dataset
        has_facts = any("fact" in tm.target_dataset.lower() for tm in requirements.table_mappings)
        has_dims = any("dim" in tm.target_dataset.lower() for tm in requirements.table_mappings)

        files = {}
        if has_facts and has_dims:
            # Need to split into two files
            fact_tables = [tm for tm in requirements.table_mappings if "fact" in tm.target_dataset.lower()]
            dim_tables = [tm for tm in requirements.table_mappings if "dim" in tm.target_dataset.lower()]
            if fact_tables:
                files[f"{source}_FACTS_Mapping_CDL.yml"] = yaml_content  # Full output for now
            if dim_tables:
                files[f"{source}_DIMENSIONS_Mapping_CDL.yml"] = self._generate_subset(dim_tables, requirements)
        elif has_dims:
            files[f"{source}_DIMENSIONS_Mapping_CDL.yml"] = yaml_content
        else:
            files[f"{source}_FACTS_Mapping_CDL.yml"] = yaml_content

        return files

    def _clean_yaml(self, content: str) -> str:
        """Remove markdown fences and leading/trailing whitespace."""
        lines = content.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()

    def _generate_programmatic(self, requirements: ParsedRequirements) -> str:
        """Fallback programmatic YAML generation if Claude output is invalid."""
        dataflow = []
        for tm in requirements.table_mappings:
            fields = []
            for f in tm.fields:
                entry = {
                    "source_col": f.source_col,
                    "target_col": f.target_col,
                    "type": f.data_type,
                }
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

    def _generate_subset(self, table_mappings: list, requirements: ParsedRequirements) -> str:
        """Generate YAML for a subset of table mappings."""
        sub_req = ParsedRequirements(
            source_name=requirements.source_name,
            layer=requirements.layer,
            table_mappings=table_mappings,
            raw_text=requirements.raw_text,
            metadata=requirements.metadata,
        )
        return self._generate_programmatic(sub_req)
