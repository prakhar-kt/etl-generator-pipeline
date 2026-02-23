"""Parse PDF requirement documents using pypdf + LLM API for interpretation."""

import json
from pathlib import Path

from pypdf import PdfReader

from ..config import (
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    LLM_PROVIDER,
    MAX_TOKENS,
)
from .csv_parser import FieldMapping, ParsedRequirements, TableMapping


class PDFParser:
    """Extract mapping requirements from PDF specification documents.

    Uses pypdf to extract text, then sends to an LLM API to interpret
    the requirements into structured field mappings.
    """

    def __init__(self, api_key: str | None = None):
        self.provider = LLM_PROVIDER
        if self.provider == "gemini":
            from google import genai
            key = api_key or GEMINI_API_KEY
            if not key:
                raise ValueError("GEMINI_API_KEY not set. Add it to your .env file.")
            self.client = genai.Client(api_key=key)
        else:
            import anthropic
            key = api_key or ANTHROPIC_API_KEY
            if not key:
                raise ValueError("ANTHROPIC_API_KEY not set. Add it to your .env file.")
            self.client = anthropic.Anthropic(api_key=key)

    def parse(self, file_path: str | Path) -> ParsedRequirements:
        file_path = Path(file_path)
        raw_text = self._extract_text(file_path)
        structured = self._interpret_with_llm(raw_text)
        return self._build_requirements(structured, raw_text)

    def _extract_text(self, file_path: Path) -> str:
        reader = PdfReader(str(file_path))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n--- PAGE BREAK ---\n\n".join(pages)

    def _interpret_with_llm(self, pdf_text: str) -> dict:
        prompt = f"""You are a data engineering assistant. Analyze this PDF document that contains
data mapping requirements for a BigQuery ETL pipeline. Extract the structured mapping information.

Return a JSON object with this exact structure:
{{
  "source_name": "the data source identifier (e.g. PRP1, SHOPIFY_MC, ANAPLAN, DOM)",
  "layer": "target layer - one of: CDL, Cleansed_RAW, BL, BR",
  "tables": [
    {{
      "source_dataset": "source dataset/schema name",
      "source_table": "source table name",
      "target_dataset": "target dataset name (e.g. CDL_Facts, CDL_Dimensions, Business_Logic, Supply_Chain)",
      "target_table": "target table name",
      "fields": [
        {{
          "source_col": "source column name (or null for computed/static columns)",
          "target_col": "target column name",
          "type": "BigQuery data type (STRING, INT64, NUMERIC, DATE, TIMESTAMP, BOOL, DATETIME, TIME)",
          "is_key": true/false,
          "transform": "transformation logic if any (SQL expression) or null",
          "value": "static value if source_col is null, otherwise null",
          "foreign_key_ref": "referenced_table.column if FK, otherwise null"
        }}
      ],
      "primary_keys": ["list", "of", "pk", "columns"],
      "join_logic": "description of how source tables should be joined (for BL/BR layers)",
      "business_rules": "any business rules or special logic described in the document"
    }}
  ],
  "metadata": {{
    "description": "brief description of the mapping purpose",
    "notes": "any additional notes or caveats from the document"
  }}
}}

PDF Document Content:
{pdf_text}

Return ONLY the JSON object, no other text."""

        if self.provider == "gemini":
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config={
                    "max_output_tokens": MAX_TOKENS,
                },
            )
            response_text = response.text.strip()
        else:
            response = self.client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = response.content[0].text.strip()

        # Handle potential markdown code blocks in response
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])

        return json.loads(response_text)

    def _build_requirements(self, data: dict, raw_text: str) -> ParsedRequirements:
        table_mappings = []
        for tbl in data.get("tables", []):
            fields = []
            for f in tbl.get("fields", []):
                fields.append(FieldMapping(
                    source_col=f.get("source_col") or "null",
                    target_col=f["target_col"],
                    data_type=f.get("type", "STRING"),
                    is_key=f.get("is_key", False),
                    transform=f.get("transform"),
                    foreign_key_ref=f.get("foreign_key_ref"),
                    value=f.get("value"),
                ))

            tm = TableMapping(
                source_dataset=tbl.get("source_dataset", ""),
                source_table=tbl.get("source_table", ""),
                target_dataset=tbl.get("target_dataset", ""),
                target_table=tbl.get("target_table", ""),
                fields=fields,
                primary_keys=tbl.get("primary_keys", []),
                foreign_keys=[
                    {"column": f.target_col, "references": f.foreign_key_ref}
                    for f in fields if f.foreign_key_ref
                ],
            )
            tm.join_logic = tbl.get("join_logic", "")
            tm.business_rules = tbl.get("business_rules", "")
            table_mappings.append(tm)

        return ParsedRequirements(
            source_name=data.get("source_name", ""),
            layer=data.get("layer", ""),
            table_mappings=table_mappings,
            raw_text=raw_text,
            metadata=data.get("metadata", {}),
        )
