"""Base generator with shared LLM API logic and example loading."""

import os
from abc import ABC, abstractmethod
from pathlib import Path

import yaml

from ..config import (
    ADMIN_COLUMNS,
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    LAYERS,
    LLM_PROVIDER,
    MAPPINGS_ROOT,
    MAX_TOKENS,
)
from ..parsers.csv_parser import ParsedRequirements


def _create_llm_client(provider: str, api_key: str | None = None):
    """Create the appropriate LLM client based on provider."""
    if provider == "gemini":
        from google import genai
        key = api_key or GEMINI_API_KEY
        if not key:
            raise ValueError("GEMINI_API_KEY not set. Add it to your .env file.")
        return genai.Client(api_key=key)
    else:
        import anthropic
        key = api_key or ANTHROPIC_API_KEY
        if not key:
            raise ValueError("ANTHROPIC_API_KEY not set. Add it to your .env file.")
        return anthropic.Anthropic(api_key=key)


class BaseGenerator(ABC):
    """Base class for all layer-specific YAML generators.

    Provides:
    - LLM API client (Gemini or Anthropic)
    - Few-shot example loading from existing Mappings
    - Common ADMIN column generation
    """

    layer: str  # "CDL", "BL", "BR", "Cleansed_RAW"

    def __init__(self, api_key: str | None = None, mappings_root: str | Path | None = None):
        self.provider = LLM_PROVIDER
        self.client = _create_llm_client(self.provider, api_key)
        self.mappings_root = Path(mappings_root) if mappings_root else MAPPINGS_ROOT

    @abstractmethod
    def generate(self, requirements: ParsedRequirements) -> dict[str, str]:
        """Generate YAML mapping files from parsed requirements.

        Returns:
            dict mapping filename -> YAML content string
        """
        pass

    def call_llm(self, system_prompt: str, user_prompt: str, max_tokens: int | None = None) -> str:
        """Call the configured LLM API with system and user prompts."""
        if self.provider == "gemini":
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=user_prompt,
                config={
                    "system_instruction": system_prompt,
                    "max_output_tokens": max_tokens or MAX_TOKENS,
                },
            )
            return response.text.strip()
        else:
            response = self.client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=max_tokens or MAX_TOKENS,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return response.content[0].text.strip()

    # Keep backward compatibility
    def call_claude(self, system_prompt: str, user_prompt: str, max_tokens: int | None = None) -> str:
        """Backward-compatible alias for call_llm."""
        return self.call_llm(system_prompt, user_prompt, max_tokens)

    def load_examples(self, layer: str, max_examples: int = 2) -> list[dict]:
        """Load existing YAML mapping files as few-shot examples.

        Scans the Mappings/{layer}/ directory for YAML files and returns
        up to max_examples parsed examples with their filenames.
        """
        layer_config = LAYERS.get(layer, {})
        subfolder = layer_config.get("subfolder", layer)
        layer_path = self.mappings_root / subfolder

        if not layer_path.exists():
            return []

        examples = []
        for yml_file in sorted(layer_path.rglob("*.yml")):
            if "project_config" in yml_file.name or "misc_config" in yml_file.name:
                continue
            try:
                content = yml_file.read_text(encoding="utf-8")
                # Only include files up to ~4000 chars to fit in context
                if len(content) > 6000:
                    content = content[:6000] + "\n# ... (truncated for brevity)"
                examples.append({
                    "filename": yml_file.name,
                    "relative_path": str(yml_file.relative_to(self.mappings_root)),
                    "content": content,
                })
            except Exception:
                continue
            if len(examples) >= max_examples:
                break

        return examples

    def format_examples_prompt(self, examples: list[dict]) -> str:
        """Format loaded examples into a prompt section."""
        if not examples:
            return "No existing examples available."
        parts = []
        for i, ex in enumerate(examples, 1):
            parts.append(
                f"### Example {i}: {ex['relative_path']}\n"
                f"```yaml\n{ex['content']}\n```"
            )
        return "\n\n".join(parts)

    def format_admin_columns_ddl(self, indent: str = "    ") -> str:
        """Generate the ADMIN columns section for CREATE TABLE DDL."""
        lines = []
        for col_name, col_type in ADMIN_COLUMNS:
            lines.append(f"{indent}{col_name:<40}{col_type}")
        return ",\n".join(lines)

    def format_field_mappings_text(self, requirements: ParsedRequirements) -> str:
        """Format the parsed field mappings into a readable text block for the LLM."""
        parts = []
        for tm in requirements.table_mappings:
            lines = [f"Table: {tm.source_dataset}.{tm.source_table} -> {tm.target_dataset}.{tm.target_table}"]
            lines.append(f"Primary Keys: {', '.join(tm.primary_keys) if tm.primary_keys else 'not specified'}")
            lines.append("Fields:")
            for f in tm.fields:
                line = f"  {f.source_col} -> {f.target_col} ({f.data_type})"
                if f.is_key:
                    line += " [KEY]"
                if f.transform:
                    line += f" [TRANSFORM: {f.transform}]"
                if f.value:
                    line += f" [STATIC VALUE: {f.value}]"
                if f.foreign_key_ref:
                    line += f" [FK -> {f.foreign_key_ref}]"
                lines.append(line)
            # Include join logic / business rules if present (from PDF parser)
            if hasattr(tm, "join_logic") and tm.join_logic:
                lines.append(f"Join Logic: {tm.join_logic}")
            if hasattr(tm, "business_rules") and tm.business_rules:
                lines.append(f"Business Rules: {tm.business_rules}")
            parts.append("\n".join(lines))
        return "\n\n".join(parts)

    def write_output(self, output_dir: str | Path, files: dict[str, str]) -> list[Path]:
        """Write generated YAML files to the output directory."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        written = []
        for filename, content in files.items():
            filepath = output_dir / filename
            filepath.write_text(content, encoding="utf-8")
            written.append(filepath)
        return written
