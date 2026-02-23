"""Check that source tables referenced in requirements exist as CDL or BL tables."""

import re
from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path

import yaml

from .parsers.csv_parser import ParsedRequirements


@dataclass
class SourceCheckResult:
    source_table: str
    found: bool
    matched_to: str = ""  # The known table it matched to
    suggestion: str = ""  # Closest match if not found


class SourceTableChecker:
    """Scan existing CDL and BL YAML mappings to verify source table references."""

    def __init__(self, mappings_root: str | Path):
        self.mappings_root = Path(mappings_root)
        self._known_tables: set[str] | None = None

    def get_known_tables(self) -> set[str]:
        """Return all known CDL and BL target table names."""
        if self._known_tables is None:
            self._known_tables = set()
            self._known_tables.update(self._scan_cdl_tables())
            self._known_tables.update(self._scan_bl_tables())
        return self._known_tables

    def _scan_cdl_tables(self) -> set[str]:
        """Collect target_table names from CDL and Cleansed_RAW dataflow YAMLs."""
        tables = set()
        for subfolder in ("CDL", "Cleansed_RAW"):
            search_dir = self.mappings_root / subfolder
            if not search_dir.exists():
                continue
            for yml_path in search_dir.rglob("*.yml"):
                if "config" in yml_path.name.lower():
                    continue
                try:
                    data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
                    if data and isinstance(data.get("dataflow"), list):
                        for entry in data["dataflow"]:
                            tgt = entry.get("target_table", "").strip()
                            if tgt:
                                tables.add(tgt)
                except Exception:
                    continue
        return tables

    def _scan_bl_tables(self) -> set[str]:
        """Collect target table names from BL SQL-style YAMLs."""
        tables = set()
        search_dir = self.mappings_root / "BL"
        if not search_dir.exists():
            return tables
        for yml_path in search_dir.rglob("*.yml"):
            if "config" in yml_path.name.lower():
                continue
            try:
                data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
                if not data:
                    continue
                # From metadata
                if isinstance(data.get("metadata"), dict):
                    tgt = data["metadata"].get("target_table_name", "").strip()
                    if tgt:
                        tables.add(tgt)
                # From create_table SQL (extract table name after dataset.)
                if "create_table" in data:
                    match = re.search(r'\.(\w+)\s*\(', data["create_table"])
                    if match:
                        tables.add(match.group(1))
            except Exception:
                continue
        return tables

    def check_requirements(self, requirements: ParsedRequirements) -> list[SourceCheckResult]:
        """Verify that all source tables in parsed requirements exist as known tables.

        For each TableMapping, checks source_table against the registry.
        Also checks comma-separated source_table_names (dataset.table format).
        """
        known = self.get_known_tables()
        known_lower = {t.lower(): t for t in known}
        results = []

        for tm in requirements.table_mappings:
            source_table = tm.source_table.strip()
            if not source_table:
                continue

            # Source may be comma-separated (e.g. "CDL_Facts.table1,Business_Logic.table2")
            source_refs = [s.strip() for s in source_table.split(",") if s.strip()]

            for ref in source_refs:
                # Strip dataset prefix if present (e.g. "CDL_Facts.cdl_fact_x" -> "cdl_fact_x")
                table_name = ref.split(".")[-1].strip()
                if not table_name:
                    continue

                # Case-insensitive lookup
                lower_name = table_name.lower()
                if lower_name in known_lower:
                    results.append(SourceCheckResult(
                        source_table=table_name,
                        found=True,
                        matched_to=known_lower[lower_name],
                    ))
                else:
                    # Find closest match
                    matches = get_close_matches(lower_name, known_lower.keys(), n=1, cutoff=0.6)
                    suggestion = known_lower[matches[0]] if matches else ""
                    results.append(SourceCheckResult(
                        source_table=table_name,
                        found=False,
                        suggestion=suggestion,
                    ))

        return results
