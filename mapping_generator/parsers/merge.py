"""Merge ParsedRequirements from multiple input sources (CSV + PDF)."""

from .csv_parser import ParsedRequirements, TableMapping


def merge_requirements(
    csv_reqs: ParsedRequirements,
    pdf_reqs: ParsedRequirements,
) -> ParsedRequirements:
    """Merge CSV-parsed and PDF-parsed requirements into a single object.

    Priority model:
    - CSV is authoritative for field definitions (columns, types, transforms)
    - PDF is authoritative for business rules, join logic, and prose descriptions
    - source_table lists are combined (PDF may identify additional source tables)
    - raw_text is concatenated so the LLM gets full context from both sources
    """
    # Prefer CSV values for source_name/layer, fall back to PDF
    source_name = csv_reqs.source_name or pdf_reqs.source_name
    layer = csv_reqs.layer or pdf_reqs.layer

    # Merge metadata dicts (PDF metadata like description + CSV metadata)
    merged_metadata = {**pdf_reqs.metadata, **csv_reqs.metadata}

    # Concatenate raw text from both sources for full LLM context
    raw_parts = []
    if csv_reqs.raw_text:
        raw_parts.append("=== CSV Field Definitions ===\n" + csv_reqs.raw_text)
    if pdf_reqs.raw_text:
        raw_parts.append("=== PDF Business Requirements ===\n" + pdf_reqs.raw_text)
    merged_raw_text = "\n\n".join(raw_parts)

    # Build lookup of PDF table mappings by target_table name
    pdf_by_target: dict[str, TableMapping] = {}
    for tm in pdf_reqs.table_mappings:
        key = tm.target_table.lower().strip()
        pdf_by_target[key] = tm

    # Merge table mappings: CSV defines which tables to generate,
    # PDF overlays business rules onto matching CSV tables.
    merged_tables = []
    for csv_tm in csv_reqs.table_mappings:
        target_key = csv_tm.target_table.lower().strip()
        pdf_tm = pdf_by_target.pop(target_key, None)

        if pdf_tm:
            # Exact match — overlay PDF rules onto CSV table
            _overlay_pdf_onto_csv(csv_tm, pdf_tm)
        elif pdf_by_target:
            # No exact match — apply all remaining PDF tables' business rules
            # and source tables to each CSV table (the PDF likely describes the
            # same logical table under a different name)
            for pdf_tm in pdf_by_target.values():
                _overlay_pdf_onto_csv(csv_tm, pdf_tm)

        merged_tables.append(csv_tm)

    return ParsedRequirements(
        source_name=source_name,
        layer=layer,
        table_mappings=merged_tables,
        raw_text=merged_raw_text,
        metadata=merged_metadata,
    )


def _overlay_pdf_onto_csv(csv_tm: TableMapping, pdf_tm: TableMapping) -> None:
    """Overlay PDF-sourced business rules and source tables onto CSV TableMapping."""
    # Business rules and join logic: PDF is authoritative
    if pdf_tm.business_rules:
        csv_tm.business_rules = pdf_tm.business_rules
    if pdf_tm.join_logic:
        csv_tm.join_logic = pdf_tm.join_logic

    # Merge source tables: PDF may identify additional sources not in CSV
    csv_sources = {s.strip().lower() for s in csv_tm.source_table.split(",") if s.strip()}
    pdf_sources = {s.strip().lower() for s in pdf_tm.source_table.split(",") if s.strip()}
    new_sources = pdf_sources - csv_sources

    if new_sources:
        # Append new PDF-identified source tables to trigger aggregation path
        existing = csv_tm.source_table
        additional = ",".join(sorted(new_sources))
        csv_tm.source_table = f"{existing},{additional}" if existing else additional

    # If PDF provides a richer source_dataset (e.g. with project prefix), use it
    if pdf_tm.source_dataset and not csv_tm.source_dataset:
        csv_tm.source_dataset = pdf_tm.source_dataset
