from .csv_parser import CSVParser, FieldMapping, ParsedRequirements, TableMapping
from .merge import merge_requirements


def PDFParser(*args, **kwargs):
    """Lazy import PDFParser to avoid requiring pypdf for CSV-only usage."""
    from .pdf_parser import PDFParser as _PDFParser
    return _PDFParser(*args, **kwargs)


__all__ = [
    "CSVParser",
    "PDFParser",
    "FieldMapping",
    "TableMapping",
    "ParsedRequirements",
    "merge_requirements",
]
