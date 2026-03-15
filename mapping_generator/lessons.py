"""Learn from past YAML generation mistakes stored in pipeline_artifacts."""

import logging
import os
import re
from collections import defaultdict

logger = logging.getLogger("lessons")

# Hard-coded lessons from known recurring issues (always included)
BASELINE_LESSONS = [
    {
        "error_pattern": "FLOAT64 cannot be assigned to.*NUMERIC",
        "rule": "SAFE_DIVIDE, ROUND, and division (/) return FLOAT64. Always wrap with CAST(... AS NUMERIC) when the target column is NUMERIC.",
    },
    {
        "error_pattern": "Name.*LOAD_DATE.*not found",
        "rule": "CDL tables use CDL_LOAD_DATE, not LOAD_DATE or LAST_MODIFY_DATE. Those columns do not exist.",
    },
    {
        "error_pattern": "not grouped nor aggregated",
        "rule": "Every non-aggregated column in a SELECT with GROUP BY must appear in GROUP BY. No exceptions.",
    },
    {
        "error_pattern": "AS.*VALUES",
        "rule": "BigQuery MERGE INSERT VALUES() does not allow AS aliases. Use bare expressions only.",
    },
    {
        "error_pattern": "Unrecognized name.*src",
        "rule": "Do not use TO_JSON_STRING(src) — the 'src' STRUCT is not defined. Use explicit column references for ADMIN_ROW_HASH.",
    },
    {
        "error_pattern": "Unexpected keyword MERGE",
        "rule": "merge_statement must contain ONLY the MERGE SQL. Do not include get_max_date or other queries before it.",
    },
    {
        "error_pattern": "GBQ Project\\.Dataset",
        "rule": "Never use 'GBQ Project.Dataset' as a placeholder. Use {{ source_projects[0] }}.CDL_NovaStar.<table> for CDL tables.",
    },
]


def get_lessons_from_bq(max_lessons: int = 10) -> list[dict]:
    """Query pipeline_artifacts for past error→fix pairs and distill into lessons.

    Returns list of dicts with keys: error_pattern, rule, example_error
    """
    try:
        from google.cloud import bigquery
        project = os.environ.get("GCP_PROJECT_ID")
        if not project:
            return []
        client = bigquery.Client(project=project)
    except Exception:
        return []

    try:
        # Find artifacts where execution or tests failed and were later fixed
        sql = f"""
        WITH failures AS (
            SELECT
                artifact_id,
                version,
                error_message,
                yaml_content,
                ROW_NUMBER() OVER (PARTITION BY artifact_id ORDER BY version ASC) as rn
            FROM `{project}.Business_Logic.pipeline_artifacts`
            WHERE error_message IS NOT NULL
              AND error_message != ''
              AND LENGTH(error_message) > 10
        ),
        fixes AS (
            SELECT
                artifact_id,
                version,
                yaml_content,
                status
            FROM `{project}.Business_Logic.pipeline_artifacts`
            WHERE status IN ('executed', 'passed')
        )
        SELECT
            f.error_message,
            f.yaml_content as broken_yaml,
            fx.yaml_content as fixed_yaml
        FROM failures f
        JOIN fixes fx ON f.artifact_id = fx.artifact_id AND fx.version > f.version
        WHERE f.rn = 1
        ORDER BY f.version DESC
        LIMIT {max_lessons}
        """
        job = client.query(sql)
        rows = list(job.result())

        if not rows:
            return []

        return _distill_lessons(rows)

    except Exception as e:
        logger.debug(f"Could not fetch lessons from BQ: {e}")
        return []


def _distill_lessons(rows: list) -> list[dict]:
    """Distill raw error→fix pairs into concise rules."""
    # Group similar errors
    error_groups = defaultdict(list)
    for row in rows:
        error_msg = row.error_message or ""
        # Normalize error: strip job IDs, timestamps, etc.
        normalized = re.sub(r'Job ID: [a-f0-9-]+', '', error_msg)
        normalized = re.sub(r'Location: \w+', '', normalized)
        normalized = re.sub(r'at \[\d+:\d+\]', '', normalized)
        normalized = normalized.strip()

        # Categorize by error type
        if "FLOAT64" in error_msg and "NUMERIC" in error_msg:
            key = "type_mismatch_float_numeric"
        elif "not found" in error_msg.lower() and "column" in error_msg.lower():
            key = "column_not_found"
        elif "not grouped" in error_msg.lower() or "not aggregated" in error_msg.lower():
            key = "group_by_missing"
        elif "syntax error" in error_msg.lower():
            key = "syntax_error"
        elif "duplicate" in error_msg.lower():
            key = "duplicate_key"
        elif "NULL" in error_msg or "null" in error_msg:
            key = "null_values"
        else:
            key = normalized[:80]

        error_groups[key].append({
            "error": error_msg[:300],
            "broken": (row.broken_yaml or "")[:500],
            "fixed": (row.fixed_yaml or "")[:500],
        })

    lessons = []
    for key, examples in error_groups.items():
        # Take the most recent example
        ex = examples[0]

        # Try to extract a concise diff
        diff_hint = _extract_diff_hint(ex["broken"], ex["fixed"])

        lessons.append({
            "error_pattern": key,
            "rule": diff_hint or f"Error occurred: {ex['error'][:200]}",
            "example_error": ex["error"][:200],
        })

    return lessons


def _extract_diff_hint(broken: str, fixed: str) -> str:
    """Try to identify what changed between broken and fixed YAML."""
    if not broken or not fixed:
        return ""

    broken_lines = set(broken.strip().split('\n'))
    fixed_lines = set(fixed.strip().split('\n'))

    added = fixed_lines - broken_lines
    removed = broken_lines - fixed_lines

    # Filter to meaningful changes (skip whitespace-only diffs)
    added = {l.strip() for l in added if l.strip() and not l.strip().startswith('#')}
    removed = {l.strip() for l in removed if l.strip() and not l.strip().startswith('#')}

    if not added and not removed:
        return ""

    hints = []
    if removed:
        hints.append("Avoid: " + "; ".join(list(removed)[:2]))
    if added:
        hints.append("Use instead: " + "; ".join(list(added)[:2]))

    return " | ".join(hints)[:300]


def format_lessons_prompt(max_dynamic: int = 5) -> str:
    """Build the lessons section for the LLM prompt.

    Combines baseline (hard-coded) lessons with dynamically learned ones from BQ.
    """
    sections = []

    # Always include baseline lessons
    sections.append("### Hard Rules (never violate these):")
    for lesson in BASELINE_LESSONS:
        sections.append(f"- {lesson['rule']}")

    # Add dynamic lessons from past mistakes
    dynamic = get_lessons_from_bq(max_lessons=max_dynamic)
    if dynamic:
        sections.append("\n### Learned from Past Mistakes (avoid repeating these):")
        for lesson in dynamic:
            example = lesson.get("example_error", "")
            rule = lesson.get("rule", "")
            if example:
                sections.append(f"- Past error: \"{example[:150]}\"")
            if rule:
                sections.append(f"  Fix: {rule[:200]}")

    return "\n".join(sections)
