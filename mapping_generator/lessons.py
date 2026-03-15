"""Learn from past YAML generation mistakes stored in pipeline_lessons."""

import logging
import os
import re
from collections import defaultdict

logger = logging.getLogger("lessons")


# ---------- Lessons storage ----------

LESSONS_DDL = """
CREATE TABLE IF NOT EXISTS `{project}.Business_Logic.pipeline_lessons` (
  id              STRING NOT NULL,
  context         STRING NOT NULL,
  error_message   STRING NOT NULL,
  fix_description STRING,
  error_category  STRING,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


def ensure_lessons_table(client, project: str):
    """Create pipeline_lessons table if it doesn't exist."""
    try:
        sql = LESSONS_DDL.format(project=project)
        job = client.query(sql)
        job.result()
    except Exception:
        pass


def store_lesson(error_message: str, fix_description: str, context: str = "execution"):
    """Store an error→fix lesson in BigQuery for future learning."""
    try:
        from google.cloud import bigquery
        project = os.environ.get("GCP_PROJECT_ID")
        if not project:
            return
        client = bigquery.Client(project=project)
        ensure_lessons_table(client, project)

        category = _categorize_error(error_message)

        import uuid
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
        sql = f"""INSERT INTO `{project}.Business_Logic.pipeline_lessons`
        (id, context, error_message, fix_description, error_category, created_at)
        VALUES (@id, @ctx, @err, @fix, @cat, CURRENT_TIMESTAMP())"""
        job_config = QueryJobConfig(query_parameters=[
            ScalarQueryParameter("id", "STRING", str(uuid.uuid4())),
            ScalarQueryParameter("ctx", "STRING", context[:50]),
            ScalarQueryParameter("err", "STRING", error_message[:500]),
            ScalarQueryParameter("fix", "STRING", fix_description[:500]),
            ScalarQueryParameter("cat", "STRING", category[:100]),
        ])
        job = client.query(sql, job_config=job_config)
        job.result()
        logger.info(f"Stored lesson: {category}")
    except Exception as e:
        logger.debug(f"Failed to store lesson: {e}")


def _categorize_error(error_message: str) -> str:
    """Categorize an error message into a bucket."""
    msg = error_message.lower()
    if "float64" in msg and "numeric" in msg:
        return "type_mismatch_float_numeric"
    if "not found" in msg and ("column" in msg or "name" in msg):
        return "column_not_found"
    if "not grouped" in msg or "not aggregated" in msg:
        return "group_by_missing"
    if "syntax error" in msg:
        return "syntax_error"
    if "duplicate" in msg:
        return "duplicate_key"
    if "as" in msg and "values" in msg:
        return "as_in_values"
    return "other"

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
    {
        "error_pattern": "must be qualified with a dataset",
        "rule": "Do NOT use reserved words as CTE or alias names. Avoid: src, final, source, target, result, data. Use descriptive names like base_demand, deduped_data, final_output instead.",
    },
    {
        "error_pattern": "NULL.*FULL OUTER JOIN|NULLs found",
        "rule": "When using FULL OUTER JOIN to combine fact tables, ALWAYS wrap ALL columns with COALESCE to prevent NULLs: COALESCE(col, 0) for INT64/NUMERIC, COALESCE(col, 'N/A') for STRING, COALESCE(col, DATE('1900-01-01')) for DATE, COALESCE(col, TIMESTAMP('1900-01-01')) for TIMESTAMP. No column in the final output should ever be NULL.",
    },
]


def get_lessons_from_bq(max_lessons: int = 10) -> list[dict]:
    """Query pipeline_lessons for past error→fix pairs.

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
        sql = f"""
        SELECT error_category, error_message, fix_description, context,
               COUNT(*) as occurrences
        FROM `{project}.Business_Logic.pipeline_lessons`
        GROUP BY error_category, error_message, fix_description, context
        ORDER BY occurrences DESC, error_category
        LIMIT {max_lessons}
        """
        job = client.query(sql)
        rows = list(job.result())

        if not rows:
            return []

        # Deduplicate by category — keep the one with most occurrences
        seen_categories = set()
        lessons = []
        for row in rows:
            cat = row.error_category or "other"
            if cat in seen_categories:
                continue
            seen_categories.add(cat)
            lessons.append({
                "error_pattern": cat,
                "rule": row.fix_description or "",
                "example_error": (row.error_message or "")[:200],
                "context": row.context or "execution",
                "occurrences": row.occurrences,
            })

        return lessons

    except Exception as e:
        logger.debug(f"Could not fetch lessons from BQ: {e}")
        return []


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
