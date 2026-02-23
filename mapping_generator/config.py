import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# LLM API configuration
# Supports "gemini" or "anthropic" as providers
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini")

# Gemini configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# Anthropic configuration (fallback)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

MAX_TOKENS = 8192

# Path to existing Smart_DTC mappings (used for few-shot examples)
MAPPINGS_ROOT = Path(os.environ.get(
    "SMART_DTC_MAPPINGS_ROOT",
    os.path.join(os.path.dirname(__file__), "..", "..", "composer", "airflow", "dags", "Smart_DTC", "Mappings")
    if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "..", "composer"))
    else os.path.expanduser("~/Mattel/composer/airflow/dags/Smart_DTC/Mappings")
))

# Layer definitions
LAYERS = {
    "CDL": {
        "subfolder": "CDL",
        "stage_name": "RAW to CDL",
        "yaml_style": "dataflow",
    },
    "Cleansed_RAW": {
        "subfolder": "Cleansed_RAW",
        "stage_name": "RAW to Cleansed",
        "yaml_style": "dataflow",
    },
    "BL": {
        "subfolder": "BL",
        "stage_name": "CDL to BL",
        "yaml_style": "sql",
    },
    "BR": {
        "subfolder": "BR",
        "stage_name": "BL to BR",
        "yaml_style": "sql",
    },
}

# Standard ADMIN columns present in all BL/BR tables
ADMIN_COLUMNS = [
    ("ADMIN_PROCESS_ID", "STRING"),
    ("ADMIN_COMPOSITEKEY_HASH", "INT64"),
    ("ADMIN_SCHEDULER_TYPE", 'STRING OPTIONS(description="Scheduler used to load table data")'),
    ("ADMIN_ROW_HASH", 'INT64 OPTIONS(description="Hash value of entire row excluding admin fields")'),
    ("ADMIN_LOAD_DATE", 'TIMESTAMP OPTIONS(description="Time of original data load")'),
    ("ADMIN_ISDELETED", 'BOOL OPTIONS(description="Indicator to identify deleted records")'),
    ("ADMIN_ISERROR", 'BOOL OPTIONS(description="Indicator to determine if a record has failed DQ validation")'),
    ("ADMIN_LAST_MODIFIED_DATE", 'TIMESTAMP OPTIONS(description="Time when a record was last updated")'),
    ("ADMIN_DML_OPERATION_FLAG", 'STRING OPTIONS(description="Flag to indicate if a record was (I)nserted, (U)pdated or (D)eleted")'),
    ("ADMIN_RECORD_STATUS", 'STRING OPTIONS(description="Indicator to determine if a record is Active or Inactive")'),
    ("ADMIN_SOURCE_SYSTEM", "STRING"),
]

# Standard ADMIN fields for MERGE INSERT values
ADMIN_MERGE_INSERT_VALUES = """      '{{ process_id }}' AS ADMIN_PROCESS_ID,
      ADMIN_COMPOSITEKEY_HASH,
      'COMPOSER' AS ADMIN_SCHEDULER_TYPE,
      FARM_FINGERPRINT(TO_JSON_STRING(src)) AS ADMIN_ROW_HASH,
      CURRENT_TIMESTAMP() AS ADMIN_LOAD_DATE,
      FALSE AS ADMIN_ISDELETED,
      FALSE AS ADMIN_ISERROR,
      CURRENT_TIMESTAMP() AS ADMIN_LAST_MODIFIED_DATE,
      'INSERT' AS ADMIN_DML_OPERATION_FLAG,
      'ACTIVE' AS ADMIN_RECORD_STATUS"""

# Standard ADMIN fields for MERGE UPDATE set clause
ADMIN_MERGE_UPDATE_SET = """      TARGET.ADMIN_LAST_MODIFIED_DATE = CURRENT_TIMESTAMP(),
      TARGET.ADMIN_DML_OPERATION_FLAG = 'UPDATE'"""
