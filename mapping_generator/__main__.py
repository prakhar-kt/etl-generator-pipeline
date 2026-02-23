"""Allow running as: python -m mapping_generator"""
from .cli import main
import sys

sys.exit(main())
