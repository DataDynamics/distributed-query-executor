"""Ensure the repository root is importable as ``coordinator`` / ``executor``."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
