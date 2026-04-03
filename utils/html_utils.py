"""Compatibility wrapper for the canonical pipeline utils.html_utils module."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "utils/html_utils.py")
