"""Compatibility wrapper for the canonical pipeline utils.job_utils module."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "utils/job_utils.py")
