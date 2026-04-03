"""Compatibility wrapper for the canonical pipeline parse module."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "parse.py")
