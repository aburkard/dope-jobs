"""Compatibility wrapper for the canonical pipeline Lever scraper."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "scrapers/lever_scraper.py")
