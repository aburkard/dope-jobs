"""Compatibility wrapper for the canonical pipeline base scraper."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "scrapers/base_scraper.py")
