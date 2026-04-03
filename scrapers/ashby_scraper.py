"""Compatibility wrapper for the canonical pipeline Ashby scraper."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "scrapers/ashby_scraper.py")
