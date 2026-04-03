"""Compatibility wrapper for the canonical pipeline Greenhouse scraper."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "scrapers/greenhouse_scraper.py")
