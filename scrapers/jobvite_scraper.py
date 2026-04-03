"""Compatibility wrapper for the canonical pipeline Jobvite scraper."""

from legacy_pipeline_bridge import reexport_pipeline_module


reexport_pipeline_module(globals(), "scrapers/jobvite_scraper.py")
