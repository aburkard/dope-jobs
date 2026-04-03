"""Compatibility wrapper for the canonical pipeline job grouping module."""

from legacy_pipeline_bridge import reexport_pipeline_module, run_pipeline_main


_module = reexport_pipeline_module(globals(), "job_groups.py")


if __name__ == "__main__":
    run_pipeline_main(_module)
