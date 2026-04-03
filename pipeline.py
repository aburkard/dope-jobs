"""Compatibility wrapper for the canonical pipeline runner module."""

from legacy_pipeline_bridge import reexport_pipeline_module, run_pipeline_main


_pipeline_module = reexport_pipeline_module(globals(), "pipeline.py")


if __name__ == "__main__":
    run_pipeline_main(_pipeline_module)
