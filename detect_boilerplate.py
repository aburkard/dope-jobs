"""Compatibility wrapper for the canonical pipeline boilerplate detector."""

from legacy_pipeline_bridge import reexport_pipeline_module, run_pipeline_main


_module = reexport_pipeline_module(globals(), "detect_boilerplate.py")


if __name__ == "__main__":
    run_pipeline_main(_module)
