from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any


PIPELINE_REPO = Path("/Users/aburkard/fun/dope-jobs-pipeline")


@lru_cache(maxsize=None)
def load_pipeline_module(relative_path: str) -> ModuleType:
    path = PIPELINE_REPO / relative_path
    if not path.exists():
        raise ImportError(f"Missing pipeline module: {path}")
    module_name = f"_dopejobs_pipeline_{relative_path.replace('/', '_').replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load pipeline module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(PIPELINE_REPO))
    try:
        spec.loader.exec_module(module)
    finally:
        if sys.path and sys.path[0] == str(PIPELINE_REPO):
            sys.path.pop(0)
    return module


def reexport_pipeline_module(module_globals: dict[str, Any], relative_path: str) -> ModuleType:
    module = load_pipeline_module(relative_path)
    public_names = getattr(module, "__all__", None)
    if public_names is None:
        public_names = [name for name in vars(module) if not name.startswith("_")]
    for name in public_names:
        module_globals[name] = getattr(module, name)
    module_globals["__all__"] = list(public_names)
    module_globals["__doc__"] = getattr(module, "__doc__", None)
    module_globals["_PIPELINE_SOURCE"] = str(PIPELINE_REPO / relative_path)
    return module


def run_pipeline_main(module: ModuleType) -> None:
    main = getattr(module, "main", None)
    if not callable(main):
        raise SystemExit(f"{module.__name__} has no callable main()")
    raise SystemExit(main())
