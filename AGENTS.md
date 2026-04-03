Archive repo notes:

- This repo is not the active app or active pipeline.
- Use `/Users/aburkard/fun/dopejobs-front` for frontend/product work.
- Use `/Users/aburkard/fun/dope-jobs-pipeline` for scraping, parsing, DB state, and Meilisearch loads.
- Treat any pipeline-like code in this repo as legacy/reference only unless the user explicitly asks to work here.

Production search notes:

- The live Meilisearch `jobs` index is loaded from `/Users/aburkard/fun/dope-jobs-pipeline`, not this repo.
- The `jobs` index primary key must be `meili_id`, not raw `id`. Raw job ids can contain `.` and are not safe as Meili document ids.
- Keep both fields in documents: `id` is the canonical job id; `meili_id` is the deterministic Meili-safe surrogate.
- If the index gets mixed/corrupted, the safe repair is: delete `jobs`, then do a clean full reload from the production pipeline repo DB loader.
