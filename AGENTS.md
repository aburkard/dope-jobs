Archive repo notes:

- This repo is not the active app or active pipeline.
- Use `/Users/aburkard/fun/dopejobs-front` for frontend/product work.
- Use `/Users/aburkard/fun/dope-jobs-pipeline` for scraping, parsing, DB state, and Meilisearch loads.
- Treat any pipeline-like code in this repo as legacy/reference only unless the user explicitly asks to work here.
- For live full-reload/import progress, use `ops/import_status.py` from this repo with the shared `.env`; it reports DB-backed progress plus recent Meili task throughput/ETA.
- For the search URL-state feature, the regression test lives in `/Users/aburkard/fun/dopejobs-front` as `npm run test:url-state`.

Production search notes:

- The live Meilisearch `jobs` index is loaded from `/Users/aburkard/fun/dope-jobs-pipeline`, not this repo.
- The `jobs` index primary key must be `meili_id`, not raw `id`. Raw job ids can contain `.` and are not safe as Meili document ids.
- Keep both fields in documents: `id` is the canonical job id; `meili_id` is the deterministic Meili-safe surrogate.
- If the index gets mixed/corrupted, the safe repair is: delete `jobs`, then do a clean full reload from the production pipeline repo DB loader.
- Internal background Meili traffic should use `https://search-internal.dopejobs.xyz`, not `search.dopejobs.xyz`.
- `search-internal.dopejobs.xyz` is Cloudflare-proxied, protected by Cloudflare Access service-token headers, and origin-blocked for non-Cloudflare source IPs.
- The embedder (`perplexity-embedder`, port 8087) must not be reachable from the internet: it proxies to OpenRouter with a paid key. A host iptables rule in `/etc/iptables/rules.v4` drops 8087 unless it arrives on `lo`; Meili still reaches it via `http://178.156.164.231:8087` because traffic to the box's own IP routes over `lo`. Keep that rule if you edit the firewall.
- The Hetzner origin exempts Cloudflare IP ranges from the coarse `80/443` connection throttle. If internal access breaks, first verify Cloudflare Access headers and then refresh the Cloudflare IP allowlist on the origin.
