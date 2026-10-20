# Safco Catalog Scraper

POC for extracting Safco's catalogue into a dated, SKU-level JSON snapshot, using HTTP/API collection, SQLite checkpoints and optional bounded agent recovery. It traverses [Dental Exam Gloves](https://www.safcodental.com/catalog/gloves) and [Sutures & surgical products](https://www.safcodental.com/catalog/sutures-surgical-products), collecting each discovered product and its item variations.

## Setup and execution

Requires Python 3.11+ on macOS or Linux. Install from the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
```

Optional: enable hosted agent recovery before starting a run. The public endpoint requires no API key or token and is available until October 27, 2026 at 23:59 UTC.

```sh
export SAFCO_RECOVERY_ENDPOINT=https://safco-recovery.vercel.app/api/recovery
export SAFCO_RECOVERY_MODEL=gpt-6-luna
```

Start a run across both default categories, attempting every discovered product. Use a fresh directory and run ID for each new run:

```sh
mkdir -p runs/2026-10-20-full
python -m safco_catalog_scraper run \
  --database runs/2026-10-20-full/catalog.sqlite --run-id 2026-10-20-full \
  --output runs/2026-10-20-full/catalog.json \
  > runs/2026-10-20-full/report.json
```

After an interruption, resume with the same database and run ID:

```sh
python -m safco_catalog_scraper resume \
  --database runs/2026-10-20-full/catalog.sqlite --run-id 2026-10-20-full \
  --output runs/2026-10-20-full/catalog.json > runs/2026-10-20-full/report.json
```

To enable recovery on a deterministic run that finished partial/failed, set the recovery variables above and replace `resume` with `recover`. It reuses saved evidence and accepted records, then continues remaining work. If interrupted, use `resume`.

Catalog JSON goes to `--output`, reports to stdout and progress logs to stderr. Exit codes: `0` complete/successful export, `1` incomplete/error, `2` invalid setup, `130` interrupted.

Use `run --help` for categories, retries and recovery limits. Add `--unresolved-product-limit N` to stop after N unresolved products; this limit is separate from the recovery budget. Use `export --help` to export saved progress.

For optional tests, run `python -m pip install '.[test]'`, then `python -m pytest`.

## Architecture overview

One local Python process runs the collector:

```text
CLI -> controller -> HTTPX -> discovery / extraction -> SQLite -> JSON
              `-> recovery loop -> evidence tools + Vercel model endpoint
```

Discovery traverses Algolia results and deduplicates product URLs. Collection saves responses before extracting SKU data. Recovery proposes source locations; evidence tools resolve them; deterministic validators check prices, identities and counts before atomic acceptance. The local controller owns scheduling, retries and the recovery loop. Four SQLite tables retain settings, tasks, items, recovery history, budget reservations and checkpoints; export reads a consistent snapshot. Progress logs and reports are produced locally. See [DATA_DESIGN.md](DATA_DESIGN.md) for contracts and data flow.

## Why this approach

Safco exposes category API results and SKU data in `window.masterData`, including rows displayed through client-side pagination. HTTP collection handles these patterns reproducibly. The agent can investigate unfamiliar dataset locations using saved evidence.

## Agent responsibilities

Deterministic checks detect structural errors. The agent inspects saved page data, can repeat a product request and proposes an alternative location for the item dataset, such as another script or JSON path. The existing extractor parses and validates that source data before accepting it.

Recovery allows 3 cases and 12 model calls per run, with at most 4 calls per case by default. SQLite reserves actions before execution and saves results and usage afterward. Resume preserves history and consumed budget; model calls with unknown outcomes are not automatically repeated.

Vercel is a stateless model gateway for the demo: it keeps the provider key off users' machines and returns one model decision and usage per request. The local collector runs the recovery loop and tools, and stores history, budgets and checkpoints.

## Deployment, configuration and secrets

| Location | Configuration |
|---|---|
| Collector | `SAFCO_RECOVERY_ENDPOINT`, `SAFCO_RECOVERY_MODEL` and CLI run settings; non-secret settings are retained in SQLite for resume |
| Vercel Config | `SAFCO_RECOVERY_MODEL` (matching the collector), `SAFCO_RECOVERY_ACCESS_MODE` and `SAFCO_RECOVERY_DEMO_EXPIRES_AT` |
| Vercel Secret | `OPENAI_API_KEY`; also `SAFCO_RECOVERY_TOKEN` for authenticated access |

Access defaults to `authenticated`, which requires the same token in the collector's environment. The POC uses `public_demo` for token-free access until the configured UTC expiry. The provider key stays on Vercel; workflow state and recovery budgets stay local.

For production, the planned setup is a scheduled backend worker with durable storage and direct model API calls. It should:

- Use scoped service credentials; authenticate and rate-limit any exposed endpoint.
- Inject credentials from a managed secret store, separate keys by environment and rotate them. Keep secrets out of source, prompts, checkpoints and logs.
- Validate configuration at startup and version the model, prompt and tool contracts alongside saved run settings.


## Sample output schema

The [catalog sample](examples/catalog.json) contains 742 item observations from 144 product pages, representing 741 distinct SKUs. The [run report](examples/report.json) accounts for all 148 discovered products: 134 complete, 10 partial and 4 with empty item datasets; no URLs remain unattempted. These two saved cases show a complete product and a partial product. Each shows one item with selected fields and notes:

```json
{
  "run": {"run_id": "2026-10-20-full", "status": "partial"},
  "products": [
    {
      "product_url": "https://www.safcodental.com/product/aurelia-reg-blush-reg",
      "product_name": "Aurelia Blush",
      "items": [
        {
          "sku": "4871204",
          "item_name": "Aurelia Blush gloves small 200/box",
          "manufacturer": "Supermax",
          "selling_unit": "box",
          "pack_size": "200/box",
          "base_price": "16.49",
          "quantity_tiers": [{"min_quantity": 10, "unit_price": "15.99"}],
          "notes": []
        }
      ]
    },
    {
      "product_url": "https://www.safcodental.com/product/collashield-trade-collagen-membrane",
      "product_name": "CollaShield Collagen Membrane",
      "items": [
        {
          "sku": "6130050",
          "item_name": "CollaShield Collagen Membrane 15x20mm",
          "manufacturer": "Surgical Esthetics",
          "selling_unit": null,
          "pack_size": null,
          "base_price": "128.99",
          "quantity_tiers": [],
          "notes": [
            {
              "field": "selling_unit",
              "reason": "unparsed_source",
              "source_field": "masterData.description",
              "source_text": "15 x 20mm"
            },
            {
              "field": "pack_size",
              "reason": "unparsed_source",
              "source_field": "masterData.description",
              "source_text": "15 x 20mm"
            }
          ]
        }
      ]
    }
  ]
}
```

- **Happy path — Aurelia Blush:** `200/box` identifies the selling unit and pack size. The box costs $16.49, or $15.99 per box when ordering at least 10 boxes. No field notes are needed.
- **Partial — CollaShield:** the collected item name and description give membrane dimensions, not a pack quantity or selling unit. Both fields remain `null` rather than assuming one membrane per pack. The item and price are retained, source wording is preserved in `notes`, and the report flags the product as partial.

Prices are decimal strings per selling unit; tiers use the same unit. Promotions and calculated unit prices are excluded. [DATA_DESIGN.md](DATA_DESIGN.md) defines all fields, nullable values and source-wording notes.

## Failure handling

| Condition | Response |
|---|---|
| Transient request failure | Bounded retries; honor Retry-After, including recovery requests |
| Structural or coverage issue | Attempt configured recovery; retain accepted records and unresolved findings |
| Recovery budget reached | Continue ordinary collection; report recovery that was deferred |
| Optional unresolved-product limit reached | Stop scheduling new products; preserve progress and report unattempted work |
| Process interruption | Resume saved checkpoints with original code/runtime/settings, preserving attempts and budgets |
| Storage or unexpected execution failure | Stop; surface safe failure locations and preserve saved progress |
| Catalog export failure | Preserve the collection outcome/report; retry with export without recrawling |

`resume` accepts running/interrupted runs. The explicit `recover` transition can reopen a partial/failed deterministic run once; other terminal runs remain closed. Export replaces the catalog file atomically and preserves the saved run status.

## Limitations

Discovery starts from supplied category URLs, with the two required categories as defaults; repeat `--category` to override them. Site-wide category discovery is deferred. Extraction supports Safco's observed patterns. Products without explicit unit/pack wording can remain partial after recovery. Family-level descriptions, specifications/attributes, image URLs and alternative products are deferred; item-row descriptions are retained. Images are not downloaded.

The [historical audit receipt](examples/source-audit.json) accounts for 148 product pages and 742 item records from 144 pages; four pages exposed empty datasets. It predates the sample run and is not a current completeness claim. Live counts may change.

Recovery is limited to relocating supported JSON datasets. It does not invent absent values or relax pricing validation. A proposal can remain partial or be rejected.

Checks on the required Safco categories and sampled products from other categories have not demonstrated a failure requiring browser navigation. Browser Use-style automation is therefore unnecessary for the demonstrated scope and deferred until such a case is observed. These checks do not establish reliability across the full catalog.

## Monitoring data quality

- **Coverage:** reports compare discovered URLs, source totals, decoded rows and accepted SKUs. Unknown counts stay `null`; known shortfalls include missing-row counts.
- **Unresolved work:** reports list incomplete and unattempted products; SKU-level `notes` explain missing fields and retain source wording.
- **LLM fallback cost:** reports show model calls and input/output tokens. Recovery budgets cap cases and calls; no dollar limit is enforced locally.
- **Diagnostics:** logs show progress, retries and safe failure locations. SQLite retains source evidence and recovery decisions; reports include tool errors.

See [report semantics](DATA_DESIGN.md#reporting).

## Scaling to full-site crawling in production

- Run the job on a scheduled container with persistent storage; add category-link discovery for full-site collection.
- Each SQLite run database supports one active collector. Distributed workers require shared task coordination and storage.
- Measure request limits and memory before adding concurrency. Incremental refreshes remain outside this snapshot-based POC.
