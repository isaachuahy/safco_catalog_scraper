# Data dictionary and data flow

Status: deterministic collection, SQLite checkpoints, bounded saved-evidence recovery, CLI run/resume/recover, validation and JSON export are implemented. The deployed endpoint passed controlled real-model recovery checks for relocated product data and category configuration. Offline checks also cover correcting a saved lookup error and resuming after a crash at that checkpoint. Broader recovery reliability evaluation remains open; browser navigation is outside this slice.

Use one SQLite database per dated run. Four tables separate run settings, resumable work, accepted catalog data, and execution evidence. JSON is the export format.

## Conventions

- All timestamps are UTC ISO 8601 strings. Monetary amounts are decimal strings. SKUs and manufacturer part numbers remain strings.
- Nullable means the column exists but may contain SQL NULL. Empty lists use JSON arrays, not NULL.
- Store JSON as validated TEXT. Use it for nested values rather than separate category, pricing-tier, or agent-step tables.
- Enable foreign keys and durable SQLite commits. Keep the database on persistent storage; transactions cannot protect against losing that storage.
- One collector owns a run. Do not keep a transaction open during network or model calls.
- Preserve source item wording alongside parsed fields. Populate selling_unit and pack_size when the source supports an unambiguous interpretation; explain unsupported or conflicting wording in notes. Unit names are unrestricted text, with no fixed allowlist.

## 1. runs

One row describes the run and its execution policy.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| run_id | TEXT, primary key | No | Stable identity retained across restarts; a new intentional collection gets a new ID |
| started_at | TEXT | No | Run creation time |
| finished_at | TEXT | Yes | Final completion or stop time |
| status | TEXT | No | running, interrupted, complete, partial, or failed |
| stop_reason | TEXT | Yes | Why scheduling stopped, including configured limits or storage errors |
| code_version | TEXT | No | Code revision used for the run |
| config_hash | TEXT | No | Hash of the saved, non-secret configuration |
| config_json | TEXT, JSON object | No | Categories, selected scope, parser/prompt/model settings, rate limits, retries, and absolute recovery/stop limits |
| budget_json | TEXT, JSON object | No | Persisted reserved and consumed recovery allowance, including uncertain usage |

Credentials are supplied separately and never stored in configuration or events. Budget reservation and event creation commit together. Resume retains the original limits and refuses incompatible code/configuration.

`recover` is an explicit one-time transition for a partial/failed deterministic run with unused recovery allowance. It enables a bounded hosted policy, adopts the current parser code and requeues unresolved products from intact saved responses. Collection settings, accepted rows, discovery checkpoints and attempts remain intact. Each requeued task receives a recovery audit event containing the previous configuration hash, code identity and stop reason. Policy, audit and requeue commit together; an interruption afterward uses ordinary `resume`. This transition cannot reset a consumed budget or re-enable an already configured recovery run.

## 2. tasks

One row represents one category results page or one product URL. A product task covers all SKU rows on that page; SKUs are not separate tasks.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| task_id | INTEGER, primary key | No | Internal work identity |
| run_id | TEXT, foreign key to runs | No | Owning run |
| kind | TEXT | No | discovery or product |
| target_key | TEXT | No | Canonical product URL, or stable category/page request identity |
| request_json | TEXT, JSON object | No | Non-secret URL, method, and parameters needed to reproduce the request |
| status | TEXT | No | pending, running, complete, partial, failed, or deferred |
| stage | TEXT | No | fetch, extract, or recovery |
| discovered_from_json | TEXT, JSON array | No | Discovery task/category references; merge identical associations without duplicates |
| checkpoint_json | TEXT, JSON object | No | Saved evidence references, pagination state, and recovery position |
| attempt_count | INTEGER | No | Attempts started; never reset on resume |
| next_retry_at | TEXT | Yes | Earliest permitted retry time |
| expected_item_count | INTEGER | Yes | Source-reported item total, when available |
| decoded_item_count | INTEGER | Yes | Rows decoded from source evidence |
| issues_json | TEXT, JSON array | No | Unresolved fields, count gaps, conflicts, and their evidence references |

Unique constraint: run_id + kind + target_key. Only verified irrelevant URL differences are removed during canonicalization. Discovery request identity includes the category, filters, and page/cursor.

Complete means the configured collection checks pass. Partial means usable records exist with unresolved required data or coverage. Failed means attempted collection produced no trustworthy item records. Deferred work is pending but not currently eligible, for example because recovery allowance is exhausted. Pending work never started is reported as not attempted.

Terminal partial/failed results remain visible on resume. Retrying them requires an explicit retry decision within the original limits; restart does not reset policy.

Unknown counts remain NULL. Known zero counts remain zero, including failed products with an explicitly empty dataset. Product failure finalization saves counts, issues, extraction outcome and task outcome in one transaction. It preserves earlier accepted rows and marks their task partial. Repeating an identical finalization is a no-op; changing its payload is rejected.

## 3. catalog_items

One accepted SKU observation per product page. Repeat parent-page fields on each row to avoid separate products and items tables.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| item_id | INTEGER, primary key | No | Internal row identity; not the Safco SKU |
| task_id | INTEGER, foreign key to tasks | No | Owning product task |
| extraction_event_id | INTEGER, foreign key to task_events | No | Validation operation that accepted the row; links back to source/recovery evidence |
| product_name | TEXT | No | Product-page heading |
| product_url | TEXT | No | Source product URL |
| captured_at | TEXT | No | Capture time of the underlying product observation, not export time |
| category_paths_json | TEXT, JSON array | Yes | Breadcrumb category paths, excluding Home and the product heading |
| sku | TEXT | No | Safco item number |
| item_name | TEXT | No | Item-row name; decode HTML entities without inventing a variation name |
| item_description | TEXT | Yes | Full item-row description from masterData.description, with HTML entities decoded; preserves source size/pack wording without interpreting it |
| manufacturer | TEXT | Yes | Manufacturer label supplied by Safco for the item |
| manufacturer_part_number | TEXT | Yes | Source Mfr # |
| selling_unit | TEXT | Yes | Parsed orderable unit, such as box; retain unfamiliar unit tokens when the source clearly identifies them, and note any standardization |
| pack_size | TEXT | Yes | Source pack expression, such as 100/bx or 50 pairs/box; preserve its wording without calculating contents or per-piece prices |
| availability | TEXT | Yes | Source stock indicator, such as in_stock or backorder |
| currency | TEXT | Yes | Explicitly established currency code |
| base_price | TEXT | Yes | Public price per selling unit |
| quantity_tiers_json | TEXT, JSON array | No | Objects containing min_quantity and unit_price; all refer to the same selling unit |
| notes_json | TEXT, JSON array | No | Final field-level explanations after validation and any eligible recovery; retains unresolved wording and explains nullable values or meaningful standardization; empty array when none apply |

Unique constraint: task_id + sku. The same SKU may retain associations with different product pages. Compare those observations without silently choosing one value.

Example: Aurelia Blush produces four rows. SKU 4871204 stores base_price 16.49 and one tier at quantity 10 with unit_price 15.99. Myco produces 36 rows in the checked baseline, including the GLASSVAN row under its source page.

Keep item_name and item_description even when their pack wording differs. Parse explicit quantity/unit expressions without restricting the unit vocabulary. Populate each supported field independently. Record an ambiguous field as an issue, attempt eligible bounded recovery, then leave it NULL with a final note if still unresolved. item_description is the item's source descriptor, not the longer product-family description. A later normalizer can extend these parsing rules using the retained wording.

Supported wording includes capacity followed by a container (`.5cc jar`, `0.5cc syringe`), explicit kit capacities and count packs (`5-pack`). When the selling unit is established, explicit `Contains:` wording from `masterData.short_description` can supply pack contents verbatim. Dimensions and bare capacities do not establish an orderable unit; no quantity or price is calculated.

### Field notes

Create final notes during validation after the recovery outcome is known. Extraction first records issues; eligible unresolved fields go through bounded recovery. Retain recovered values and remove resolved issues. Notes explain remaining gaps after partial or unsuccessful recovery. Confirmed source absence and nonpublic values need no recovery, but are still explained at final validation.

Export notes_json as notes. Each entry contains field, reason, source_field and source_text. source_field identifies the source location, such as masterData.name. source_text preserves the relevant wording, is NULL when absent, and uses a JSON representation for an unexpected non-text value. The row's extraction_event_id links to the saved evidence and recovery outcome. If recovery was deferred or unavailable, record why in the linked extraction operation and report that distinction; do not describe it as an attempted recovery.

| Reason | Meaning |
|---|---|
| source_absent | Evidence checks confirm no value at the expected source location; recovery is not required |
| unparsed_source | Source wording remains uninterpreted after eligible recovery or an explicitly recorded deferral |
| conflicting_source | Source locations still disagree after eligible recovery or an explicitly recorded deferral; preserve both values in separate notes for that field |
| nonpublic | The source explicitly withholds a public value, such as call-for-price |
| normalized_source_wording | A meaningful standardization was applied, such as bx to box; retain the original wording |

Every NULL in a nullable catalog field needs an explanation. Apply this to item_description, manufacturer, manufacturer_part_number, category_paths, availability, currency and base_price as well as unit/pack fields. Do not infer a manufacturer from a brand or turn a missing price into zero.

Illustrative JSON fragment after recovery establishes the selling unit but cannot establish the pack size:

```json
{
  "selling_unit": "box",
  "pack_size": null,
  "manufacturer_part_number": null,
  "notes": [
    {
      "field": "pack_size",
      "reason": "unparsed_source",
      "source_field": "masterData.description",
      "source_text": "Assorted sizes; pack quantities vary"
    },
    {
      "field": "manufacturer_part_number",
      "reason": "source_absent",
      "source_field": "masterData.manufacturer_part_number",
      "source_text": null
    }
  ]
}
```

Notes are the final explanation, not the trigger for recovery. tasks.issues_json and validation findings drive recovery eligibility and task status. Confirmed absence of optional data does not make a product partial. An unresolved expected field, unexplained price gap, structural failure or count gap remains an issue after recovery; adding a note does not clear it. Rejected rows retain their findings in task events/issues because they have no accepted catalog row.

Quantity tiers may arrive as a JSON list or an object keyed by row position. Both are decoded into min_quantity/unit_price records. Public price markup confirms which tiers are offered; an unrendered tier at or above the public base price is not exported as a discount. A lower tier missing expected markup remains an issue.

No promotion fields, calculated unit prices, or schema_version are included. Family-level descriptions, specifications/attributes, image URLs and alternative products are deferred; item-row descriptions are retained. A changed storage contract requires a new database; automatic migrations are outside this POC.

## 4. task_events

The work history: one row for an attempted operation, retaining its input, outcome, and evidence. This is an operation record, not one log message per state change.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| event_id | INTEGER, primary key | No | Stable operation identity |
| task_id | INTEGER, foreign key to tasks | No | Task being worked on |
| attempt_no | INTEGER | No | Task attempt containing this operation |
| type | TEXT | No | fetch, extract, or recovery |
| status | TEXT | No | started, succeeded, failed, or uncertain |
| started_at | TEXT | No | Saved before execution |
| finished_at | TEXT | Yes | Outcome time |
| details_json | TEXT, JSON object | No | Type-specific inputs, results, issues, and references |
| body | BLOB | Yes | Saved response bytes for a fetch |
| content_hash | TEXT | Yes | SHA-256 of the saved body |

Type-specific details:

| Type | details_json contents |
|---|---|
| fetch | Request context, HTTP status, response metadata, error if any |
| extract | Input event references, decoded/accepted counts, validation findings, traversal-completion evidence |
| recovery | Failure/objective, model and prompt references, ordered steps, proposed source locations, usage, optional external execution IDs; or an explicit enable_recovery policy audit |

A recovery row contains its intermediate steps. Persist each intended action and allowance reservation before execution, then its observation and usage afterward. Retain operational inputs/results, not private model reasoning. Stable request IDs correlate operations; they do not provide remote exactly-once execution. A saved result is replayed locally without resubmission.

Evidence observations use short references such as `observation:4`, scoped to this recovery case. Internal step IDs retain the case hash for correlation. Hosted function schemas enumerate the permitted evidence references; local validation still requires an exact match. Model requests include the saved task request and the remaining model turns, including the current decision.

A known `read_source` location error is saved as a failed step with `result.error.code`. The next model decision receives that error and can correct the lookup within the original budget. Failed reads are excluded from candidate evidence choices. Other adapter failures and uncertain remote outcomes stop recovery. The report retains tool error codes and step references even when the case later completes.

When local response validation fails, retain the rejected decision within the output byte limit in that step. Store sanitized validation locations alongside it and include those locations in the run report. This uses the existing recovery event; full source prose and exception messages stay out of report diagnostics.

Recovery proposals contain `record: {kind: items|product_total|category_config|category_page}` and one evidence reference per dataset. References identify a source event or saved observation and a JSON location, such as `script:0:variantData#/items`. The controller resolves that location against checksum-verified bytes from the same task request. It passes those raw values through the existing deterministic validators. Model-provided catalog values, foreign sources and discarded previously decoded SKUs are rejected.

The controller explicitly sets the primary `source_event_id`. Saving an additional fetch does not promote it. Metadata-only event reads omit response bodies; selected evidence is loaded by event ID. Reviewer/provider credentials are kept out of observations, events and configuration; public storefront keys in retained source bytes are redacted from JSON observations. Unexpected failure diagnostics contain at most five package file/function/line locations in console output and the error receipt, without source lines, exception text, local variables or extra database records.

Recovery fetch events retain `Retry-After` in response metadata. The controller reconstructs the global request deadline from saved fetches after recovery and on resume, so a recovery rate-limit response also delays subsequent collection.

Illustrative history: fetch succeeds; extract reports a missing expected dataset; recovery finds candidate records; extract validates them. Only the final validated records enter catalog_items.

## Data flow and commit boundaries

1. Create the run with its configuration, versions, limits, and initial budget.
2. Create discovery tasks. Save each fetched response and the controller-selected primary-source checkpoint in a short transaction.
3. Decode a discovery page. In one transaction, insert/deduplicate product tasks, merge discovery associations, create any next-page task, and mark the current page complete.
4. Fetch a product and save its evidence. Decode preliminary candidates and record unresolved fields or coverage as issues; do not finalize notes or commit individual SKUs during parsing.
5. Run bounded recovery for eligible unresolved issues. The local loop calls scoped evidence tools and a hosted decision endpoint. The endpoint defaults to authenticated access; its explicit, expiring public demo mode requires no collector token. Vercel performs one model decision; it does not run the collector or own SQLite. Persist recovery steps, or the reason recovery was not attempted. Browser navigation is not enabled in this slice. Deployment configuration and secrets are described in the [README](README.md#deployment-configuration-and-secrets).
6. Validate the combined deterministic and recovered candidates. Create final notes for remaining gaps and retained source wording, with recovery references in the extraction operation. In one transaction, replace the product's accepted catalog rows, finalize its extraction event, and update task status/checkpoint. Failed attempts leave previously accepted rows intact.
7. Export committed catalog rows grouped by product URL, together with run metadata. Generate the report from tasks and events. Report discovery scope separately from selected detail-page scope.

## Validation and resumption

- Reject candidate rows missing product name/URL, item name, or SKU. Preserve their source evidence and report them.
- A confirmed nonpublic price is NULL and does not make the task partial. An unexplained missing or undecodable expected price qualifies for recovery and remains an issue if unresolved.
- Collapse identical SKU duplicates within a page. Investigate conflicting duplicates; exclude unresolved conflicting rows from accepted output.
- Reconcile source totals, decoded rows, accepted unique SKUs, and verified identical duplicates separately. Missing optional fields do not create a count gap. Unknown completeness is reported explicitly.
- Safco can encode an explicitly empty item dataset as []. Report zero decoded/accepted items and the unresolved product; do not manufacture item SKUs from family metadata.
- After a crash, establish exclusive run ownership, inspect started operations, and resume unfinished tasks from saved evidence. Committed results stay committed; attempts and allowances stay consumed/reserved.
- A remote request may succeed before its response is saved locally. Record that uncertainty and preserve its reservation; SQLite does not guarantee exactly-once remote execution.
- `unresolved_product_limit: null` attempts every discovered product; it is the default when the CLI option is omitted. An explicit positive limit stops scheduling new products once reached, drains bounded in-flight work, and reports the remainder. The saved setting is retained on resume. Recovery-budget exhaustion defers recovery while deterministic collection continues; it does not prevent a full pass. Storage failure stops execution immediately.

The crash demonstration must interrupt discovery before its commit, product acceptance before and after its commit, and recovery after a saved observation. Resumed accepted output must match an uninterrupted run over the same fixed evidence.

## Reporting

Each `unresolved` report entry includes `notes` alongside its status and issues. These are the accepted items' final persisted notes, with `sku` added to identify the affected item; catalog items retain those notes under `items[].notes`. No second explanation or recovery decision is generated. A task without accepted items has an empty notes list; its failure remains in `issues`.

`item_coverage` retains expected, decoded and accepted counts for every discovered product. `count_status` is `unknown` when either source count is unavailable; otherwise it is `shortfall`, `excess` or `matched`. `missing_source_rows` is NULL for unknown counts and otherwise the nonnegative difference between expected and decoded rows. These are report calculations, not persisted catalog fields. Identical duplicate rows can produce fewer accepted unique SKUs than decoded source rows without a collection shortfall. A matched count does not clear price, identity or empty-dataset issues.

Category reports separately reconcile reported hits with distinct source object IDs, decoded hits, page counts and product URLs. `missing_source_hits` stays NULL when no consistent total is known. Aggregate discovery failures enter recovery before product scheduling and remain visible when unresolved.

The recovery report records reservations, completed turns, known token usage, case outcomes and final task outcomes. `proposal` means the agent identified evidence, not that catalog acceptance succeeded. `attempted: false` distinguishes a case rejected by its budget before inference. An uncertain call keeps its reservation and may have incurred unreported provider usage.

The end-to-end offline checks exercise the real HTTP client, endpoint envelope, evidence tools and catalog commits using simulated inference. Forced process exits after observations/proposals and before/after acceptance reproduce the uninterrupted catalog and final report. A response lost before persistence instead produces an uncertain outcome and no repeat model call.
