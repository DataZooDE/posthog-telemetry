# DataZoo Telemetry Schema

**`telemetry_schema: 2`** · single source of truth for every DataZoo product
(extensions **erpl**, **anofox-statistics**, **flapi**; CLIs **erpl-adk**).

One PostHog **project**, one schema, every product. Discriminate by the
`product` property, not by separate keys or event names. This is what lets us
slice the whole DataZoo stack *and* drill into one product, and stitch C++ and
Python implementations into one analysable dataset.

> This document is vendored by each product. The C++ implementation lives in
> `DataZooDE/posthog-telemetry`; Python/other tools must emit the **same**
> envelope, event names, and `$groups` to stay compatible.

---

## 1. Envelope — attached to **every** event

The library attaches these automatically (via `BuildEnvelope()`); callers only
pass event-specific properties.

| Key | Type | Meaning |
|---|---|---|
| `product` | string | `erpl` \| `anofox_statistics` \| `flapi` \| `erpl_adk` (falls back to the extension name if `SetProduct` unset) |
| `product_version` | string | e.g. `1.4.2` |
| `product_edition` | string | `oss` \| `enterprise` (default `oss`) |
| `telemetry_schema` | **number** | schema version, currently `2` |
| `duckdb_version` | string | host DuckDB version (extensions); `unknown` if unset |
| `os` | string | `linux` \| `macos` \| `windows` |
| `arch` | string | `amd64` \| `arm64` |
| `platform` | string | e.g. `linux_amd64` (kept for continuity) |
| `is_ci` | **bool** | true under `CI`/`GITHUB_ACTIONS`/`GITLAB_CI`/`BUILDKITE`/`JENKINS_URL`/`TEAMCITY_VERSION`/`TF_BUILD`/`CIRCLECI` |
| `is_container` | **bool** | true under `/.dockerenv` or a container cgroup |
| `$session_id` | string | one UUID per process/run (enables Paths) |
| `$groups` | object | e.g. `{ "deployment": "<hash>" }`, present once a group is associated |

`distinct_id` is the SHA-256 of the OS machine id (MAC fallback) — a **stable,
pseudonymous person id**. It powers Retention/Lifecycle/Stickiness and must not
regress to a per-process random id.

**Property types are load-bearing.** `is_ci` / `is_container` serialise as JSON
booleans and `telemetry_schema` / `call_count` / `duration_ms*` as JSON numbers
(not quoted strings) so PostHog treats them as numeric/boolean for Trends math
and HogQL `sum()/avg()`.

---

## 2. Event catalogue

Past-tense names; the *thing that varies* lives in a property so Trends can
break it down.

| Event | Fires when | Key properties (beyond envelope) |
|---|---|---|
| `extension_loaded` | extension init | (envelope only) |
| `cli_started` | CLI/command process start | `command`, `args_shape` (flags present, **not values**) |
| `server_started` | server boot (flapi) | `endpoint_count`, `auth_kind` |
| `feature_used` | a *named* capability is exercised | `feature` (enum), `feature_detail` (bounded), `duration_ms` |
| `function_executed` | DuckDB function runs (**aggregated**) | `function_name`, `call_count`, `duration_ms_p50`, `sample_rate?` |
| `$exception` | a caught error | `error_class` (enum, **never** message/data), `feature`, `phase` |

The legacy `extension_load` name is **dual-emitted for one release**
(`telemetry_schema: 2`, same shape) so existing dashboards don't go dark, then
dropped. The legacy per-call `function_execution` is **not** dual-emitted:
aggregation changes its shape (per-call → per-function `call_count`), so reusing
the old name would silently corrupt count-based dashboards. Migrate those queries
to `function_executed` with `sum(call_count)`.

**Delivery / short sessions.** Regular events (`extension_loaded`,
`feature_used`, `$exception`, `cli_started`, …) are sent **promptly** per
capture. `function_executed` uses a **hybrid** model: the first few calls of
each function are emitted **promptly, per call** (`call_count: 1`) so short
sessions never lose them; once a function exceeds that, further calls are
**aggregated** into one `function_executed` (`call_count: N`, `duration_ms_p50`)
to prevent a firehose. `sum(call_count)` is correct across both forms. Aggregated
remainders ship on a recorded-call threshold, by piggybacking on the next
regular event, or on **`Flush()`** — and the at-exit path discards buffered work
by design (OpenSSL teardown safety), so CLIs/servers should still call `Flush()`
before exit to capture the tail of a heavy session.

### Per-product `feature` values (illustrative; keep the set small + enumerated)

- **erpl**: `sap_rfc`, `bapi_call`, `odp_extract`, `odata_read`, `cds_read`,
  `rfc_table_read`, `connection_opened` (+ `auth_kind`: `basic|sso|x509|snc`).
- **anofox-statistics**: `stat_test` (+ `test_family`:
  `hypothesis|regression|timeseries|distribution`), `model_fit`, `forecast`.
- **flapi**: `rest_endpoint_served` (+ `method`, **route template not filled
  path**), `mcp_tool_called` (+ `tool`), `cache_hit`, `auth_enforced`.
- **erpl-adk**: `command_run` (+ `command`), `template_scaffolded`,
  `sap_object_read` (+ object *type* only: `cds|odata|rfc`).

---

## 3. Cardinality rule (load-bearing)

A property value **must** come from a *known, small enumeration the code
controls*. `function_name` and enumerated `feature`/`tool` are fine.

**Never** put into properties: table names, SQL text, file paths, hostnames,
connection strings, user names, row data, or free-form error messages.

The library enforces a **512-byte clamp** on every outgoing string property as a
backstop, but bounded-by-design is the contract. This protects GDPR posture
*and* PostHog cost (person/property cardinality drives both). Make it a
code-review checklist item.

### Taming high-volume events

1. **Aggregate, don't stream.** `RecordFunctionCall(fn, duration_ms)` increments
   an in-process `{count, duration}` map; one `function_executed` per function
   is flushed on `Flush()` / session end. Millions of calls → O(#functions)
   rows, preserving the "which functions, how often, how slow" signal.
2. **Client-side sampling.** `SetSampling(rate)` decimates still-hot events and
   stamps `sample_rate` so counts scale back up.

---

## 4. Group analytics

`$groupidentify` on first association, then `$groups` on every event
(≤ 5 group types per project).

| Group type | Key (bounded, non-PII) | Answers |
|---|---|---|
| `deployment` | install/machine hash (= `distinct_id`) | active deployments, deployment retention |
| `account` *(opt-in)* | hashed **license id** (enterprise) | active *customers*, per-account adoption, at-risk accounts |
| `product` | `"erpl"` etc. | portfolio roll-ups |

Extensions auto-associate `deployment` at load. Enterprise builds additionally
call `AssociateGroup("account", sha256(license_id), {edition, first_seen_version})`.

---

## 5. Privacy / opt-out contract

- **EU cloud ingestion**; anonymous-by-default pseudonymous `distinct_id`.
- **Bounded, enumerated properties only** (§3) — the technical guarantee no
  query text, hostnames, or user data ever leave the machine.
- **Opt-out**, honoured everywhere and enforced at the transport (nothing
  leaves the machine when disabled):
  - env `DATAZOO_DISABLE_TELEMETRY=1|true|yes`
  - DuckDB setting `SET <ext>_telemetry_enabled = false`
  - (recommended) `DO_NOT_TRACK=1`, a per-CLI `--no-telemetry` flag, and a
    first-run notice pointing at a per-product `TELEMETRY.md`.

---

## 6. Example HogQL (real users only)

```sql
SELECT properties.product AS product,
       properties.function_name AS fn,
       sum(toInt(properties.call_count)) AS calls,
       uniq(distinct_id) AS deployments
FROM events
WHERE event = 'function_executed'
  AND properties.is_ci = false
  AND timestamp > now() - INTERVAL 30 DAY
GROUP BY product, fn
ORDER BY deployments DESC, calls DESC
LIMIT 50
```
