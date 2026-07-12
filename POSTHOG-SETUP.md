# Configuring the PostHog project

One-time setup of the **PostHog project side** so the data this library sends
(see [`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md) for the event schema) is clean
and usable. Do this once per project.

The library posts to the **EU ingestion host** `https://eu.i.posthog.com/batch/`
with the project's public capture key (`phc_…`). These steps configure how that
data is interpreted; they need a PostHog login (UI) or a **personal** API key
(the automation snippets).

---

## 1. Confirm project & region

PostHog → **Settings → Project**. Confirm the project whose **Project API Key**
matches the `phc_…` key compiled into the library (or set via `SetAPIKey`) is the
one you're configuring, and that it's the **EU** cloud project.

## 2. Exclude CI / bot noise (highest value)

Every CI run that loads an extension would otherwise look like a real user and
make DAU fiction. The library already tags those events (`is_ci: true`) and never
creates a Person for them (`$process_person_profile: false`), but Trends/Funnels
still count the *events* unless you filter them.

- **Settings → Project → "Filter out internal and test users" → Add filter**:
  - event property **`is_ci`** **`= true`**
- Save. Then enable **"Filter out internal and test users"** on each insight (a
  toggle in the insight's filter bar). CI is now excluded wherever that toggle
  is on.

Automate it instead (run it yourself; the key stays with you):

```bash
PH_KEY='phx_your_personal_api_key'      # Settings → Personal API keys (write scope)
PROJECT_ID='your_numeric_project_id'    # Settings → Project → Project ID

curl -sS -X PATCH "https://eu.posthog.com/api/projects/${PROJECT_ID}/" \
  -H "Authorization: Bearer ${PH_KEY}" -H "Content-Type: application/json" \
  -d '{"test_account_filters":[{"key":"is_ci","value":["true"],"operator":"exact","type":"event"}]}' \
  -w '\nHTTP %{http_code}\n'
```

## 3. Restrict returning-user analyses to reliable identity

`distinct_id` is a stable **machine/deployment** id, but only the `machine_id`
source is reliably the same machine across sessions; `mac` and `ephemeral`
(containers/CI with no stable hardware id) are not. For **Retention / Lifecycle /
Stickiness** and any "returning user" insight, add an event-property filter:

- **`identity_source`** **`= machine_id`**

Keep general adoption Trends on just `is_ci = false` (step 2); reserve the
`identity_source` filter for returning-user analyses.

## 4. Group analytics (deployment / account)

Group types **register automatically** on the first `$groupidentify` event — no
manual creation:

- **`deployment`** — registers as soon as any extension loads (auto-associated
  by the library; key = the machine hash). Powers active-deployment counts and
  deployment retention.
- **`account`** *(opt-in)* — registers when an enterprise build calls
  `AssociateGroup("account", sha256(license_id), …)`. Powers account-level
  adoption/retention.

PostHog → **Settings → Product analytics → Group analytics**: confirm they appear
and give them display names (e.g. "Deployment", "Account"). Budget is **5 group
types**; this schema uses 2. (Group analytics is a paid capability — if the tab
is missing it's a plan/add-on matter, not a data problem.)

## 5. Person profiles — keep the default

Leave **Settings → Project → Person profiles** on its default (process person
profiles for all events). The library sends `distinct_id` on every event *without*
calling `$identify`, so switching to "identified events only" would drop your
machine-based Persons entirely. Exclusions are already handled **per event** via
`$process_person_profile: false` (ephemeral identity and CI).

## 6. (Optional) Property definitions

**Data management → Properties**: PostHog auto-discovers the envelope properties.
Add short descriptions for the team, and confirm the types are inferred correctly
(they will be, because the library serializes them as real JSON types):

- **Boolean**: `is_ci`, `is_container`, `$process_person_profile`
- **Numeric**: `telemetry_schema`, `call_count`, `duration_ms_p50`, `duration_ms`,
  `sample_rate`
- **String**: `product`, `product_version`, `product_edition`, `os`, `arch`,
  `platform`, `identity_source`, `feature`, `function_name`

## 7. What to build next

With the above in place, the analyses in
[`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md) (§6 example HogQL) and the README's
payoff section become straightforward: adoption Trends (`is_ci = false`, broken
down by `product`/`product_version`), activation funnels, feature-adoption
matrices, reliability funnels, Retention/Lifecycle (filtered to
`identity_source = machine_id`), CLI Paths (via `$session_id`), and
group/account analytics.

---

**Privacy / opt-out** posture is described in [`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md) §5.
Nothing here collects new data; it only configures interpretation of the bounded,
enumerated properties the library already sends.
