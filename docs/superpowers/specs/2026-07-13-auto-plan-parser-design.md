<!--
Copyright (c) 2026, NVIDIA CORPORATION.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# auto-plan-parser — Agentic Parser-Maintenance Skill

**Status:** Design / spec (v2.1 — review incorporated; stage/mode count, log-privacy boundary, and skill layout clarified)
**Date:** 2026-07-13
**Author:** Partho Sarthi (psarthi@nvidia.com)
**Scope:** `spark-rapids-tools` core (Scala plan parser + metrics parser)

> **Revision note (v2).** A structured review flagged that v1 (a) reinvented an
> existing CSV-sync workflow, (b) ignored the platform name-normalization layer,
> (c) mis-described the metric consumers, and (d) had a circular risk gate.
> All four were verified against the code and corrected here. See §11 for the
> mapping from each review item to the section that resolves it.

---

## 1. Problem

The RAPIDS Accelerator plugin (`spark-rapids`) continuously adds GPU operators,
expressions, data-source formats, and SQL metrics. `spark-rapids-tools` must keep
its **plan parser** and **metrics parser** in step so qualification and profiling
stay accurate.

Support **verdicts** are *semi*-automated already, but not for free:

- The plugin generates `supportedExecs.csv`, `supportedExprs.csv`,
  `supportedDataSource.csv`, and per-platform `operatorsScore-*.csv`
  (via `SupportedOpsForTools` in the plugin's `TypeChecks.scala`).
- The tools **do not** consume those raw files directly. An existing sync
  workflow — `scripts/sync_plugin_files/process_supported_files.py` — unions the
  plugin's per-Spark-version CSVs, applies local overrides
  (`override_supported_configs.json`), preserves tools-removed rows, marks every
  newly-seen exec/expr/format as `Supported = TNEW`, and emits a human-readable
  `operators_plugin_sync_report.txt` plus `new_operators.txt`. Only after that
  runs do the vendored `core/src/main/resources/*.csv` update.

The gap is the **parsing logic**, which the CSVs never describe: which inner
expressions to extract, which accumulator is an operator's duration, how children
combine, how a scan's format/schema is read, how a platform-specific name
normalizes to its OSS equivalent, and which metric display-string maps to which
profiling view. That logic is hand-written:

- the dispatch `match` in `SQLPlanParser.parseGraphNode`,
- one `ExecParser` subclass per special operator (with duration metric names
  embedded via `durationSqlMetrics` and related overrides),
- name normalization in the `OssOpMapperTrait` implementations,
- metric classification in `StageAccumDiagnosticMetrics`,
  `IOAccumDiagnosticMetrics`, and the engine-specific `IoMetrics` labels.

The sync README's own "Next Steps" name the manual work precisely: *"Parse and
test the execs or expressions… which have support level of `TNEW`… Once done,
remove the corresponding entries in `override_supported_configs.json`."*
**That manual step is exactly what this skill automates.**

**Goal:** cut that per-item maintenance burden with an AI agent that generates
and self-verifies the parser code, gated by humans in proportion to risk.

## 2. Non-goals

- **No runtime LLM calls.** The shipped tools remain offline, deterministic, fast
  batch jobs. The agent operates only at dev time; it never runs inside the parser.
- **No architectural rewrite of the dispatch layer.** The agent produces code in
  the current hand-written `ExecParser` + dispatch style.
- **Not a re-implementation of the CSV sync.** The sync workflow is a dependency
  (Stage 0), not something the agent rebuilds.
- **Not fully autonomous merges in v1.** A human always merges; PR creation is an
  optional publish step, not part of correctness.

## 3. Key decisions

| Decision | Choice |
| --- | --- |
| Primary goal | Reduce dev maintenance of the parser |
| Mechanism | Pure AI agent generating parser code (not a data-driven registry, not codegen templates) |
| Agent locus | Dev-time / CI-time code agent — never in the running tool |
| Detection source | The existing sync workflow's `TNEW` rows + `new_operators.txt` + sync report (not a from-scratch metadata diff) |
| Correctness signals | (1) plugin-source cross-check, (2) synthetic plan fixtures, (3) real GPU event logs |
| Scope | New execs, new/renamed metrics, new expressions, new read/write formats |
| Human-in-the-loop | **Gate scales with `riskTier`**, assigned pre-generation and independent of verification results |
| v1 vehicle | Manual Claude Code skill (`/auto-plan-parser`) with independent `sync`/`detect`/`research`/`generate`/`verify`/`publish` modes |

### Compound-engineering framing

The design is a closed loop — **sync → detect → research → generate → verify →
compound** — where each run leaves the repo with more handled operators, more
fixtures, and a richer analog library, so the next run's gap list is smaller.
v1 is the manual first turn of that loop.

## 4. Inputs, provenance & prerequisites

- A **plugin checkout / release** supplying the per-Spark-version generated CSVs.
- The **sync workflow output** for this tools release: updated vendored CSVs with
  `TNEW` rows, `new_operators.txt`, `operators_plugin_sync_report.txt`, and the
  current `override_supported_configs.json`.
- A **GPU event-log corpus** from an approved/redacted source (see §9).
- The **tools repo** (target of edits).

Because the tools support a Spark-version *matrix* and the sync unions across
versions with local overrides, a single `generated_files/<sparkVersion>` input is
insufficient. Every artifact carries a provenance envelope:

```json
{
  "schemaVersion": 1,
  "runId": "...",
  "toolsCommit": "...",
  "pluginCommit": "...",
  "sparkVersions": ["350", "351", "352", "353", "354", "355", "356", "357"],
  "inputHashes": {"path": "sha256"},
  "detectorVersion": "..."
}
```

Stage boundaries validate against JSON Schemas defined in the skill and **reject
stale artifacts** when commits or input hashes differ. Gap IDs are stable,
derived from `gapType` + normalized operator/metric name + affected version set.

## 5. Architecture — staged, independently-runnable modes

The skill (`SKILL.md` orchestrator) exposes **six modes** (`sync`, `detect`,
`research`, `generate`, `verify`, `publish`) that also run as one pipeline. Each
emits a schema-validated artifact so stages compose, are inspectable, and wrap
cleanly into CI.

### Stage 0 — Sync mode (reuse, don't reinvent)

`sync` is a first-class mode, not merely an external prerequisite: it wraps the
existing `process_supported_files.py` and `sync_operator_scores.py` (calling
their library functions) so the full pipeline is runnable from the skill alone,
while still allowing a dev to run the scripts by hand and start from `detect`.
Output: vendored CSVs with `TNEW` rows, `new_operators.txt`, the sync report, and
appended per-platform operator scores. Operator-score sync stays **separate** —
there are many `operatorsScore-<platform>.csv` files, not one canonical file.

### Stage 1 — Detect  → `gaps.json`

The detector consumes Stage 0's outputs as primary inputs and emits, per item, a
**capability matrix** rather than a set-membership boolean. "Handled" decomposes
into:

| Capability | Meaning |
| --- | --- |
| `normalized` | platform/GPU name maps to the intended OSS name |
| `recognized` | node does not fall through to the unknown `ExecInfo` path |
| `supportChecked` | queries `PluginTypeChecker` with the correct registered name |
| `expressionAware` | relevant expressions are extracted and support-checked |
| `durationAware` | correct accumulator IDs + driver/executor source used |
| `metadataAware` | format/schema/join-type/write metadata extracted |
| `diagnosticAware` | relevant metrics classified for profiling views |

Each gap records `currentCapabilities`, `requiredCapabilities`, and the platform
names: `pluginExecName`, `observedPlanNames` (from real logs), and
`normalizedExecName` (post-`OssOpMapper`). A support-only CSV change may require
*no* parser code; a plain new exec may need only `recognized` + `supportChecked`.
This prevents false positives and gives the researcher an explicit target.

A dedicated detector rule flags GPU names that **do not normalize correctly** —
including names that do not fit the current `Gpu[A-Z][a-zA-Z]+` pattern — since
those silently fall through today.

### Stage 2 — Research & classify  → `plan.json`

Per gap, a researcher subagent:
- reads the **plugin source** for that op, following references *from the
  `GpuExec`* (declared metrics, expression handling, scan/write behavior) rather
  than scanning only a global constant list,
- selects the **closest existing tools parser** as an analogy reference (an
  example to imitate — the agent writes fresh code shaped like it),
- writes a per-gap parse spec citing source symbols,
- assigns a **`riskTier`** from novelty, parser complexity, source ambiguity, and
  analog quality — **independent of any verification outcome**.

`plan.json` items include: `riskTier`, source citations, chosen analog + why,
rejected analogs, proposed files, test cases, open questions, and explicit
**non-changes**.

### Stage 3 — Generate

For gated gaps, the generator writes, shaped after the analog: dispatch entry +
`ExecParser` (reusing `GenericExecParser` where possible), normalization-map
edits, metric classification edits (into the correct consumer — see §6.2),
format rows, expression wiring, plus synthetic fixtures and unit tests.

### Stage 4 — Verify  → dossier

Runs the three-signal check (§6) and populates a **`verificationStatus`** per
signal: `passed | failed | not_applicable | not_exercised`. Verification results
**never silently raise or lower the pre-codegen gate**; a `failed` signal stops
the item or explicitly returns it to research.

### Stage 5 — Publish (optional)

Opens a branch/PR bundling code, tests, and the dossier. Decoupled from
correctness so the skill runs in local checkouts, CI, and restricted
environments. Supports `--gap`, `--max-gaps`, and `--tier` selection; defaults to
a small batch grouped by parser family, so one bad Tier-2 op never blocks
unrelated Tier-1 output.

## 6. Correctness

### 6.1 Three verification signals (cheap → expensive)

1. **Plugin-source cross-check.** Assert every metric display-string, expression
   name, and normalized operator name the generated parser keys on *actually
   exists* in the plugin source, with source file + symbol + line captured. Fails
   closed: "not extracted" is never treated as "not present."
2. **Synthetic fixtures.** Deterministic unit tests. When a gap claims
   `durationAware`, the fixture **must include accumulator updates and an
   `AppBase` fixture** — a bare `SparkPlanInfo` cannot prove duration calculation.
3. **Real GPU logs.** Confirm the *actual emitted node name* (not the assumed
   plugin class name), and produce a structured **coverage record** per gap that
   compares stable semantic outputs — not whole generated reports, which carry
   unrelated ordering/environment noise. A **deterministic local script** runs the
   tools over the corpus and extracts only the redacted, structured signals the
   agent needs (emitted node names, support verdicts, extracted expression sets,
   duration values, diagnostic classifications). The agent reasons over that
   script's output; raw log content is never read into the agent context (see §9).

### 6.2 Per-capability oracles (not "no longer unknown")

"Recognized" and "supported" are different outcomes; an op can be correctly
recognized *and* intentionally unsupported. Assertions are declared per
capability:

- expected normalized and reported operator names;
- expected support verdict **and** unsupported reason;
- exact extracted expression set;
- exact duration metric IDs and computed duration;
- expected children and removal behavior;
- expected format/schema/write metadata;
- expected diagnostic classification.

A newly-discovered **metric** is classified before any edit as: `duration` (parser
subclass `durationSqlMetrics`), `io-diagnostic` (`IOAccumDiagnosticMetrics`),
`stage-diagnostic` (`StageAccumDiagnosticMetrics`), engine-I/O label (`IoMetrics`),
`informational`, or `ignored-with-rationale`. `AccumNameRef` interns/normalizes
names and is **not** a catalog of plugin display strings. Every generated mapping
cites the plugin source symbol and the specific tools consumer it affects.

### 6.3 Tier-scaled human gates

| Tier | What it is | Gate |
| --- | --- | --- |
| **1 — routine** | plain exec/metric, clear verdict + clear analog | autonomous through generate + verify; surfaces only at the **PR gate** |
| **2 — special logic** | scans/joins/writes/expr-bearing ops | **plan gate first**, then PR gate |
| **3 — novel** | no analog / ambiguous source | gap report + stub + questions; **escalates at the plan gate**; never claims it works |

`riskTier` is assigned in Stage 2 and read before generation, breaking the v1
circular dependency between gating and verification.

## 7. Risks & mitigations

1. **Corpus coverage.** A brand-new op may appear in no log → `not_exercised`,
   flagged explicitly; correctness rests on source cross-check + synthetic
   fixtures. Never counted as verified.
2. **Source-extraction fragility.** Extraction is isolated behind a deterministic
   script with fixture tests against a pinned plugin checkout; the metric-inventory
   stage **fails whole** if expected source structures cannot be parsed.
3. **Template misfit (Tier 2).** Mandatory plan gate + cited analog + rejected
   analogs.
4. **Version/provenance drift.** Provenance envelope + input-hash rejection.
5. **Normalization blind spots.** Detector rule for non-normalizing GPU names;
   real-log confirmation of emitted names.

## 8. Idempotency

Re-detection alone does not prove idempotency (model variance, partial edits,
stale scratch artifacts). Acceptance check: **a second run on the generated tree
produces zero actionable gaps and no diff.** Runs resume by stable `gapId` and
require a clean worktree or a recorded baseline diff at start.

## 9. Data protection

Real customer logs may contain paths, SQL text, schemas, identifiers. Logs stay
**local**, are never copied into prompts or committed, and must come from an
approved/redacted corpus. A deterministic local script (§6.1 signal 3) is the
*only* component that touches raw logs; it emits redacted structured results, and
the agent reasons solely over those. The dossier records corpus IDs and hashes,
not raw log content.

## 10. Skill layout & test commands

```
skills/auto-plan-parser/
  SKILL.md            # orchestrator: modes, provenance, gate logic, schemas
  schemas/            # JSON Schemas for gaps.json, plan.json, dossier
  scripts/            # deterministic, testable, non-agent code:
                      #   extract_plugin_metrics.py  (fail-closed source extraction, §6.1/§7.2)
                      #   validate_artifact.py        (schema validation at stage boundaries)
                      #   verify_logs.py              (local log run → redacted structured results, §6.1/§9)
                      #   sync wrappers               (call process_supported_files / sync_operator_scores)
  fixtures/           # deterministic test inputs:
                      #   detector/                   (human-labeled gap fixtures, §12 criterion 1)
                      #   plugin-source/              (pinned snippets for extraction tests, §7.2)
                      #   synthetic-plans/            (SparkPlanInfo + AppBase fixtures for oracles, §6.1)
  detector.md         # Stage 1 subagent prompt
  researcher.md       # Stage 2 subagent prompt
  generator.md        # Stage 3 subagent prompt
  verifier.md         # Stage 4 subagent prompt
```

The `scripts/` directory holds the deterministic, independently-testable code the
correctness story depends on (source extraction, schema validation, log
verification) — kept out of the agent prompts so it can be unit-tested and pinned.

The verifier maps edits to focused suites first, then broadens: the relevant
parser suite, `PluginTypeCheckerSuite`, diagnostic-metric tests, style checks, and
the declared Spark/Scala build matrix. The dossier lists exact commands, exit
codes, and any skipped matrix entries.

## 11. Review-item resolution map

| Review item | Resolved in |
| --- | --- |
| 1 — build on existing CSV sync | §1, §3 (detection source), §5 Stage 0 |
| 2 — capability matrix | §5 Stage 1 |
| 3 — platform normalization | §5 Stage 1 (names + rule), §6.1(3) |
| 4 — correct metric inventory | §6.2 |
| 5 — separate risk from verification | §5 Stage 2/4, §6.3 |
| 6 — versioned artifacts & provenance | §4 |
| 7 — fail-closed extraction | §6.1(1), §7.2 |
| 8 — stronger oracles | §6.1(2), §6.2 |
| idempotency testable | §8 |
| bound PR size | §5 Stage 5 |
| explicit test commands | §10 |
| protect event-log data | §9 |
| decouple analysis from PR | §5 (six modes), Stage 5 |

## 12. v1 success criteria (measurable)

1. Detector output matches a human-labeled fixture set with no false negatives and
   an agreed false-positive threshold.
2. Every generated edit traces to a stable `gapId` and plugin source evidence.
3. Tier-1 changes pass focused unit tests, style checks, source cross-checks, and
   all available real-log assertions.
4. Tier-2/Tier-3 items cannot reach generation without their required gate.
5. Unsupported or unexercised cases are reported explicitly, never counted as
   verified.
6. Re-running against the resulting tree produces no actionable gaps or diff.
7. Existing parser, qualification, profiling, and golden tests show no regression
   in the declared build matrix.

## 13. Proposed v1 build order

1. Define and validate artifact JSON Schemas.
2. Wrap the existing CSV sync workflow; produce a deterministic gap inventory
   with the capability matrix.
3. Build a small human-labeled detector fixture set.
4. Implement research for one routine exec and one metric rename.
5. Add generation with focused synthetic tests only.
6. Add real-log coverage accounting and semantic baseline comparison.
7. Add optional branch/PR publication last.

This order tests the highest-risk assumption — reliable gap detection — before
investing in autonomous Scala generation.

## 14. Future (post-v1)

- Wrap the modes in a CI action triggered on plugin release (artifacts are
  already schema'd and provenanced for this).
- Feed unknown operators encountered in real customer logs back into the gap list,
  closing the compound loop from the runtime side.
