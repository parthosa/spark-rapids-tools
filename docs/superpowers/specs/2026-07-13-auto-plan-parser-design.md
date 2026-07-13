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

**Status:** Design / spec
**Date:** 2026-07-13
**Author:** Partho Sarthi (psarthi@nvidia.com)
**Scope:** `spark-rapids-tools` core (Scala plan parser + metrics parser)

---

## 1. Problem

The RAPIDS Accelerator plugin (`spark-rapids`) continuously adds GPU operators,
expressions, data-source formats, and SQL metrics. `spark-rapids-tools` must keep
its **plan parser** and **metrics parser** in step so qualification and profiling
stay accurate.

Today only *half* of that keeps up automatically:

- **Support verdicts do stay current.** The plugin generates
  `supportedExecs.csv`, `supportedExprs.csv`, `supportedDataSource.csv`, and
  `operatorsScore.csv` (via `SupportedOpsForTools` in the plugin's
  `TypeChecks.scala`). The tools vendor these under
  `core/src/main/resources/` and read them through `PluginTypeChecker`. A new
  operator's *S/NS/PS* verdict therefore appears with no tools code change.

- **Parsing logic does *not* stay current.** The CSVs say *whether* an op is
  GPU-able, never *how to parse it*: which inner expressions to extract, which
  accumulator is its duration, how children combine, how a scan's format/schema
  is read, or which metric display-string maps to which diagnostic. That logic
  lives in hand-written code:
  - the dispatch `match` in `SQLPlanParser.parseGraphNode`,
  - one `ExecParser` subclass per special operator,
  - metric name-maps in `DiagnosticMetrics`, `IoMetrics`, `AccumNameRef`.

  Every new/changed operator, metric, expression, or format needs a human to
  edit these. This work lags plugin releases and is error-prone.

**Goal:** cut that per-item maintenance burden with an AI agent that generates
and self-verifies the parser code, with humans acting as a tier-scaled
verification gate.

## 2. Non-goals

- **No runtime LLM calls.** The shipped tools remain offline, deterministic,
  fast batch jobs. The agent operates only at dev time; it never runs inside the
  parser.
- **No architectural rewrite of the dispatch layer.** We deliberately keep the
  current hand-written `ExecParser` + dispatch style and let the agent produce
  code in that style (the "pure agent" decision). A generic-fallback refactor was
  considered and rejected for v1 to avoid coupling two large changes.
- **Not fully autonomous merges in v1.** A human always merges.

## 3. Key decisions (from brainstorming)

| Decision | Choice |
| --- | --- |
| Primary goal | Reduce dev maintenance of the parser |
| Mechanism | Pure AI agent generating parser code (not a data-driven registry, not codegen templates) |
| Agent locus | Dev-time / CI-time code agent — never in the running tool |
| Correctness signals | (1) plugin-source cross-check, (2) synthetic plan fixtures, (3) real GPU event logs |
| Scope | New execs, new/renamed metrics, new expressions, new read/write formats |
| Human-in-the-loop | **Gate scales with confidence tier** (see §6) |
| v1 vehicle | Manual Claude Code skill (`/auto-plan-parser`) |

### Compound-engineering framing

The design is a closed loop — **detect → research → generate → verify → learn** —
where each run leaves the repo with more handled operators and more fixtures, so
the next run's gap list is smaller and its analogy templates are richer. The
agent maintains the system that the agent depends on. That compounding is the
point; v1 is the manual first turn of that loop.

## 4. Inputs & prerequisites

- A **plugin checkout** (`~/Work/spark-rapids`) or a specific
  `tools/generated_files/<sparkVersion>/` metadata set — the *authoritative*
  source of truth (not the tools' vendored copies).
- A **GPU event-log corpus** — real event logs from GPU runs, used for
  end-to-end verification. Coverage is partial by nature (see §7, Risk 1).
- The **tools repo** itself (the target of edits).
- The skill records which plugin version/commit it ran against.

## 5. Architecture — four staged subagents

The skill (`SKILL.md` orchestrator) drives four discrete stages. Each stage
emits a machine-readable artifact into the scratchpad so stages compose, are
independently inspectable, and are trivial to wrap in CI later.

### Stage 1 — Detect  → `gaps.json`

A detector subagent diffs the plugin's authoritative metadata against the tools'
current handling and emits a tagged gap list.

Compared on the **plugin side**:
- `supportedExecs.csv`, `supportedExprs.csv`, `supportedDataSource.csv`,
  `operatorsScore.csv`
- metric display-strings enumerated from the plugin's `GpuMetrics.scala`
  (`DESCRIPTION_*` constants)

Compared on the **tools side**:
- dispatch entries in `SQLPlanParser.parseGraphNode`
- the set of `ExecParser` subclasses (which ops have special handling)
- metric name-maps: `DiagnosticMetrics`, `IoMetrics`, `AccumNameRef`
- the tools' vendored CSV copies under `core/src/main/resources/`

Each gap is tagged:
`new-exec | support-change | new-expr | new-format | new-or-renamed-metric`.

### Stage 2 — Research & classify  → `plan.json`

Per gap, a researcher subagent:
- reads the **plugin source** for that op (its `GpuExec`: declared metrics,
  expression handling, scan/write behavior),
- selects the **closest existing tools parser** as an analogy reference
  (an example to imitate — not a mechanical template; the agent writes fresh
  code shaped like the analog),
- writes a per-gap **parse spec** (what to extract, which metric = duration,
  child handling, expression list, format detection),
- assigns a **confidence tier** (1/2/3, see §6).

### Stage 3 — Generate

For approved gaps, a generator writes, shaped after the analog parser:
- the dispatch entry + `ExecParser` (reusing `GenericExecParser` where possible),
- metric-map / format-row / expression-wiring edits,
- **synthetic fixtures + unit tests**.

### Stage 4 — Verify → PR

Runs the three-signal check (§6), assembles a **verification dossier**, and
opens a PR on a branch. Humans review and merge.

**Idempotency:** re-running detects only *remaining* gaps (the diff is always
against current tools state), so partial merges and repeated runs are safe.

## 6. Correctness — three signals + tier-scaled gates

### Three verification signals (cheap → expensive)

1. **Plugin-source cross-check.** Assert every metric display-string and
   expression name the generated parser keys on *actually exists* in the plugin
   `GpuExec` / `GpuMetric` source. Zero build cost; catches stale/typo'd names.
2. **Synthetic fixtures.** Run agent-generated unit tests over fabricated
   `SparkPlanInfo` / node-description fixtures. Deterministic; no logs required.
3. **Real GPU logs.** Run the tools over the corpus; assert the op is now
   recognized (no longer "unknown/unsupported"), metrics populate, and a
   baseline diff shows no regressions elsewhere.

The agent iterates until green or gives up and reports why.

### Tier-scaled human gates

| Tier | What it is | Gate |
| --- | --- | --- |
| **1 — routine** | plain exec/metric, clear CSV verdict + clear analog | Autonomous through generate + verify; surfaces only at the **PR gate** |
| **2 — special logic** | scans/joins/writes/expr-bearing ops needing bespoke extraction | **Plan gate first** (human approves `plan.json` before codegen), then PR gate |
| **3 — novel** | no analog, or ambiguous plugin behavior | Gap report + stub + specific questions; **escalates at the plan gate**; never claims it works |

Confidence tier is derived from: does an analog parser exist? do the three
signals pass? does the plugin source unambiguously define the metrics/exprs?

### Honesty requirement

The PR dossier states, per change, **which signals passed**, and **explicitly
flags operators the log corpus did not exercise** — for those, correctness rests
on synthetic fixtures + source cross-check only. No implied end-to-end proof.

## 7. Risks & mitigations

1. **Corpus coverage.** A brand-new op may appear in no available log. This is
   expected, not a bug: verification leans on plugin-source cross-check +
   synthetic fixtures, and the dossier flags the missing end-to-end proof.
2. **Metric-enumeration fragility.** Detection parses `GpuMetrics.scala`
   `DESCRIPTION_*` constants; if the plugin restructures that, the detector needs
   updating. Mitigation: isolate the enumeration logic; fail loudly if the
   expected shape is absent rather than silently missing metrics.
3. **Template misfit (Tier 2).** Analogy can copy a subtly-wrong pattern.
   Mitigation: mandatory plan gate + "review carefully" flag citing the analog
   used.
4. **Two-repo drift.** The skill pins/records the plugin version it ran against
   in the dossier so a reviewer can reproduce.

## 8. Skill layout

```
skills/auto-plan-parser/
  SKILL.md            # orchestrator: inputs, stage sequencing, gate logic
  detector.md         # Stage 1 subagent prompt
  researcher.md       # Stage 2 subagent prompt
  generator.md        # Stage 3 subagent prompt
  verifier.md         # Stage 4 subagent prompt
```

Stage artifacts (`gaps.json`, `plan.json`, dossier) live in the scratchpad; code
changes land on a branch for PR.

## 9. Success criteria

- Running the skill against a plugin release with N genuinely-new operators
  produces a PR that: recognizes all Tier-1 ops with passing synthetic + (where
  covered) real-log verification, and correctly escalates Tier-2/3 ops to a plan
  gate rather than guessing.
- No regression in existing golden/unit tests introduced by generated code.
- A reviewer can, from the dossier alone, see which signals backed each change
  and which ops lack end-to-end proof.

## 10. Future (post-v1)

- Wrap the four stages in a CI action triggered on plugin release
  (the artifacts are already machine-readable for this).
- Feed unknown operators encountered in real customer logs back into the gap
  list (closing the compound loop from the runtime side).
