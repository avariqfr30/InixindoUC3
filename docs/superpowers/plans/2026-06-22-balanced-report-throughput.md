# Balanced Report Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce report completion time and improve effective report throughput without changing visible UX, report structure, source datasets, authentication, or invoice behavior.

**Architecture:** Keep the Flask VPS as the control plane and the existing Ollama cloud model as the inference provider. Introduce one focused concurrency module that executes independent report-section passes through a bounded global gate, preserves deterministic ordering, and temporarily degrades to sequential execution after recoverable provider failures. Keep one report job active in the first rollout and expose no new user-facing controls.

**Tech Stack:** Python 3.12, `concurrent.futures`, `threading`, Ollama Python client, `unittest`, Waitress, systemd, SQLite job metrics.

---

## File Structure

- Create `Payment predictor/report_concurrency.py`: bounded section-pass execution, recoverable-error classification, cooldown state, and deterministic result ordering.
- Create `Payment predictor/tests/test_report_concurrency.py`: concurrency, ordering, retry, degradation, and integration tests.
- Modify `Payment predictor/config.py`: private environment settings for section parallelism, provider timeout, and cooldown.
- Modify `Payment predictor/report_generation.py`: delegate section scheduling to the new module, create an isolated model client per pass, and log timing.
- Modify `README.md`: document operator-only settings and rollout limits; no UI documentation changes.

No database schema, template, JavaScript, API response, dataset, invoice, authentication, prompt, report-structure, or DOCX-rendering file changes are in scope.

## Priority 0: Lock the Safety Contract

### Task 1: Add disabled-by-default configuration

**Files:**
- Modify: `Payment predictor/config.py:85-110`
- Create: `Payment predictor/tests/test_report_concurrency.py`

- [ ] **Step 1: Write failing configuration-boundary tests**

Add tests that construct the scheduler directly from representative parsed values; avoid reloading global `config` during the suite:

```python
import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))


class ReportConcurrencyTest(unittest.TestCase):
    def test_executor_clamps_parallelism_to_at_least_one(self):
        from report_concurrency import SectionPassExecutor

        executor = SectionPassExecutor(max_parallel=0, cooldown_seconds=60)

        self.assertEqual(executor.max_parallel, 1)

    def test_executor_clamps_cooldown_to_non_negative_value(self):
        from report_concurrency import SectionPassExecutor

        executor = SectionPassExecutor(max_parallel=2, cooldown_seconds=-1)

        self.assertEqual(executor.cooldown_seconds, 0)
```

- [ ] **Step 2: Run the focused test and verify the missing-module failure**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: `ModuleNotFoundError: No module named 'report_concurrency'`.

- [ ] **Step 3: Add private configuration with sequential defaults**

Add to `config.py` beside the existing report settings:

```python
REPORT_SECTION_PARALLELISM = max(_int_env("REPORT_SECTION_PARALLELISM", 1), 1)
REPORT_LLM_TIMEOUT_SECONDS = max(_int_env("REPORT_LLM_TIMEOUT_SECONDS", 180), 30)
REPORT_CONCURRENCY_COOLDOWN_SECONDS = max(
    _int_env("REPORT_CONCURRENCY_COOLDOWN_SECONDS", 120),
    0,
)
```

The committed default must remain `1`, which exactly preserves current sequential behavior.

- [ ] **Step 4: Create the minimal scheduler constructor**

Create `report_concurrency.py` with:

```python
import time


class SectionPassExecutor:
    def __init__(self, max_parallel=1, cooldown_seconds=120, clock=None):
        self.max_parallel = max(int(max_parallel), 1)
        self.cooldown_seconds = max(int(cooldown_seconds), 0)
        self.clock = clock or time.monotonic
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: both tests pass.

- [ ] **Step 6: Commit the configuration boundary**

```bash
git add "Payment predictor/config.py" "Payment predictor/report_concurrency.py" "Payment predictor/tests/test_report_concurrency.py"
git commit -m "Add guarded report concurrency settings"
```

## Priority 1: Build Bounded, Deterministic Scheduling

### Task 2: Execute independent section passes safely

**Files:**
- Modify: `Payment predictor/report_concurrency.py`
- Modify: `Payment predictor/tests/test_report_concurrency.py`

- [ ] **Step 1: Write failing ordering and overlap tests**

Add tests using events rather than fragile sleep-only assertions:

```python
import threading


def test_parallel_execution_overlaps_work_and_preserves_input_order(self):
    from report_concurrency import SectionPassExecutor

    release = threading.Event()
    both_started = threading.Event()
    state_lock = threading.Lock()
    started = []

    def worker(item):
        with state_lock:
            started.append(item)
            if len(started) == 2:
                both_started.set()
        self.assertTrue(release.wait(timeout=1))
        return f"result-{item}"

    executor = SectionPassExecutor(max_parallel=2, cooldown_seconds=60)
    result_holder = {}
    thread = threading.Thread(
        target=lambda: result_holder.setdefault("value", executor.run(["a", "b"], worker))
    )
    thread.start()

    self.assertTrue(both_started.wait(timeout=1))
    release.set()
    thread.join(timeout=2)

    self.assertEqual(result_holder["value"], ["result-a", "result-b"])

def test_sequential_mode_never_overlaps_work(self):
    from report_concurrency import SectionPassExecutor

    active = 0
    peak = 0

    def worker(item):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        active -= 1
        return item

    result = SectionPassExecutor(max_parallel=1).run([1, 2, 3], worker)

    self.assertEqual(result, [1, 2, 3])
    self.assertEqual(peak, 1)
```

- [ ] **Step 2: Run tests and verify `run` is missing**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: failures report that `SectionPassExecutor` has no `run` method.

- [ ] **Step 3: Implement bounded execution and deterministic collection**

Expand `report_concurrency.py`:

```python
import concurrent.futures
import threading
import time


class SectionPassExecutor:
    def __init__(self, max_parallel=1, cooldown_seconds=120, clock=None):
        self.max_parallel = max(int(max_parallel), 1)
        self.cooldown_seconds = max(int(cooldown_seconds), 0)
        self.clock = clock or time.monotonic
        self._state_lock = threading.Lock()
        self._global_slots = threading.BoundedSemaphore(self.max_parallel)
        self._degraded_until = 0.0

    def effective_parallelism(self):
        with self._state_lock:
            if self.clock() < self._degraded_until:
                return 1
        return self.max_parallel

    def _invoke(self, worker, item):
        with self._global_slots:
            return worker(item)

    def run(self, items, worker):
        ordered_items = list(items)
        if len(ordered_items) < 2 or self.effective_parallelism() == 1:
            return [self._invoke(worker, item) for item in ordered_items]

        worker_count = min(self.effective_parallelism(), len(ordered_items))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(self._invoke, worker, item) for item in ordered_items]
            return [future.result() for future in futures]
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: all concurrency tests pass without timing failures.

- [ ] **Step 5: Commit the scheduler**

```bash
git add "Payment predictor/report_concurrency.py" "Payment predictor/tests/test_report_concurrency.py"
git commit -m "Add bounded report section scheduler"
```

## Priority 2: Degrade Gracefully Under Provider Pressure

### Task 3: Retry only failed passes and enter cooldown

**Files:**
- Modify: `Payment predictor/report_concurrency.py`
- Modify: `Payment predictor/tests/test_report_concurrency.py`

- [ ] **Step 1: Write failing recoverable-error tests**

Add:

```python
def test_recoverable_failure_retries_only_failed_item_and_degrades_next_run(self):
    from report_concurrency import SectionPassExecutor

    class CapacityError(Exception):
        status_code = 503

    now = [100.0]
    attempts = {"a": 0, "b": 0}

    def worker(item):
        attempts[item] += 1
        if item == "b" and attempts[item] == 1:
            raise CapacityError("busy")
        return f"result-{item}"

    executor = SectionPassExecutor(
        max_parallel=2,
        cooldown_seconds=60,
        clock=lambda: now[0],
    )

    self.assertEqual(executor.run(["a", "b"], worker), ["result-a", "result-b"])
    self.assertEqual(attempts, {"a": 1, "b": 2})
    self.assertEqual(executor.effective_parallelism(), 1)
    now[0] = 161.0
    self.assertEqual(executor.effective_parallelism(), 2)

def test_nonrecoverable_failure_is_not_retried(self):
    from report_concurrency import SectionPassExecutor

    attempts = 0

    def worker(_item):
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid response")

    with self.assertRaises(ValueError):
        SectionPassExecutor(max_parallel=2).run(["a", "b"], worker)

    self.assertEqual(attempts, 2)
```

- [ ] **Step 2: Verify the recoverable test fails**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: the `503` error escapes instead of being retried.

- [ ] **Step 3: Add explicit recoverable-error classification and selective retry**

Add to `SectionPassExecutor`:

```python
    @staticmethod
    def is_recoverable_error(exc):
        status_code = getattr(exc, "status_code", None)
        if status_code in {429, 502, 503, 504}:
            return True
        error_name = type(exc).__name__.lower()
        return "timeout" in error_name or "connect" in error_name

    def _enter_cooldown(self):
        with self._state_lock:
            self._degraded_until = max(
                self._degraded_until,
                self.clock() + self.cooldown_seconds,
            )
```

Replace the parallel collection in `run` with indexed result/error collection. Wait for all submitted work, retain successful results, retry only recoverable failures once through `_invoke`, and raise the first nonrecoverable failure without retrying it:

```python
        results = [None] * len(ordered_items)
        failures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(self._invoke, worker, item) for item in ordered_items]
            for index, future in enumerate(futures):
                try:
                    results[index] = future.result()
                except Exception as exc:
                    failures.append((index, exc))

        nonrecoverable = [failure for failure in failures if not self.is_recoverable_error(failure[1])]
        if nonrecoverable:
            raise nonrecoverable[0][1]

        if failures:
            self._enter_cooldown()
            for index, _exc in failures:
                results[index] = self._invoke(worker, ordered_items[index])

        return results
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: ordering, overlap, retry, and cooldown tests pass.

- [ ] **Step 5: Commit graceful degradation**

```bash
git add "Payment predictor/report_concurrency.py" "Payment predictor/tests/test_report_concurrency.py"
git commit -m "Degrade report concurrency under provider pressure"
```

## Priority 3: Integrate Without Changing Report Semantics

### Task 4: Route section passes through the scheduler

**Files:**
- Modify: `Payment predictor/report_generation.py:1-35,125-180,195-245`
- Modify: `Payment predictor/tests/test_report_concurrency.py`

- [ ] **Step 1: Write failing ReportGenerator integration tests**

Add tests that avoid real model and OSINT calls:

```python
def test_report_generator_preserves_section_pass_order(self):
    from report_generation import ReportGenerator

    generator = ReportGenerator(None)
    generator.SECTION_PASSES = (
        {"label": "first", "sections": ("A",), "include_visuals": False},
        {"label": "second", "sections": ("B",), "include_visuals": True},
    )
    generator._run_generation_pass = lambda *args: args[-1]

    generated = generator._generate_sections({}, "", "", "")

    self.assertEqual(generated, ["first", "second"])

def test_report_generator_uses_isolated_client_per_generation_pass(self):
    from report_generation import ReportGenerator

    clients_created = []

    class FakeClient:
        def chat(self, **_kwargs):
            return {"done_reason": "stop", "message": {"content": "# Section"}, "eval_count": 2}

    generator = ReportGenerator(None, model_client_factory=lambda: clients_created.append(FakeClient()) or clients_created[-1])
    generator._run_generation_pass({}, "", "", "", ("A",), False, "a")
    generator._run_generation_pass({}, "", "", "", ("B",), False, "b")

    self.assertEqual(len(clients_created), 2)
```

- [ ] **Step 2: Run focused tests and verify missing interfaces**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: `ReportGenerator.__init__` rejects `model_client_factory` and `_generate_sections` is missing.

- [ ] **Step 3: Inject the scheduler and model-client factory**

Update imports in `report_generation.py`:

```python
import concurrent.futures
import logging
import time

from ollama import Client

from config import (
    REPORT_CONCURRENCY_COOLDOWN_SECONDS,
    REPORT_LLM_TIMEOUT_SECONDS,
    REPORT_SECTION_PARALLELISM,
)
from report_concurrency import SectionPassExecutor
```

Update construction:

```python
    def __init__(self, kb_instance, model_client_factory=None):
        self.kb = kb_instance
        self.model_client_factory = model_client_factory or (
            lambda: Client(host=OLLAMA_HOST, timeout=REPORT_LLM_TIMEOUT_SECONDS)
        )
        self.section_executor = SectionPassExecutor(
            max_parallel=REPORT_SECTION_PARALLELISM,
            cooldown_seconds=REPORT_CONCURRENCY_COOLDOWN_SECONDS,
        )
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
```

Inside `_run_generation_pass`, construct `client = self.model_client_factory()` once before its attempt loop and call `client.chat(...)`. Do not share one HTTP client between concurrent section passes.

- [ ] **Step 4: Add ordered section generation**

Add:

```python
    def _generate_sections(self, report_context, notes, analysis_context, macro_osint):
        def generate(section_pass):
            started_at = time.monotonic()
            content = self._run_generation_pass(
                report_context,
                notes,
                analysis_context,
                macro_osint,
                section_pass["sections"],
                section_pass["include_visuals"],
                section_pass["label"],
            ).strip()
            logger.info(
                "Generation pass %s wall time %.2fs with effective parallelism %s.",
                section_pass["label"],
                time.monotonic() - started_at,
                self.section_executor.effective_parallelism(),
            )
            return content

        return self.section_executor.run(self.SECTION_PASSES, generate)
```

Replace the existing `for section_pass in self.SECTION_PASSES` block in `run` with:

```python
            generated_sections = self._generate_sections(
                report_context,
                notes,
                analysis_context,
                macro_osint,
            )
```

Do not alter prompt construction, generation options, retry-on-length behavior, section concatenation, quality scoring, fallback generation, editing, final QA, or DOCX assembly.

- [ ] **Step 5: Run focused integration tests**

Run:

```bash
cd "Payment predictor"
python3 -m unittest tests.test_report_concurrency -v
```

Expected: scheduler and ReportGenerator tests pass.

- [ ] **Step 6: Run report regression tests**

Run:

```bash
cd "Payment predictor"
python3 -m unittest \
  tests.test_report_sanitization \
  tests.test_reader_facing_document_contract \
  tests.test_finance_accountability -v
```

Expected: all report-content, sanitization, and accountability tests pass unchanged.

- [ ] **Step 7: Commit semantic-preserving integration**

```bash
git add "Payment predictor/report_generation.py" "Payment predictor/tests/test_report_concurrency.py"
git commit -m "Schedule report sections with bounded concurrency"
```

## Priority 4: Verify Sequential Equivalence Before Enabling Parallelism

### Task 5: Complete regression and instrumentation-only deployment

**Files:**
- Modify: `README.md:188-215`
- Test: all `Payment predictor/tests/test_*.py`

- [ ] **Step 1: Document operator-only settings**

Add to the existing environment-setting section:

```text
REPORT_SECTION_PARALLELISM=1
REPORT_LLM_TIMEOUT_SECONDS=180
REPORT_CONCURRENCY_COOLDOWN_SECONDS=120
```

Document that `1` preserves sequential generation, production must not exceed `2` on the current deployment, and `REPORT_MAX_CONCURRENT_JOBS` remains `1` during the first rollout.

- [ ] **Step 2: Run syntax and whitespace checks**

Run:

```bash
python3 -m py_compile \
  "Payment predictor/config.py" \
  "Payment predictor/report_concurrency.py" \
  "Payment predictor/report_generation.py"
git diff --check
```

Expected: both commands exit successfully with no output from `git diff --check`.

- [ ] **Step 3: Run the complete local suite**

Run:

```bash
cd "Payment predictor"
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Expected: all tests pass. Any pre-existing unrelated failure must be recorded separately and must not be masked by changing throughput code.

- [ ] **Step 4: Deploy with parallelism still set to one**

Back up only the modified production source files under a neutral timestamped path, copy `config.py`, `report_concurrency.py`, and `report_generation.py`, compile them on the VPS, and keep:

```text
REPORT_SECTION_PARALLELISM=1
REPORT_MAX_CONCURRENT_JOBS=1
```

Do not deploy local tests, benchmark scripts, caches, generated documents, or temporary artifacts.

- [ ] **Step 5: Restart and verify unchanged behavior**

Verify:

```text
systemd service: active
public /health: HTTP 200
dataReady: true
internalDataContractReady: true
financialData.syncStatus: ready
maxConcurrentJobs: 1
```

Generate one controlled report and confirm logs show effective parallelism `1`, all three passes complete, final QA passes, and the DOCX remains downloadable.

- [ ] **Step 6: Commit documentation**

```bash
git add README.md
git commit -m "Document guarded report throughput settings"
```

## Priority 5: Enable a Two-Way Production Canary

### Task 6: Measure effective throughput at parallelism two

**Files:**
- Production environment only; no source modification

- [ ] **Step 1: Record the serial baseline**

From existing job metrics and service logs, record at least ten successful reports or the most recent seven days when ten are available:

```text
report duration p50 and p95
generation-pass wall time p50 and p95
quality-gate pass rate
fallback rate
job error rate
429/502/503/504 and timeout count
```

- [ ] **Step 2: Enable only section parallelism two**

Set:

```text
REPORT_SECTION_PARALLELISM=2
REPORT_MAX_CONCURRENT_JOBS=1
REPORT_MAX_PENDING_JOBS=5
```

Do not increase report-job concurrency in the same change.

- [ ] **Step 3: Restart and run one controlled report**

Confirm two passes overlap in timestamps, results are concatenated in configured section order, final QA passes, and no provider-pressure cooldown is entered.

- [ ] **Step 4: Observe the next ten real reports**

Keep parallelism `2` only if all gates hold:

```text
quality-gate pass rate does not decrease by more than 2 percentage points
fallback rate does not increase by more than 2 percentage points
job error rate does not increase
report p50 duration improves by at least 20 percent
no sustained 429/503 or timeout sequence occurs
VPS available memory remains above 400 MiB and swap growth remains bounded
```

- [ ] **Step 5: Roll back automatically when a stop condition occurs**

Return `REPORT_SECTION_PARALLELISM` to `1` when any gate fails, restart the service, and verify `/health` before investigating. Source rollback is unnecessary because sequential mode uses the same tested scheduler path.

## Priority 6: Consider Two Concurrent Report Jobs Only From Evidence

### Task 7: Decide whether queue throughput needs another increment

**Files:**
- Production environment only; no source modification

- [ ] **Step 1: Review queue pressure after the parallelism-two sample**

Keep `REPORT_MAX_CONCURRENT_JOBS=1` when queued jobs are rare or report p95 duration already meets the operational target.

- [ ] **Step 2: Test a global budget of two only when queueing remains material**

For a controlled load window, set:

```text
REPORT_MAX_CONCURRENT_JOBS=2
REPORT_SECTION_PARALLELISM=1
```

This allocates one model call to each report rather than allowing four simultaneous model calls. Do not combine two report jobs with section parallelism two on the current VPS.

- [ ] **Step 3: Compare accepted reports per hour, not request count**

Promote the two-job setting only when completed quality-approved reports per hour improves while all Task 6 quality, error, memory, and latency gates continue to hold.

- [ ] **Step 4: Retain the safer configuration when gains are marginal**

Prefer:

```text
REPORT_MAX_CONCURRENT_JOBS=1
REPORT_SECTION_PARALLELISM=2
```

when the two-job test does not produce a clear effective-throughput improvement.

## Explicitly Deferred

Tensor parallelism, GPU provisioning, model replacement, multi-node inference, prompt rewrites, report-section redesign, database migrations, and UI controls are excluded. The current VPS has no GPU, and the cloud provider owns model-level parallelism. These items should not be introduced to solve the present throughput problem.

## Final Acceptance Criteria

- Existing UI, API payloads, report headings, datasets, invoice rules, accounts, and generated DOCX contract are unchanged.
- The committed default remains sequential and production can return to it through one environment value.
- Independent section passes may overlap, but their final order remains deterministic.
- Provider pressure causes selective retry and temporary sequential degradation rather than a retry storm.
- Effective throughput is evaluated by quality-approved completed reports, not raw model calls.
- No test, benchmark, cache, or generated report artifact is left on the VPS.
