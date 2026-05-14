# Cohesion Coupling Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve high-cohesion / low-coupling boundaries without changing existing routes, JSON contracts, report output shape, source activation behavior, or VPS runtime behavior.

**Architecture:** Keep current public facades stable (`create_app()`, `KnowledgeBase`, `ReportGenerator.run(...)`, `Researcher`, `InternalAPIClient`, route URLs). Extract duplicated or overloaded internals into focused helper modules and adapters, then verify with existing characterization tests plus focused regression tests.

**Tech Stack:** Flask, pandas, python-docx, ChromaDB, Ollama client, requests, unittest, Waitress/systemd deployment.

---

### Task 1: Shared Data-Source Normalization And Preview Service

**Files:**
- Modify: `Payment predictor/data_contract.py`
- Modify: `Payment predictor/cashflow_analysis.py`
- Modify: `Payment predictor/internal_api_doctor.py`
- Modify: `Payment predictor/finance_api_clients.py`
- Create: `Payment predictor/source_preview_service.py`
- Test: `Payment predictor/tests/test_internal_data_contract.py`

- [ ] Add failing tests for shared record normalization and preview profile behavior.
- [ ] Implement `normalize_records(records)` in `data_contract.py`.
- [ ] Switch the current duplicated normalization callers to the shared function.
- [ ] Add `preview_source_profile(profile, preview_rows)` for route/doctor reuse.
- [ ] Keep existing response payloads unchanged.
- [ ] Run targeted internal-data tests and full suite.

### Task 2: Report Structure Contract And Report Generator Split

**Files:**
- Create: `Payment predictor/report_structure.py`
- Create: `Payment predictor/report_prompting.py`
- Create: `Payment predictor/report_quality.py`
- Create: `Payment predictor/report_finalization.py`
- Create: `Payment predictor/report_document.py`
- Modify: `Payment predictor/config.py`
- Modify: `Payment predictor/report_generation.py`
- Test: `Payment predictor/tests/test_report_sanitization.py`

- [ ] Add failing tests proving section requirements and prompt/finalization use one shared structure.
- [ ] Move section/subheading/table contracts into `REPORT_STRUCTURE`.
- [ ] Move prompt construction, quality scoring, finalization, and document assembly behind focused helper classes.
- [ ] Keep `ReportGenerator.run(...)` and existing private wrappers compatible for tests.
- [ ] Run focused report tests and full suite.

### Task 3: Readiness Runtime Profile Decoupling

**Files:**
- Modify: `Payment predictor/financial_analyzer_readiness.py`
- Modify: `Payment predictor/cashflow_analysis.py`
- Test: `Payment predictor/tests/test_report_sanitization.py`

- [ ] Add failing test showing readiness accepts an injected runtime profile.
- [ ] Preserve default text by deriving the same profile from config when no explicit profile is passed.
- [ ] Pass runtime profile through `KnowledgeBase.get_report_context(...)`.
- [ ] Run focused report tests and full suite.

### Task 4: API Transport And Cash-Out Client Coupling Cleanup

**Files:**
- Modify: `Payment predictor/finance_api_clients.py`
- Test: `Payment predictor/tests/test_internal_data_contract.py`
- Test: `Payment predictor/tests/test_dashboard_operations.py`

- [ ] Add failing characterization test for configured `CashOutAPIClient.fetch_records()`.
- [ ] Make `CashOutAPIClient` initialize the parent transport contract correctly or use a shared transport helper.
- [ ] Keep cash-out status and normalization outputs unchanged.
- [ ] Run dashboard/internal-data tests and full suite.

### Task 5: Route Boundary Extraction

**Files:**
- Create: `Payment predictor/app_services.py`
- Create: `Payment predictor/auth_routes.py`
- Create: `Payment predictor/report_routes.py`
- Create: `Payment predictor/data_source_routes.py`
- Create: `Payment predictor/forecast_routes.py`
- Create: `Payment predictor/health_routes.py`
- Modify: `Payment predictor/app.py`
- Test: `Payment predictor/tests/test_auth_flow.py`
- Test: `Payment predictor/tests/test_dashboard_operations.py`
- Test: `Payment predictor/tests/test_internal_data_contract.py`

- [ ] Add route contract tests where missing before extraction.
- [ ] Move runtime service construction into `app_services.py`.
- [ ] Move auth/report/data-source/forecast/health routes into registration modules.
- [ ] Keep all endpoint names, URLs, HTTP status codes, and JSON keys stable.
- [ ] Run route-focused tests and full suite.

### Task 6: Deployment Verification

**Files:**
- Deploy changed production files under `Payment predictor/`

- [ ] Run `python3 -m compileall 'Payment predictor'`.
- [ ] Run `python3 -m unittest discover -s 'Payment predictor/tests' -p 'test_*.py'`.
- [ ] Run `git diff --check`.
- [ ] Sync production files to `/opt/ai-adoption/payment-app/` on the VPS.
- [ ] Compile on VPS, restart `payment-app`, and verify `https://payment.inworx.id/health` returns HTTP 200.
