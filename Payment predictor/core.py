"""Compatibility facade for the app's core services.

Implementation lives in focused modules so web routes, analysis, OSINT,
DOCX rendering, and data-source clients can evolve independently.
"""

import importlib

import cashflow_analysis as _cashflow_analysis
import docx_rendering as _docx_rendering
import finance_api_clients as _finance_api_clients
import osint_research as _osint_research
import report_generation as _report_generation
import requests

# Keep `core.requests` available for older tests/tools that monkeypatch HTTP calls
# through the historical monolithic module.
_finance_api_clients = importlib.reload(_finance_api_clients)
_cashflow_analysis = importlib.reload(_cashflow_analysis)
_osint_research = importlib.reload(_osint_research)
_docx_rendering = importlib.reload(_docx_rendering)
_report_generation = importlib.reload(_report_generation)

FinancialAnalyzer = _cashflow_analysis.FinancialAnalyzer
KnowledgeBase = _cashflow_analysis.KnowledgeBase
ChartEngine = _docx_rendering.ChartEngine
DocumentBuilder = _docx_rendering.DocumentBuilder
StyleEngine = _docx_rendering.StyleEngine
CashOutAPIClient = _finance_api_clients.CashOutAPIClient
CashOutStore = _finance_api_clients.CashOutStore
InternalAPIClient = _finance_api_clients.InternalAPIClient
InsightSchema = _osint_research.InsightSchema
Researcher = _osint_research.Researcher
ReportGenerator = _report_generation.ReportGenerator

__all__ = [
    "InsightSchema",
    "InternalAPIClient",
    "CashOutAPIClient",
    "CashOutStore",
    "KnowledgeBase",
    "FinancialAnalyzer",
    "Researcher",
    "StyleEngine",
    "ChartEngine",
    "DocumentBuilder",
    "ReportGenerator",
    "requests",
]
