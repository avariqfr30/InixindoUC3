from financial_analyzer_context import FinancialAnalyzerContextMixin
from financial_analyzer_evidence import FinancialAnalyzerEvidenceMixin
from financial_analyzer_metrics import FinancialAnalyzerMetricsMixin
from financial_analyzer_readiness import FinancialAnalyzerReadinessMixin


class FinancialAnalyzer(
    FinancialAnalyzerContextMixin,
    FinancialAnalyzerReadinessMixin,
    FinancialAnalyzerEvidenceMixin,
    FinancialAnalyzerMetricsMixin,
):
    pass
