"""Internal dataset roles for the production APIDog integration.

Only real source datasets are modeled here. Placeholder or aggregate-like
values from older configs are treated as configuration noise by callers.
"""

INVOICE_TRAINING_DATASET = "InvoiceTraining"
INVOICE_CONSULTANT_DATASET = "InvoiceConsultant"
INVOICE_DATASET_CODES = (INVOICE_TRAINING_DATASET, INVOICE_CONSULTANT_DATASET)

REFERENCE_ACCOUNT_DATASET = "ReferenceAccount"
REFERENCE_INTERNAL_ACCOUNT_DATASET = "ReferenceInternalAccount"
CASH_OUT_DATASET = "BankDisbursement"

DATE_SENSITIVE_DATASETS = frozenset(
    {
        CASH_OUT_DATASET,
        INVOICE_CONSULTANT_DATASET,
        INVOICE_TRAINING_DATASET,
        REFERENCE_ACCOUNT_DATASET,
    }
)

DATASET_SERVICE_LABELS = {
    INVOICE_TRAINING_DATASET: "Training",
    INVOICE_CONSULTANT_DATASET: "Consulting",
}


def invoice_dataset_codes(configured_dataset=None):
    """Return the real invoice datasets to query for a configured source."""
    configured_dataset = str(configured_dataset or "").strip()
    if configured_dataset in INVOICE_DATASET_CODES:
        return (configured_dataset,)
    return INVOICE_DATASET_CODES


def source_dataset_label(dataset_code):
    """Return a reader-friendly service label for a source dataset code."""
    return DATASET_SERVICE_LABELS.get(str(dataset_code or "").strip(), "")
