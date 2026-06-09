from datetime import datetime


def expected_invoice_amount(nominal_amount, retention_probability, is_actual=False):
    nominal = int(nominal_amount or 0)
    if is_actual:
        return nominal
    probability = max(0.0, min(float(retention_probability or 0) / 100, 1.0))
    return int(round(nominal * probability))


def build_cash_in_formula(predicted_payments, pipeline_payments=None):
    predicted_payments = predicted_payments or []
    pipeline_payments = pipeline_payments or []
    nominal_invoice_component = sum(int(item.get("nominal_amount", item.get("amount", 0)) or 0) for item in predicted_payments)
    expected_invoice_component = sum(int(item.get("amount") or 0) for item in predicted_payments)
    pipeline_component = sum(int(item.get("amount") or 0) for item in pipeline_payments)
    return {
        "formula": "Cash In = expected invoice collections + probability-adjusted pipeline",
        "nominal_invoice_component": nominal_invoice_component,
        "expected_invoice_component": expected_invoice_component,
        "pipeline_component": pipeline_component,
        "retention_adjustment": expected_invoice_component - nominal_invoice_component,
        "total_expected_cash_in": expected_invoice_component + pipeline_component,
        "pipeline": {
            "status": "available" if pipeline_payments else "not_available",
            "record_count": len(pipeline_payments),
        },
    }


def build_cash_out_formula(scheduled_disbursement, fixed_cost_component, activity_multiplier, source):
    scheduled = int(scheduled_disbursement or 0)
    fixed = int(fixed_cost_component or 0)
    base = scheduled + fixed
    multiplier = max(float(activity_multiplier or 1), 0)
    total = int(round(base * multiplier))
    return {
        "formula": "Cash Out = scheduled disbursement + fixed cost baseline + activity/planning adjustment",
        "scheduled_disbursement_component": scheduled,
        "fixed_cost_component": fixed,
        "activity_multiplier": round(multiplier, 3),
        "activity_adjustment": total - base,
        "planning_source": "BankDisbursement" if source == "live_schedule" else "monthly_fixed_cost_baseline",
        "total_expected_cash_out": total,
    }


def build_horizon_assumptions(horizon_key, pipeline_available, cash_out_source):
    profiles = {
        "short_term": {
            "confidence": "high",
            "basis": "Invoice due dates, paid/unpaid status, and scheduled BankDisbursement items dominate this horizon.",
        },
        "mid_term": {
            "confidence": "medium",
            "basis": "Invoice behavior remains useful, but timing risk increases beyond immediate collection windows.",
        },
        "long_term": {
            "confidence": "low" if not pipeline_available else "medium",
            "basis": "Long horizon needs pipeline and planning assumptions; output is directional when pipeline is absent.",
        },
    }
    profile = profiles.get(horizon_key, profiles["short_term"])
    return {
        "confidence": profile["confidence"],
        "basis": profile["basis"],
        "pipeline": {
            "status": "available" if pipeline_available else "not_available",
            "usage": "Included as probability-adjusted future cash-in." if pipeline_available else "Not included; no reliable pipeline dataset/input is available.",
        },
        "cash_out": {
            "source": cash_out_source,
            "usage": "Scheduled disbursement is used when available; monthly fixed cost is fallback baseline.",
        },
    }


def parse_pipeline_payments(pipeline_records, start_date, end_date, amount_parser):
    payments = []
    for record in pipeline_records or []:
        if not isinstance(record, dict):
            continue
        date_value = (
            record.get("expected_payment_date")
            or record.get("payment_date")
            or record.get("due_date")
            or record.get("expected_close_date")
        )
        parsed_date = datetime.fromisoformat(str(date_value)) if date_value else None
        if parsed_date is None or not (start_date <= parsed_date <= end_date):
            continue
        probability = float(record.get("probability") or record.get("retention_probability") or 0) / 100
        probability = max(0.0, min(probability, 1.0))
        amount = amount_parser(record.get("amount") or record.get("value") or record.get("expected_amount") or 0)
        expected_amount = int(round(amount * probability))
        if expected_amount <= 0:
            continue
        payments.append(
            {
                "source": "pipeline",
                "name": str(record.get("name") or record.get("opportunity") or "Pipeline"),
                "nominal_amount": amount,
                "amount": expected_amount,
                "probability": round(probability * 100, 1),
                "estimated_payment_date": parsed_date.isoformat(),
            }
        )
    return payments
