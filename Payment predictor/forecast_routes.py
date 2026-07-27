import calendar
from datetime import datetime, timedelta

import pandas as pd
from flask import current_app, jsonify, request, session

from app_services import build_sync_snapshot
from core import Researcher
from forecast_engine import CashflowForecaster, parse_idr_amount
from learning_feedback import dashboard_snapshot_id


def _register_dashboard_feedback_snapshot(sync_status, cash_on_hand, start_date, monthly_cost):
    snapshot_id = dashboard_snapshot_id(
        session.get("username", ""),
        sync_status,
        cash_on_hand,
        start_date.isoformat(),
        monthly_cost,
        current_app.secret_key,
    )
    recent = [item for item in session.get("dashboard_feedback_runs", []) if isinstance(item, str)]
    session["dashboard_feedback_runs"] = [snapshot_id, *[item for item in recent if item != snapshot_id]][:5]
    return snapshot_id


def register_forecast_routes(app, logger):
    @app.route("/api/forecast/periods", methods=["GET"])
    def get_forecast_periods():
        date_bounds = _get_cached_dataset_date_bounds()
        return jsonify({"periods": _build_forecast_periods(date_bounds=date_bounds), "date_bounds": _format_date_bounds(date_bounds)})

    @app.route("/api/forecast", methods=["POST"])
    def generate_forecast():
        payload = request.get_json(silent=True) or {}
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400

        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        try:
            start_iso = payload.get("start_date")
            end_iso = payload.get("end_date")

            if not start_iso or not end_iso:
                return jsonify({"error": "start_date and end_date required"}), 400

            start_date = datetime.fromisoformat(start_iso)
            end_date = datetime.fromisoformat(end_iso)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid date format (use ISO format)"}), 400

        try:
            forecast = dict(_get_or_build_single_forecast(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
                end_date=end_date,
            ))
            forecast["currency"] = currency_code
            forecast["external_factors"] = _get_sanitized_external_factors(start_date, end_date)
            sync_status = build_sync_snapshot()
            forecast["sync_status"] = sync_status
            forecast["feedback_snapshot_id"] = _register_dashboard_feedback_snapshot(
                sync_status, cash_on_hand, start_date, monthly_cost
            )
            return jsonify(forecast)
        except Exception as e:
            logger.error("Forecast error: %s", e, exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/forecast/by-horizon", methods=["POST"])
    def generate_forecast_by_horizon():
        payload = request.get_json(silent=True) or {}
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400

        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        start_date_iso = payload.get("start_date")

        date_bounds = _get_cached_dataset_date_bounds()
        if not start_date_iso:
            start_date = _select_forecast_anchor(date_bounds, "integrated") or datetime.now()
            anchor_policy = "latest_available_operational_date"
        else:
            try:
                start_date = datetime.fromisoformat(start_date_iso)
                anchor_policy = "user_supplied"
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400

        try:
            forecasts = _get_or_build_horizon_forecasts(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
            )
            horizon_end = start_date + timedelta(days=365)
            sync_status = build_sync_snapshot()
            return jsonify({
                "start_date": start_date.isoformat(),
                "cash_on_hand": cash_on_hand,
                "currency": currency_code,
                "forecasts": forecasts,
                "time_horizons": CashflowForecaster.TIME_HORIZONS,
                "external_factors": _get_sanitized_external_factors(start_date, horizon_end),
                "sync_status": sync_status,
                "feedback_snapshot_id": _register_dashboard_feedback_snapshot(
                    sync_status, cash_on_hand, start_date, monthly_cost
                ),
                "date_bounds": _format_date_bounds(date_bounds),
                "anchor_policy": anchor_policy,
            })
        except Exception as e:
            logger.error("Multi-horizon forecast error: %s", e, exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/forecast/outstanding", methods=["GET"])
    def get_outstanding():
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400

        try:
            forecaster = current_app.config["forecaster"]
            invoices = forecaster._parse_invoices(
                knowledge_base.df,
                start_date=datetime.now(),
                end_date=datetime.now(),
            )
            result = forecaster._analyze_outstanding(invoices)
            result["invoice_count"] = len(invoices)
            result["sync_status"] = build_sync_snapshot()
            return jsonify(result)
        except Exception as e:
            logger.error("Outstanding analysis error: %s", e, exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/forecast/drilldown/top-overdue", methods=["POST"])
    def get_top_overdue_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        forecasts = _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date)
        active_forecast = forecasts.get(mode) or forecasts.get("short_term")
        dashboard_snapshot = active_forecast.get("dashboard_snapshot", {}) if active_forecast else {}
        return jsonify(
            {
                "horizon": mode,
                "items": dashboard_snapshot.get("top_overdue_accounts", []),
                "alertLines": dashboard_snapshot.get("alert_recommendation_lines", []),
                "sync_status": build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/payment-class-trend", methods=["GET"])
    def get_payment_class_trend_drilldown():
        return jsonify(
            {
                **_build_payment_class_trend(),
                "sync_status": build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/concentration", methods=["POST"])
    def get_concentration_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        active_forecast = (_get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date).get(mode)) or {}
        forecaster = current_app.config["forecaster"]
        invoices = forecaster._parse_invoices(
            current_app.config["knowledge_base"].df,
            start_date=start_date,
            end_date=start_date,
        )
        return jsonify(
            {
                "horizon": mode,
                "riskSummary": (active_forecast.get("dashboard_snapshot", {}) or {}).get("risk_summary", {}),
                "concentration": _build_concentration_view(invoices),
                "sync_status": build_sync_snapshot(),
            }
        )


def _build_forecast_periods(month_count=3, date_bounds=None):
    date_bounds = date_bounds or _get_cached_dataset_date_bounds()
    anchor = date_bounds.get("max_date") if isinstance(date_bounds, dict) else None
    if not isinstance(anchor, datetime):
        anchor = datetime.now()
    base_date = anchor.replace(day=1)
    periods = []
    windows = [
        (1, 10, "1-10"),
        (11, 20, "11-20"),
        (21, None, "21-akhir bulan"),
    ]

    for offset in range(month_count):
        year = base_date.year + ((base_date.month - 1 + offset) // 12)
        month = ((base_date.month - 1 + offset) % 12) + 1
        first_day = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]

        for start_day, end_day, label in windows:
            start = first_day.replace(day=start_day)
            resolved_end_day = last_day if end_day is None else min(end_day, last_day)
            end = first_day.replace(day=resolved_end_day)
            periods.append(
                {
                    "id": f"{year}-{month:02d}_{label.replace(' ', '_')}",
                    "label": f"{label} {first_day.strftime('%B %Y')}",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                }
            )

    return periods


def _default_forecast_start_date(stream="integrated"):
    date_bounds = _get_cached_dataset_date_bounds()
    return _select_forecast_anchor(date_bounds, stream) or datetime.now()


def _select_forecast_anchor(date_bounds, stream="integrated"):
    if not isinstance(date_bounds, dict):
        return None
    stream_key = {
        "receivables": "invoice",
        "cash_out": "bank_disbursement",
        "integrated": None,
    }.get(str(stream or "integrated").strip().lower())
    if stream_key:
        anchor = (date_bounds.get(stream_key) or {}).get("max_date")
    else:
        anchor = date_bounds.get("max_date")
    return anchor if isinstance(anchor, datetime) else None


def _format_date_bounds(date_bounds):
    empty_stream = {"start": None, "end": None, "date_columns": [], "record_count": 0}
    if not date_bounds:
        return {
            "source": "unavailable",
            "start": None,
            "end": None,
            "date_columns": [],
            "record_count": 0,
            "invoice": dict(empty_stream),
            "bank_disbursement": dict(empty_stream),
            "combined": dict(empty_stream),
            "freshness_gap_days": None,
            "anchors": {"receivables": None, "cash_out": None, "integrated": None},
            "anchor_policy": {
                "receivables": "latest_invoice_date",
                "cash_out": "latest_bank_disbursement_date",
                "integrated": "latest_available_operational_date",
            },
        }

    def format_stream(stream):
        stream = stream or {}
        stream_min = stream.get("min_date")
        stream_max = stream.get("max_date")
        return {
            "start": stream_min.date().isoformat() if isinstance(stream_min, datetime) else None,
            "end": stream_max.date().isoformat() if isinstance(stream_max, datetime) else None,
            "date_columns": list(stream.get("date_columns") or []),
            "record_count": int(stream.get("record_count") or 0),
        }

    min_date = date_bounds.get("min_date")
    max_date = date_bounds.get("max_date")
    combined = {
        "min_date": min_date,
        "max_date": max_date,
        "date_columns": date_bounds.get("date_columns"),
        "record_count": date_bounds.get("record_count"),
    }
    invoice = format_stream(date_bounds.get("invoice"))
    bank_disbursement = format_stream(date_bounds.get("bank_disbursement"))
    combined_formatted = format_stream(combined)
    return {
        "source": date_bounds.get("source") or "cached_apidog_dataset",
        "start": combined_formatted["start"],
        "end": combined_formatted["end"],
        "date_columns": combined_formatted["date_columns"],
        "record_count": combined_formatted["record_count"],
        "invoice": invoice,
        "bank_disbursement": bank_disbursement,
        "combined": combined_formatted,
        "freshness_gap_days": date_bounds.get("freshness_gap_days"),
        "anchors": {
            "receivables": invoice["end"],
            "cash_out": bank_disbursement["end"],
            "integrated": combined_formatted["end"],
        },
        "anchor_policy": {
            "receivables": "latest_invoice_date",
            "cash_out": "latest_bank_disbursement_date",
            "integrated": "latest_available_operational_date",
        },
    }


def _coerce_datetime(value):
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.to_pydatetime().replace(tzinfo=None)


def _series_date_bounds(dataset, column):
    if dataset is None or dataset.empty or not column or column not in dataset.columns:
        return None
    parsed = pd.to_datetime(dataset[column], errors="coerce").dropna()
    if parsed.empty:
        return None
    return parsed.min().to_pydatetime().replace(tzinfo=None), parsed.max().to_pydatetime().replace(tzinfo=None)


def _get_cached_dataset_date_bounds():
    knowledge_base = current_app.config.get("knowledge_base")
    dataset = getattr(knowledge_base, "df", None)
    if dataset is None or dataset.empty:
        return {}

    resolved_columns = getattr(knowledge_base, "data_contract_summary", {}).get("sourceColumns", {}) or {}
    candidate_columns = []
    for key in ("period", "invoice_date", "invoice_due_date", "invoice_paid_date"):
        column = resolved_columns.get(key)
        if column and column not in candidate_columns:
            candidate_columns.append(column)
    for column in dataset.columns:
        normalized = str(column or "").strip().lower()
        if any(token in normalized for token in ("tanggal", "date", "periode", "due", "paid")) and column not in candidate_columns:
            candidate_columns.append(column)

    invoice_dates = []
    used_columns = []
    for column in candidate_columns:
        bounds = _series_date_bounds(dataset, column)
        if not bounds:
            continue
        used_columns.append(str(column))
        invoice_dates.extend(bounds)

    invoice_bounds = {}
    if invoice_dates:
        invoice_bounds = {
            "min_date": min(invoice_dates),
            "max_date": max(invoice_dates),
            "date_columns": list(used_columns),
            "record_count": int(len(dataset)),
        }

    cash_out_store = current_app.config.get("cash_out_store")
    cash_out_records = cash_out_store.get_records() if cash_out_store else []
    cash_out_dates = []
    for record in cash_out_records or []:
        if not isinstance(record, dict):
            continue
        for key in ("due_date", "date", "period"):
            parsed = _coerce_datetime(record.get(key))
            if parsed:
                cash_out_dates.append(parsed)
    if cash_out_dates:
        bank_disbursement_bounds = {
            "min_date": min(cash_out_dates),
            "max_date": max(cash_out_dates),
            "date_columns": ["BankDisbursement.due_date"],
            "record_count": int(len(cash_out_records)),
        }
    else:
        bank_disbursement_bounds = {}

    collected_dates = list(invoice_dates)
    if cash_out_dates:
        collected_dates.extend((min(cash_out_dates), max(cash_out_dates)))
    if not collected_dates:
        return {}

    invoice_max = invoice_bounds.get("max_date")
    disbursement_max = bank_disbursement_bounds.get("max_date")
    freshness_gap_days = None
    if isinstance(invoice_max, datetime) and isinstance(disbursement_max, datetime):
        freshness_gap_days = abs((disbursement_max.date() - invoice_max.date()).days)

    combined_columns = list(used_columns)
    if cash_out_dates:
        combined_columns.append("BankDisbursement.due_date")

    return {
        "source": "cached_apidog_dataset",
        "min_date": min(collected_dates),
        "max_date": max(collected_dates),
        "date_columns": combined_columns,
        "record_count": int(len(dataset) + len(cash_out_records)),
        "invoice": invoice_bounds,
        "bank_disbursement": bank_disbursement_bounds,
        "freshness_gap_days": freshness_gap_days,
    }


def _build_external_context(start_date, end_date):
    dataset = current_app.config["knowledge_base"].df
    partner_types = []
    services = []
    if dataset is not None and not dataset.empty:
        partner_column = next(
            (column for column in dataset.columns if str(column).strip().lower() in {"tipe partner", "partner type", "partner_type"}),
            None,
        )
        service_column = next(
            (column for column in dataset.columns if str(column).strip().lower() in {"layanan", "service", "service_name"}),
            None,
        )
        if partner_column:
            partner_types = (
                dataset[partner_column]
                .dropna()
                .astype(str)
                .value_counts()
                .head(3)
                .index
                .tolist()
            )
        if service_column:
            services = (
                dataset[service_column]
                .dropna()
                .astype(str)
                .value_counts()
                .head(3)
                .index
                .tolist()
            )
    partner_snippet = ", ".join(partner_types)
    service_snippet = ", ".join(services)
    return (
        f"periode {start_date.strftime('%d %B %Y')} sampai {end_date.strftime('%d %B %Y')} "
        f"partner {partner_snippet} "
        f"layanan {service_snippet}"
    ).strip()


def _parse_request_idr_amount(raw_value, field_name, default_value):
    value = default_value if raw_value is None else raw_value
    try:
        return parse_idr_amount(value)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must be provided in Rupiah (IDR) only. Foreign currencies are not supported."
        ) from exc


def _validate_currency_code(payload):
    currency = str(payload.get("currency", "IDR")).strip().upper()
    if currency not in {"IDR", "RP", "RUPIAH"}:
        raise ValueError("This app only accepts Rupiah (IDR) amounts.")
    return "IDR"


def _get_sanitized_external_factors(start_date, end_date):
    return Researcher.sanitize_dashboard_external_factors(
        Researcher.get_payment_delay_risks(_build_external_context(start_date, end_date))
    )


def _get_cash_out_records():
    return current_app.config["cash_out_store"].get_records()


def _build_forecast_cache_key(kind, cash_on_hand, monthly_cost, start_date, end_date=None):
    knowledge_state = current_app.config["knowledge_base"].get_sync_status()
    cash_out_state = current_app.config["cash_out_store"].get_status()
    return (
        kind,
        knowledge_state["dataVersion"],
        cash_out_state["version"],
        int(cash_on_hand),
        int(monthly_cost),
        start_date.isoformat(),
        end_date.isoformat() if end_date else None,
    )


def _get_or_build_single_forecast(cash_on_hand, monthly_cost, start_date, end_date):
    cache_key = _build_forecast_cache_key("single_forecast", cash_on_hand, monthly_cost, start_date, end_date)
    cached_value = current_app.config["forecast_cache"].get(cache_key)
    if cached_value is not None:
        return cached_value

    forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
    forecast = forecaster.forecast(
        df=current_app.config["knowledge_base"].df,
        cash_on_hand=cash_on_hand,
        start_date=start_date,
        end_date=end_date,
        cash_out_records=_get_cash_out_records(),
    )
    return current_app.config["forecast_cache"].set(cache_key, forecast)


def _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date):
    cache_key = _build_forecast_cache_key("horizon_forecast", cash_on_hand, monthly_cost, start_date)
    cached_value = current_app.config["forecast_cache"].get(cache_key)
    if cached_value is not None:
        return cached_value

    forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
    forecasts = forecaster.forecast_by_horizon(
        df=current_app.config["knowledge_base"].df,
        cash_on_hand=cash_on_hand,
        start_date=start_date,
        cash_out_records=_get_cash_out_records(),
    )
    return current_app.config["forecast_cache"].set(cache_key, forecasts)


def _build_payment_class_trend():
    dataset = current_app.config["knowledge_base"].df
    if dataset is None or dataset.empty:
        return {"series": [], "topPeriods": []}

    resolved_columns = current_app.config["knowledge_base"].data_contract_summary.get("sourceColumns", {})
    period_column = resolved_columns.get("period")
    payment_class_column = resolved_columns.get("payment_class")
    invoice_value_column = resolved_columns.get("invoice_value")
    if not period_column or not payment_class_column or not invoice_value_column:
        return {"series": [], "topPeriods": []}

    working_df = dataset[[period_column, payment_class_column, invoice_value_column]].copy()
    working_df.columns = ["period", "payment_class", "invoice_value"]
    working_df["payment_class"] = working_df["payment_class"].astype(str).str.extract(r"(Kelas [A-E])", expand=False).fillna("Tidak Diketahui")
    working_df["invoice_value"] = working_df["invoice_value"].apply(
        lambda value: parse_idr_amount(value) if value is not None and str(value).strip() else 0
    )
    working_df["period"] = working_df["period"].astype(str).fillna("Tidak Diketahui")

    grouped = (
        working_df.groupby(["period", "payment_class"], as_index=False)
        .agg(amount=("invoice_value", "sum"), invoice_count=("invoice_value", "size"))
    )
    period_totals = (
        grouped.groupby("period", as_index=False)
        .agg(total_amount=("amount", "sum"))
        .sort_values("total_amount", ascending=False)
    )
    return {
        "series": grouped.to_dict(orient="records"),
        "topPeriods": period_totals.head(10).to_dict(orient="records"),
    }


def _build_concentration_view(invoices):
    if not invoices:
        return {"partners": [], "services": []}

    partner_totals = {}
    service_totals = {}
    total_amount = sum(invoice["amount"] for invoice in invoices) or 1

    for invoice in invoices:
        partner = invoice["partner_type"] or "Tidak Diketahui"
        service = invoice["service"] or "Tidak Diketahui"
        partner_totals[partner] = partner_totals.get(partner, 0) + invoice["amount"]
        service_totals[service] = service_totals.get(service, 0) + invoice["amount"]

    def _rank_items(source_map):
        return [
            {
                "label": label,
                "amount": amount,
                "sharePct": round((amount / total_amount) * 100, 1),
            }
            for label, amount in sorted(source_map.items(), key=lambda item: item[1], reverse=True)[:10]
        ]

    return {
        "partners": _rank_items(partner_totals),
        "services": _rank_items(service_totals),
    }
