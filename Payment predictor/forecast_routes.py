import calendar
from datetime import datetime, timedelta

from flask import current_app, jsonify, request

from app_services import build_sync_snapshot
from core import Researcher
from forecast_engine import CashflowForecaster, parse_idr_amount


def register_forecast_routes(app, logger):
    @app.route("/api/forecast/periods", methods=["GET"])
    def get_forecast_periods():
        return jsonify({"periods": _build_forecast_periods()})

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
            forecast = _get_or_build_single_forecast(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
                end_date=end_date,
            )
            forecast["currency"] = currency_code
            forecast["external_factors"] = _get_sanitized_external_factors(start_date, end_date)
            forecast["sync_status"] = build_sync_snapshot()
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

        if not start_date_iso:
            start_date = datetime.now()
        else:
            try:
                start_date = datetime.fromisoformat(start_date_iso)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400

        try:
            forecasts = _get_or_build_horizon_forecasts(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
            )
            horizon_end = start_date + timedelta(days=365)
            return jsonify({
                "start_date": start_date.isoformat(),
                "cash_on_hand": cash_on_hand,
                "currency": currency_code,
                "forecasts": forecasts,
                "time_horizons": CashflowForecaster.TIME_HORIZONS,
                "external_factors": _get_sanitized_external_factors(start_date, horizon_end),
                "sync_status": build_sync_snapshot(),
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


def _build_forecast_periods(month_count=3):
    base_date = datetime.now().replace(day=1)
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
