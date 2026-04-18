"""
Cashflow Forecast Engine
Generates predictions for Cash In, Cash Out, and Safety Status
"""
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import statistics

from data_contract import resolve_financial_columns


FOREIGN_CURRENCY_MARKERS = (
    "usd", "eur", "sgd", "aud", "jpy", "gbp", "cad", "cny", "myr", "$", "€", "£", "¥"
)


def parse_idr_amount(value) -> int:
    """Parse amounts that must already be in Rupiah/IDR."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)

    text = str(value).strip()
    lowered = text.lower()
    if any(marker in lowered for marker in FOREIGN_CURRENCY_MARKERS):
        raise ValueError(f"Non-IDR currency marker detected: {text}")
    if any(character.isalpha() for character in text) and "rp" not in lowered and "idr" not in lowered:
        raise ValueError(f"Unsupported currency format: {text}")

    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


class PaymentBehaviorAnalyzer:
    """Analyzes payment behavior from historical data and Kelas Pembayaran"""
    
    # Map Kelas Pembayaran to days delay and retention/satisfaction scores
    PAYMENT_CLASS_PROFILE = {
        'Kelas A': {'days_delay': 0, 'retention': 95, 'satisfaction': 95, 'description': 'Tepat Waktu'},
        'Kelas B': {'days_delay': 10, 'retention': 80, 'satisfaction': 75, 'description': 'Telat 1-2 Minggu'},
        'Kelas C': {'days_delay': 45, 'retention': 60, 'satisfaction': 50, 'description': 'Telat 1-2 Bulan'},
        'Kelas D': {'days_delay': 120, 'retention': 30, 'satisfaction': 25, 'description': 'Telat 3-6 Bulan'},
        'Kelas E': {'days_delay': 180, 'retention': 10, 'satisfaction': 5, 'description': 'Telat > 6 Bulan'},
    }

    AGING_BUCKETS = [
        (0, 30, '0-30 hari'),
        (31, 60, '31-60 hari'),
        (61, 90, '61-90 hari'),
        (91, 180, '91-180 hari'),
        (181, 10_000, '>180 hari'),
    ]
    
    @staticmethod
    def extract_kelas(kelas_str: str) -> str:
        """Extract Kelas from string like 'Kelas A (Tepat Waktu)'"""
        for kelas in ['Kelas A', 'Kelas B', 'Kelas C', 'Kelas D', 'Kelas E']:
            if kelas in kelas_str:
                return kelas
        return 'Kelas C'  # Default fallback
    
    @staticmethod
    def get_payment_profile(kelas: str) -> Dict:
        """Get payment behavior profile for a Kelas"""
        return PaymentBehaviorAnalyzer.PAYMENT_CLASS_PROFILE.get(
            kelas, 
            PaymentBehaviorAnalyzer.PAYMENT_CLASS_PROFILE['Kelas C']
        )
    
    @staticmethod
    def get_retention_score(kelas: str) -> int:
        """Get retention probability (0-100) for a Kelas"""
        profile = PaymentBehaviorAnalyzer.get_payment_profile(kelas)
        return profile['retention']
    
    @staticmethod
    def get_satisfaction_score(kelas: str) -> int:
        """Get satisfaction score (0-100) for a Kelas"""
        profile = PaymentBehaviorAnalyzer.get_payment_profile(kelas)
        return profile['satisfaction']
    
    @staticmethod
    def estimate_payment_date(invoice_date: datetime, kelas: str) -> datetime:
        """Estimate when invoice will be paid based on Kelas"""
        profile = PaymentBehaviorAnalyzer.get_payment_profile(kelas)
        delay_days = profile['days_delay']
        return invoice_date + timedelta(days=delay_days)

    @staticmethod
    def get_age_bucket(days_delay: int) -> str:
        """Convert days delay to aging bucket"""
        for minimum, maximum, label in PaymentBehaviorAnalyzer.AGING_BUCKETS:
            if minimum <= days_delay <= maximum:
                return label
        return '>180 hari'

    @staticmethod
    def describe_customer_characteristic(kelas: str) -> str:
        """Narrative characteristic for recommendation framing"""
        mapping = {
            'Kelas A': 'Disiplin tinggi dan loyal',
            'Kelas B': 'Relatif sehat namun perlu ritme follow-up',
            'Kelas C': 'Mulai rapuh dan sensitif terhadap pengalaman layanan',
            'Kelas D': 'Risiko penundaan tinggi dan relasi perlu intervensi',
            'Kelas E': 'Sangat berisiko, perlu eskalasi atau strategi pemulihan khusus',
        }
        return mapping.get(kelas, 'Perilaku pembayaran perlu dipantau')


class CashOutProjector:
    """Projects cash outflows (operating expenses)"""
    
    def __init__(self, monthly_operating_cost_idr: int = 200_000_000):
        """
        Initialize with monthly operating costs
        Args:
            monthly_operating_cost_idr: Fixed monthly operating cost in IDR
        """
        self.monthly_operating_cost = monthly_operating_cost_idr
        self.daily_rate = monthly_operating_cost_idr / 30
    
    def project_cash_out(self, start_date: datetime, end_date: datetime, cash_out_records: List[Dict] = None) -> Dict:
        """
        Project cash outflows for a period
        Returns daily, weekly, and total cash out
        """
        days = (end_date - start_date).days + 1

        if cash_out_records:
            relevant_records = [
                record
                for record in cash_out_records
                if record.get("is_open", True)
                and record.get("due_date")
                and start_date <= record["due_date"] <= end_date
            ]
            category_totals = {}
            for record in relevant_records:
                category = str(record.get("category") or "Tanpa Kategori").strip()
                category_totals[category] = category_totals.get(category, 0) + int(record.get("amount") or 0)

            sorted_categories = sorted(
                (
                    {"category": category, "amount": amount}
                    for category, amount in category_totals.items()
                ),
                key=lambda item: item["amount"],
                reverse=True,
            )
            live_total = int(sum(record.get("amount") or 0 for record in relevant_records))
            return {
                'total_cash_out': live_total,
                'daily_rate': int(live_total / max(days, 1)),
                'monthly_rate': self.monthly_operating_cost,
                'period_days': days,
                'source': 'live_schedule',
                'event_count': len(relevant_records),
                'category_breakdown': sorted_categories,
            }

        return {
            'total_cash_out': int(self.daily_rate * days),
            'daily_rate': int(self.daily_rate),
            'monthly_rate': self.monthly_operating_cost,
            'period_days': days,
            'source': 'modeled_monthly_rate',
            'event_count': 0,
            'category_breakdown': [],
        }


class CashflowForecaster:
    """Main forecaster combining Cash In predictions, Cash Out projections, and safety analysis"""
    
    AMAN_THRESHOLD_IDR = 100_000_000  # IDR 100M minimum safe buffer
    HEALTH_WEIGHTS = {
        'liquidity': 30,
        'stability': 20,
        'conversion': 20,
        'coverage': 20,
        'risk': 10,
    }
    
    # Time horizons per Slide 6
    TIME_HORIZONS = {
        'short_term': {'days': 30, 'label': 'Short Term (0-30 hari)', 'focus': 'Likuiditas - Apakah ada cash deficit?'},
        'mid_term': {'days': 90, 'label': 'Mid Term (1-3 bulan)', 'focus': 'Stabilitas operasional'},
        'long_term': {'days': 365, 'label': 'Long Term (3-12 bulan)', 'focus': 'Growth planning'},
    }
    
    def __init__(self, monthly_operating_cost_idr: int = 200_000_000):
        self.behavior_analyzer = PaymentBehaviorAnalyzer()
        self.out_projector = CashOutProjector(monthly_operating_cost_idr)
        self.monthly_cost = monthly_operating_cost_idr
    
    def forecast_by_horizon(
        self, 
        df: pd.DataFrame,
        cash_on_hand: int,
        start_date: datetime,
        cash_out_records: List[Dict] = None,
    ) -> Dict:
        """Generate forecasts for all time horizons"""
        forecasts = {}
        
        for horizon_key, horizon_config in self.TIME_HORIZONS.items():
            end_date = start_date + timedelta(days=horizon_config['days'])
            forecast = self.forecast(
                df=df,
                cash_on_hand=cash_on_hand,
                start_date=start_date,
                end_date=end_date,
                horizon_key=horizon_key,
                cash_out_records=cash_out_records,
            )
            forecasts[horizon_key] = forecast
        
        return forecasts
    
    def forecast(
        self, 
        df: pd.DataFrame,
        cash_on_hand: int,
        start_date: datetime,
        end_date: datetime,
        horizon_key: str = None,
        cash_out_records: List[Dict] = None,
    ) -> Dict:
        """
        Generate comprehensive cashflow forecast
        
        Args:
            df: DataFrame with invoice data (columns: Periode Laporan, Kelas Pembayaran, Nilai Invoice)
            cash_on_hand: Current cash position in IDR
            start_date: Forecast start date
            end_date: Forecast end date
        
        Returns:
            Dictionary with forecast, alerts, recommendations, and safety status
        """
        
        # Parse and filter invoices
        invoices = self._parse_invoices(df, start_date, end_date)
        
        # Project cash in (by payment behavior)
        cash_in_forecast = self._forecast_cash_in(invoices, start_date, end_date)
        
        # Project cash out
        cash_out_forecast = self.out_projector.project_cash_out(
            start_date,
            end_date,
            cash_out_records=cash_out_records,
        )
        
        # Calculate ending cash
        total_cash_in = sum(item['amount'] for item in cash_in_forecast['predicted_payments'])
        total_cash_out = cash_out_forecast['total_cash_out']
        ending_cash = cash_on_hand + total_cash_in - total_cash_out

        # Outstanding analysis
        outstanding = self._analyze_outstanding(invoices)

        cashflow_health = self._build_cashflow_health(
            cash_on_hand=cash_on_hand,
            total_cash_in=total_cash_in,
            total_cash_out=total_cash_out,
            ending_cash=ending_cash,
            invoices=invoices,
            predicted_payments=cash_in_forecast['predicted_payments'],
            outstanding=outstanding,
        )
        
        # Alerts
        alerts = self._generate_alerts(
            cash_on_hand,
            ending_cash,
            outstanding,
            invoices,
            cashflow_health,
        )
        
        # Recommendations
        recommendations = self._generate_recommendations(
            cashflow_health,
            outstanding, 
            invoices,
            ending_cash
        )
        
        return {
            'time_horizon': {
                'key': horizon_key or 'custom',
                'label': self.TIME_HORIZONS.get(horizon_key, {}).get('label', 'Custom Period') if horizon_key else 'Custom Period',
                'focus': self.TIME_HORIZONS.get(horizon_key, {}).get('focus', '') if horizon_key else '',
            },
            'forecast_period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat(),
                'days': (end_date - start_date).days + 1,
            },
            'current_state': {
                'cash_on_hand': cash_on_hand,
                'buffer_status': cashflow_health['operating_signal'],
            },
            'forecast': {
                'cash_in': cash_in_forecast,
                'cash_out': cash_out_forecast,
                'ending_cash': ending_cash,
                'formula': f'{cash_on_hand:,} + {total_cash_in:,} - {total_cash_out:,} = {ending_cash:,}',
            },
            'cashflow_health': cashflow_health,
            'working_capital_signal': self._build_working_capital_signal(
                cash_on_hand=cash_on_hand,
                total_cash_in=total_cash_in,
                total_cash_out=total_cash_out,
                ending_cash=ending_cash,
                horizon_key=horizon_key,
                cashflow_health=cashflow_health,
            ),
            'dashboard_snapshot': self._build_dashboard_snapshot(
                cash_on_hand=cash_on_hand,
                total_cash_out=total_cash_out,
                invoices=invoices,
                predicted_payments=cash_in_forecast['predicted_payments'],
                ending_cash=ending_cash,
                cashflow_health=cashflow_health,
                outstanding=outstanding,
                start_date=start_date,
                end_date=end_date,
                horizon_key=horizon_key,
            ),
            'outstanding': outstanding,
            'alerts': alerts,
            'recommendations': recommendations,
            'invoices_processed': len(invoices),
        }
    
    def _parse_invoices(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Parse invoice data from DataFrame"""
        invoices = []
        resolved_columns = resolve_financial_columns(df)
        partner_column = resolved_columns.get('partner_type')
        service_column = resolved_columns.get('service')
        payment_class_column = resolved_columns.get('payment_class')
        invoice_value_column = resolved_columns.get('invoice_value')
        delay_note_column = resolved_columns.get('delay_note')
        
        for idx, row in df.iterrows():
            try:
                nilai = parse_idr_amount(row.get(invoice_value_column, '')) if invoice_value_column else 0
                
                invoices.append({
                    'index': idx,
                    'partner_type': str(row.get(partner_column, '')) if partner_column else '',
                    'service': str(row.get(service_column, '')) if service_column else '',
                    'kelas': self.behavior_analyzer.extract_kelas(str(row.get(payment_class_column, ''))) if payment_class_column else 'Kelas C',
                    'amount': nilai,
                    'note': str(row.get(delay_note_column, '')) if delay_note_column else '',
                })
            except (ValueError, TypeError):
                continue
        
        return invoices
    
    def _forecast_cash_in(self, invoices: List[Dict], start_date: datetime, end_date: datetime) -> Dict:
        """Forecast cash in based on payment behavior"""
        predicted_payments = []
        
        for invoice in invoices:
            kelas = invoice['kelas']
            profile = self.behavior_analyzer.get_payment_profile(kelas)
            
            # Assume invoice issued at start of period for simplicity
            estimated_payment_date = start_date + timedelta(days=profile['days_delay'])
            
            # Only count if payment falls within forecast period
            if estimated_payment_date <= end_date:
                predicted_payments.append({
                    'partner_type': invoice['partner_type'],
                    'service': invoice['service'],
                    'kelas': kelas,
                    'amount': invoice['amount'],
                    'estimated_payment_date': estimated_payment_date.isoformat(),
                    'days_delay': profile['days_delay'],
                    'retention_probability': profile['retention'],
                    'satisfaction_score': profile['satisfaction'],
                })
        
        total = sum(p['amount'] for p in predicted_payments)
        behavior_summary = self._summarize_payment_behavior(predicted_payments)
        
        return {
            'predicted_payments': predicted_payments,
            'total_predicted_cash_in': total,
            'payment_count': len(predicted_payments),
            'behavior_summary': behavior_summary,
        }
    
    def _analyze_outstanding(self, invoices: List[Dict]) -> Dict:
        """Analyze outstanding payments by age and character"""
        
        # Group by Kelas (character)
        by_character = {}
        by_age = {}
        by_age_and_character = {}
        for invoice in invoices:
            kelas = invoice['kelas']
            profile = self.behavior_analyzer.get_payment_profile(kelas)
            age_bucket = self.behavior_analyzer.get_age_bucket(profile['days_delay'])
            if kelas not in by_character:
                by_character[kelas] = {'invoices': [], 'total': 0}
            by_character[kelas]['invoices'].append(invoice)
            by_character[kelas]['total'] += invoice['amount']

            if age_bucket not in by_age:
                by_age[age_bucket] = {'count': 0, 'total': 0}
            by_age[age_bucket]['count'] += 1
            by_age[age_bucket]['total'] += invoice['amount']

            if age_bucket not in by_age_and_character:
                by_age_and_character[age_bucket] = {}
            if kelas not in by_age_and_character[age_bucket]:
                by_age_and_character[age_bucket][kelas] = {'count': 0, 'total': 0}
            by_age_and_character[age_bucket][kelas]['count'] += 1
            by_age_and_character[age_bucket][kelas]['total'] += invoice['amount']
        
        outstanding_by_character = {
            kelas: {
                'count': len(data['invoices']),
                'total': data['total'],
                'profile': self.behavior_analyzer.get_payment_profile(kelas),
            }
            for kelas, data in by_character.items()
        }
        
        return {
            'by_character': outstanding_by_character,
            'by_age': by_age,
            'by_age_and_character': by_age_and_character,
            'total_outstanding': sum(d['total'] for d in by_character.values()),
            'note': 'Outstanding saat ini dipisahkan berdasarkan karakter pembayaran dan bucket umur keterlambatan berbasis perilaku historis yang tersedia.',
        }
    
    def _generate_alerts(
        self, 
        cash_on_hand: int,
        ending_cash: int,
        outstanding: Dict,
        invoices: List[Dict],
        cashflow_health: Dict,
    ) -> List[Dict]:
        """Generate alerts based on risk indicators"""
        alerts = []
        
        # Cash deficit alert
        if ending_cash < 0:
            alerts.append({
                'type': 'CRITICAL',
                'message': f'Proyeksi cash negatif: IDR {abs(ending_cash):,.0f}',
                'severity': 5,
            })
        elif ending_cash < self.AMAN_THRESHOLD_IDR:
            days_to_zero = 0
            if ending_cash < cash_on_hand:  # Cash is decreasing
                daily_burn = (cash_on_hand - ending_cash) / 30
                days_to_zero = int(ending_cash / daily_burn) if daily_burn > 0 else 999
                alerts.append({
                    'type': 'WARNING',
                    'message': f'Cash akan habis dalam ~{days_to_zero} hari jika tren terjadi',
                    'severity': 4,
                })
            else:
                alerts.append({
                    'type': 'INFO',
                    'message': f'Cash buffer di bawah threshold aman (IDR {self.AMAN_THRESHOLD_IDR:,.0f})',
                    'severity': 2,
                })
        
        # High concentration risk
        high_risk_invoices = [inv for inv in invoices if inv['kelas'] in ['Kelas D', 'Kelas E']]
        if high_risk_invoices:
            high_risk_total = sum(inv['amount'] for inv in high_risk_invoices)
            pct = (high_risk_total / outstanding['total_outstanding'] * 100) if outstanding['total_outstanding'] > 0 else 0
            if pct > 20:
                alerts.append({
                    'type': 'WARNING',
                    'message': f'{pct:.1f}% revenue outstanding dari high-risk clients (Kelas D-E)',
                    'severity': 3,
                })

        weakest_dimensions = sorted(
            cashflow_health['dimensions'].values(),
            key=lambda item: item['score'],
        )[:2]
        for dimension in weakest_dimensions:
            if dimension['score'] >= 60:
                continue
            alerts.append({
                'type': 'WARNING' if dimension['score'] >= 40 else 'CRITICAL',
                'message': f"{dimension['label']} lemah: {dimension['summary']}",
                'severity': 4 if dimension['score'] < 40 else 3,
            })
        
        return alerts
    
    def _generate_recommendations(
        self,
        cashflow_health: Dict,
        outstanding: Dict,
        invoices: List[Dict],
        ending_cash: int,
    ) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        dimension_scores = {
            key: payload['score']
            for key, payload in cashflow_health['dimensions'].items()
        }
        
        if cashflow_health['internal_status'] != 'aman':
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Percepat penagihan dari high-priority clients',
                'rationale': 'Buffer operasional menipis dan perlu dijaga agar kewajiban jangka pendek tetap tertutup',
                'estimated_impact': 'Dapat meningkatkan arus kas masuk 10-30 hari',
                'sop': 'Hubungi Client Relations, escalate ke Finance VP',
                'customer_characteristic': 'Likuiditas internal tertekan',
                'retention_signal': 'Pertahankan akun yang masih responsif agar arus kas masuk tidak makin tertunda',
                'satisfaction_signal': 'Pastikan komunikasi tetap jelas agar percepatan penagihan tidak menurunkan trust',
            })
        
        # High risk clients
        high_risk = [inv for inv in invoices if inv['kelas'] in ['Kelas D', 'Kelas E']]
        if high_risk:
            high_risk_total = sum(inv['amount'] for inv in high_risk)
            dominant_class = statistics.mode([inv['kelas'] for inv in high_risk]) if high_risk else 'Kelas D'
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Intervensi untuk clients dengan Kelas D-E',
                'rationale': f'IDR {high_risk_total:,.0f} at high default risk',
                'estimated_impact': 'Retention improvement, reduce bad debt',
                'sop': 'Assign dedicated account manager, schedule satisfaction survey, prepare settlement options',
                'customer_characteristic': self.behavior_analyzer.describe_customer_characteristic(dominant_class),
                'retention_signal': 'Retention rendah; akun seperti ini butuh intervensi yang lebih personal atau opsi restrukturisasi',
                'satisfaction_signal': 'Satisfaction score rendah mengindikasikan potensi friksi layanan yang memperpanjang pembayaran',
            })
        
        # Medium risk - retention focus
        medium_risk = [inv for inv in invoices if inv['kelas'] == 'Kelas C']
        if len(medium_risk) > 3:
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Relationship management untuk Kelas C clients',
                'rationale': f'{len(medium_risk)} invoices dengan delay 1-2 bulan',
                'estimated_impact': 'Improve retention, reduce future delays',
                'sop': 'Quarterly check-in call, satisfaction assessment, offer flexible terms',
                'customer_characteristic': self.behavior_analyzer.describe_customer_characteristic('Kelas C'),
                'retention_signal': 'Retention berada di level menengah dan masih bisa ditingkatkan dengan ritme hubungan yang lebih baik',
                'satisfaction_signal': 'Satisfaction menengah berarti pengalaman layanan dan kejelasan dokumen masih berpengaruh pada kecepatan bayar',
            })
        
        # Good performers
        good_clients = [inv for inv in invoices if inv['kelas'] in ['Kelas A', 'Kelas B']]
        if good_clients:
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Upselling & deepening untuk Kelas A-B performers',
                'rationale': f'Proven payment discipline ({len(good_clients)} invoices)',
                'estimated_impact': 'Revenue growth, improved pipeline',
                'sop': 'Schedule business review, identify upsell opportunities, propose longer contracts',
                'customer_characteristic': self.behavior_analyzer.describe_customer_characteristic('Kelas A'),
                'retention_signal': 'Retention kuat; akun sehat bisa dijaga untuk menopang kestabilan arus kas masuk jangka menengah',
                'satisfaction_signal': 'Satisfaction tinggi memberi ruang untuk cross-sell tanpa menambah risiko pembayaran berarti',
            })

        if dimension_scores.get('stability', 100) < 60:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Perkuat visibilitas jadwal arus kas masuk mingguan',
                'rationale': 'Pola masuk kas masih fluktuatif atau terlalu sedikit invoice yang punya timing realisasi yang jelas dalam periode ini',
                'estimated_impact': 'Meningkatkan akurasi forecast kas dan menurunkan kejutan pada jadwal pembayaran',
                'sop': 'Buat weekly collection calendar, locking invoice target mingguan, dan review aging tiap awal minggu',
                'customer_characteristic': 'Timing pembayaran belum cukup predictable untuk kebutuhan operasional',
                'retention_signal': 'Komunikasi rutin menjaga akun tetap engaged sambil mendorong kepastian jadwal bayar',
                'satisfaction_signal': 'Kepastian dokumen dan follow-up yang rapi membantu klien membayar lebih terjadwal',
            })

        if dimension_scores.get('conversion', 100) < 60:
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Percepat konversi invoice menjadi kas',
                'rationale': 'Rata-rata delay pembayaran masih terlalu panjang dibanding kebutuhan arus kas jangka pendek',
                'estimated_impact': 'Memendekkan jeda invoice-to-cash dan menurunkan outstanding yang menua',
                'sop': 'Prioritaskan invoice dengan dokumen lengkap, percepat approval internal, dan gunakan reminder bertahap sebelum jatuh tempo',
                'customer_characteristic': 'Revenue sudah ada, namun kecepatan konversi ke kas belum sehat',
                'retention_signal': 'Percepatan harus tetap menjaga hubungan akun yang masih bisa diselamatkan',
                'satisfaction_signal': 'Hambatan layanan atau administrasi perlu dibersihkan agar klien tidak menunda pembayaran lebih lama',
            })

        if dimension_scores.get('risk', 100) < 60:
            recommendations.append({
                'priority': 'MEDIUM',
                'action': 'Turunkan konsentrasi risiko pada segmen yang paling dominan',
                'rationale': 'Eksposur outstanding terlalu terkonsentrasi pada partner/service tertentu atau terlalu berat di kelas D-E',
                'estimated_impact': 'Mengurangi dampak jika satu segmen besar menunda atau menghentikan pembayaran',
                'sop': 'Mapping top exposure per partner type dan layanan, lalu susun jalur eskalasi serta diversifikasi pipeline penagihan',
                'customer_characteristic': 'Risiko saat ini datang dari konsentrasi, bukan hanya dari total outstanding',
                'retention_signal': 'Retensi akun besar tetap penting, tetapi ketergantungan berlebih perlu dikendalikan',
                'satisfaction_signal': 'Perlu dibedakan mana isu relasi, mana isu struktur portofolio agar tindakan tidak salah sasaran',
            })
        
        return recommendations

    def _summarize_payment_behavior(self, predicted_payments: List[Dict]) -> List[Dict]:
        """Group predicted payments by payment character, retention, and satisfaction"""
        grouped = {}
        for payment in predicted_payments:
            kelas = payment['kelas']
            group = grouped.setdefault(
                kelas,
                {
                    'character': kelas,
                    'description': self.behavior_analyzer.get_payment_profile(kelas)['description'],
                    'count': 0,
                    'total_amount': 0,
                    'retention_total': 0,
                    'satisfaction_total': 0,
                    'days_delay_total': 0,
                },
            )
            group['count'] += 1
            group['total_amount'] += payment['amount']
            group['retention_total'] += payment['retention_probability']
            group['satisfaction_total'] += payment['satisfaction_score']
            group['days_delay_total'] += payment['days_delay']

        summary = []
        for kelas, payload in grouped.items():
            count = payload['count'] or 1
            summary.append(
                {
                    'character': kelas,
                    'description': payload['description'],
                    'count': payload['count'],
                    'total_amount': payload['total_amount'],
                    'average_retention': round(payload['retention_total'] / count, 1),
                    'average_satisfaction': round(payload['satisfaction_total'] / count, 1),
                    'average_days_delay': round(payload['days_delay_total'] / count, 1),
                    'customer_characteristic': self.behavior_analyzer.describe_customer_characteristic(kelas),
                }
            )

        return sorted(summary, key=lambda row: row['total_amount'], reverse=True)

    def _build_working_capital_signal(
        self,
        cash_on_hand: int,
        total_cash_in: int,
        total_cash_out: int,
        ending_cash: int,
        horizon_key: str = None,
        cashflow_health: Dict = None,
    ) -> Dict:
        """Silent operating status that can guide UI without exposing the internal label directly."""
        gap = ending_cash - self.AMAN_THRESHOLD_IDR
        if cashflow_health:
            label = cashflow_health['operating_signal']
            weakest_dimensions = cashflow_health['weakest_dimensions']
            readiness_checks = cashflow_health['readiness_checks']
        elif gap >= 150_000_000:
            label = 'buffer kuat'
            weakest_dimensions = []
            readiness_checks = {}
        elif gap >= 0:
            label = 'buffer tipis'
            weakest_dimensions = []
            readiness_checks = {}
        else:
            label = 'perlu intervensi'
            weakest_dimensions = []
            readiness_checks = {}

        return {
            'label': label,
            'starting_cash': cash_on_hand,
            'predicted_cash_in': total_cash_in,
            'predicted_cash_out': total_cash_out,
            'ending_cash': ending_cash,
            'gap_to_safe_buffer': gap,
            'horizon': self.TIME_HORIZONS.get(horizon_key, {}).get('label', 'Custom Period') if horizon_key else 'Custom Period',
            'weakest_dimensions': weakest_dimensions,
            'readiness_checks': readiness_checks,
        }

    def _build_cashflow_health(
        self,
        cash_on_hand: int,
        total_cash_in: int,
        total_cash_out: int,
        ending_cash: int,
        invoices: List[Dict],
        predicted_payments: List[Dict],
        outstanding: Dict,
    ) -> Dict:
        liquidity = self._score_liquidity(cash_on_hand, total_cash_out, ending_cash)
        stability = self._score_stability(invoices, predicted_payments, total_cash_in, outstanding['total_outstanding'])
        conversion = self._score_conversion(invoices)
        coverage = self._score_coverage(total_cash_in, total_cash_out)
        risk = self._score_risk(invoices, outstanding['total_outstanding'])

        dimensions = {
            'liquidity': liquidity,
            'stability': stability,
            'conversion': conversion,
            'coverage': coverage,
            'risk': risk,
        }

        overall_score = round(
            sum(
                payload['score'] * (self.HEALTH_WEIGHTS[key] / 100)
                for key, payload in dimensions.items()
            ),
            1,
        )

        if overall_score >= 80:
            internal_status = 'aman'
            operating_signal = 'buffer operasional terjaga'
        elif overall_score >= 60:
            internal_status = 'waspada'
            operating_signal = 'buffer perlu dipantau ketat'
        else:
            internal_status = 'bahaya'
            operating_signal = 'buffer memerlukan intervensi cepat'

        weakest_dimensions = [
            payload['label']
            for payload in sorted(dimensions.values(), key=lambda item: item['score'])[:2]
        ]

        readiness_checks = {
            'cash_available_now': {
                'ok': liquidity['metrics']['current_runway_months'] >= 1,
                'label': 'Cash tersedia saat ini',
                'detail': f"Runway saat ini {liquidity['metrics']['current_runway_months']:.1f} bulan.",
            },
            'cash_in_visibility': {
                'ok': stability['metrics']['visibility_ratio'] >= 0.35 and stability['metrics']['predicted_payment_count'] > 0,
                'label': 'Timing arus kas masuk dapat dipetakan',
                'detail': (
                    f"{stability['metrics']['visibility_ratio_pct']:.1f}% nilai invoice memiliki proyeksi jatuh ke periode/horizon yang dipilih."
                ),
            },
            'risk_controlled': {
                'ok': risk['score'] >= 60,
                'label': 'Risiko cashflow terkendali',
                'detail': (
                    f"Eksposur partner terbesar {risk['metrics']['top_partner_share_pct']:.1f}% "
                    f"dan porsi high-risk {risk['metrics']['high_risk_share_pct']:.1f}%."
                ),
            },
        }

        return {
            'overall_score': overall_score,
            'internal_status': internal_status,
            'operating_signal': operating_signal,
            'dimensions': dimensions,
            'weakest_dimensions': weakest_dimensions,
            'readiness_checks': readiness_checks,
        }

    def _score_liquidity(self, cash_on_hand: int, total_cash_out: int, ending_cash: int) -> Dict:
        current_runway_months = (cash_on_hand / self.monthly_cost) if self.monthly_cost else 0
        projected_runway_months = (max(ending_cash, 0) / self.monthly_cost) if self.monthly_cost else 0
        cash_vs_obligation_ratio = cash_on_hand / total_cash_out if total_cash_out else 999

        runway_score = self._score_runway(projected_runway_months)
        obligation_score = self._score_obligation_ratio(cash_vs_obligation_ratio)
        score = round((runway_score * 0.7) + (obligation_score * 0.3), 1)

        return {
            'label': 'Likuiditas',
            'score': score,
            'weight': self.HEALTH_WEIGHTS['liquidity'],
            'summary': (
                f"Runway proyeksi {projected_runway_months:.1f} bulan dengan rasio kas terhadap kewajiban jangka pendek "
                f"{cash_vs_obligation_ratio:.2f}x."
            ),
            'metrics': {
                'current_runway_months': round(current_runway_months, 2),
                'projected_runway_months': round(projected_runway_months, 2),
                'cash_vs_obligation_ratio': round(cash_vs_obligation_ratio, 2),
            },
        }

    def _score_stability(
        self,
        invoices: List[Dict],
        predicted_payments: List[Dict],
        total_cash_in: int,
        total_outstanding: int,
    ) -> Dict:
        payment_date_totals = {}
        for payment in predicted_payments:
            payment_date = payment['estimated_payment_date'][:10]
            payment_date_totals[payment_date] = payment_date_totals.get(payment_date, 0) + payment['amount']

        totals = list(payment_date_totals.values())
        if len(totals) > 1 and statistics.mean(totals) > 0:
            coefficient_variation = statistics.pstdev(totals) / statistics.mean(totals)
        else:
            coefficient_variation = 0

        total_predicted_amount = sum(payment['amount'] for payment in predicted_payments)
        low_delay_share = (
            sum(payment['amount'] for payment in predicted_payments if payment['days_delay'] <= 14) / total_predicted_amount
            if total_predicted_amount else 0
        )
        high_delay_share = (
            sum(payment['amount'] for payment in predicted_payments if payment['days_delay'] > 30) / total_predicted_amount
            if total_predicted_amount else 0
        )
        visibility_ratio = (total_cash_in / total_outstanding) if total_outstanding else 0

        variability_score = self._score_variability(coefficient_variation)
        consistency_score = self._score_stability_mix(low_delay_share, high_delay_share)
        visibility_score = self._score_visibility(visibility_ratio)
        score = round(
            (consistency_score * 0.4)
            + (variability_score * 0.3)
            + (visibility_score * 0.3),
            1,
        )

        return {
            'label': 'Stabilitas Cashflow',
            'score': score,
            'weight': self.HEALTH_WEIGHTS['stability'],
            'summary': (
                f"Visibilitas arus kas masuk {visibility_ratio * 100:.1f}% dari outstanding dengan variasi realisasi "
                f"{coefficient_variation:.2f} dan porsi pembayaran cepat {low_delay_share * 100:.1f}%."
            ),
            'metrics': {
                'visibility_ratio': round(visibility_ratio, 3),
                'visibility_ratio_pct': round(visibility_ratio * 100, 1),
                'coefficient_variation': round(coefficient_variation, 2),
                'low_delay_share_pct': round(low_delay_share * 100, 1),
                'high_delay_share_pct': round(high_delay_share * 100, 1),
                'predicted_payment_count': len(predicted_payments),
            },
        }

    def _score_conversion(self, invoices: List[Dict]) -> Dict:
        total_amount = sum(invoice['amount'] for invoice in invoices)
        weighted_delay = sum(
            self.behavior_analyzer.get_payment_profile(invoice['kelas'])['days_delay'] * invoice['amount']
            for invoice in invoices
        )
        average_delay = (weighted_delay / total_amount) if total_amount else 0
        quick_conversion_share = (
            sum(
                invoice['amount']
                for invoice in invoices
                if self.behavior_analyzer.get_payment_profile(invoice['kelas'])['days_delay'] <= 14
            ) / total_amount
            if total_amount else 0
        )

        delay_score = self._score_delay_days(average_delay)
        quick_share_score = self._score_quick_share(quick_conversion_share)
        score = round((delay_score * 0.7) + (quick_share_score * 0.3), 1)

        return {
            'label': 'Konversi Invoice ke Kas',
            'score': score,
            'weight': self.HEALTH_WEIGHTS['conversion'],
            'summary': (
                f"Rata-rata delay tertimbang {average_delay:.1f} hari dengan porsi konversi cepat "
                f"{quick_conversion_share * 100:.1f}%."
            ),
            'metrics': {
                'average_delay_days': round(average_delay, 1),
                'quick_conversion_share_pct': round(quick_conversion_share * 100, 1),
            },
        }

    def _score_coverage(self, total_cash_in: int, total_cash_out: int) -> Dict:
        coverage_ratio = (total_cash_in / total_cash_out) if total_cash_out else 999
        score = self._score_coverage_ratio(coverage_ratio)
        return {
            'label': 'Coverage',
            'score': score,
            'weight': self.HEALTH_WEIGHTS['coverage'],
            'summary': f"Rasio arus kas masuk terhadap arus kas keluar berada di {coverage_ratio:.2f}x.",
            'metrics': {
                'coverage_ratio': round(coverage_ratio, 2),
            },
        }

    def _score_risk(self, invoices: List[Dict], total_outstanding: int) -> Dict:
        partner_totals = {}
        service_totals = {}
        high_risk_total = 0

        for invoice in invoices:
            amount = invoice['amount']
            partner_totals[invoice['partner_type']] = partner_totals.get(invoice['partner_type'], 0) + amount
            service_totals[invoice['service']] = service_totals.get(invoice['service'], 0) + amount
            if invoice['kelas'] in {'Kelas D', 'Kelas E'}:
                high_risk_total += amount

        top_partner_share = (max(partner_totals.values()) / total_outstanding) if partner_totals and total_outstanding else 0
        top_service_share = (max(service_totals.values()) / total_outstanding) if service_totals and total_outstanding else 0
        high_risk_share = (high_risk_total / total_outstanding) if total_outstanding else 0

        concentration_score = self._score_concentration(top_partner_share, top_service_share)
        overdue_risk_score = self._score_high_risk_share(high_risk_share)
        score = round((concentration_score * 0.6) + (overdue_risk_score * 0.4), 1)

        return {
            'label': 'Risk Exposure',
            'score': score,
            'weight': self.HEALTH_WEIGHTS['risk'],
            'summary': (
                f"Konsentrasi partner terbesar {top_partner_share * 100:.1f}%, layanan terbesar {top_service_share * 100:.1f}%, "
                f"dan porsi kelas D-E {high_risk_share * 100:.1f}%."
            ),
            'metrics': {
                'top_partner_share_pct': round(top_partner_share * 100, 1),
                'top_service_share_pct': round(top_service_share * 100, 1),
                'high_risk_share_pct': round(high_risk_share * 100, 1),
            },
        }

    @staticmethod
    def _score_runway(runway_months: float) -> float:
        if runway_months >= 3:
            return 100
        if runway_months >= 2:
            return 85
        if runway_months >= 1.5:
            return 70
        if runway_months >= 1:
            return 45
        if runway_months >= 0.5:
            return 25
        return 10

    @staticmethod
    def _score_obligation_ratio(ratio: float) -> float:
        if ratio >= 1.2:
            return 100
        if ratio >= 1.0:
            return 85
        if ratio >= 0.75:
            return 60
        if ratio >= 0.5:
            return 35
        return 10

    @staticmethod
    def _score_variability(coefficient_variation: float) -> float:
        if coefficient_variation <= 0.35:
            return 100
        if coefficient_variation <= 0.6:
            return 80
        if coefficient_variation <= 0.9:
            return 60
        if coefficient_variation <= 1.2:
            return 40
        return 20

    @staticmethod
    def _score_stability_mix(low_delay_share: float, high_delay_share: float) -> float:
        if low_delay_share >= 0.6 and high_delay_share <= 0.15:
            return 100
        if low_delay_share >= 0.45 and high_delay_share <= 0.25:
            return 80
        if low_delay_share >= 0.3 and high_delay_share <= 0.35:
            return 60
        if low_delay_share >= 0.2 and high_delay_share <= 0.45:
            return 40
        return 20

    @staticmethod
    def _score_visibility(visibility_ratio: float) -> float:
        if visibility_ratio >= 0.7:
            return 100
        if visibility_ratio >= 0.5:
            return 80
        if visibility_ratio >= 0.35:
            return 60
        if visibility_ratio >= 0.2:
            return 40
        return 20

    @staticmethod
    def _score_delay_days(delay_days: float) -> float:
        if delay_days <= 7:
            return 100
        if delay_days <= 14:
            return 75
        if delay_days <= 30:
            return 50
        if delay_days <= 60:
            return 25
        return 10

    @staticmethod
    def _score_quick_share(share: float) -> float:
        if share >= 0.7:
            return 100
        if share >= 0.5:
            return 80
        if share >= 0.35:
            return 60
        if share >= 0.2:
            return 40
        return 20

    @staticmethod
    def _score_coverage_ratio(ratio: float) -> float:
        if ratio >= 1.5:
            return 100
        if ratio >= 1.2:
            return 85
        if ratio >= 1.0:
            return 70
        if ratio >= 0.8:
            return 45
        return 20

    @staticmethod
    def _score_concentration(top_partner_share: float, top_service_share: float) -> float:
        if top_partner_share <= 0.3 and top_service_share <= 0.3:
            return 100
        if top_partner_share <= 0.5 and top_service_share <= 0.5:
            return 70
        if top_partner_share <= 0.65 and top_service_share <= 0.65:
            return 45
        return 20

    @staticmethod
    def _score_high_risk_share(high_risk_share: float) -> float:
        if high_risk_share <= 0.15:
            return 100
        if high_risk_share <= 0.3:
            return 70
        if high_risk_share <= 0.5:
            return 45
        return 20

    def _build_dashboard_snapshot(
        self,
        cash_on_hand: int,
        total_cash_out: int,
        invoices: List[Dict],
        predicted_payments: List[Dict],
        ending_cash: int,
        cashflow_health: Dict,
        outstanding: Dict,
        start_date: datetime,
        end_date: datetime,
        horizon_key: str = None,
    ) -> Dict:
        liquidity = cashflow_health['dimensions']['liquidity']['metrics']
        coverage = cashflow_health['dimensions']['coverage']['metrics']
        conversion = cashflow_health['dimensions']['conversion']['metrics']
        risk = cashflow_health['dimensions']['risk']['metrics']

        ratio_now = liquidity['cash_vs_obligation_ratio']
        ratio_forecast = coverage['coverage_ratio']
        projected_runway = liquidity['projected_runway_months']
        horizon_days = self.TIME_HORIZONS.get(horizon_key, {}).get('days')
        dashboard_days = int(horizon_days or max((end_date - start_date).days + 1, 1))

        delay_distribution = self._build_delay_distribution(invoices)
        top_overdue_accounts = self._build_top_overdue_accounts(invoices)
        balance_points = self._build_balance_projection_points(
            cash_on_hand=cash_on_hand,
            predicted_payments=predicted_payments,
            daily_rate=self.out_projector.daily_rate,
            start_date=start_date,
            total_days=dashboard_days,
        )
        alert_recommendation_lines = self._build_dashboard_alert_lines(
            cashflow_health=cashflow_health,
            top_overdue_accounts=top_overdue_accounts,
            ending_cash=ending_cash,
            total_cash_out=total_cash_out,
            outstanding=outstanding,
        )

        return {
            'status': cashflow_health['internal_status'].upper(),
            'status_label': cashflow_health['operating_signal'],
            'current_cash': cash_on_hand,
            'runway_months': projected_runway,
            'coverage_ratio': ratio_forecast,
            'average_delay_days': conversion['average_delay_days'],
            'horizon_key': horizon_key or 'custom',
            'horizon_label': self.TIME_HORIZONS.get(horizon_key, {}).get('label', 'Custom Period') if horizon_key else 'Custom Period',
            'horizon_focus': self.TIME_HORIZONS.get(horizon_key, {}).get('focus', '') if horizon_key else '',
            'period_days': dashboard_days,
            'runway_chart': {
                'projected_months': projected_runway,
                'current_months': liquidity['current_runway_months'],
                'safe_minimum_months': 2,
                'critical_minimum_months': 1,
            },
            'coverage_chart': {
                'bars': [
                    {'label': 'Cash now', 'value': round(ratio_now, 2), 'variant': 'current'},
                    {'label': 'Cash in/out', 'value': round(ratio_forecast, 2), 'variant': 'forecast'},
                    {'label': 'Target minimum', 'value': 1.2, 'variant': 'target'},
                    {'label': 'Critical minimum', 'value': 1.0, 'variant': 'danger'},
                ],
            },
            'balance_projection_30d': balance_points,
            'delay_distribution': delay_distribution,
            'top_overdue_accounts': top_overdue_accounts,
            'alert_recommendation_lines': alert_recommendation_lines,
            'risk_summary': {
                'top_partner_share_pct': risk['top_partner_share_pct'],
                'top_service_share_pct': risk['top_service_share_pct'],
                'high_risk_share_pct': risk['high_risk_share_pct'],
            },
        }

    def _build_delay_distribution(self, invoices: List[Dict]) -> List[Dict]:
        buckets = [
            {'label': '0-5 hari', 'min': 0, 'max': 5, 'variant': 'good'},
            {'label': '6-10 hari', 'min': 6, 'max': 10, 'variant': 'watch'},
            {'label': '> 10 hari', 'min': 11, 'max': 10_000, 'variant': 'risk'},
        ]
        total = len(invoices) or 1
        distribution = []
        for bucket in buckets:
            count = 0
            for invoice in invoices:
                delay = self.behavior_analyzer.get_payment_profile(invoice['kelas'])['days_delay']
                if bucket['min'] <= delay <= bucket['max']:
                    count += 1
            distribution.append({
                'label': bucket['label'],
                'count': count,
                'percentage': round((count / total) * 100, 1),
                'variant': bucket['variant'],
            })
        return distribution

    def _build_top_overdue_accounts(self, invoices: List[Dict]) -> List[Dict]:
        ranked = sorted(
            invoices,
            key=lambda invoice: (
                self.behavior_analyzer.get_payment_profile(invoice['kelas'])['days_delay'],
                invoice['amount'],
            ),
            reverse=True,
        )[:5]
        accounts = []
        for invoice in ranked:
            days_overdue = self.behavior_analyzer.get_payment_profile(invoice['kelas'])['days_delay']
            account_name = invoice['partner_type']
            if invoice['service']:
                account_name = f"{invoice['partner_type']} - {invoice['service']}"
            accounts.append({
                'name': account_name,
                'amount': invoice['amount'],
                'days_overdue': days_overdue,
            })
        return accounts

    def _build_balance_projection_points(
        self,
        cash_on_hand: int,
        predicted_payments: List[Dict],
        daily_rate: float,
        start_date: datetime,
        total_days: int,
    ) -> List[Dict]:
        if total_days <= 30:
            checkpoints = [1, 7, 14, 21, 28, total_days]
        elif total_days <= 90:
            checkpoints = [1, 14, 30, 45, 60, total_days]
        else:
            checkpoints = [1, 30, 90, 180, 270, total_days]

        normalized = []
        seen = set()
        for day in checkpoints:
            bounded = min(max(int(day), 1), total_days)
            if bounded in seen:
                continue
            seen.add(bounded)
            normalized.append(bounded)

        balance_points = []
        for day in normalized:
            checkpoint = start_date + timedelta(days=day - 1)
            cumulative_cash_in = sum(
                payment['amount']
                for payment in predicted_payments
                if datetime.fromisoformat(payment['estimated_payment_date']) <= checkpoint
            )
            projected_balance = cash_on_hand + cumulative_cash_in - int(daily_rate * day)
            balance_points.append({
                'label': f'H{day}',
                'day': day,
                'balance': projected_balance,
            })
        return balance_points

    def _build_dashboard_alert_lines(
        self,
        cashflow_health: Dict,
        top_overdue_accounts: List[Dict],
        ending_cash: int,
        total_cash_out: int,
        outstanding: Dict,
    ) -> List[str]:
        lines = []
        if ending_cash < self.AMAN_THRESHOLD_IDR:
            lines.append('Saldo proyeksi mendekati atau berada di bawah buffer minimum operasional.')
        elif ending_cash < total_cash_out:
            lines.append('Saldo proyeksi masih positif, tetapi buffer belum cukup nyaman dibanding kebutuhan keluar kas.')
        else:
            lines.append('Saldo proyeksi masih berada di atas kebutuhan keluar kas jangka pendek.')

        if top_overdue_accounts:
            highest_days = top_overdue_accounts[0]['days_overdue']
            lines.append(f'Paparan overdue tertinggi saat ini berada pada akun dengan delay sekitar {highest_days} hari.')

        if cashflow_health['weakest_dimensions']:
            weakest = ', '.join(cashflow_health['weakest_dimensions'])
            lines.append(f'Fokus intervensi utama saat ini ada pada: {weakest}.')

        if outstanding['total_outstanding'] > 0:
            lines.append('Prioritaskan penagihan dan kontrol expense non-priority bila ada tekanan pada buffer kas.')

        return lines[:4]
