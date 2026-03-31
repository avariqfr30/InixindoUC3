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
    
    def project_cash_out(self, start_date: datetime, end_date: datetime) -> Dict:
        """
        Project cash outflows for a period
        Returns daily, weekly, and total cash out
        """
        days = (end_date - start_date).days + 1
        
        return {
            'total_cash_out': int(self.daily_rate * days),
            'daily_rate': int(self.daily_rate),
            'monthly_rate': self.monthly_operating_cost,
            'period_days': days
        }


class CashflowForecaster:
    """Main forecaster combining Cash In predictions, Cash Out projections, and safety analysis"""
    
    AMAN_THRESHOLD_IDR = 100_000_000  # IDR 100M minimum safe buffer
    
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
        cash_out_forecast = self.out_projector.project_cash_out(start_date, end_date)
        
        # Calculate ending cash
        total_cash_in = sum(item['amount'] for item in cash_in_forecast['predicted_payments'])
        total_cash_out = cash_out_forecast['total_cash_out']
        ending_cash = cash_on_hand + total_cash_in - total_cash_out
        
        # Safety analysis
        is_aman = ending_cash >= self.AMAN_THRESHOLD_IDR
        safety_status = 'Aman' if is_aman else 'Tidak Aman'
        
        # Outstanding analysis
        outstanding = self._analyze_outstanding(invoices)
        
        # Alerts
        alerts = self._generate_alerts(cash_on_hand, ending_cash, outstanding, invoices)
        
        # Recommendations
        recommendations = self._generate_recommendations(
            safety_status, 
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
                'safety_status': safety_status,
                'is_aman': is_aman,
            },
            'forecast': {
                'cash_in': cash_in_forecast,
                'cash_out': cash_out_forecast,
                'ending_cash': ending_cash,
                'formula': f'{cash_on_hand:,} + {total_cash_in:,} - {total_cash_out:,} = {ending_cash:,}',
            },
            'working_capital_signal': self._build_working_capital_signal(
                cash_on_hand=cash_on_hand,
                total_cash_in=total_cash_in,
                total_cash_out=total_cash_out,
                ending_cash=ending_cash,
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
        
        for idx, row in df.iterrows():
            try:
                nilai = parse_idr_amount(row.get('Nilai Invoice', ''))
                
                invoices.append({
                    'index': idx,
                    'partner_type': str(row.get('Tipe Partner', '')),
                    'service': str(row.get('Layanan', '')),
                    'kelas': self.behavior_analyzer.extract_kelas(str(row.get('Kelas Pembayaran', ''))),
                    'amount': nilai,
                    'note': str(row.get('Catatan Historis Keterlambatan', '')),
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
        invoices: List[Dict]
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
        
        return alerts
    
    def _generate_recommendations(
        self,
        safety_status: str,
        outstanding: Dict,
        invoices: List[Dict],
        ending_cash: int,
    ) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if safety_status == 'Tidak Aman':
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Percepat penagihan dari high-priority clients',
                'rationale': 'Ending cash di bawah threshold aman',
                'estimated_impact': 'Dapat meningkatkan cash in 10-30 hari',
                'sop': 'Hubungi Client Relations, escalate ke Finance VP',
                'customer_characteristic': 'Likuiditas internal tertekan',
                'retention_signal': 'Pertahankan akun yang masih responsif agar cash in tidak makin tertunda',
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
                'retention_signal': 'Retention kuat; akun sehat bisa dijaga untuk menopang kestabilan cash in jangka menengah',
                'satisfaction_signal': 'Satisfaction tinggi memberi ruang untuk cross-sell tanpa menambah risiko pembayaran berarti',
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
    ) -> Dict:
        """Silent operating status that can guide UI without exposing the internal label directly."""
        gap = ending_cash - self.AMAN_THRESHOLD_IDR
        if gap >= 150_000_000:
            label = 'buffer kuat'
        elif gap >= 0:
            label = 'buffer tipis'
        else:
            label = 'perlu intervensi'

        return {
            'label': label,
            'starting_cash': cash_on_hand,
            'predicted_cash_in': total_cash_in,
            'predicted_cash_out': total_cash_out,
            'ending_cash': ending_cash,
            'gap_to_safe_buffer': gap,
            'horizon': self.TIME_HORIZONS.get(horizon_key, {}).get('label', 'Custom Period') if horizon_key else 'Custom Period',
        }
