"""
Cashflow Forecast Engine
Generates predictions for Cash In, Cash Out, and Safety Status
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import statistics


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
                nilai_str = str(row.get('Nilai Invoice', '')).replace('Rp', '').replace('.', '').strip()
                nilai = int(nilai_str) if nilai_str else 0
                
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
        
        return {
            'predicted_payments': predicted_payments,
            'total_predicted_cash_in': total,
            'payment_count': len(predicted_payments),
        }
    
    def _analyze_outstanding(self, invoices: List[Dict]) -> Dict:
        """Analyze outstanding payments by age and character"""
        
        # Group by Kelas (character)
        by_character = {}
        for invoice in invoices:
            kelas = invoice['kelas']
            if kelas not in by_character:
                by_character[kelas] = {'invoices': [], 'total': 0}
            by_character[kelas]['invoices'].append(invoice)
            by_character[kelas]['total'] += invoice['amount']
        
        # For now, all invoices are "outstanding" (simplification)
        # In production, would track actual payment dates
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
            'total_outstanding': sum(d['total'] for d in by_character.values()),
            'note': 'Analysis based on payment character and historical behavior patterns',
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
            })
        
        # High risk clients
        high_risk = [inv for inv in invoices if inv['kelas'] in ['Kelas D', 'Kelas E']]
        if high_risk:
            high_risk_total = sum(inv['amount'] for inv in high_risk)
            recommendations.append({
                'priority': 'HIGH',
                'action': 'Intervensi untuk clients dengan Kelas D-E',
                'rationale': f'IDR {high_risk_total:,.0f} at high default risk',
                'estimated_impact': 'Retention improvement, reduce bad debt',
                'sop': 'Assign dedicated account manager, schedule satisfaction survey, prepare settlement options',
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
            })
        
        return recommendations
