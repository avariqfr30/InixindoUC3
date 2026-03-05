import pandas as pd
from ollama import Client
import config

def get_data():
    """Reads and sanitizes the local CSV database."""
    try:
        df = pd.read_csv(config.DB_PATH)
        df['DueDate'] = pd.to_datetime(df['DueDate'], errors='coerce')
        df['PaidDate'] = pd.to_datetime(df['PaidDate'], errors='coerce')
        return df
    except Exception as e:
        print(f"Data load error: {e}")
        return pd.DataFrame()

def calculate_risk(client_name):
    """Calculates payment delay averages and assigns a risk score."""
    df = get_data()
    if df.empty:
        return {}

    client_history = df[(df['Client'] == client_name) & (df['Status'] == 'Paid')].copy()
    outstanding = df[(df['Client'] == client_name) & (df['Status'] == 'Outstanding')]
    
    outstanding_amt = int(outstanding['Amount'].sum())

    # Handle clients with no paid history yet
    if client_history.empty:
        return {
            "client": client_name,
            "risk": "Unknown", 
            "avg_delay": 0, 
            "delay_display": "Belum ada histori",
            "history_count": 0, 
            "outstanding": outstanding_amt
        }

    # Delay math (Positive = Late, Negative = Early)
    client_history['delay'] = (client_history['PaidDate'] - client_history['DueDate']).dt.days
    
    avg_delay = client_history['delay'].mean()
    late_count = len(client_history[client_history['delay'] > 0])
    total_tx = len(client_history)
    late_percentage = (late_count / total_tx) * 100

    # Risk thresholds
    risk = "Low"
    if avg_delay > 3 or late_percentage > 30:
        risk = "Medium"
    if avg_delay > 10 or late_percentage > 60:
        risk = "High"

    # Human-readable delay formatting
    if avg_delay < 0:
        delay_str = f"Lebih Cepat {abs(int(avg_delay))} Hari"
    elif int(avg_delay) == 0:
        delay_str = "Tepat Waktu"
    else:
        delay_str = f"Telat {int(avg_delay)} Hari"

    return {
        "client": client_name,
        "risk": risk,
        "avg_delay": int(avg_delay),
        "delay_display": delay_str,
        "late_pct": int(late_percentage),
        "history_count": total_tx,
        "outstanding": outstanding_amt
    }

def generate_ai_reminder(client_name):
    """Drafts a reminder message using the local Ollama instance."""
    stats = calculate_risk(client_name)
    if not stats:
        return "Error: Client data unavailable."

    tone = "tegas" if stats['risk'] == 'High' else "sopan dan ramah"
    
    prompt = f"""
    Buatkan pesan WhatsApp pendek untuk menagih pembayaran ke klien: {client_name}.
    Data:
    - Total Tagihan: Rp {stats['outstanding']:,}
    - Status Risiko: {stats['risk']} (Biasanya {stats['delay_display']})
    
    Gunakan nada yang {tone}. 
    Langsung tulis pesannya saja tanpa basa-basi. Bahasa Indonesia.
    """

    try:
        client = Client(host=config.OLLAMA_HOST)
        res = client.chat(
            model=config.LLM_MODEL,
            messages=[{'role': 'user', 'content': prompt}]
        )
        return res['message']['content'].strip()
    except Exception as e:
        print(f"Ollama connection error: {e}")
        return "Error connecting to AI. Please draft manually."