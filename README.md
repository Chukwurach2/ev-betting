# 📈 EV Betting Dashboard

A data-driven sports betting dashboard built with **Streamlit** that calculates expected value (EV), applies Kelly Criterion position sizing, and tracks bankroll performance with persistent Google Sheets storage.

Designed for disciplined, process-driven betting.

---

## 🚀 Features

### ✅ Expected Value Engine
- Input book odds + fair odds
- Calculates:
  - Implied probability
  - True probability
  - Edge %
  - Kelly fraction
  - Suggested stake size

### ✅ Bankroll Management
- Adjustable bankroll + unit size
- Kelly scaling (fractional Kelly supported)
- Parlay support
- Boost handling

### ✅ CLV Tracking
- Logs closing odds
- Tracks Closing Line Value (CLV)
- Measures edge quality independent of variance

### ✅ Google Sheets Backend
- Persistent ledger storage
- Cloud-accessible
- Mobile-friendly logging
- Local JSON fallback if Sheets unavailable

---

## 🏗 Architecture

User (Desktop / iPhone)
→ Streamlit App (Local or Cloud)
→ Google Sheets (Primary Storage)
→ Local JSON Fallback (`data/ev_ledger.json`)

Storage auto-detects:
- If Google secrets exist → use Sheets
- Otherwise → fallback to local JSON

---

## 📊 Metrics Tracked

- Total units staked
- Win %
- ROI
- EV %
- CLV %
- Bankroll trajectory
- Bet-level PnL
- Parlay stats

---

## 🛠 Tech Stack

- Python 3.10+
- Streamlit
- gspread
- Google Service Account Auth
- Pandas / NumPy

---

# 🔧 Local Setup

### 1️⃣ Clone repo

```bash
git clone https://github.com/Chukwurach2/ev-betting.git
cd ev-betting
```

### 2️⃣ Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Run app

```bash
streamlit run app.py
```

---

# ☁️ Google Sheets Setup (Optional but Recommended)

The app supports persistent cloud storage via Google Sheets.

## Step 1 – Create Service Account

- Go to Google Cloud Console
- Enable **Google Sheets API**
- Create a Service Account
- Generate a JSON key

## Step 2 – Share Your Google Sheet

Share the target sheet with:

```
<service-account-email>@<project>.iam.gserviceaccount.com
```

Grant **Editor** access.

## Step 3 – Add Secrets

Create `.streamlit/secrets.toml` locally (DO NOT COMMIT):

```toml
spreadsheet_id = "YOUR_SPREADSHEET_ID"
worksheet_name = "ledger"

[gcp_service_account]
type = "service_account"
project_id = "..."
private_key_id = "..."
private_key = """-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"""
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
```

For Streamlit Cloud:
App → Settings → Secrets → paste the same values.

---

# 📈 Kelly Formula Used

For decimal odds:

```
f* = (bp - q) / b
```

Where:

- b = decimal odds - 1  
- p = true probability  
- q = 1 - p  

American odds are converted internally.

Suggested stake = Kelly fraction × bankroll (or scaled Kelly).

---

# 📂 Project Structure

```
ev-betting/
│
├── app.py
├── storage.py
├── pages/
│   ├── 1_Mobile_Stake.py
│   └── ...
├── data/
│   └── ev_ledger.json (local fallback)
├── requirements.txt
└── README.md
```

---

# ⚠️ Disclaimer

This project is for educational and analytical purposes only.  
No betting advice is provided.  
Past performance does not guarantee future results.

---

# 👤 Author

Chris Chukwura  
