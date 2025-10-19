# 📈 Stock Data Pipeline & Analysis Tool

An automated Python pipeline that collects, cleans, and stores stock market data daily.  
Includes a command-line interface for analysis and visualization.

---

## 🚀 Features
- Automatic daily updates via Task Scheduler
- Historical data backfill from Yahoo Finance
- PostgreSQL database with clean, ready-to-use structure
- Rolling metrics: **daily return**, **MA(7)**, **volatility**
- Interactive console for analysis and charting
- Logging of updates and error
- Docker

---

## 🛠️ Tech Stack
**Python**, **Pandas**, **PostgreSQL**, **yFinance**, **Schedule**, **Matplotlib**, **Docker**

---

## 📂 Project Structure
```
project_stock/
├── backfill/
│   └── historical_loader.py   # Populate the database with historical prices
├── console_app.py             # Interactive explorer for stored prices
├── main.py                    # Daily scheduler and incremental updater
├── docker-compose.yml         # Optional Postgres + app containers
├── requirements.txt           # Python dependencies
├── migrate_sqlite_to_postgres.py
├── test_db.py
└── README.md
```

---

## ⚙️ Setup

Clone the repository and install dependencies:
```bash
git clone https://github.com/<your-username>/stock-data-pipeline.git
cd stock-data-pipeline
pip install -r requirements.txt
```

Create a `.env` file with your configuration:
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=123
POSTGRES_DB=stocks
TICKERS=AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META,AMD,NFLX,INTC
DATABASE_URL=postgresql+psycopg2://postgres:123@stock_db:5432/stocks
```

Make sure the PostgreSQL database exists and the user has permission to create tables.

If you don’t have a `requirements.txt`, install manually:
```bash
pip install yfinance pandas numpy sqlalchemy matplotlib loguru python-dotenv schedule tabulate tqdm psycopg2-binary
```

Run the initial backfill:
```bash
python backfill/historical_loader.py
```

Start the automatic updater:
```bash
python main.py
```

Use the interactive console for analysis:
```bash
python console_app.py
```

---

## 📊 Example Console Output
```
=== 📈 Stock Data Console ===
Available tickers:
AAPL, MSFT, TSLA, NVDA, GOOGL

Enter ticker symbol: AAPL
Enter start date (YYYY-MM-DD): 2024-01-01
Enter end date (YYYY-MM-DD): 2024-03-01

📊 Summary statistics:
+-----------------------------+----------+
| Metric                     | Value    |
+-----------------------------+----------+
| Average daily return (%)    | 0.341    |
| Average volatility          | 2.113    |
| Total change over period (%)| 11.42    |
+-----------------------------+----------+
```

---

## 🧠 Author
**Alikhan Zhaksylyk**  
Data Engineering & Machine Learning enthusiast  
📍 Based in Italy 🇮🇹  

---

## 📄 License
MIT License © 2025 Alikhan Zhaksylyk
