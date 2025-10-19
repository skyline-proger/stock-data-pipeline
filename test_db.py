from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:12345@localhost:5433/stocks")

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print("✅ Connected to:", result.scalar())