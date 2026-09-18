import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute(
    """
    INSERT INTO raw_sales (order_id, customer_email, amount, order_date)
    VALUES ('ORD-DIRTY-001', NULL, '150.00', '2026-09-13')
    """
)
conn.commit()
conn.close()
print("Seeded one dirty row with NULL email.")