import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("TRUNCATE raw_sales, clean_sales, clean_sales_dead_letter")
conn.commit()
conn.close()
print("Tables reset.")
