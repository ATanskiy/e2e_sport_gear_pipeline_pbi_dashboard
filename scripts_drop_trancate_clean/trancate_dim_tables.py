import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_connection
from config import TRUNCATE_DIM_TABLES

def run_truncate_sql():
    with open(TRUNCATE_DIM_TABLES, "r") as f:
        sql = f.read()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print("🧹 Dim tables truncated using.")

if __name__ == "__main__":
    run_truncate_sql()