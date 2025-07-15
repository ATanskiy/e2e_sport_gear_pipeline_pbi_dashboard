import time
from config import TIME_TO_SLEEP_ETL
from scripts.etl_upsert_customers_sales import etl_upsert_customer_sales

while True:
    print(f"\n🔁 Running ETL: etl_upsert_customer_sales\n{'-'*50}")
    try:
        etl_upsert_customer_sales()  # ✅ Call function directly
        print(f"\n✅ Finished etl_upsert_customer_sales\n{'='*50}")
    except Exception as e:
        print(f"\n❌ Script failed with error: {e}\n")

    print(f"🕒 Sleeping for {TIME_TO_SLEEP_ETL} seconds...\n")
    time.sleep(TIME_TO_SLEEP_ETL)
