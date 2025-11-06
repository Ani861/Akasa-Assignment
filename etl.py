

import os
import xmltodict
import hashlib
import argparse
import pandas as pd
import mysql.connector
from datetime import datetime, timezone
from decimal import Decimal


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password",     
    "database": "etl"           #
}


CUSTOMER_FILE = r"C:\Users\Aniruddha G K\Downloads\Akasa-Assignment\sampledata\Task_DE_new_customers.csv"
ORDER_FILE = r"C:\Users\Aniruddha G K\Downloads\Akasa-Assignment\sampledata\Task_DE_new_orders.xml"



def get_conn():
   
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        return cnx
    except mysql.connector.Error as err:
        print(f" Database connection failed: {err}")
        exit(1)


def normalize_mobile(m):
    if pd.isna(m):
        return None
    s = str(m).strip().replace(" ", "").replace("-", "")
    if s.startswith("+"):
        s = s[1:]
   
    s = s.lstrip("0")
    if len(s) > 10:
        s = s[-10:]
    return s



def file_checksum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()



def init_db():
    """Create schema by executing schema.sql."""
    if not os.path.exists("schema.sql"):
        print("schema.sql not found!")
        return

    with open("schema.sql", "r") as f:
        sql = f.read()

    cnx = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    cursor = cnx.cursor()

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cursor.execute(stmt)
            except Exception as e:
                print(" Skipping statement:", e)

    cnx.commit()
    cursor.close()
    cnx.close()
    print("Database schema initialized successfully.")


def ingest_customers():
    if not os.path.exists(CUSTOMER_FILE):
        print("Customer file not found.")
        return 0

    df = pd.read_csv(CUSTOMER_FILE, dtype=str)
    df.fillna("", inplace=True)
    df["mobile_number"] = df["mobile_number"].apply(normalize_mobile)

    cnx = get_conn()
    cur = cnx.cursor()

    sql = """
        INSERT INTO customers (customer_id, customer_name, mobile_number, region)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        customer_name = VALUES(customer_name),
        mobile_number = VALUES(mobile_number),
        region = VALUES(region)
    """

    count = 0
    for _, r in df.iterrows():
        try:
            cur.execute(sql, (
                r["customer_id"].strip(),
                r["customer_name"].strip(),
                r["mobile_number"],
                r["region"] or "Unknown"
            ))
            count += 1
        except Exception as e:
            cur2 = cnx.cursor()
            cur2.execute(
                "INSERT INTO dead_letter (source, raw_data, error_message) VALUES (%s, %s, %s)",
                ("customers", str(dict(r)), str(e))
            )
            cnx.commit()
            cur2.close()

    cnx.commit()
    cur.close()
    cnx.close()
    print(f" Ingested {count} customer rows.")
    return count


def ingest_orders():
    if not os.path.exists(ORDER_FILE):
        print(" Orders XML not found.")
        return 0

    with open(ORDER_FILE, "r") as f:
        data = xmltodict.parse(f.read())

    orders = data.get("orders", {}).get("order", [])
    if isinstance(orders, dict):
        orders = [orders]

    rows = []
    for o in orders:
        try:
            order_id = o.get("order_id")
            mobile = normalize_mobile(o.get("mobile_number"))
            dt = o.get("order_date_time")

            parsed = None
            try:
                parsed = datetime.fromisoformat(dt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                parsed = None

            sku = o.get("sku_id")
            sku_count = int(o.get("sku_count") or 0)
            total_amount = Decimal(str(o.get("total_amount") or "0"))

            rows.append({
                "order_id": order_id,
                "mobile_number": mobile,
                "order_date_time": parsed,
                "sku_id": sku,
                "sku_count": sku_count,
                "total_amount": total_amount
            })
        except Exception as e:
            cnx = get_conn()
            cur = cnx.cursor()
            cur.execute(
                "INSERT INTO dead_letter (source, raw_data, error_message) VALUES (%s, %s, %s)",
                ("orders", str(o), str(e))
            )
            cnx.commit()
            cur.close()
            cnx.close()

    df = pd.DataFrame(rows)
    if df.empty:
        print("No orders to process.")
        return 0

   
    order_totals = (
        df.groupby("order_id", as_index=False)
        .agg({
            "mobile_number": "first",
            "order_date_time": "first",
            "total_amount": "max"
        })
        .rename(columns={"total_amount": "order_total"})
    )

    cnx = get_conn()
    cur = cnx.cursor()
    print(f"[DEBUG] Order {order_id}: mobile={mobile}")


    item_sql = """
        INSERT INTO order_items (order_id, sku_id, sku_count, line_amount)
        VALUES (%s, %s, %s, %s)
    """
    for _, r in df.iterrows():
        try:
            cur.execute(item_sql, (
                r["order_id"],
                r["sku_id"],
                int(r["sku_count"]),
                float(r["total_amount"])
            ))
        except Exception as e:
            cur2 = cnx.cursor()
            cur2.execute(
                "INSERT INTO dead_letter (source, raw_data, error_message) VALUES (%s, %s, %s)",
                ("order_items", str(r.to_dict()), str(e))
            )
            cnx.commit()
            cur2.close()

        

  
    order_sql = """
        INSERT INTO orders (order_id, mobile_number, customer_id, order_date_time, order_total)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        mobile_number = VALUES(mobile_number),
        order_date_time = VALUES(order_date_time),
        order_total = VALUES(order_total)
    """

    for _, r in order_totals.iterrows():
        cust_id = None
        try:
            cur2 = cnx.cursor()
            cur2.execute("SELECT customer_id FROM customers WHERE mobile_number = %s", (r["mobile_number"],))
            res = cur2.fetchone()
            if res:
                cust_id = res[0]
            cur2.close()
        except Exception:
            cust_id = None

        try:
            cur.execute(order_sql, (
                r["order_id"],
                r["mobile_number"],
                cust_id,
                r["order_date_time"].strftime("%Y-%m-%d %H:%M:%S") if r["order_date_time"] else None,
                float(r["order_total"])
            ))
        except Exception as e:
            cur3 = cnx.cursor()
            cur3.execute(
                "INSERT INTO dead_letter (source, raw_data, error_message) VALUES (%s, %s, %s)",
                ("orders_upsert", str(r.to_dict()), str(e))
            )
            cnx.commit()
            cur3.close()

    cnx.commit()
    cur.close()
    cnx.close()
    print(f"Ingested {len(df)} order lines and {len(order_totals)} unique orders.")
    return len(df)


def fix_missing_customer_ids():
    cnx = get_conn()
    cur = cnx.cursor()
    cur.execute("""
        UPDATE orders o
        JOIN customers c ON o.mobile_number = c.mobile_number
        SET o.customer_id = c.customer_id
        WHERE o.customer_id IS NULL;
    """)
    cnx.commit()
    cur.close()
    cnx.close()
    print(" Fixed missing customer_id links.")

def compute_sql_kpis():
    cnx = get_conn()
    cur = cnx.cursor()

    print("\n Repeat Customers:")
    cur.execute("""
        SELECT c.customer_id, c.customer_name, COUNT(DISTINCT o.order_id) AS orders_count
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        HAVING orders_count > 1
    """)
    for row in cur.fetchall():
        print(row)

    print("\n📅 Monthly Order Trends:")
    cur.execute("""
        SELECT DATE_FORMAT(order_date_time, '%Y-%m') AS month, COUNT(DISTINCT order_id) AS total_orders
        FROM orders
        GROUP BY month
        ORDER BY month
    """)
    for row in cur.fetchall():
        print(row)

    print("\n Regional Revenue:")
    cur.execute("""
        SELECT c.region, SUM(o.order_total) AS total_revenue
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.region
        ORDER BY total_revenue DESC
    """)
    for row in cur.fetchall():
        print(row)

    print("\n Top Spenders (Last 30 Days):")
    cur.execute("""
        SELECT c.customer_name, SUM(o.order_total) AS total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.order_date_time >= NOW() - INTERVAL 30 DAY
        GROUP BY c.customer_name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(row)

    cur.close()
    cnx.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-db", action="store_true", help="Initialize DB schema")
    parser.add_argument("--run", action="store_true", help="Run ETL and compute KPIs")
    args = parser.parse_args()

    if args.init_db:
        init_db()

    if args.run:
        ingest_customers()
        ingest_orders()
        fix_missing_customer_ids()
        compute_sql_kpis()
