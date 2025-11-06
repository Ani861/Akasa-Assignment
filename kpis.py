
import pandas as pd
import xmltodict
from datetime import datetime, timedelta, timezone
import os



SAMPLE_DIR = "sample_data"
CUSTOMER_FILE = r"C:\Users\Aniruddha G K\Downloads\Akasa-Assignment\sampledata\Task_DE_new_customers.csv"
ORDER_FILE = r"C:\Users\Aniruddha G K\Downloads\Akasa-Assignment\sampledata\Task_DE_new_orders.xml"




def normalize_mobile(m):
   
    if pd.isna(m):
        return None
    s = str(m).strip()
    s = s.replace(" ", "").replace("-", "")
    if s.startswith("+"):
        s = s[1:]
    s = s.lstrip("0")
    return s


def load_customers():
    """Load and clean customer CSV."""
    df = pd.read_csv(CUSTOMER_FILE, dtype=str)
    df.fillna("", inplace=True)
    df["mobile_number"] = df["mobile_number"].apply(normalize_mobile)
    return df


def load_orders():
    
    with open(ORDER_FILE, "r") as f:
        data_dict = xmltodict.parse(f.read())

    orders = data_dict.get("orders", {}).get("order", [])
    if isinstance(orders, dict): 
        orders = [orders]

    rows = []
    for o in orders:
        dt = o.get("order_date_time")
        try:
            parsed = datetime.fromisoformat(dt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            parsed = None

        rows.append({
            "order_id": o.get("order_id"),
            "mobile_number": normalize_mobile(o.get("mobile_number")),
            "order_date_time": parsed,
            "sku_id": o.get("sku_id"),
            "sku_count": int(o.get("sku_count") or 0),
            "total_amount": float(o.get("total_amount") or 0.0),
        })

    df = pd.DataFrame(rows)
    return df



def repeat_customers(customers, orders):
   
    merged = orders.merge(customers, on="mobile_number", how="left")
    repeat_df = (
        merged.groupby("customer_id")["order_id"]
        .nunique()
        .reset_index(name="orders_count")
    )
    repeat_df = repeat_df[repeat_df["orders_count"] > 1]
    return repeat_df


def monthly_order_trends(orders):
   
    orders["month"] = orders["order_date_time"].dt.to_period("M")
    return (
        orders.groupby("month")["order_id"]
        .nunique()
        .reset_index(name="total_orders")
        .sort_values("month")
    )


def regional_revenue(customers, orders):
    
    merged = orders.merge(customers, on="mobile_number", how="left")
    return (
        merged.groupby("region")["total_amount"]
        .sum()
        .reset_index(name="total_revenue")
        .sort_values("total_revenue", ascending=False)
    )


def top_spenders_last_30_days(customers, orders, days=30):
   
    orders["order_date_time"] = pd.to_datetime(orders["order_date_time"], utc=True, errors="coerce")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    
    recent_orders = orders[orders["order_date_time"] >= cutoff]

    merged = recent_orders.merge(customers, on="mobile_number", how="left")
    top_spenders = (
        merged.groupby(["customer_id", "customer_name"])["total_amount"]
        .sum()
        .reset_index(name="total_spent")
        .sort_values("total_spent", ascending=False)
    )
    return top_spenders.head(10)



if __name__ == "__main__":
    print(" Loading data...")
    customers_df = load_customers()
    orders_df = load_orders()

    print("\nData Loaded Successfully")
    print(f"Customers: {len(customers_df)} rows")
    print(f"Orders: {len(orders_df)} rows")

    print("\nRepeat Customers:")
    print(repeat_customers(customers_df, orders_df))

    print("\n Monthly Order Trends:")
    print(monthly_order_trends(orders_df))

    print("\n Regional Revenue:")
    print(regional_revenue(customers_df, orders_df))

    print("\n Top Spenders (Last 30 Days):")
    print(top_spenders_last_30_days(customers_df, orders_df))
