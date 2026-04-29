"""
Hotel Booking Data Wrangling & Platform Analysis
=================================================
Creates SQLite database and runs all 16 SQL queries.

Run: python sql/setup_and_run.py
"""

import sqlite3
import pandas as pd
import random
import os
from datetime import date, timedelta

DB_PATH = "sql/hotel_bookings.db"
random.seed(42)


def create_and_load(conn):
    """Create tables and load sample data."""
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT NOT NULL,
        booking_date DATE NOT NULL, check_in_date DATE NOT NULL, check_out_date DATE NOT NULL,
        room_number INTEGER NOT NULL, company TEXT, guest_name TEXT NOT NULL,
        room_type TEXT NOT NULL, num_guests INTEGER NOT NULL, price_per_night REAL NOT NULL,
        total_revenue REAL NOT NULL, status TEXT NOT NULL,
        payment_method TEXT NOT NULL, country TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS platforms (
        platform_id INTEGER PRIMARY KEY AUTOINCREMENT, platform_name TEXT NOT NULL,
        commission_pct REAL NOT NULL, region TEXT NOT NULL, launch_year INTEGER NOT NULL
    );
    """)

    cur.executemany(
        "INSERT INTO platforms (platform_name,commission_pct,region,launch_year) VALUES (?,?,?,?)",
        [("Expedia",18.5,"Global",2001),("Booking.com",15.0,"Global",1996),
         ("Hotels",12.0,"USA-focused",2001),("Cleartrip",8.5,"Asia-focused",2006)]
    )

    platforms   = ["Expedia","Booking.com","Hotels","Cleartrip"]
    room_types  = ["Standard","Deluxe","Suite","Executive"]
    payments    = ["Credit Card","Debit Card","UPI","Net Banking","PayPal"]
    statuses    = ["Confirmed","Cancelled","Completed","No-Show"]
    status_wts  = [0.10,0.12,0.72,0.06]
    countries   = ["USA","UK","India","Germany","France","Canada","Australia","UAE","Japan","Brazil"]
    companies   = ["Acme Corp","Beta Ltd","Gamma Inc","Delta Co","Epsilon LLC",None,None,None]
    price_map   = {"Standard":(80,150),"Deluxe":(150,280),"Suite":(280,500),"Executive":(200,400)}
    plat_wts    = [0.35,0.30,0.22,0.13]
    start       = date(2023,1,1)

    rows = []
    for i in range(1200):
        plat   = random.choices(platforms, weights=plat_wts)[0]
        rtype  = random.choices(room_types, weights=[0.40,0.30,0.15,0.15])[0]
        bdate  = start + timedelta(days=random.randint(0,364))
        nights = random.randint(1,7)
        cin    = bdate + timedelta(days=random.randint(1,30))
        cout   = cin   + timedelta(days=nights)
        pnight = round(random.uniform(*price_map[rtype]),2)
        status = random.choices(statuses, weights=status_wts)[0]
        total  = round(pnight*nights,2) if status!="Cancelled" else 0.0
        rows.append((plat,bdate.isoformat(),cin.isoformat(),cout.isoformat(),
                     random.randint(100,500),random.choice(companies),
                     f"Guest_{i+1:04d}",rtype,random.randint(1,4),
                     pnight,total,status,random.choice(payments),random.choice(countries)))

    cur.executemany("""INSERT INTO bookings
        (platform,booking_date,check_in_date,check_out_date,room_number,company,
         guest_name,room_type,num_guests,price_per_night,total_revenue,status,
         payment_method,country) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()


def run_query(conn, sql, title):
    """Run a query and display results."""
    print(f"\n{'='*65}\n  {title}\n{'='*65}")
    df = pd.read_sql_query(sql, conn)
    print(df.to_string(index=False))
    return df


if __name__ == "__main__":
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    create_and_load(conn)

    for t in ["bookings","platforms"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"✅ {t}: {n:,} rows")

    # Run all 16 queries
    run_query(conn, """
        SELECT b.platform, p.commission_pct, COUNT(*) AS total_bookings,
               ROUND(SUM(b.total_revenue),2) AS gross_revenue,
               ROUND(SUM(b.total_revenue)*(1-p.commission_pct/100.0),2) AS net_revenue,
               ROUND(AVG(CASE WHEN b.total_revenue>0 THEN b.total_revenue END),2) AS avg_booking_value
        FROM bookings b JOIN platforms p ON b.platform=p.platform_name
        GROUP BY b.platform ORDER BY gross_revenue DESC
    """, "QUERY 1 — Platform Performance Summary")

    run_query(conn, """
        SELECT platform,COUNT(*) AS bookings,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS booking_share_pct,
               ROUND(SUM(total_revenue),2) AS revenue,
               ROUND(100.0*SUM(total_revenue)/SUM(SUM(total_revenue)) OVER(),2) AS revenue_share_pct
        FROM bookings GROUP BY platform ORDER BY revenue DESC
    """, "QUERY 2 — Platform Market Share")

    run_query(conn, """
        SELECT platform, COUNT(*) AS total,
               SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END) AS cancellations,
               ROUND(100.0*SUM(CASE WHEN status IN ('Cancelled','No-Show') THEN 1 ELSE 0 END)/COUNT(*),2) AS dropout_rate_pct
        FROM bookings GROUP BY platform ORDER BY dropout_rate_pct DESC
    """, "QUERY 3 — Cancellation Rate by Platform")

    run_query(conn, """
        SELECT platform, room_type, COUNT(*) AS bookings,
               ROUND(AVG(price_per_night),2) AS avg_price,
               ROUND(SUM(total_revenue),2) AS total_revenue
        FROM bookings WHERE status!='Cancelled'
        GROUP BY platform, room_type ORDER BY platform, total_revenue DESC
    """, "QUERY 4 — Room Type Analysis by Platform")

    run_query(conn, """
        SELECT STRFTIME('%Y-%m',booking_date) AS month, COUNT(*) AS bookings,
               ROUND(SUM(total_revenue),2) AS revenue
        FROM bookings GROUP BY month ORDER BY month
    """, "QUERY 5 — Monthly Booking Trend")

    run_query(conn, """
        SELECT STRFTIME('%Y-%m',booking_date) AS month,
               ROUND(SUM(CASE WHEN platform='Expedia' THEN total_revenue ELSE 0 END),2) AS expedia,
               ROUND(SUM(CASE WHEN platform='Booking.com' THEN total_revenue ELSE 0 END),2) AS bookingcom,
               ROUND(SUM(CASE WHEN platform='Hotels' THEN total_revenue ELSE 0 END),2) AS hotels,
               ROUND(SUM(CASE WHEN platform='Cleartrip' THEN total_revenue ELSE 0 END),2) AS cleartrip
        FROM bookings GROUP BY month ORDER BY month
    """, "QUERY 6 — Monthly Revenue by Platform (Pivot)")

    run_query(conn, """
        SELECT platform, booking_date,
               ROUND(SUM(SUM(total_revenue)) OVER (PARTITION BY platform ORDER BY booking_date),2) AS cum_revenue,
               SUM(COUNT(*)) OVER (PARTITION BY platform ORDER BY booking_date) AS cum_bookings
        FROM bookings GROUP BY platform, booking_date ORDER BY platform, booking_date LIMIT 16
    """, "QUERY 7 — Cumulative Revenue (SUM OVER Window)")

    run_query(conn, """
        WITH dr AS (SELECT booking_date, platform, ROUND(SUM(total_revenue),2) AS rev FROM bookings GROUP BY booking_date,platform),
        rk AS (SELECT *, RANK() OVER (PARTITION BY booking_date ORDER BY rev DESC) AS rnk FROM dr)
        SELECT booking_date, platform, rev FROM rk WHERE rnk=1 ORDER BY booking_date LIMIT 12
    """, "QUERY 8 — Top Platform per Day (RANK Window)")

    run_query(conn, """
        SELECT platform,
               ROUND(AVG(JULIANDAY(check_in_date)-JULIANDAY(booking_date)),1) AS avg_lead_days,
               MIN(CAST(JULIANDAY(check_in_date)-JULIANDAY(booking_date) AS INTEGER)) AS min_lead,
               MAX(CAST(JULIANDAY(check_in_date)-JULIANDAY(booking_date) AS INTEGER)) AS max_lead
        FROM bookings WHERE status!='Cancelled' GROUP BY platform ORDER BY avg_lead_days DESC
    """, "QUERY 9 — Lead Time Analysis")

    run_query(conn, """
        SELECT platform, room_type,
               ROUND(AVG(JULIANDAY(check_out_date)-JULIANDAY(check_in_date)),1) AS avg_nights,
               ROUND(AVG(price_per_night),2) AS avg_rate, ROUND(AVG(total_revenue),2) AS avg_total
        FROM bookings WHERE status!='Cancelled' GROUP BY platform, room_type ORDER BY platform, avg_total DESC
    """, "QUERY 10 — Length of Stay Analysis")

    run_query(conn, """
        SELECT country, COUNT(*) AS bookings, ROUND(SUM(total_revenue),2) AS revenue,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS pct
        FROM bookings WHERE status!='Cancelled' GROUP BY country ORDER BY revenue DESC LIMIT 10
    """, "QUERY 11 — Top 10 Countries by Revenue")

    run_query(conn, """
        SELECT platform, payment_method, COUNT(*) AS bookings,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY platform),2) AS pct_of_platform
        FROM bookings WHERE status!='Cancelled' GROUP BY platform, payment_method ORDER BY platform, bookings DESC
    """, "QUERY 12 — Payment Method by Platform")

    run_query(conn, """
        WITH rk AS (SELECT *, NTILE(10) OVER (ORDER BY total_revenue DESC) AS decile FROM bookings WHERE status!='Cancelled')
        SELECT platform, room_type, COUNT(*) AS high_value_bookings,
               ROUND(AVG(total_revenue),2) AS avg_revenue, ROUND(MAX(total_revenue),2) AS max_revenue
        FROM rk WHERE decile=1 GROUP BY platform, room_type ORDER BY avg_revenue DESC
    """, "QUERY 13 — High Value Bookings Top 10% (NTILE)")

    run_query(conn, """
        WITH m AS (SELECT platform, STRFTIME('%Y-%m',booking_date) AS month,
                          ROUND(SUM(total_revenue),2) AS revenue FROM bookings GROUP BY platform,month)
        SELECT platform, month, revenue,
               LAG(revenue) OVER (PARTITION BY platform ORDER BY month) AS prev_month,
               ROUND(100.0*(revenue-LAG(revenue) OVER (PARTITION BY platform ORDER BY month))
                     /NULLIF(LAG(revenue) OVER (PARTITION BY platform ORDER BY month),0),1) AS mom_growth_pct
        FROM m ORDER BY platform, month LIMIT 16
    """, "QUERY 14 — Month-over-Month Growth (LAG Window)")

    run_query(conn, """
        SELECT platform, CASE WHEN company IS NOT NULL THEN 'Corporate' ELSE 'Leisure' END AS type,
               COUNT(*) AS bookings, ROUND(AVG(total_revenue),2) AS avg_revenue,
               ROUND(100.0*SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END)/COUNT(*),2) AS cancel_rate_pct
        FROM bookings GROUP BY platform, type ORDER BY platform, type
    """, "QUERY 15 — Corporate vs Leisure Bookings")

    run_query(conn, """
        WITH s AS (
            SELECT b.platform, p.commission_pct, COUNT(*) AS bookings,
                   ROUND(SUM(b.total_revenue),2) AS gross_revenue,
                   ROUND(SUM(b.total_revenue)*(1-p.commission_pct/100.0),2) AS net_revenue,
                   ROUND(100.0*SUM(CASE WHEN b.status='Cancelled' THEN 1 ELSE 0 END)/COUNT(*),2) AS cancel_pct
            FROM bookings b JOIN platforms p ON b.platform=p.platform_name GROUP BY b.platform,p.commission_pct
        )
        SELECT platform, bookings, gross_revenue, net_revenue, cancel_pct,
               RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank,
               RANK() OVER (ORDER BY cancel_pct ASC)   AS reliability_rank
        FROM s ORDER BY revenue_rank
    """, "QUERY 16 — Platform Efficiency Scorecard")

    conn.close()
    print(f"\n{'='*65}\n✅ All 16 queries complete! DB: {DB_PATH}\n{'='*65}")
