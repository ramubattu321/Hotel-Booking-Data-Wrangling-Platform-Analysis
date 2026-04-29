-- ============================================================
-- Hotel Booking Data Wrangling & Platform Analysis
-- 16 Production SQL Queries
-- Run in: SQLite / MySQL / PostgreSQL
-- Author: Ramu Battu — MS Data Analytics, CSU Fresno
-- ============================================================


-- ── QUERY 1: PLATFORM PERFORMANCE SUMMARY ────────────────────────────────────
-- Total bookings, revenue, and avg booking value per platform
SELECT
    b.platform,
    p.commission_pct,
    COUNT(*)                                        AS total_bookings,
    SUM(CASE WHEN b.status != 'Cancelled' THEN 1 ELSE 0 END) AS confirmed_bookings,
    ROUND(SUM(b.total_revenue), 2)                  AS total_revenue,
    ROUND(AVG(CASE WHEN b.total_revenue > 0
              THEN b.total_revenue END), 2)          AS avg_booking_value,
    ROUND(SUM(b.total_revenue)
          * (1 - p.commission_pct/100.0), 2)         AS net_revenue_after_commission
FROM bookings b
JOIN platforms p ON b.platform = p.platform_name
GROUP BY b.platform, p.commission_pct
ORDER BY total_revenue DESC;


-- ── QUERY 2: PLATFORM MARKET SHARE ───────────────────────────────────────────
-- Percentage share of bookings and revenue per platform
SELECT
    platform,
    COUNT(*)                                          AS bookings,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS booking_share_pct,
    ROUND(SUM(total_revenue), 2)                      AS revenue,
    ROUND(100.0 * SUM(total_revenue)
          / SUM(SUM(total_revenue)) OVER (), 2)        AS revenue_share_pct
FROM bookings
GROUP BY platform
ORDER BY revenue DESC;


-- ── QUERY 3: CANCELLATION RATE BY PLATFORM ───────────────────────────────────
-- Which platform has the highest cancellation rate?
SELECT
    platform,
    COUNT(*)                                            AS total_bookings,
    SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancellations,
    SUM(CASE WHEN status = 'No-Show'   THEN 1 ELSE 0 END) AS no_shows,
    ROUND(100.0 * SUM(CASE WHEN status IN ('Cancelled','No-Show') THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                AS dropout_rate_pct,
    ROUND(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                        AS cancellation_rate_pct
FROM bookings
GROUP BY platform
ORDER BY cancellation_rate_pct DESC;


-- ── QUERY 4: ROOM TYPE ANALYSIS BY PLATFORM ──────────────────────────────────
-- Which room types are most popular on each platform?
SELECT
    platform,
    room_type,
    COUNT(*)                                AS bookings,
    ROUND(AVG(price_per_night), 2)          AS avg_price_per_night,
    ROUND(SUM(total_revenue), 2)            AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY platform), 2) AS pct_of_platform
FROM bookings
WHERE status != 'Cancelled'
GROUP BY platform, room_type
ORDER BY platform, total_revenue DESC;


-- ── QUERY 5: MONTHLY BOOKING TREND ───────────────────────────────────────────
-- Monthly bookings and revenue trend across all platforms
SELECT
    STRFTIME('%Y-%m', booking_date)         AS month,
    COUNT(*)                                AS total_bookings,
    SUM(CASE WHEN status='Completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END) AS cancelled,
    ROUND(SUM(total_revenue), 2)            AS total_revenue,
    ROUND(AVG(total_revenue), 2)            AS avg_booking_value
FROM bookings
GROUP BY month
ORDER BY month;


-- ── QUERY 6: MONTHLY REVENUE BY PLATFORM (PIVOT) ─────────────────────────────
-- Side-by-side monthly revenue for each platform
SELECT
    STRFTIME('%Y-%m', booking_date)          AS month,
    ROUND(SUM(CASE WHEN platform='Expedia'     THEN total_revenue ELSE 0 END), 2) AS expedia_revenue,
    ROUND(SUM(CASE WHEN platform='Booking.com' THEN total_revenue ELSE 0 END), 2) AS bookingcom_revenue,
    ROUND(SUM(CASE WHEN platform='Hotels'      THEN total_revenue ELSE 0 END), 2) AS hotels_revenue,
    ROUND(SUM(CASE WHEN platform='Cleartrip'   THEN total_revenue ELSE 0 END), 2) AS cleartrip_revenue
FROM bookings
GROUP BY month
ORDER BY month;


-- ── QUERY 7: RUNNING CUMULATIVE REVENUE (WINDOW FUNCTION) ────────────────────
-- Cumulative revenue over time per platform
SELECT
    platform,
    booking_date,
    ROUND(SUM(total_revenue), 2)                          AS daily_revenue,
    ROUND(SUM(SUM(total_revenue)) OVER (
        PARTITION BY platform ORDER BY booking_date), 2)  AS cumulative_revenue,
    COUNT(*)                                              AS daily_bookings,
    SUM(COUNT(*)) OVER (
        PARTITION BY platform ORDER BY booking_date)      AS cumulative_bookings
FROM bookings
GROUP BY platform, booking_date
ORDER BY platform, booking_date
LIMIT 20;


-- ── QUERY 8: REVENUE RANK PER DAY (RANK WINDOW FUNCTION) ─────────────────────
-- Which platform ranked #1 in revenue on each day?
WITH daily_revenue AS (
    SELECT
        booking_date,
        platform,
        ROUND(SUM(total_revenue), 2) AS daily_revenue
    FROM bookings
    GROUP BY booking_date, platform
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY booking_date ORDER BY daily_revenue DESC) AS revenue_rank
    FROM daily_revenue
)
SELECT booking_date, platform, daily_revenue, revenue_rank
FROM ranked
WHERE revenue_rank = 1
ORDER BY booking_date
LIMIT 15;


-- ── QUERY 9: LEAD TIME ANALYSIS (BOOKING DATE TO CHECK-IN) ───────────────────
-- How far in advance do guests book on each platform?
SELECT
    platform,
    ROUND(AVG(JULIANDAY(check_in_date) - JULIANDAY(booking_date)), 1) AS avg_lead_days,
    MIN(CAST(JULIANDAY(check_in_date) - JULIANDAY(booking_date) AS INTEGER)) AS min_lead_days,
    MAX(CAST(JULIANDAY(check_in_date) - JULIANDAY(booking_date) AS INTEGER)) AS max_lead_days,
    COUNT(*) AS bookings
FROM bookings
WHERE status != 'Cancelled'
GROUP BY platform
ORDER BY avg_lead_days DESC;


-- ── QUERY 10: LENGTH OF STAY ANALYSIS ────────────────────────────────────────
-- Average nights stayed per platform and room type
SELECT
    platform,
    room_type,
    ROUND(AVG(JULIANDAY(check_out_date) - JULIANDAY(check_in_date)), 1) AS avg_nights,
    MIN(CAST(JULIANDAY(check_out_date) - JULIANDAY(check_in_date) AS INTEGER)) AS min_nights,
    MAX(CAST(JULIANDAY(check_out_date) - JULIANDAY(check_in_date) AS INTEGER)) AS max_nights,
    ROUND(AVG(price_per_night), 2) AS avg_nightly_rate,
    ROUND(AVG(total_revenue), 2)   AS avg_total_revenue
FROM bookings
WHERE status != 'Cancelled'
GROUP BY platform, room_type
ORDER BY platform, avg_total_revenue DESC;


-- ── QUERY 11: TOP 10 COUNTRIES BY REVENUE ────────────────────────────────────
-- Which countries generate the most bookings and revenue?
SELECT
    country,
    COUNT(*)                        AS bookings,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(total_revenue), 2)    AS avg_booking_value,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_bookings
FROM bookings
WHERE status != 'Cancelled'
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;


-- ── QUERY 12: PAYMENT METHOD DISTRIBUTION ────────────────────────────────────
-- Which payment methods are most popular on each platform?
SELECT
    platform,
    payment_method,
    COUNT(*)                            AS bookings,
    ROUND(SUM(total_revenue), 2)        AS revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY platform), 2) AS pct_of_platform
FROM bookings
WHERE status != 'Cancelled'
GROUP BY platform, payment_method
ORDER BY platform, bookings DESC;


-- ── QUERY 13: HIGH VALUE BOOKINGS (TOP 10%) ───────────────────────────────────
-- Identify top 10% bookings by revenue using NTILE
WITH ranked AS (
    SELECT *,
           NTILE(10) OVER (ORDER BY total_revenue DESC) AS decile
    FROM bookings
    WHERE status != 'Cancelled'
)
SELECT
    platform,
    COUNT(*)                        AS high_value_bookings,
    ROUND(AVG(total_revenue), 2)    AS avg_revenue,
    ROUND(MIN(total_revenue), 2)    AS min_revenue,
    ROUND(MAX(total_revenue), 2)    AS max_revenue,
    room_type
FROM ranked
WHERE decile = 1
GROUP BY platform, room_type
ORDER BY avg_revenue DESC;


-- ── QUERY 14: MONTH-OVER-MONTH GROWTH (LAG WINDOW FUNCTION) ──────────────────
-- Revenue growth rate month over month per platform
WITH monthly AS (
    SELECT
        platform,
        STRFTIME('%Y-%m', booking_date) AS month,
        ROUND(SUM(total_revenue), 2)    AS revenue
    FROM bookings
    GROUP BY platform, month
)
SELECT
    platform, month, revenue,
    LAG(revenue) OVER (PARTITION BY platform ORDER BY month) AS prev_month,
    ROUND(revenue - LAG(revenue) OVER (PARTITION BY platform ORDER BY month), 2) AS change,
    ROUND(100.0 * (revenue - LAG(revenue) OVER (PARTITION BY platform ORDER BY month))
          / NULLIF(LAG(revenue) OVER (PARTITION BY platform ORDER BY month), 0), 1) AS mom_growth_pct
FROM monthly
ORDER BY platform, month;


-- ── QUERY 15: CORPORATE VS LEISURE BOOKINGS ──────────────────────────────────
-- Compare corporate (company provided) vs leisure (individual) bookings
SELECT
    platform,
    CASE WHEN company IS NOT NULL THEN 'Corporate' ELSE 'Leisure' END AS booking_type,
    COUNT(*)                        AS bookings,
    ROUND(AVG(total_revenue), 2)    AS avg_revenue,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(num_guests), 1)       AS avg_guests,
    ROUND(100.0 * SUM(CASE WHEN status='Cancelled' THEN 1 ELSE 0 END)
          / COUNT(*), 2)            AS cancellation_rate_pct
FROM bookings
GROUP BY platform, booking_type
ORDER BY platform, booking_type;


-- ── QUERY 16: PLATFORM EFFICIENCY SCORECARD ──────────────────────────────────
-- Overall ranking of platforms across all key metrics using CTE
WITH stats AS (
    SELECT
        b.platform,
        p.commission_pct,
        COUNT(*)                                                  AS total_bookings,
        ROUND(SUM(b.total_revenue), 2)                            AS gross_revenue,
        ROUND(SUM(b.total_revenue) * (1-p.commission_pct/100.0), 2) AS net_revenue,
        ROUND(AVG(CASE WHEN b.total_revenue>0 THEN b.total_revenue END), 2) AS avg_booking_value,
        ROUND(100.0 * SUM(CASE WHEN b.status='Cancelled' THEN 1 ELSE 0 END)
              / COUNT(*), 2)                                      AS cancellation_rate_pct,
        ROUND(AVG(JULIANDAY(b.check_in_date) - JULIANDAY(b.booking_date)), 1) AS avg_lead_days
    FROM bookings b
    JOIN platforms p ON b.platform = p.platform_name
    GROUP BY b.platform, p.commission_pct
)
SELECT
    platform,
    total_bookings,
    gross_revenue,
    net_revenue,
    avg_booking_value,
    cancellation_rate_pct,
    avg_lead_days,
    RANK() OVER (ORDER BY net_revenue DESC)           AS revenue_rank,
    RANK() OVER (ORDER BY cancellation_rate_pct ASC)  AS reliability_rank,
    RANK() OVER (ORDER BY avg_booking_value DESC)     AS value_rank
FROM stats
ORDER BY revenue_rank;
