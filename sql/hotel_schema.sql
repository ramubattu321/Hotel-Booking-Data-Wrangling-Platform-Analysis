-- ============================================================
-- Hotel Booking Data Wrangling & Platform Analysis — Schema
-- Compatible with: SQLite, MySQL, PostgreSQL
-- Author: Ramu Battu — MS Data Analytics, CSU Fresno
-- ============================================================

DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS platforms;

-- ── TABLE 1: BOOKINGS ─────────────────────────────────────────────────────────
-- One row per booking — extracted via marker row detection & backfilling
CREATE TABLE bookings (
    booking_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT    NOT NULL,       -- Expedia / Booking.com / Hotels / Cleartrip
    booking_date    DATE    NOT NULL,       -- Date booking was made
    check_in_date   DATE    NOT NULL,       -- Guest check-in date
    check_out_date  DATE    NOT NULL,       -- Guest check-out date
    room_number     INTEGER NOT NULL,       -- Room assigned
    company         TEXT,                   -- Corporate booking company (nullable)
    guest_name      TEXT    NOT NULL,       -- Guest name
    room_type       TEXT    NOT NULL,       -- Standard / Deluxe / Suite / Executive
    num_guests      INTEGER NOT NULL,       -- Number of guests
    price_per_night REAL    NOT NULL,       -- Nightly rate in USD
    total_revenue   REAL    NOT NULL,       -- Total booking value (0 if cancelled)
    status          TEXT    NOT NULL,       -- Confirmed / Completed / Cancelled / No-Show
    payment_method  TEXT    NOT NULL,       -- Credit Card / PayPal / UPI / etc.
    country         TEXT    NOT NULL        -- Guest country of origin
);

-- ── TABLE 2: PLATFORMS ────────────────────────────────────────────────────────
-- Reference table for booking platform metadata
CREATE TABLE platforms (
    platform_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_name   TEXT    NOT NULL,       -- Platform name
    commission_pct  REAL    NOT NULL,       -- Commission charged to hotel (%)
    region          TEXT    NOT NULL,       -- Market focus
    launch_year     INTEGER NOT NULL        -- Year platform launched
);

-- ── INDEXES ───────────────────────────────────────────────────────────────────
CREATE INDEX idx_platform      ON bookings(platform);
CREATE INDEX idx_status        ON bookings(status);
CREATE INDEX idx_room_type     ON bookings(room_type);
CREATE INDEX idx_booking_date  ON bookings(booking_date);
CREATE INDEX idx_check_in      ON bookings(check_in_date);
CREATE INDEX idx_country       ON bookings(country);
CREATE INDEX idx_payment       ON bookings(payment_method);
