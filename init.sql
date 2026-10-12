
CREATE TABLE IF NOT EXISTS raw_transactions (
    id                SERIAL PRIMARY KEY,
    account_id        TEXT,
    txn_id            TEXT,
    txn_date_time     TEXT,
    settle_date       TEXT,
    txn_type          TEXT,
    txn_amount        TEXT,
    txn_ccy           TEXT,
    billing_amount    TEXT,
    billing_currency  TEXT,
    fx_rate           TEXT,
    merchant_name     TEXT,
    mcc_code          TEXT,
    merchant_country  TEXT,
    merchant_city     TEXT,
    processing_code   TEXT,
    processing_type   TEXT,
    terminal_id       TEXT,
    auth_code         TEXT,
    matches_status    TEXT
);


CREATE TABLE IF NOT EXISTS transactions_cleaned (
    id                INT PRIMARY KEY,
    txn_id            TEXT,
    account_id        TEXT,
    txn_type          TEXT,
    txn_ccy           TEXT,
    billing_currency  TEXT,
    mcc_code          TEXT,
    processing_code   TEXT,
    processing_type   TEXT,
    matches_status    TEXT,
    txn_date_time     TIMESTAMP,
    settle_date       TIMESTAMP,
    txn_amount        NUMERIC,
    billing_amount    NUMERIC,
    fx_rate           NUMERIC,
    merchant_name     TEXT,
    merchant_city     TEXT,
    merchant_country  TEXT,
    terminal_id       TEXT,
    auth_code         TEXT,
    payment_via       TEXT,
    is_online         BOOLEAN,
    country_mismatch  BOOLEAN,
    amount_mismatch   BOOLEAN,
    has_terminal      BOOLEAN,
    auth_suspicious   BOOLEAN
);


INSERT INTO raw_transactions (
    account_id, txn_id, txn_date_time, settle_date, txn_type, txn_amount, txn_ccy,
    billing_amount, billing_currency, fx_rate, merchant_name, mcc_code,
    merchant_country, merchant_city, processing_code, processing_type,
    terminal_id, auth_code, matches_status
) VALUES (
    '854466067', '4775863', '2022-06-17 08:26:49', '18/06/2022', 'Settled', '176.711,00', 'LBP',
    '-6.21', 'USD', '3.51421e-05', 'ZAATAR W ZEIT #97', '5812',
    'LB', 'BEIRUT LB', '0', 'Purchase',
    '00000000', 'T789M8', 'Match'
);