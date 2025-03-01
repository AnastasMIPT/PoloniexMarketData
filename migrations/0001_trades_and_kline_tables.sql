CREATE TABLE recent_trades (
    tid TEXT PRIMARY KEY,
    pair TEXT NOT NULL,
    price TEXT NOT NULL,
    amount TEXT NOT NULL,
    side TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);

CREATE TABLE klines (
    pair TEXT NOT NULL,
    time_frame TEXT NOT NULL,
    o DOUBLE PRECISION NOT NULL,
    h DOUBLE PRECISION NOT NULL,
    l DOUBLE PRECISION NOT NULL,
    c DOUBLE PRECISION NOT NULL,
    utc_begin BIGINT NOT NULL,
    buy_base DOUBLE PRECISION NOT NULL,
    sell_base DOUBLE PRECISION NOT NULL,
    buy_quote DOUBLE PRECISION NOT NULL,
    sell_quote DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (pair, time_frame, utc_begin)
);