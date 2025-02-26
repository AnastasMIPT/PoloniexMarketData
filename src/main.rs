use futures_util::{stream::{SplitSink, SplitStream}, SinkExt, StreamExt};
use futures::future;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use log::Level;
use tokio::time;
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize, Deserializer};
use serde::de::Error as DeError;
use chrono::NaiveDate;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use reqwest::Client;
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
struct RecentTrade {
    #[serde(rename = "id")]
    tid: String,
    #[serde(rename = "symbol")]
    pair: String,
    price: String,
    amount: String,
    #[serde(rename = "takerSide")]
    side: String,
    #[serde(rename = "createTime")]
    timestamp: i64,
}

#[derive(Deserialize, Debug)]
struct TradeMessage {
    channel: String,
    data: Vec<RecentTrade>,
}

#[derive(Debug)]
struct VBS {
    buy_base: f64,
    sell_base: f64,
    buy_quote: f64,
    sell_quote: f64,
}

#[derive(Debug)]
struct Kline {
    pair: String,
    time_frame: String,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    utc_begin: i64,
    volume_bs: VBS,
}

fn parse_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(DeError::custom)
}

#[derive(Deserialize, Debug)]
struct ApiKline {
    #[serde(rename = "low", deserialize_with = "parse_f64")]
    l: f64,
    #[serde(rename = "high", deserialize_with = "parse_f64")]
    h: f64,
    #[serde(rename = "open", deserialize_with = "parse_f64")]
    o: f64,
    #[serde(rename = "close", deserialize_with = "parse_f64")]
    c: f64,
    #[serde(deserialize_with = "parse_f64")]
    amount: f64,
    #[serde(deserialize_with = "parse_f64")]
    quantity: f64,
    #[serde(rename = "buyTakerAmount", deserialize_with = "parse_f64")]
    buy_taker_amount: f64,
    #[serde(rename = "buyTakerQuantity", deserialize_with = "parse_f64")]
    buy_taker_quantity: f64,
    #[serde(rename = "tradeCount")]
    trade_count: u64,
    ts: i64,
    #[serde(deserialize_with = "parse_f64")]
    #[serde(rename = "weightedAverage")]
    weighted_average: f64,
    interval: String,
    #[serde(rename = "startTime")]
    utc_begin: i64,
    #[serde(rename = "closeTime")]
    close_time: i64,
}

async fn fetch_klines_request(client: &Client, pair: &str, interval: &str, start_time: i64, end_time: i64, limit: usize) -> Result<Vec<ApiKline>, Box<dyn Error>> {
    let url = format!(
        "https://api.poloniex.com/markets/{}/candles?interval={}&startTime={}&endTime={}&limit={}",
        pair, interval, start_time, end_time, limit
    );
    log::info!("Fetching klines from {}", url);
    let response = client.get(&url).send().await?;
    log::info!("Response status: {}", response.status());
    let api_klines: Vec<ApiKline> = response.json().await?;
    log::info!("Fetched {} klines for {pair}", api_klines.len());
    Ok(api_klines)
}

async fn fetch_klines_to_db(pair: &str, interval: &str, start_time: i64, limit: usize, pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let mut current_start_time = start_time;
    let (interval_str, interval_ms) = match interval {
        "1m" => ("MINUTE_1", 60_000),
        "15m" => ("MINUTE_15", 900_000),
        "1h" => ("HOUR_1", 3_600_000),
        "1d" => ("DAY_1", 86_400_000),
        _ => return Err("Invalid interval".into()),
    };
    let mut current_end_time = current_start_time + (interval_ms as i64) * limit as i64;

    loop {
        let api_klines = fetch_klines_request(&client, pair, interval_str, current_start_time, current_end_time, limit).await?;
        if api_klines.is_empty() {
            break;
        }

        for api_kline in api_klines.iter() {
            let kline = Kline {
                pair: pair.to_string(),
                time_frame: interval.to_string(),
                o: api_kline.o,
                h: api_kline.h,
                l: api_kline.l,
                c: api_kline.c,
                utc_begin: api_kline.utc_begin,
                volume_bs: VBS {
                    buy_base: api_kline.buy_taker_quantity,
                    sell_base: api_kline.quantity - api_kline.buy_taker_quantity,
                    buy_quote: api_kline.buy_taker_amount,
                    sell_quote: api_kline.amount - api_kline.buy_taker_amount,
                },
            };
            db_insert_kline(&kline, pool).await;
        }

        if api_klines.len() < limit {
            break;
        }

        current_start_time = current_end_time;
        current_end_time = current_end_time + (interval_ms as i64) * limit as i64;
    }

    Ok(())
}

async fn db_insert_trade(trade: &RecentTrade, pool: &sqlx::PgPool) {
    let query = "INSERT INTO recent_trades (tid, pair, price, amount, side, timestamp) VALUES ($1, $2, $3, $4, $5, $6)";
    match sqlx::query(query)
        .bind(&trade.tid)
        .bind(&trade.pair)
        .bind(&trade.price)
        .bind(&trade.amount)
        .bind(&trade.side)
        .bind(&trade.timestamp)
        .execute(pool)
        .await {
            Ok(_) => {},
            Err(e) => log::error!("Failed to insert trade with tid = {}: {}", &trade.tid, e),
        }
}

async fn db_get_kline(pair: &str, time_frame: &str, timestamp: i64, pool: &sqlx::PgPool) -> Option<Kline> {
    let row = sqlx::query(
        "SELECT * FROM klines WHERE pair = $1 AND time_frame = $2 AND utc_begin = $3"
    )
    .bind(pair)
    .bind(time_frame)
    .bind(timestamp)
    .fetch_one(pool)
    .await;

    match row {
        Ok(row) => Some(Kline {
            pair: row.get("pair"),
            time_frame: row.get("time_frame"),
            o: row.get("o"),
            h: row.get("h"),
            l: row.get("l"),
            c: row.get("c"),
            utc_begin: row.get("utc_begin"),
            volume_bs: VBS {
                buy_base: row.get("buy_base"),
                sell_base: row.get("sell_base"),
                buy_quote: row.get("buy_quote"),
                sell_quote: row.get("sell_quote"),
            },
        }),
        Err(sqlx::Error::RowNotFound) => None,
        Err(e) => {
            log::error!("Failed to fetch kline: {}", e);
            None
        }
    }
}

async fn db_update_kline(kline: &Kline, pool: &sqlx::PgPool) {
    match sqlx::query(
        "UPDATE klines SET o = $1, h = $2, l = $3, c = $4, buy_base = $5, sell_base = $6, buy_quote = $7, sell_quote = $8 WHERE pair = $9 AND time_frame = $10 AND utc_begin = $11"
    )
    .bind(kline.o)
    .bind(kline.h)
    .bind(kline.l)
    .bind(kline.c)
    .bind(kline.volume_bs.buy_base)
    .bind(kline.volume_bs.sell_base)
    .bind(kline.volume_bs.buy_quote)
    .bind(kline.volume_bs.sell_quote)
    .bind(&kline.pair)
    .bind(&kline.time_frame)
    .bind(kline.utc_begin)
    .execute(pool)
    .await {
        Ok(_) => {},
        Err(e) => log::error!("Failed to update kline with pair{} time_frame {}, utc_begin {}: {}",
                                    &kline.pair, &kline.time_frame, kline.utc_begin, e),
    }
}

async fn db_insert_kline(kline: &Kline, pool: &sqlx::PgPool) {
    match sqlx::query(
        "INSERT INTO klines (pair, time_frame, o, h, l, c, utc_begin, buy_base, sell_base, buy_quote, sell_quote) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
    )
    .bind(&kline.pair)
    .bind(&kline.time_frame)
    .bind(kline.o)
    .bind(kline.h)
    .bind(kline.l)
    .bind(kline.c)
    .bind(kline.utc_begin)
    .bind(kline.volume_bs.buy_base)
    .bind(kline.volume_bs.sell_base)
    .bind(kline.volume_bs.buy_quote)
    .bind(kline.volume_bs.sell_quote)
    .execute(pool)
    .await {
        Ok(_) => {},
        Err(e) => log::error!("Failed to insert kline with pair {} time_frame {}, utc_begin {}: {}",
                                    &kline.pair, &kline.time_frame, kline.utc_begin, e),
    }
}

async fn update_kline(trade: &RecentTrade, time_frame: &str, interval: i64, pool: &sqlx::PgPool) {
    let timestamp = (trade.timestamp / interval) * interval; // округление вниз до ближайшего интервала
    let price: f64 = trade.price.parse().unwrap();
    let amount: f64 = trade.amount.parse().unwrap();
    let side = &trade.side;

    let kline = db_get_kline(&trade.pair, time_frame, timestamp, pool).await;

    if let Some(mut kline) = kline {
        kline.c = price;
        kline.h = kline.h.max(price);
        kline.l = kline.l.min(price);
        if side == "buy" {
            kline.volume_bs.buy_base += amount;
            kline.volume_bs.buy_quote += amount * price;
        } else {
            kline.volume_bs.sell_base += amount;
            kline.volume_bs.sell_quote += amount * price;
        }
        db_update_kline(&kline, pool).await;
    } else {
        let volume_bs = if side == "buy" {
            VBS {
                buy_base: amount,
                sell_base: 0.0,
                buy_quote: amount * price,
                sell_quote: 0.0,
            }
        } else {
            VBS {
                buy_base: 0.0,
                sell_base: amount,
                buy_quote: 0.0,
                sell_quote: amount * price,
            }
        };
        let new_kline = Kline {
            pair: trade.pair.clone(),
            time_frame: time_frame.to_string(),
            o: price,
            h: price,
            l: price,
            c: price,
            utc_begin: timestamp,
            volume_bs,
        };
        db_insert_kline(&new_kline, pool).await;
    }
}

async fn handle_incoming_messages(mut ws_read: SplitStream<WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>>, pool: sqlx::PgPool) {
    while let Some(msg) = ws_read.next().await {
        match msg {
            Ok(Message::Text(msg)) => {
                if let Ok(trade_message) = serde_json::from_str::<TradeMessage>(&msg) {
                    for trade in trade_message.data {
                        log::info!("Parsed trade: {:?}", trade);
                        db_insert_trade(&trade, &pool).await;
                        update_kline(&trade, "1m", 60000, &pool).await;
                        update_kline(&trade, "15m", 900000, &pool).await;
                        update_kline(&trade, "1h", 3600000, &pool).await;
                        update_kline(&trade, "1d", 86400000, &pool).await;
                    }
                } else {
                    log::info!("Received: {msg}");
                }
            }
            Err(e) => {
                log::info!("Error: {}", e);
            }
            _ => {}
        }
    }
}

async fn heartbeat(tx: mpsc::Sender<Message>) {
    loop {
        tx.send(Message::Text("{\"event\": \"ping\"}".into())).await.expect("Failed to send message");
        time::sleep(time::Duration::from_secs(29)).await;
    }
}

async fn write_messages(mut ws_write: SplitSink<WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>, Message>, mut rx: mpsc::Receiver<Message>) {
    while let Some(msg) = rx.recv().await {
        ws_write.send(msg).await.expect("Failed to send message");    
    }
}

async fn send_message(tx: mpsc::Sender<Message>, message: String) {
    tx.send(Message::Text(message.into())).await.expect("Failed to send message");
}

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(Level::Info).expect("Failed to initialize logger");

    let database_url = "postgres://postgres:1234@localhost:5432/poloniex";
    let url = "wss://ws.poloniex.com/ws/public";
    let pairs = ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"];
    let intervals = ["1d", "1h", "15m", "1m"];
    // Преобразование даты 2024-12-01 в Unix timestamp, чтобы  в дальнейшем
    // получить историю котировок с этой даты
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).expect("Invalid date").and_hms_opt(0, 0, 0).expect("Invalid time");
    let timestamp = date.and_utc().timestamp_millis();

 
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("Failed to create pool");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    
    log::info!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    log::info!("Connected");

    let (ws_write, ws_read) = ws.split();
    let (tx, rx) = mpsc::channel(32);

    // Отправляем запросы ping, чтобы нас не отключили
    let heartbeat_handle = tokio::spawn(heartbeat(tx.clone()));
    // Отправляем все сообщения через одну задачу, агрегирующую все сообщения из канала
    let write_handle = tokio::spawn(write_messages(ws_write, rx));
    // Обрабатываем входящие сообщения
    let read_handle = tokio::spawn(handle_incoming_messages(ws_read, pool.clone()));

    // Подписка на получение данных о последних сделках
    let subscribe_message = format!(
        "{{\"event\": \"subscribe\", \"channel\": [\"trades\"], \"symbols\": [{}]}}",
        pairs.iter().map(|&s| format!("\"{}\"", s)).collect::<Vec<String>>().join(", ")
    );
    send_message(tx.clone(), subscribe_message).await;

    // Собираем по REST API историю котировок для нужных пар и таймфреймов
    let mut handles = vec![];

    for &interval in &intervals {
        for &pair in &pairs {
            let pool = pool.clone();
            let handle = tokio::spawn(async move {
                fetch_klines_to_db(pair, interval, timestamp, 500, &pool).await.expect("Failed to fetch klines");
            });
            handles.push(handle);
        }
    }

    let _ = tokio::try_join!(read_handle, write_handle, heartbeat_handle, futures::future::try_join_all(handles));
}
