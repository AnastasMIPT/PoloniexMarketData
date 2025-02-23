use futures_util::{stream::{SplitSink, SplitStream}, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use log::Level;
use tokio::time;
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};
use chrono::{NaiveDate, Utc};
use sqlx::{postgres::PgPoolOptions, query};

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

struct VBS {
    buy_base: f64,
    sell_base: f64,
    buy_quote: f64,
    sell_quote: f64,
}

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

async fn insert_trade_to_db(trade: &RecentTrade, pool: &sqlx::PgPool) {
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

async fn handle_incoming_messages(mut ws_read: SplitStream<WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>>, pool: sqlx::PgPool) {
    while let Some(msg) = ws_read.next().await {
        match msg {
            Ok(Message::Text(msg)) => {
                if let Ok(trade_message) = serde_json::from_str::<TradeMessage>(&msg) {
                    for trade in trade_message.data {
                        log::info!("Parsed trade: {:?}", trade);
                        insert_trade_to_db(&trade, &pool).await;
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
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("Failed to create pool");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let _url = "wss://ws.poloniex.com/ws/public";
    let url = "wss://ws.postman-echo.com/raw/";
    
    log::info!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    log::info!("Connected");

    let (ws_write, ws_read) = ws.split();
    let (tx, rx) = mpsc::channel(32);

    let heartbeat_handle = tokio::spawn(heartbeat(tx.clone()));
    let write_handle = tokio::spawn(write_messages(ws_write, rx));
    let read_handle = tokio::spawn(handle_incoming_messages(ws_read, pool.clone()));

    // Пример отправки произвольного сообщения
    send_message(tx.clone(), "{\"event\": \"subscribe\", \"channel\": [\"trades\"], \"symbols\": [\"BTC_USDT\"]}".into()).await;

    let buy_message = "{\"channel\":\"trades\",\"data\":[{\"symbol\":\"BTC_USDT\",\"amount\":\"1378.48500036\",\"quantity\":\"0.014274\",\"takerSide\":\"buy\",\"createTime\":1740250153282,\"price\":\"96573.14\",\"id\":\"123346679\",\"ts\":1740250153291}]}";
    let sel_message = "{\"channel\":\"trades\",\"data\":[{\"symbol\":\"BTC_USDT\",\"amount\":\"354.13425443\",\"quantity\":\"0.003667\",\"takerSide\":\"sell\",\"createTime\":1740250152125,\"price\":\"96573.29\",\"id\":\"123346678\",\"ts\":1740250152142}]}";

    send_message(tx.clone(), buy_message.into()).await;
    send_message(tx.clone(), sel_message.into()).await;

    // Преобразование даты 2024-12-01 в Unix timestamp
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).expect("Invalid date").and_hms(0, 0, 0);
    let timestamp = date.and_utc().timestamp_millis();
    log::info!("Timestamp for 2024-12-01: {}", timestamp);

    let _ = tokio::try_join!(read_handle, write_handle, heartbeat_handle);
}
