use futures_util::{stream::{SplitSink, SplitStream}, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use log::Level;
use tokio::time;
use tokio::sync::mpsc;

async fn handle_incoming_messages(mut ws_read: SplitStream<WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>>) {
    while let Some(msg) = ws_read.next().await {
        match msg {
            Ok(msg) => {
                match msg {
                    Message::Text(txt) => log::info!("Received: {}", txt),
                    Message::Close(_) => {
                        log::info!("Connection closed");
                        break;
                    }
                    _ => (),
                }
            }
            Err(e) => {
                log::info!("Error: {}", e);
                break;
            }
        }
    }
}

async fn heartbeat(tx: mpsc::Sender<Message>) {
    loop {
        tx.send(Message::Text("{\"event\": \"ping\"}".into())).await.expect("Failed to send message");
        time::sleep(time::Duration::from_secs(3)).await;
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

    let url = "wss://ws.postman-echo.com/raw/";
    
    log::info!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    log::info!("Connected");

    let (ws_write, ws_read) = ws.split();
    let (tx, rx) = mpsc::channel(32);

    // Отправка трех сообщений "Hello"
    for _ in 0..3 {
        tx.send(Message::Text("Hello".into())).await.expect("Failed to send message");
    }

    let heartbeat_handle = tokio::spawn(heartbeat(tx.clone()));
    let write_handle = tokio::spawn(write_messages(ws_write, rx));
    let read_handle = tokio::spawn(handle_incoming_messages(ws_read));

    // Пример отправки произвольного сообщения
    send_message(tx.clone(), "Custom message".into()).await;

    let _ = tokio::try_join!(read_handle, write_handle, heartbeat_handle);
}
