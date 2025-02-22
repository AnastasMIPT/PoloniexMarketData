use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use log::Level;

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

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(Level::Info).expect("Failed to initialize logger");

    let url = "wss://ws.poloniex.com/ws/public";
    
    log::info!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    log::info!("Connected");

    let (mut ws_write, ws_read) = ws.split();

    ws_write.send(Message::Text("{\"event\": \"ping\"}".into())).await.expect("Failed to send message");
    ws_write.send(Message::Close(None)).await.expect("Failed to send message");


    let read_handle = tokio::spawn(handle_incoming_messages(ws_read));
    let _ = tokio::try_join!(read_handle);
}
