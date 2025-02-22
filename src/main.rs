use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};

async fn handle_incoming_messages(mut ws_read: SplitStream<WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>>) {
    while let Some(msg) = ws_read.next().await {
        match msg {
            Ok(msg) => {
                match msg {
                    Message::Text(txt) => println!("Received: {}", txt),
                    Message::Close(_) => {
                        println!("Connection closed");
                        break;
                    }
                    _ => (),
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let url = "wss://ws.poloniex.com/ws/public";
    println!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected");

    let (mut ws_write, ws_read) = ws.split();

    ws_write.send(Message::Text("{\"event\": \"ping\"}".into())).await.expect("Failed to send message");
    ws_write.send(Message::Close(None)).await.expect("Failed to send message");


    let read_handle = tokio::spawn(handle_incoming_messages(ws_read));
    let _ = tokio::try_join!(read_handle);
}
