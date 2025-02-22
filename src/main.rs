use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() {
    let url = "wss://ws.postman-echo.com/raw/";
    println!("Connecting to {}", url);
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected");

    let (mut ws_write, mut ws_read) = ws.split();

    ws_write.send(Message::Text("Hello WebSocket".into())).await.expect("Failed to send message");
    ws_write.send(Message::Text("Second message".into())).await.expect("Failed to send message");
    while let Some(msg) = ws_read.next().await {
        let msg = msg.unwrap();
        match msg {
            Message::Text(txt) => println!("Received: {}", txt),
            Message::Close(_) => {
                println!("Connection closed");
                break;
            }
            _ => (),
        }
    }
}
