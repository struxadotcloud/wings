use crate::{
    routes::GetState,
    server::{
        permissions::Permission,
        websocket::{WebsocketEvent, WebsocketJwtPayload, WebsocketMessage},
    },
};
use axum::{
    body::Bytes,
    extract::{WebSocketUpgrade, ws::Message},
    response::Response,
};
use futures::{
    SinkExt, StreamExt,
    stream::SplitSink,
};
use std::{pin::Pin, sync::Arc};
use tokio::sync::Notify;

type Sender = tokio::sync::Mutex<SplitSink<axum::extract::ws::WebSocket, Message>>;

async fn send_message(sender: &Sender, message: WebsocketMessage) -> anyhow::Result<()> {
    let json = serde_json::to_string(&message)?;
    sender.lock().await.send(Message::Text(json.into())).await?;
    Ok(())
}

pub async fn handle_ws(ws: WebSocketUpgrade, state: GetState) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (sender, mut receiver) = socket.split();
        let sender = Arc::new(Sender::new(sender));
        let authenticated = Arc::new(Notify::new());

        type ReturnType = dyn Future<Output = Result<(), anyhow::Error>> + Send;
        let futures: [Pin<Box<ReturnType>>; 3] = [
            // Authentication listener
            Box::pin({
                let state = Arc::clone(&state);
                let sender = Arc::clone(&sender);
                let authenticated = Arc::clone(&authenticated);

                async move {
                    loop {
                        let message = receiver.next().await;

                        match message {
                            Some(Ok(Message::Text(text))) => {
                                let message: WebsocketMessage =
                                    match serde_json::from_str(&text) {
                                        Ok(message) => message,
                                        Err(_) => continue,
                                    };

                                if !matches!(message.event, WebsocketEvent::Authentication) {
                                    continue;
                                }

                                let token = message.args.first().map_or("", |v| v.as_str());

                                match state
                                    .config
                                    .jwt
                                    .verify::<WebsocketJwtPayload>(token)
                                {
                                    Ok(jwt) => {
                                        if let Err(err) =
                                            jwt.base.validate(&state.config.jwt, Some("websocket"))
                                        {
                                            send_message(
                                                &sender,
                                                WebsocketMessage::builder(WebsocketEvent::JwtError)
                                                    .arg(format!("invalid token: {err}"))
                                                    .build(),
                                            )
                                            .await?;
                                            continue;
                                        }

                                        if !jwt
                                            .permissions
                                            .has_permission(Permission::WebsocketConnect)
                                        {
                                            send_message(
                                                &sender,
                                                WebsocketMessage::builder(WebsocketEvent::JwtError)
                                                    .arg("missing permission to connect to websocket")
                                                    .build(),
                                            )
                                            .await?;
                                            continue;
                                        }

                                        authenticated.notify_one();

                                        send_message(
                                            &sender,
                                            WebsocketMessage::builder(
                                                WebsocketEvent::AuthenticationSuccess,
                                            )
                                            .build(),
                                        )
                                        .await?;
                                    }
                                    Err(err) => {
                                        send_message(
                                            &sender,
                                            WebsocketMessage::builder(WebsocketEvent::JwtError)
                                                .arg(format!("failed to verify jwt: {err}"))
                                                .build(),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(Ok(_)) => continue,
                            Some(Err(_)) | None => {
                                return Err(anyhow::anyhow!("socket closed"));
                            }
                        }
                    }
                }
            }),
            // Stats Listener
            Box::pin({
                let state = Arc::clone(&state);
                let sender = Arc::clone(&sender);
                let authenticated = Arc::clone(&authenticated);

                async move {
                    authenticated.notified().await;

                    loop {
                        let stats = state.stats_manager.get_stats();
                        let stats_json = match serde_json::to_string(&*stats) {
                            Ok(json) => json,
                            Err(err) => {
                                tracing::error!("failed to serialize stats to JSON: {}", err);
                                continue;
                            }
                        };

                        sender
                            .lock()
                            .await
                            .send(Message::Text(stats_json.into()))
                            .await?;

                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }),
            // Pinger
            Box::pin(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

                    sender
                        .lock()
                        .await
                        .send(Message::Ping(Bytes::from_static(&[1, 2, 3])))
                        .await?;
                }
            }),
        ];

        if let Err(err) = futures::future::try_join_all(futures).await {
            tracing::debug!("error while serving stats websocket: {:?}", err);
        }
    })
}
