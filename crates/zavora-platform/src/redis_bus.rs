use anyhow::Result;
use redis::{AsyncCommands, Client};
use serde::Serialize;

#[derive(Clone)]
pub struct RedisBus {
    client: Client,
}

impl RedisBus {
    pub fn connect(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn publish_json<T: Serialize>(&self, channel: &str, payload: &T) -> Result<()> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let serialized = serde_json::to_string(payload)?;
        let _: i64 = connection.publish(channel, serialized).await?;
        Ok(())
    }
}
