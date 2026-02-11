use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::events::DomainEvent;

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub sequence: i64,
    pub stream_id: Uuid,
    pub event: DomainEvent,
    pub stored_at: DateTime<Utc>,
}

#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, stream_id: Uuid, event: DomainEvent) -> anyhow::Result<EventEnvelope>;
    async fn stream(&self, stream_id: Uuid) -> anyhow::Result<Vec<EventEnvelope>>;
}

#[async_trait]
pub trait ProjectionStore: Send + Sync {
    async fn rebuild(&self) -> anyhow::Result<()>;
}
