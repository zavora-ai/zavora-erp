use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;
use uuid::Uuid;
use zavora_core::{DomainEvent, EventEnvelope, EventStore, ProjectionStore};

#[derive(Default)]
pub struct InMemoryEventStore {
    streams: RwLock<HashMap<Uuid, Vec<EventEnvelope>>>,
    sequence: RwLock<i64>,
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(&self, stream_id: Uuid, event: DomainEvent) -> anyhow::Result<EventEnvelope> {
        let mut sequence_guard = self.sequence.write().await;
        *sequence_guard += 1;

        let envelope = EventEnvelope {
            sequence: *sequence_guard,
            stream_id,
            event,
            stored_at: Utc::now(),
        };

        let mut streams = self.streams.write().await;
        streams.entry(stream_id).or_default().push(envelope.clone());

        Ok(envelope)
    }

    async fn stream(&self, stream_id: Uuid) -> anyhow::Result<Vec<EventEnvelope>> {
        let streams = self.streams.read().await;
        Ok(streams.get(&stream_id).cloned().unwrap_or_default())
    }
}

#[derive(Default)]
pub struct NoopProjectionStore;

#[async_trait]
impl ProjectionStore for NoopProjectionStore {
    async fn rebuild(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
