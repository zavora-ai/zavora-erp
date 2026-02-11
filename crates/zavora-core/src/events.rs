use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DomainEventKind {
    CommitmentCreated,
    ObligationsAssigned,
    StockReceived,
    StockIssued,
    InvoiceIssued,
    SettlementConfirmed,
    BoardActionFrozen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub kind: DomainEventKind,
    pub occurred_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}
