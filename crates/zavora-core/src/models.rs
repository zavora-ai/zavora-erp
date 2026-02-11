use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommitmentStatus {
    Draft,
    Active,
    Fulfilled,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commitment {
    pub id: Uuid,
    pub commitment_type: String,
    pub from_party: String,
    pub to_party: String,
    pub terms: String,
    pub risk_class: String,
    pub status: CommitmentStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Obligation {
    pub id: Uuid,
    pub commitment_id: Uuid,
    pub owner: String,
    pub due_at: DateTime<Utc>,
    pub depends_on: Vec<Uuid>,
    pub closed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub id: Uuid,
    pub linked_id: Uuid,
    pub source: String,
    pub payload_ref: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settlement {
    pub id: Uuid,
    pub commitment_id: Uuid,
    pub amount: Decimal,
    pub currency: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}
