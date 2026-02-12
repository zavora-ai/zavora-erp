use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub customer_email: String,
    #[serde(default = "default_transaction_type")]
    pub transaction_type: String,
    pub item_code: String,
    pub quantity: Decimal,
    pub unit_price: Decimal,
    pub currency: String,
    #[serde(default = "default_requesting_agent")]
    pub requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLeadRequest {
    pub contact_email: String,
    pub source_channel: String,
    pub note: Option<String>,
    #[serde(default = "default_requesting_agent")]
    pub requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLeadResponse {
    pub lead_id: Uuid,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOpportunityRequest {
    pub lead_id: Uuid,
    pub customer_email: String,
    #[serde(default = "default_transaction_type")]
    pub transaction_type: String,
    pub item_code: String,
    pub quantity: Decimal,
    pub target_unit_price: Decimal,
    pub currency: String,
    pub risk_class: Option<String>,
    #[serde(default = "default_requesting_agent")]
    pub requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOpportunityResponse {
    pub opportunity_id: Uuid,
    pub stage: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateQuoteRequest {
    pub opportunity_id: Uuid,
    pub unit_price: Decimal,
    pub quantity: Option<Decimal>,
    pub currency: Option<String>,
    pub payment_terms_days: Option<i32>,
    pub valid_for_days: Option<i64>,
    pub risk_note: Option<String>,
    #[serde(default = "default_requesting_agent")]
    pub requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateQuoteResponse {
    pub quote_id: Uuid,
    pub opportunity_id: Uuid,
    pub status: String,
    pub valid_until: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptQuoteRequest {
    pub accepted_by: String,
    pub acceptance_channel: String,
    pub proof_ref: String,
    #[serde(default = "default_requesting_agent")]
    pub requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptQuoteResponse {
    pub quote_id: Uuid,
    pub opportunity_id: Uuid,
    pub acceptance_id: Uuid,
    pub order_id: Uuid,
    pub status: String,
    pub escalation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderResponse {
    pub order_id: Uuid,
    pub status: String,
    pub transaction_type: String,
    pub requested_by_agent_id: String,
    pub escalation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedEvent {
    pub order_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFulfilledEvent {
    pub order_id: Uuid,
    pub settled_amount: Decimal,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardPack {
    pub generated_at: DateTime<Utc>,
    pub orders_total: i64,
    pub orders_fulfilled: i64,
    pub orders_open: i64,
    pub orders_pending_approval: i64,
    pub leads_total: i64,
    pub opportunities_open: i64,
    pub quotes_issued: i64,
    pub quotes_accepted: i64,
    pub governance_escalations_pending: i64,
    pub revenue: Decimal,
    pub cash_collected: Decimal,
    pub inventory_value: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryWriteRequest {
    pub agent_name: String,
    pub scope: String,
    pub entity_id: Option<Uuid>,
    pub content: String,
    #[serde(default)]
    pub keywords: Vec<String>,
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryWriteResponse {
    pub memory_id: Uuid,
    pub stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchRequest {
    pub agent_name: String,
    pub query: String,
    pub scope: Option<String>,
    pub entity_id: Option<Uuid>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchHit {
    pub memory_id: Uuid,
    pub agent_name: String,
    pub scope: String,
    pub entity_id: Option<Uuid>,
    pub content: String,
    pub keywords: Vec<String>,
    pub source_ref: Option<String>,
    pub score: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchResponse {
    pub hits: Vec<MemorySearchHit>,
}

fn default_transaction_type() -> String {
    "PRODUCT".to_string()
}

fn default_requesting_agent() -> String {
    "sales-agent".to_string()
}
