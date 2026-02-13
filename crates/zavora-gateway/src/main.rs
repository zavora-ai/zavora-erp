use std::{
    cmp::{max, min},
    net::SocketAddr,
};

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};
use tracing::{error, info};
use uuid::Uuid;
use zavora_platform::{
    AcceptQuoteRequest, AcceptQuoteResponse, CreateLeadRequest, CreateLeadResponse,
    CreateOpportunityRequest, CreateOpportunityResponse, CreateOrderRequest, CreateOrderResponse,
    CreateQuoteRequest, CreateQuoteResponse, OrderCreatedEvent, RedisBus, ServiceConfig,
    connect_database,
};

const REGISTERED_AGENT_IDS: [&str; 10] = [
    "strategy-agent",
    "sales-agent",
    "procurement-agent",
    "warehouse-agent",
    "ar-agent",
    "controller-agent",
    "board-agent",
    "ops-orchestrator-agent",
    "audit-agent",
    "payroll-agent",
];

const GOVERNANCE_ACTOR_IDS: [&str; 3] = ["board-agent", "strategy-agent", "controller-agent"];
const FINOPS_ACTOR_IDS: [&str; 3] = ["payroll-agent", "controller-agent", "board-agent"];
const ACTION_ORDER_EXECUTION_PRODUCT: &str = "ORDER_EXECUTION_PRODUCT";
const ACTION_ORDER_EXECUTION_SERVICE: &str = "ORDER_EXECUTION_SERVICE";
const PAYROLL_EXPENSE_ACCOUNT: &str = "5100";
const PAYROLL_CLEARING_ACCOUNT: &str = "2200";

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    redis: RedisBus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetThresholdRequest {
    action_type: String,
    max_auto_amount: Decimal,
    currency: Option<String>,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetThresholdResponse {
    action_type: String,
    max_auto_amount: Decimal,
    currency: String,
    active: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetFreezeRequest {
    action_type: String,
    is_frozen: bool,
    reason: Option<String>,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetFreezeResponse {
    action_type: String,
    is_frozen: bool,
    reason: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListEscalationsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GovernanceEscalationView {
    escalation_id: Uuid,
    action_type: String,
    reference_type: String,
    reference_id: Uuid,
    status: String,
    reason_code: String,
    amount: Decimal,
    currency: String,
    requested_by_agent_id: String,
    created_at: DateTime<Utc>,
    decided_at: Option<DateTime<Utc>>,
    decided_by_agent_id: Option<String>,
    decision_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GovernanceEscalationListResponse {
    items: Vec<GovernanceEscalationView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecideEscalationRequest {
    decision: String,
    decided_by_agent_id: String,
    decision_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecideEscalationResponse {
    escalation_id: Uuid,
    status: String,
    order_id: Option<Uuid>,
    dispatched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestTokenUsageRequest {
    order_id: Option<Uuid>,
    agent_id: String,
    skill_id: Option<String>,
    action_name: String,
    input_tokens: i64,
    output_tokens: i64,
    token_unit_cost: Decimal,
    total_cost: Option<Decimal>,
    currency: String,
    occurred_at: Option<DateTime<Utc>>,
    source_ref: Option<String>,
    ingested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestTokenUsageResponse {
    usage_id: Uuid,
    total_tokens: i64,
    total_cost: Decimal,
    currency: String,
    occurred_at: DateTime<Utc>,
    stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestCloudCostRequest {
    order_id: Option<Uuid>,
    provider: String,
    cost_type: String,
    usage_quantity: Decimal,
    unit_cost: Decimal,
    total_cost: Option<Decimal>,
    currency: String,
    occurred_at: Option<DateTime<Utc>>,
    source_ref: Option<String>,
    ingested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestCloudCostResponse {
    cloud_cost_id: Uuid,
    total_cost: Decimal,
    currency: String,
    occurred_at: DateTime<Utc>,
    stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestSubscriptionCostRequest {
    tool_name: String,
    subscription_name: String,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    total_cost: Decimal,
    currency: String,
    source_ref: Option<String>,
    ingested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestSubscriptionCostResponse {
    subscription_cost_id: Uuid,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    total_cost: Decimal,
    currency: String,
    stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AllocateCostsRequest {
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AllocateCostsResponse {
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    orders_allocated: i64,
    source_total: Decimal,
    allocated_total: Decimal,
    journal_total: Decimal,
    variance_amount: Decimal,
    variance_pct: Decimal,
    status: String,
    completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct FulfilledOrder {
    order_id: Uuid,
    revenue: Decimal,
}

#[derive(Debug, Clone)]
struct AllocationInput {
    source_type: &'static str,
    source_id: Uuid,
    order_id: Option<Uuid>,
    amount: Decimal,
    currency: String,
    agent_id: Option<String>,
    skill_id: Option<String>,
}

struct PolicyGateResult {
    is_frozen: bool,
    freeze_reason: Option<String>,
    requires_escalation: bool,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "zavora_gateway=info,tower_http=info".to_string()),
        )
        .init();

    let config = ServiceConfig::from_env("0.0.0.0:8080")?;
    let pool = connect_database(&config.database_url).await?;
    let redis = RedisBus::connect(&config.redis_url)?;

    let state = AppState { pool, redis };
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/orders", post(create_order))
        .route("/origination/leads", post(create_lead))
        .route("/origination/opportunities", post(create_opportunity))
        .route("/origination/quotes", post(create_quote))
        .route("/origination/quotes/{quote_id}/accept", post(accept_quote))
        .route("/governance/thresholds", post(set_threshold))
        .route("/governance/freeze", post(set_freeze))
        .route("/governance/escalations", get(list_escalations))
        .route("/finops/token-usage", post(ingest_token_usage))
        .route("/finops/cloud-costs", post(ingest_cloud_cost))
        .route("/finops/subscriptions", post(ingest_subscription_cost))
        .route("/finops/allocate", post(allocate_costs))
        .route(
            "/governance/escalations/{escalation_id}/decide",
            post(decide_escalation),
        )
        .with_state(state);

    let addr: SocketAddr = config.http_addr.parse()?;
    info!("gateway listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn create_lead(
    State(state): State<AppState>,
    Json(payload): Json<CreateLeadRequest>,
) -> Result<(StatusCode, Json<CreateLeadResponse>), (StatusCode, String)> {
    if payload.contact_email.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "contact_email is required".to_string(),
        ));
    }
    if payload.source_channel.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "source_channel is required".to_string(),
        ));
    }

    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let lead_id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO leads (
            id, contact_email, source_channel, note, status, requested_by_agent_id, created_at
        )
        VALUES ($1, $2, $3, $4, 'NEW', $5, $6)
        "#,
    )
    .bind(lead_id)
    .bind(payload.contact_email.trim())
    .bind(payload.source_channel.trim())
    .bind(payload.note.as_deref().map(str::trim))
    .bind(&requested_by_agent_id)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(CreateLeadResponse {
            lead_id,
            status: "NEW".to_string(),
            created_at: now,
        }),
    ))
}

async fn create_opportunity(
    State(state): State<AppState>,
    Json(payload): Json<CreateOpportunityRequest>,
) -> Result<(StatusCode, Json<CreateOpportunityResponse>), (StatusCode, String)> {
    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.customer_email.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "customer_email is required".to_string(),
        ));
    }
    if payload.item_code.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "item_code is required".to_string()));
    }
    if payload.currency.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "currency is required".to_string()));
    }
    if payload.quantity <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "quantity must be positive".to_string(),
        ));
    }
    if payload.target_unit_price <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "target_unit_price must be positive".to_string(),
        ));
    }

    let transaction_type =
        normalize_transaction_type(&payload.transaction_type).map_err(invalid_request)?;

    let lead_exists =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM leads WHERE id = $1)")
            .bind(payload.lead_id)
            .fetch_one(&state.pool)
            .await
            .map_err(internal_error)?;

    if !lead_exists {
        return Err((StatusCode::NOT_FOUND, "lead not found".to_string()));
    }

    let opportunity_id = Uuid::new_v4();
    let now = Utc::now();
    let risk_class = payload
        .risk_class
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("STANDARD")
        .to_ascii_uppercase();

    sqlx::query(
        r#"
        INSERT INTO opportunities (
            id, lead_id, customer_email, transaction_type, item_code, quantity,
            target_unit_price, currency, risk_class, stage, requested_by_agent_id, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'QUALIFIED', $10, $11, $11)
        "#,
    )
    .bind(opportunity_id)
    .bind(payload.lead_id)
    .bind(payload.customer_email.trim())
    .bind(&transaction_type)
    .bind(payload.item_code.trim())
    .bind(payload.quantity)
    .bind(payload.target_unit_price)
    .bind(payload.currency.trim())
    .bind(&risk_class)
    .bind(&requested_by_agent_id)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(CreateOpportunityResponse {
            opportunity_id,
            stage: "QUALIFIED".to_string(),
            created_at: now,
        }),
    ))
}

async fn create_quote(
    State(state): State<AppState>,
    Json(payload): Json<CreateQuoteRequest>,
) -> Result<(StatusCode, Json<CreateQuoteResponse>), (StatusCode, String)> {
    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.unit_price <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "unit_price must be positive".to_string(),
        ));
    }

    let valid_for_days = payload.valid_for_days.unwrap_or(14);
    if !(1..=90).contains(&valid_for_days) {
        return Err((
            StatusCode::BAD_REQUEST,
            "valid_for_days must be between 1 and 90".to_string(),
        ));
    }

    let payment_terms_days = payload.payment_terms_days.unwrap_or(30);
    if !(0..=180).contains(&payment_terms_days) {
        return Err((
            StatusCode::BAD_REQUEST,
            "payment_terms_days must be between 0 and 180".to_string(),
        ));
    }

    let opportunity_row = sqlx::query(
        r#"
        SELECT stage, quantity, currency
        FROM opportunities
        WHERE id = $1
        "#,
    )
    .bind(payload.opportunity_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_error)?;

    let Some(opportunity_row) = opportunity_row else {
        return Err((StatusCode::NOT_FOUND, "opportunity not found".to_string()));
    };

    let stage: String = opportunity_row.try_get("stage").map_err(internal_error)?;
    if stage == "LOST" {
        return Err((
            StatusCode::BAD_REQUEST,
            "opportunity is closed as LOST".to_string(),
        ));
    }

    let default_quantity: Decimal = opportunity_row
        .try_get("quantity")
        .map_err(internal_error)?;
    let default_currency: String = opportunity_row
        .try_get("currency")
        .map_err(internal_error)?;

    let quantity = payload.quantity.unwrap_or(default_quantity);
    if quantity <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "quote quantity must be positive".to_string(),
        ));
    }

    let currency = payload
        .currency
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_currency.as_str())
        .to_string();

    let now = Utc::now();
    let valid_until = now + Duration::days(valid_for_days);
    let quote_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO quotes (
            id, opportunity_id, unit_price, quantity, currency, payment_terms_days,
            valid_until, terms_json, risk_metadata, status, requested_by_agent_id, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, 'ISSUED', $10, $11, $11)
        "#,
    )
    .bind(quote_id)
    .bind(payload.opportunity_id)
    .bind(payload.unit_price)
    .bind(quantity)
    .bind(&currency)
    .bind(payment_terms_days)
    .bind(valid_until)
    .bind(json!({
        "payment_terms_days": payment_terms_days,
        "valid_for_days": valid_for_days,
        "quoted_by": requested_by_agent_id,
    }))
    .bind(json!({
        "risk_note": payload
            .risk_note
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("policy-default"),
    }))
    .bind(&requested_by_agent_id)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    sqlx::query("UPDATE opportunities SET stage = 'PROPOSAL', updated_at = $2 WHERE id = $1")
        .bind(payload.opportunity_id)
        .bind(now)
        .execute(&state.pool)
        .await
        .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(CreateQuoteResponse {
            quote_id,
            opportunity_id: payload.opportunity_id,
            status: "ISSUED".to_string(),
            valid_until,
            created_at: now,
        }),
    ))
}

async fn accept_quote(
    State(state): State<AppState>,
    Path(quote_id): Path<Uuid>,
    Json(payload): Json<AcceptQuoteRequest>,
) -> Result<(StatusCode, Json<AcceptQuoteResponse>), (StatusCode, String)> {
    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.accepted_by.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "accepted_by is required".to_string(),
        ));
    }
    if payload.acceptance_channel.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "acceptance_channel is required".to_string(),
        ));
    }
    if payload.proof_ref.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "proof_ref is required".to_string()));
    }

    let now = Utc::now();
    let mut tx = state.pool.begin().await.map_err(internal_error)?;

    let quote_row = sqlx::query(
        r#"
        SELECT
            q.opportunity_id,
            q.status,
            q.valid_until,
            q.unit_price,
            q.quantity,
            q.currency,
            o.customer_email,
            o.transaction_type,
            o.item_code
        FROM quotes q
        INNER JOIN opportunities o ON o.id = q.opportunity_id
        WHERE q.id = $1
        FOR UPDATE OF q, o
        "#,
    )
    .bind(quote_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_error)?;

    let Some(quote_row) = quote_row else {
        return Err((StatusCode::NOT_FOUND, "quote not found".to_string()));
    };

    let opportunity_id: Uuid = quote_row
        .try_get("opportunity_id")
        .map_err(internal_error)?;
    let quote_status: String = quote_row.try_get("status").map_err(internal_error)?;
    let valid_until = quote_row
        .try_get::<chrono::DateTime<Utc>, _>("valid_until")
        .map_err(internal_error)?;

    if quote_status != "ISSUED" {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("quote status must be ISSUED, found {quote_status}"),
        ));
    }

    if valid_until < now {
        sqlx::query("UPDATE quotes SET status = 'EXPIRED', updated_at = $2 WHERE id = $1")
            .bind(quote_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;
        tx.commit().await.map_err(internal_error)?;

        return Err((StatusCode::BAD_REQUEST, "quote has expired".to_string()));
    }

    let customer_email: String = quote_row
        .try_get("customer_email")
        .map_err(internal_error)?;
    let transaction_type: String = quote_row
        .try_get("transaction_type")
        .map_err(internal_error)?;
    let item_code: String = quote_row.try_get("item_code").map_err(internal_error)?;
    let quantity: Decimal = quote_row.try_get("quantity").map_err(internal_error)?;
    let unit_price: Decimal = quote_row.try_get("unit_price").map_err(internal_error)?;
    let currency: String = quote_row.try_get("currency").map_err(internal_error)?;

    let action_type = action_type_for_transaction(&transaction_type);
    let amount = (quantity * unit_price).round_dp(4);
    let policy = evaluate_policy_gate(&mut tx, action_type, amount)
        .await
        .map_err(internal_error)?;

    if policy.is_frozen {
        return Err((
            StatusCode::LOCKED,
            format!(
                "action frozen by governance: {}",
                policy
                    .freeze_reason
                    .unwrap_or_else(|| "no reason provided".to_string())
            ),
        ));
    }

    let order_status = if policy.requires_escalation {
        "PENDING_APPROVAL"
    } else {
        "NEW"
    };

    let order_id = Uuid::new_v4();
    let acceptance_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO orders (
            id, customer_email, transaction_type, requested_by_agent_id, item_code, quantity, unit_price, currency, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
        "#,
    )
    .bind(order_id)
    .bind(customer_email)
    .bind(transaction_type)
    .bind(&requested_by_agent_id)
    .bind(item_code)
    .bind(quantity)
    .bind(unit_price)
    .bind(&currency)
    .bind(order_status)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    sqlx::query(
        r#"
        INSERT INTO quote_acceptances (
            id, quote_id, opportunity_id, order_id, accepted_by, acceptance_channel,
            proof_ref, requested_by_agent_id, accepted_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(acceptance_id)
    .bind(quote_id)
    .bind(opportunity_id)
    .bind(order_id)
    .bind(payload.accepted_by.trim())
    .bind(payload.acceptance_channel.trim())
    .bind(payload.proof_ref.trim())
    .bind(&requested_by_agent_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    sqlx::query("UPDATE quotes SET status = 'ACCEPTED', updated_at = $2 WHERE id = $1")
        .bind(quote_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    sqlx::query("UPDATE opportunities SET stage = 'ACCEPTED', updated_at = $2 WHERE id = $1")
        .bind(opportunity_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let escalation_id = if policy.requires_escalation {
        Some(
            insert_escalation(
                &mut tx,
                action_type,
                "ORDER",
                order_id,
                "AMOUNT_THRESHOLD_EXCEEDED",
                amount,
                &currency,
                &requested_by_agent_id,
            )
            .await
            .map_err(internal_error)?,
        )
    } else {
        None
    };

    tx.commit().await.map_err(internal_error)?;

    if escalation_id.is_none() {
        dispatch_order_event(&state, order_id).await?;
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(AcceptQuoteResponse {
            quote_id,
            opportunity_id,
            acceptance_id,
            order_id,
            status: if escalation_id.is_some() {
                "ORDER_PENDING_APPROVAL".to_string()
            } else {
                "ORDER_ACCEPTED".to_string()
            },
            escalation_id,
        }),
    ))
}

async fn create_order(
    State(state): State<AppState>,
    Json(payload): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<CreateOrderResponse>), (StatusCode, String)> {
    let (transaction_type, requested_by_agent_id) =
        validate_order_request(&payload).map_err(invalid_request)?;

    let action_type = action_type_for_transaction(&transaction_type);
    let amount = (payload.quantity * payload.unit_price).round_dp(4);

    let mut tx = state.pool.begin().await.map_err(internal_error)?;
    let policy = evaluate_policy_gate(&mut tx, action_type, amount)
        .await
        .map_err(internal_error)?;

    if policy.is_frozen {
        return Err((
            StatusCode::LOCKED,
            format!(
                "action frozen by governance: {}",
                policy
                    .freeze_reason
                    .unwrap_or_else(|| "no reason provided".to_string())
            ),
        ));
    }

    let order_id = Uuid::new_v4();
    let now = Utc::now();
    let order_status = if policy.requires_escalation {
        "PENDING_APPROVAL"
    } else {
        "NEW"
    };

    if let Err(err) = sqlx::query(
        r#"
        INSERT INTO orders (
            id, customer_email, transaction_type, requested_by_agent_id, item_code, quantity, unit_price, currency, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
        "#,
    )
    .bind(order_id)
    .bind(payload.customer_email.trim())
    .bind(&transaction_type)
    .bind(&requested_by_agent_id)
    .bind(payload.item_code.trim())
    .bind(payload.quantity)
    .bind(payload.unit_price)
    .bind(payload.currency.trim())
    .bind(order_status)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        error!("failed to insert order: {err}");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist order".to_string(),
        ));
    }

    let escalation_id = if policy.requires_escalation {
        Some(
            insert_escalation(
                &mut tx,
                action_type,
                "ORDER",
                order_id,
                "AMOUNT_THRESHOLD_EXCEEDED",
                amount,
                payload.currency.trim(),
                &requested_by_agent_id,
            )
            .await
            .map_err(internal_error)?,
        )
    } else {
        None
    };

    tx.commit().await.map_err(internal_error)?;

    if escalation_id.is_none() {
        dispatch_order_event(&state, order_id).await?;
    }

    let response = CreateOrderResponse {
        order_id,
        status: if escalation_id.is_some() {
            "PENDING_APPROVAL".to_string()
        } else {
            "ACCEPTED".to_string()
        },
        transaction_type,
        requested_by_agent_id,
        escalation_id,
    };

    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn set_threshold(
    State(state): State<AppState>,
    Json(payload): Json<SetThresholdRequest>,
) -> Result<Json<SetThresholdResponse>, (StatusCode, String)> {
    let actor = validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.max_auto_amount <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "max_auto_amount must be positive".to_string(),
        ));
    }

    let action_type = normalize_action_type(&payload.action_type)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let currency = payload
        .currency
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("USD")
        .to_ascii_uppercase();

    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO governance_thresholds (
            action_type, max_auto_amount, currency, active, updated_by_agent_id, updated_at
        )
        VALUES ($1, $2, $3, TRUE, $4, $5)
        ON CONFLICT (action_type)
        DO UPDATE SET
            max_auto_amount = EXCLUDED.max_auto_amount,
            currency = EXCLUDED.currency,
            active = TRUE,
            updated_by_agent_id = EXCLUDED.updated_by_agent_id,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&action_type)
    .bind(payload.max_auto_amount)
    .bind(&currency)
    .bind(&actor)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(SetThresholdResponse {
        action_type,
        max_auto_amount: payload.max_auto_amount,
        currency,
        active: true,
        updated_at: now,
    }))
}

async fn set_freeze(
    State(state): State<AppState>,
    Json(payload): Json<SetFreezeRequest>,
) -> Result<Json<SetFreezeResponse>, (StatusCode, String)> {
    let actor = validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let action_type = normalize_action_type(&payload.action_type)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let reason = payload
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO governance_freeze_controls (
            action_type, is_frozen, reason, updated_by_agent_id, updated_at
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (action_type)
        DO UPDATE SET
            is_frozen = EXCLUDED.is_frozen,
            reason = EXCLUDED.reason,
            updated_by_agent_id = EXCLUDED.updated_by_agent_id,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&action_type)
    .bind(payload.is_frozen)
    .bind(&reason)
    .bind(&actor)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(SetFreezeResponse {
        action_type,
        is_frozen: payload.is_frozen,
        reason,
        updated_at: now,
    }))
}

async fn list_escalations(
    State(state): State<AppState>,
    Query(query): Query<ListEscalationsQuery>,
) -> Result<Json<GovernanceEscalationListResponse>, (StatusCode, String)> {
    let status_filter = query
        .status
        .as_deref()
        .map(normalize_decision_status)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let limit = query.limit.unwrap_or(50).clamp(1, 200);

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            action_type,
            reference_type,
            reference_id,
            status,
            reason_code,
            amount,
            currency,
            requested_by_agent_id,
            created_at,
            decided_at,
            decided_by_agent_id,
            decision_note
        FROM governance_escalations
        WHERE ($1::text IS NULL OR status = $1)
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(status_filter)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(GovernanceEscalationView {
            escalation_id: row.try_get("id").map_err(internal_error)?,
            action_type: row.try_get("action_type").map_err(internal_error)?,
            reference_type: row.try_get("reference_type").map_err(internal_error)?,
            reference_id: row.try_get("reference_id").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            reason_code: row.try_get("reason_code").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            requested_by_agent_id: row
                .try_get("requested_by_agent_id")
                .map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            decided_at: row.try_get("decided_at").map_err(internal_error)?,
            decided_by_agent_id: row.try_get("decided_by_agent_id").map_err(internal_error)?,
            decision_note: row.try_get("decision_note").map_err(internal_error)?,
        });
    }

    Ok(Json(GovernanceEscalationListResponse { items }))
}

async fn decide_escalation(
    State(state): State<AppState>,
    Path(escalation_id): Path<Uuid>,
    Json(payload): Json<DecideEscalationRequest>,
) -> Result<Json<DecideEscalationResponse>, (StatusCode, String)> {
    let decided_by_agent_id = validate_governance_actor(&payload.decided_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let decision = normalize_decision_status(&payload.decision)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let now = Utc::now();
    let mut tx = state.pool.begin().await.map_err(internal_error)?;

    let escalation_row = sqlx::query(
        r#"
        SELECT action_type, reference_type, reference_id, status
        FROM governance_escalations
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(escalation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_error)?;

    let Some(escalation_row) = escalation_row else {
        return Err((StatusCode::NOT_FOUND, "escalation not found".to_string()));
    };

    let action_type: String = escalation_row
        .try_get("action_type")
        .map_err(internal_error)?;
    let reference_type: String = escalation_row
        .try_get("reference_type")
        .map_err(internal_error)?;
    let reference_id: Uuid = escalation_row
        .try_get("reference_id")
        .map_err(internal_error)?;
    let current_status: String = escalation_row.try_get("status").map_err(internal_error)?;

    if current_status != "PENDING" {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("escalation already decided with status {current_status}"),
        ));
    }

    sqlx::query(
        r#"
        UPDATE governance_escalations
        SET status = $2, decided_at = $3, decided_by_agent_id = $4, decision_note = $5
        WHERE id = $1
        "#,
    )
    .bind(escalation_id)
    .bind(&decision)
    .bind(now)
    .bind(&decided_by_agent_id)
    .bind(payload.decision_note.as_deref().map(str::trim))
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    let mut order_id: Option<Uuid> = None;
    let mut dispatch_required = false;

    if reference_type == "ORDER" {
        order_id = Some(reference_id);
        match decision.as_str() {
            "APPROVED" => {
                let updated = sqlx::query(
                    "UPDATE orders SET status = 'NEW', updated_at = $2 WHERE id = $1 AND status = 'PENDING_APPROVAL'",
                )
                .bind(reference_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;

                if updated.rows_affected() == 0 {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "order is not in PENDING_APPROVAL status".to_string(),
                    ));
                }
                dispatch_required = true;
            }
            "REJECTED" => {
                sqlx::query(
                    "UPDATE orders SET status = 'FAILED', failure_reason = 'governance_rejected', updated_at = $2 WHERE id = $1",
                )
                .bind(reference_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;
            }
            "FROZEN" => {
                sqlx::query(
                    "UPDATE orders SET status = 'FROZEN', failure_reason = 'governance_frozen', updated_at = $2 WHERE id = $1",
                )
                .bind(reference_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;

                sqlx::query(
                    r#"
                    INSERT INTO governance_freeze_controls (
                        action_type, is_frozen, reason, updated_by_agent_id, updated_at
                    )
                    VALUES ($1, TRUE, $2, $3, $4)
                    ON CONFLICT (action_type)
                    DO UPDATE SET
                        is_frozen = TRUE,
                        reason = EXCLUDED.reason,
                        updated_by_agent_id = EXCLUDED.updated_by_agent_id,
                        updated_at = EXCLUDED.updated_at
                    "#,
                )
                .bind(&action_type)
                .bind(payload.decision_note.as_deref().map(str::trim))
                .bind(&decided_by_agent_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(internal_error)?;
            }
            _ => {
                return Err((StatusCode::BAD_REQUEST, "unsupported decision".to_string()));
            }
        }
    }

    tx.commit().await.map_err(internal_error)?;

    if dispatch_required {
        if let Some(approved_order_id) = order_id {
            dispatch_order_event(&state, approved_order_id).await?;
        }
    }

    Ok(Json(DecideEscalationResponse {
        escalation_id,
        status: decision,
        order_id,
        dispatched: dispatch_required,
    }))
}

async fn ingest_token_usage(
    State(state): State<AppState>,
    Json(payload): Json<IngestTokenUsageRequest>,
) -> Result<(StatusCode, Json<IngestTokenUsageResponse>), (StatusCode, String)> {
    let ingested_by_agent_id = validate_finops_actor(&payload.ingested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let agent_id = validate_agent_id(&payload.agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.action_name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "action_name is required".to_string(),
        ));
    }
    if payload.input_tokens < 0 || payload.output_tokens < 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "token counters must be non-negative".to_string(),
        ));
    }
    if payload.token_unit_cost < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "token_unit_cost must be non-negative".to_string(),
        ));
    }

    if let Some(order_id) = payload.order_id {
        ensure_order_exists(&state.pool, order_id).await?;
    }

    let total_tokens = payload.input_tokens + payload.output_tokens;
    if total_tokens < 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "total_tokens overflowed".to_string(),
        ));
    }

    let computed_total_cost = (Decimal::from(total_tokens) * payload.token_unit_cost).round_dp(4);
    let total_cost = payload
        .total_cost
        .unwrap_or(computed_total_cost)
        .round_dp(4);
    if total_cost < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "total_cost must be non-negative".to_string(),
        ));
    }

    let occurred_at = payload.occurred_at.unwrap_or_else(Utc::now);
    let stored_at = Utc::now();
    let usage_id = Uuid::new_v4();
    let currency = normalize_currency(&payload.currency).map_err(invalid_request)?;

    sqlx::query(
        r#"
        INSERT INTO finops_token_usage (
            id, order_id, agent_id, skill_id, action_name, input_tokens, output_tokens,
            total_tokens, token_unit_cost, total_cost, currency, source_ref, occurred_at,
            ingested_by_agent_id, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        "#,
    )
    .bind(usage_id)
    .bind(payload.order_id)
    .bind(agent_id)
    .bind(payload.skill_id.as_deref().map(str::trim))
    .bind(payload.action_name.trim())
    .bind(payload.input_tokens)
    .bind(payload.output_tokens)
    .bind(total_tokens)
    .bind(payload.token_unit_cost)
    .bind(total_cost)
    .bind(&currency)
    .bind(payload.source_ref.as_deref().map(str::trim))
    .bind(occurred_at)
    .bind(&ingested_by_agent_id)
    .bind(stored_at)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(IngestTokenUsageResponse {
            usage_id,
            total_tokens,
            total_cost,
            currency,
            occurred_at,
            stored_at,
        }),
    ))
}

async fn ingest_cloud_cost(
    State(state): State<AppState>,
    Json(payload): Json<IngestCloudCostRequest>,
) -> Result<(StatusCode, Json<IngestCloudCostResponse>), (StatusCode, String)> {
    let ingested_by_agent_id = validate_finops_actor(&payload.ingested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if let Some(order_id) = payload.order_id {
        ensure_order_exists(&state.pool, order_id).await?;
    }
    if payload.provider.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "provider is required".to_string()));
    }
    if payload.usage_quantity < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "usage_quantity must be non-negative".to_string(),
        ));
    }
    if payload.unit_cost < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "unit_cost must be non-negative".to_string(),
        ));
    }

    let cost_type = normalize_cloud_cost_type(&payload.cost_type).map_err(invalid_request)?;
    let currency = normalize_currency(&payload.currency).map_err(invalid_request)?;
    let occurred_at = payload.occurred_at.unwrap_or_else(Utc::now);
    let stored_at = Utc::now();
    let cloud_cost_id = Uuid::new_v4();
    let computed_total_cost = (payload.usage_quantity * payload.unit_cost).round_dp(4);
    let total_cost = payload
        .total_cost
        .unwrap_or(computed_total_cost)
        .round_dp(4);
    if total_cost < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "total_cost must be non-negative".to_string(),
        ));
    }

    sqlx::query(
        r#"
        INSERT INTO finops_cloud_costs (
            id, order_id, provider, cost_type, usage_quantity, unit_cost, total_cost,
            currency, source_ref, occurred_at, ingested_by_agent_id, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(cloud_cost_id)
    .bind(payload.order_id)
    .bind(payload.provider.trim())
    .bind(cost_type)
    .bind(payload.usage_quantity)
    .bind(payload.unit_cost)
    .bind(total_cost)
    .bind(&currency)
    .bind(payload.source_ref.as_deref().map(str::trim))
    .bind(occurred_at)
    .bind(&ingested_by_agent_id)
    .bind(stored_at)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(IngestCloudCostResponse {
            cloud_cost_id,
            total_cost,
            currency,
            occurred_at,
            stored_at,
        }),
    ))
}

async fn ingest_subscription_cost(
    State(state): State<AppState>,
    Json(payload): Json<IngestSubscriptionCostRequest>,
) -> Result<(StatusCode, Json<IngestSubscriptionCostResponse>), (StatusCode, String)> {
    let ingested_by_agent_id = validate_finops_actor(&payload.ingested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.tool_name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "tool_name is required".to_string()));
    }
    if payload.subscription_name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "subscription_name is required".to_string(),
        ));
    }
    if payload.total_cost < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "total_cost must be non-negative".to_string(),
        ));
    }
    if payload.period_end <= payload.period_start {
        return Err((
            StatusCode::BAD_REQUEST,
            "period_end must be greater than period_start".to_string(),
        ));
    }

    let currency = normalize_currency(&payload.currency).map_err(invalid_request)?;
    let stored_at = Utc::now();
    let subscription_cost_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO finops_subscription_costs (
            id, tool_name, subscription_name, period_start, period_end, total_cost,
            currency, source_ref, ingested_by_agent_id, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(subscription_cost_id)
    .bind(payload.tool_name.trim())
    .bind(payload.subscription_name.trim())
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(payload.total_cost.round_dp(4))
    .bind(&currency)
    .bind(payload.source_ref.as_deref().map(str::trim))
    .bind(&ingested_by_agent_id)
    .bind(stored_at)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(IngestSubscriptionCostResponse {
            subscription_cost_id,
            period_start: payload.period_start,
            period_end: payload.period_end,
            total_cost: payload.total_cost.round_dp(4),
            currency,
            stored_at,
        }),
    ))
}

async fn allocate_costs(
    State(state): State<AppState>,
    Json(payload): Json<AllocateCostsRequest>,
) -> Result<Json<AllocateCostsResponse>, (StatusCode, String)> {
    let requested_by_agent_id = validate_finops_actor(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    if payload.period_end <= payload.period_start {
        return Err((
            StatusCode::BAD_REQUEST,
            "period_end must be greater than period_start".to_string(),
        ));
    }

    let mut tx = state.pool.begin().await.map_err(internal_error)?;
    let orders = list_fulfilled_orders(&mut tx, payload.period_start, payload.period_end)
        .await
        .map_err(internal_error)?;
    if orders.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "no fulfilled orders found in the requested period".to_string(),
        ));
    }

    let period_start = payload.period_start;
    let period_end = payload.period_end;
    let period_key = format!("{}|{}", period_start.to_rfc3339(), period_end.to_rfc3339());
    let order_ids: Vec<Uuid> = orders.iter().map(|order| order.order_id).collect();
    let delete_memo_pattern = format!("PAYROLL_ALLOC|{period_key}|%");

    sqlx::query(
        r#"
        DELETE FROM journals
        WHERE order_id = ANY($1)
          AND memo LIKE $2
        "#,
    )
    .bind(&order_ids)
    .bind(&delete_memo_pattern)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    sqlx::query("DELETE FROM finops_cost_allocations WHERE period_start = $1 AND period_end = $2")
        .bind(period_start)
        .bind(period_end)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let mut source_total = Decimal::ZERO;
    let mut allocated_total = Decimal::ZERO;

    let token_rows = sqlx::query(
        r#"
        SELECT id, order_id, agent_id, skill_id, total_cost, currency
        FROM finops_token_usage
        WHERE occurred_at >= $1
          AND occurred_at < $2
        ORDER BY occurred_at, id
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    for row in token_rows {
        let amount: Decimal = row.try_get("total_cost").map_err(internal_error)?;
        let input = AllocationInput {
            source_type: "TOKEN",
            source_id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            amount: amount.round_dp(4),
            currency: row.try_get("currency").map_err(internal_error)?,
            agent_id: row.try_get("agent_id").map_err(internal_error)?,
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
        };
        source_total += input.amount;
        allocated_total += allocate_input_cost(&mut tx, &orders, period_start, period_end, &input)
            .await
            .map_err(internal_error)?;
    }

    let cloud_rows = sqlx::query(
        r#"
        SELECT id, order_id, total_cost, currency
        FROM finops_cloud_costs
        WHERE occurred_at >= $1
          AND occurred_at < $2
        ORDER BY occurred_at, id
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    for row in cloud_rows {
        let amount: Decimal = row.try_get("total_cost").map_err(internal_error)?;
        let input = AllocationInput {
            source_type: "CLOUD",
            source_id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            amount: amount.round_dp(4),
            currency: row.try_get("currency").map_err(internal_error)?,
            agent_id: None,
            skill_id: None,
        };
        source_total += input.amount;
        allocated_total += allocate_input_cost(&mut tx, &orders, period_start, period_end, &input)
            .await
            .map_err(internal_error)?;
    }

    let subscription_rows = sqlx::query(
        r#"
        SELECT id, period_start, period_end, total_cost, currency
        FROM finops_subscription_costs
        WHERE period_start < $2
          AND period_end > $1
        ORDER BY period_start, id
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    for row in subscription_rows {
        let src_period_start: DateTime<Utc> =
            row.try_get("period_start").map_err(internal_error)?;
        let src_period_end: DateTime<Utc> = row.try_get("period_end").map_err(internal_error)?;
        let src_total_cost: Decimal = row.try_get("total_cost").map_err(internal_error)?;
        let seconds_total = (src_period_end - src_period_start).num_seconds();
        if seconds_total <= 0 {
            continue;
        }

        let overlap_start = max(src_period_start, period_start);
        let overlap_end = min(src_period_end, period_end);
        let overlap_seconds = (overlap_end - overlap_start).num_seconds();
        if overlap_seconds <= 0 {
            continue;
        }

        let overlap_ratio =
            (Decimal::from(overlap_seconds) / Decimal::from(seconds_total)).round_dp(8);
        let prorated_cost = (src_total_cost * overlap_ratio).round_dp(4);

        let input = AllocationInput {
            source_type: "SUBSCRIPTION",
            source_id: row.try_get("id").map_err(internal_error)?,
            order_id: None,
            amount: prorated_cost,
            currency: row.try_get("currency").map_err(internal_error)?,
            agent_id: None,
            skill_id: None,
        };
        source_total += input.amount;
        allocated_total += allocate_input_cost(&mut tx, &orders, period_start, period_end, &input)
            .await
            .map_err(internal_error)?;
    }

    let per_order_rows = sqlx::query(
        r#"
        SELECT order_id, currency, COALESCE(SUM(allocated_cost), 0) AS total_cost
        FROM finops_cost_allocations
        WHERE period_start = $1
          AND period_end = $2
        GROUP BY order_id, currency
        ORDER BY order_id
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    let completed_at = Utc::now();
    let mut journal_total = Decimal::ZERO;
    for row in per_order_rows {
        let order_id: Uuid = row.try_get("order_id").map_err(internal_error)?;
        let currency: String = row.try_get("currency").map_err(internal_error)?;
        let cost: Decimal = row.try_get("total_cost").map_err(internal_error)?;
        let rounded_cost = cost.round_dp(4);
        if rounded_cost <= Decimal::ZERO {
            continue;
        }

        let memo_prefix = format!(
            "PAYROLL_ALLOC|{}|{}|{}",
            period_start.to_rfc3339(),
            period_end.to_rfc3339(),
            order_id
        );
        insert_journal_line(
            &mut tx,
            order_id,
            PAYROLL_EXPENSE_ACCOUNT,
            rounded_cost,
            Decimal::ZERO,
            &format!("{memo_prefix}|DEBIT"),
        )
        .await
        .map_err(internal_error)?;
        insert_journal_line(
            &mut tx,
            order_id,
            PAYROLL_CLEARING_ACCOUNT,
            Decimal::ZERO,
            rounded_cost,
            &format!("{memo_prefix}|CREDIT"),
        )
        .await
        .map_err(internal_error)?;
        journal_total += rounded_cost;

        sqlx::query(
            r#"
            INSERT INTO agent_semantic_memory (
                id, agent_name, scope, entity_id, content, keywords, source_ref, created_at
            )
            VALUES ($1, 'payroll-agent', 'ORDER_COST_ALLOCATION', $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order_id)
        .bind(format!(
            "Allocated autonomous operating cost {} {} for order {} in period {} to {}",
            rounded_cost, currency, order_id, period_key, PAYROLL_EXPENSE_ACCOUNT
        ))
        .bind(vec![
            "payroll".to_string(),
            "allocation".to_string(),
            "autonomy-cost".to_string(),
        ])
        .bind(format!("finops-period:{period_key}"))
        .bind(completed_at)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;
    }

    let source_total = source_total.round_dp(4);
    let allocated_total = allocated_total.round_dp(4);
    let journal_total = journal_total.round_dp(4);
    let variance_amount = (source_total - journal_total).abs().round_dp(4);
    let variance_pct = if source_total > Decimal::ZERO {
        ((variance_amount / source_total) * Decimal::new(100, 0)).round_dp(4)
    } else {
        Decimal::ZERO
    };

    let status = if source_total == Decimal::ZERO {
        "NO_SOURCE_COSTS".to_string()
    } else if variance_pct <= finops_variance_threshold_pct() {
        "BALANCED".to_string()
    } else {
        "OUT_OF_TOLERANCE".to_string()
    };

    sqlx::query(
        r#"
        INSERT INTO finops_period_reconciliations (
            period_start, period_end, source_total, allocated_total, journal_total,
            variance_amount, variance_pct, orders_allocated, status, completed_by_agent_id, completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (period_start, period_end)
        DO UPDATE SET
            source_total = EXCLUDED.source_total,
            allocated_total = EXCLUDED.allocated_total,
            journal_total = EXCLUDED.journal_total,
            variance_amount = EXCLUDED.variance_amount,
            variance_pct = EXCLUDED.variance_pct,
            orders_allocated = EXCLUDED.orders_allocated,
            status = EXCLUDED.status,
            completed_by_agent_id = EXCLUDED.completed_by_agent_id,
            completed_at = EXCLUDED.completed_at
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .bind(source_total)
    .bind(allocated_total)
    .bind(journal_total)
    .bind(variance_amount)
    .bind(variance_pct)
    .bind(orders.len() as i64)
    .bind(&status)
    .bind(&requested_by_agent_id)
    .bind(completed_at)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(AllocateCostsResponse {
        period_start,
        period_end,
        orders_allocated: orders.len() as i64,
        source_total,
        allocated_total,
        journal_total,
        variance_amount,
        variance_pct,
        status,
        completed_at,
    }))
}

async fn list_fulfilled_orders(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> AnyResult<Vec<FulfilledOrder>> {
    let rows = sqlx::query(
        r#"
        SELECT id, (quantity * unit_price) AS revenue
        FROM orders
        WHERE status = 'FULFILLED'
          AND fulfilled_at IS NOT NULL
          AND fulfilled_at >= $1
          AND fulfilled_at < $2
        ORDER BY id
        "#,
    )
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut **tx)
    .await?;

    let mut orders = Vec::with_capacity(rows.len());
    for row in rows {
        orders.push(FulfilledOrder {
            order_id: row.try_get("id")?,
            revenue: row.try_get::<Decimal, _>("revenue")?.round_dp(4),
        });
    }

    Ok(orders)
}

async fn allocate_input_cost(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    orders: &[FulfilledOrder],
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    input: &AllocationInput,
) -> AnyResult<Decimal> {
    if input.amount <= Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }

    let allocations = if let Some(order_id) = input.order_id {
        vec![(order_id, input.amount.round_dp(4), "DIRECT_ORDER")]
    } else {
        let total_revenue = orders
            .iter()
            .fold(Decimal::ZERO, |acc, order| acc + order.revenue)
            .round_dp(4);

        if total_revenue > Decimal::ZERO {
            let mut remaining = input.amount.round_dp(4);
            let mut distributed = Vec::with_capacity(orders.len());
            for (idx, order) in orders.iter().enumerate() {
                let amount = if idx == orders.len() - 1 {
                    remaining.round_dp(4)
                } else {
                    let provisional = (input.amount * order.revenue / total_revenue).round_dp(4);
                    remaining = (remaining - provisional).round_dp(4);
                    provisional
                };
                distributed.push((order.order_id, amount, "REVENUE_SHARE"));
            }
            distributed
        } else {
            let count = Decimal::from(orders.len() as i64);
            let per_order = (input.amount / count).round_dp(4);
            let mut remaining = input.amount.round_dp(4);
            let mut distributed = Vec::with_capacity(orders.len());
            for (idx, order) in orders.iter().enumerate() {
                let amount = if idx == orders.len() - 1 {
                    remaining.round_dp(4)
                } else {
                    remaining = (remaining - per_order).round_dp(4);
                    per_order
                };
                distributed.push((order.order_id, amount, "REVENUE_SHARE"));
            }
            distributed
        }
    };

    let mut allocated_total = Decimal::ZERO;
    for (order_id, amount, basis) in allocations {
        if amount <= Decimal::ZERO {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO finops_cost_allocations (
                id, period_start, period_end, order_id, source_type, source_id, agent_id,
                skill_id, allocation_basis, allocated_cost, currency, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(period_start)
        .bind(period_end)
        .bind(order_id)
        .bind(input.source_type)
        .bind(input.source_id)
        .bind(input.agent_id.as_deref())
        .bind(input.skill_id.as_deref())
        .bind(basis)
        .bind(amount.round_dp(4))
        .bind(input.currency.as_str())
        .bind(Utc::now())
        .execute(&mut **tx)
        .await?;

        allocated_total += amount;
    }

    Ok(allocated_total.round_dp(4))
}

async fn insert_journal_line(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    account: &str,
    debit: Decimal,
    credit: Decimal,
    memo: &str,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO journals (id, order_id, account, debit, credit, memo, posted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(account)
    .bind(debit)
    .bind(credit)
    .bind(memo)
    .bind(Utc::now())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn ensure_order_exists(pool: &PgPool, order_id: Uuid) -> Result<(), (StatusCode, String)> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM orders WHERE id = $1)")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .map_err(internal_error)?;

    if !exists {
        return Err((StatusCode::NOT_FOUND, "order not found".to_string()));
    }

    Ok(())
}

async fn evaluate_policy_gate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    action_type: &str,
    amount: Decimal,
) -> AnyResult<PolicyGateResult> {
    let freeze_row = sqlx::query(
        "SELECT is_frozen, reason FROM governance_freeze_controls WHERE action_type = $1",
    )
    .bind(action_type)
    .fetch_optional(&mut **tx)
    .await?;

    let (is_frozen, freeze_reason) = if let Some(row) = freeze_row {
        (
            row.try_get::<bool, _>("is_frozen")?,
            row.try_get::<Option<String>, _>("reason")?,
        )
    } else {
        (false, None)
    };

    let max_auto_amount = sqlx::query_scalar::<_, Decimal>(
        "SELECT max_auto_amount FROM governance_thresholds WHERE action_type = $1 AND active = TRUE",
    )
    .bind(action_type)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or_else(default_auto_approval_limit);

    Ok(PolicyGateResult {
        is_frozen,
        freeze_reason,
        requires_escalation: amount > max_auto_amount,
    })
}

async fn insert_escalation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    action_type: &str,
    reference_type: &str,
    reference_id: Uuid,
    reason_code: &str,
    amount: Decimal,
    currency: &str,
    requested_by_agent_id: &str,
) -> AnyResult<Uuid> {
    let escalation_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO governance_escalations (
            id, action_type, reference_type, reference_id, status, reason_code,
            amount, currency, requested_by_agent_id, created_at
        )
        VALUES ($1, $2, $3, $4, 'PENDING', $5, $6, $7, $8, $9)
        "#,
    )
    .bind(escalation_id)
    .bind(action_type)
    .bind(reference_type)
    .bind(reference_id)
    .bind(reason_code)
    .bind(amount)
    .bind(currency)
    .bind(requested_by_agent_id)
    .bind(Utc::now())
    .execute(&mut **tx)
    .await?;

    Ok(escalation_id)
}

async fn dispatch_order_event(
    state: &AppState,
    order_id: Uuid,
) -> Result<(), (StatusCode, String)> {
    let event = OrderCreatedEvent { order_id };
    if let Err(err) = state.redis.publish_json("orders.created", &event).await {
        error!("failed to publish order event: {err}");
        let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = $2 WHERE id = $1")
            .bind(order_id)
            .bind(Utc::now())
            .execute(&state.pool)
            .await;

        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to dispatch workflow".to_string(),
        ));
    }

    Ok(())
}

fn validate_order_request(payload: &CreateOrderRequest) -> AnyResult<(String, String)> {
    if payload.customer_email.trim().is_empty() {
        anyhow::bail!("customer_email is required");
    }

    let transaction_type = normalize_transaction_type(&payload.transaction_type)?;

    if payload.item_code.trim().is_empty() {
        anyhow::bail!("item_code is required (SKU for PRODUCT, service code for SERVICE)");
    }
    if payload.currency.trim().is_empty() {
        anyhow::bail!("currency is required");
    }
    if payload.quantity <= Decimal::ZERO {
        anyhow::bail!("quantity must be positive");
    }
    if payload.unit_price <= Decimal::ZERO {
        anyhow::bail!("unit_price must be positive");
    }

    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)?;

    Ok((transaction_type, requested_by_agent_id))
}

fn validate_agent_id(agent_id: &str) -> AnyResult<String> {
    let normalized = agent_id.trim().to_string();
    if normalized.is_empty() {
        anyhow::bail!("requested_by_agent_id is required");
    }

    if !REGISTERED_AGENT_IDS
        .iter()
        .any(|registered| *registered == normalized.as_str())
    {
        anyhow::bail!("requested_by_agent_id is not registered");
    }

    Ok(normalized)
}

fn validate_governance_actor(agent_id: &str) -> AnyResult<String> {
    let normalized = validate_agent_id(agent_id)?;
    if !GOVERNANCE_ACTOR_IDS
        .iter()
        .any(|registered| *registered == normalized.as_str())
    {
        anyhow::bail!("agent is not authorized for governance decisions");
    }

    Ok(normalized)
}

fn validate_finops_actor(agent_id: &str) -> AnyResult<String> {
    let normalized = validate_agent_id(agent_id)?;
    if !FINOPS_ACTOR_IDS
        .iter()
        .any(|registered| *registered == normalized.as_str())
    {
        anyhow::bail!("agent is not authorized for finops operations");
    }

    Ok(normalized)
}

fn action_type_for_transaction(transaction_type: &str) -> &'static str {
    if transaction_type == "SERVICE" {
        ACTION_ORDER_EXECUTION_SERVICE
    } else {
        ACTION_ORDER_EXECUTION_PRODUCT
    }
}

fn normalize_transaction_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "PRODUCT" | "SERVICE" => Ok(normalized),
        _ => anyhow::bail!("transaction_type must be PRODUCT or SERVICE"),
    }
}

fn normalize_currency(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        anyhow::bail!("currency is required");
    }
    if normalized.len() != 3 {
        anyhow::bail!("currency must be a 3-letter code");
    }
    Ok(normalized)
}

fn normalize_cloud_cost_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "COMPUTE" | "STORAGE" | "NETWORK" => Ok(normalized),
        _ => anyhow::bail!("cost_type must be one of COMPUTE, STORAGE, NETWORK"),
    }
}

fn normalize_action_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        ACTION_ORDER_EXECUTION_PRODUCT | ACTION_ORDER_EXECUTION_SERVICE => Ok(normalized),
        _ => anyhow::bail!("unsupported action_type"),
    }
}

fn normalize_decision_status(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "PENDING" | "APPROVED" | "REJECTED" | "FROZEN" => Ok(normalized),
        _ => anyhow::bail!("status must be one of PENDING, APPROVED, REJECTED, FROZEN"),
    }
}

fn default_auto_approval_limit() -> Decimal {
    Decimal::new(100000000, 2) // 1,000,000.00
}

fn finops_variance_threshold_pct() -> Decimal {
    Decimal::new(5, 1) // 0.5%
}

fn invalid_request(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
