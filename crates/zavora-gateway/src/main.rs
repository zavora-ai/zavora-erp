use std::net::SocketAddr;

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
const ACTION_ORDER_EXECUTION_PRODUCT: &str = "ORDER_EXECUTION_PRODUCT";
const ACTION_ORDER_EXECUTION_SERVICE: &str = "ORDER_EXECUTION_SERVICE";

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

fn invalid_request(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
