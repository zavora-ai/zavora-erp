use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
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

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    redis: RedisBus,
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

    let order_id = Uuid::new_v4();
    let acceptance_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO orders (
            id, customer_email, transaction_type, requested_by_agent_id, item_code, quantity, unit_price, currency, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'NEW', $9, $9)
        "#,
    )
    .bind(order_id)
    .bind(customer_email)
    .bind(transaction_type)
    .bind(&requested_by_agent_id)
    .bind(item_code)
    .bind(quantity)
    .bind(unit_price)
    .bind(currency)
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

    tx.commit().await.map_err(internal_error)?;

    dispatch_order_event(&state, order_id).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(AcceptQuoteResponse {
            quote_id,
            opportunity_id,
            acceptance_id,
            order_id,
            status: "ORDER_ACCEPTED".to_string(),
        }),
    ))
}

async fn create_order(
    State(state): State<AppState>,
    Json(payload): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<CreateOrderResponse>), (StatusCode, String)> {
    let (transaction_type, requested_by_agent_id) =
        validate_order_request(&payload).map_err(invalid_request)?;

    let order_id = Uuid::new_v4();
    let now = Utc::now();

    if let Err(err) = sqlx::query(
        r#"
        INSERT INTO orders (
            id, customer_email, transaction_type, requested_by_agent_id, item_code, quantity, unit_price, currency, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'NEW', $9, $9)
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
    .bind(now)
    .execute(&state.pool)
    .await
    {
        error!("failed to insert order: {err}");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist order".to_string(),
        ));
    }

    dispatch_order_event(&state, order_id).await?;

    let response = CreateOrderResponse {
        order_id,
        status: "ACCEPTED".to_string(),
        transaction_type,
        requested_by_agent_id,
    };

    Ok((StatusCode::ACCEPTED, Json(response)))
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

fn normalize_transaction_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "PRODUCT" | "SERVICE" => Ok(normalized),
        _ => anyhow::bail!("transaction_type must be PRODUCT or SERVICE"),
    }
}

fn invalid_request(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
