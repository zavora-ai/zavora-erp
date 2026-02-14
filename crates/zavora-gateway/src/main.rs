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
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
const CASH_ACCOUNT: &str = "1000";
const PROCUREMENT_AP_ACCOUNT: &str = "2100";
const SERVICE_COST_CLEARING_ACCOUNT: &str = "2200";
const PAYROLL_EXPENSE_ACCOUNT: &str = "5100";
const PAYROLL_AP_ACCOUNT: &str = "2300";
const AP_DEFAULT_TERMS_DAYS: i64 = 30;

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
    settle_payroll_ap: Option<bool>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettleApRequest {
    ap_obligation_id: Uuid,
    requested_by_agent_id: String,
    settlement_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettleApResponse {
    ap_obligation_id: Uuid,
    order_id: Uuid,
    source_type: String,
    previous_status: String,
    status: String,
    settled_amount: Decimal,
    outstanding_before: Decimal,
    outstanding_after: Decimal,
    settled_at: DateTime<Utc>,
    already_settled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpsertSkillRegistryRequest {
    skill_id: String,
    skill_version: String,
    capability: String,
    owner_agent_id: String,
    approval_status: String,
    required_input_fields: Vec<String>,
    required_output_fields: Vec<String>,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkillRegistryView {
    skill_id: String,
    skill_version: String,
    capability: String,
    owner_agent_id: String,
    approval_status: String,
    required_input_fields: Vec<String>,
    required_output_fields: Vec<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListSkillRegistryResponse {
    items: Vec<SkillRegistryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListSkillRegistryQuery {
    capability: Option<String>,
    approval_status: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpsertSkillRoutingRequest {
    intent: String,
    transaction_type: String,
    capability: String,
    primary_skill_id: String,
    primary_skill_version: String,
    fallback_skill_id: Option<String>,
    fallback_skill_version: Option<String>,
    max_retries: i32,
    escalation_action_type: Option<String>,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkillRoutingPolicyView {
    intent: String,
    transaction_type: String,
    capability: String,
    primary_skill_id: String,
    primary_skill_version: String,
    fallback_skill_id: Option<String>,
    fallback_skill_version: Option<String>,
    max_retries: i32,
    escalation_action_type: String,
    updated_by_agent_id: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListSkillRoutingResponse {
    items: Vec<SkillRoutingPolicyView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListSkillRoutingQuery {
    intent: Option<String>,
    transaction_type: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpsertStrategyOfferingRequest {
    offering_code: String,
    offering_type: String,
    name: String,
    unit_of_measure: String,
    default_unit_price: Option<Decimal>,
    currency: Option<String>,
    active: Option<bool>,
    owner_agent_id: String,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrategyOfferingView {
    id: Uuid,
    offering_code: String,
    offering_type: String,
    name: String,
    unit_of_measure: String,
    default_unit_price: Option<Decimal>,
    currency: String,
    active: bool,
    owner_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListStrategyOfferingsQuery {
    offering_type: Option<String>,
    active: Option<bool>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListStrategyOfferingsResponse {
    items: Vec<StrategyOfferingView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpsertKpiTargetRequest {
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    target_value: Decimal,
    warning_threshold_pct: Option<Decimal>,
    critical_threshold_pct: Option<Decimal>,
    currency: Option<String>,
    updated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KpiTargetView {
    id: Uuid,
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    target_value: Decimal,
    warning_threshold_pct: Decimal,
    critical_threshold_pct: Decimal,
    currency: String,
    updated_by_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListKpiTargetsQuery {
    period_start: Option<NaiveDate>,
    period_end: Option<NaiveDate>,
    business_unit: Option<String>,
    mandate: Option<String>,
    metric_name: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListKpiTargetsResponse {
    items: Vec<KpiTargetView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpsertForecastRequest {
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    forecast_value: Decimal,
    confidence_pct: Option<Decimal>,
    assumptions_json: Option<Value>,
    currency: Option<String>,
    generated_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ForecastView {
    id: Uuid,
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    forecast_value: Decimal,
    confidence_pct: Option<Decimal>,
    assumptions_json: Value,
    currency: String,
    generated_by_agent_id: String,
    generated_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListForecastsQuery {
    period_start: Option<NaiveDate>,
    period_end: Option<NaiveDate>,
    business_unit: Option<String>,
    mandate: Option<String>,
    metric_name: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListForecastsResponse {
    items: Vec<ForecastView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvaluateVarianceRequest {
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    actual_value: Option<Decimal>,
    notes: Option<String>,
    requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvaluateVarianceResponse {
    variance_id: Uuid,
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    target_value: Decimal,
    actual_value: Decimal,
    forecast_value: Option<Decimal>,
    variance_amount: Decimal,
    variance_pct: Decimal,
    severity: String,
    corrective_action_id: Option<Uuid>,
    escalation_id: Option<Uuid>,
    evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrategyVarianceView {
    id: Uuid,
    period_start: NaiveDate,
    period_end: NaiveDate,
    business_unit: String,
    mandate: String,
    metric_name: String,
    target_value: Decimal,
    actual_value: Decimal,
    forecast_value: Option<Decimal>,
    variance_amount: Decimal,
    variance_pct: Decimal,
    severity: String,
    evaluated_by_agent_id: String,
    evaluated_at: DateTime<Utc>,
    notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListVariancesQuery {
    period_start: Option<NaiveDate>,
    period_end: Option<NaiveDate>,
    business_unit: Option<String>,
    mandate: Option<String>,
    metric_name: Option<String>,
    severity: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListVariancesResponse {
    items: Vec<StrategyVarianceView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrategyCorrectiveActionView {
    id: Uuid,
    variance_id: Uuid,
    status: String,
    reason_code: String,
    action_note: Option<String>,
    linked_escalation_id: Option<Uuid>,
    created_by_agent_id: String,
    created_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListCorrectiveActionsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListCorrectiveActionsResponse {
    items: Vec<StrategyCorrectiveActionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestEmailProofRequest {
    message_id: String,
    from_email: String,
    to_email: Option<String>,
    subject: Option<String>,
    body_excerpt: Option<String>,
    metadata: Option<Value>,
    contact_email: Option<String>,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    auto_create_lead: Option<bool>,
    lead_note: Option<String>,
    received_at: Option<DateTime<Utc>>,
    requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestWebhookProofRequest {
    event_id: String,
    source_system: String,
    event_type: String,
    contact_email: Option<String>,
    payload: Option<Value>,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    auto_create_lead: Option<bool>,
    lead_note: Option<String>,
    received_at: Option<DateTime<Utc>>,
    requested_by_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OriginationProofResponse {
    proof_id: Uuid,
    proof_ref: String,
    channel_type: String,
    message_id: String,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    contact_email: Option<String>,
    captured_at: DateTime<Utc>,
    deduplicated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListOriginationProofsQuery {
    channel_type: Option<String>,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OriginationProofView {
    proof_id: Uuid,
    proof_ref: String,
    channel_type: String,
    message_id: String,
    contact_email: Option<String>,
    subject: Option<String>,
    source_ref: Option<String>,
    payload_json: Value,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    captured_by_agent_id: String,
    received_at: DateTime<Utc>,
    captured_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListOriginationProofsResponse {
    items: Vec<OriginationProofView>,
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
        .route(
            "/origination/proofs/email",
            post(ingest_email_origination_proof),
        )
        .route(
            "/origination/proofs/webhook",
            post(ingest_webhook_origination_proof),
        )
        .route("/origination/proofs", get(list_origination_proofs))
        .route(
            "/strategy/offerings",
            get(list_strategy_offerings).post(upsert_strategy_offering),
        )
        .route(
            "/strategy/kpi-targets",
            get(list_kpi_targets).post(upsert_kpi_target),
        )
        .route(
            "/strategy/forecasts",
            get(list_strategy_forecasts).post(upsert_strategy_forecast),
        )
        .route(
            "/strategy/variance/evaluate",
            post(evaluate_strategy_variance),
        )
        .route("/strategy/variance", get(list_strategy_variances))
        .route(
            "/strategy/corrective-actions",
            get(list_strategy_corrective_actions),
        )
        .route("/governance/thresholds", post(set_threshold))
        .route("/governance/freeze", post(set_freeze))
        .route("/governance/escalations", get(list_escalations))
        .route("/finops/token-usage", post(ingest_token_usage))
        .route("/finops/cloud-costs", post(ingest_cloud_cost))
        .route("/finops/subscriptions", post(ingest_subscription_cost))
        .route("/finops/allocate", post(allocate_costs))
        .route("/finance/ap/settle", post(settle_ap))
        .route("/finops/payroll-ap/settle", post(settle_payroll_ap))
        .route(
            "/skills/registry",
            get(list_skill_registry).post(upsert_skill_registry),
        )
        .route(
            "/skills/routing",
            get(list_skill_routing).post(upsert_skill_routing),
        )
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

async fn upsert_strategy_offering(
    State(state): State<AppState>,
    Json(payload): Json<UpsertStrategyOfferingRequest>,
) -> Result<Json<StrategyOfferingView>, (StatusCode, String)> {
    validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let offering_code = payload.offering_code.trim().to_ascii_uppercase();
    if offering_code.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "offering_code is required".to_string(),
        ));
    }

    let offering_type = normalize_offering_type(&payload.offering_type)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let name = payload.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name is required".to_string()));
    }

    let unit_of_measure = payload.unit_of_measure.trim().to_string();
    if unit_of_measure.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "unit_of_measure is required".to_string(),
        ));
    }

    if let Some(default_unit_price) = payload.default_unit_price {
        if default_unit_price < Decimal::ZERO {
            return Err((
                StatusCode::BAD_REQUEST,
                "default_unit_price must be non-negative".to_string(),
            ));
        }
    }

    let currency = payload
        .currency
        .as_deref()
        .map(normalize_currency)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?
        .unwrap_or_else(|| "USD".to_string());
    let owner_agent_id = validate_agent_id(&payload.owner_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let active = payload.active.unwrap_or(true);
    let now = Utc::now();

    let row = sqlx::query(
        r#"
        INSERT INTO strategy_offerings (
            id,
            offering_code,
            offering_type,
            name,
            unit_of_measure,
            default_unit_price,
            currency,
            active,
            owner_agent_id,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
        ON CONFLICT (offering_code)
        DO UPDATE SET
            offering_type = EXCLUDED.offering_type,
            name = EXCLUDED.name,
            unit_of_measure = EXCLUDED.unit_of_measure,
            default_unit_price = EXCLUDED.default_unit_price,
            currency = EXCLUDED.currency,
            active = EXCLUDED.active,
            owner_agent_id = EXCLUDED.owner_agent_id,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            offering_code,
            offering_type,
            name,
            unit_of_measure,
            default_unit_price,
            currency,
            active,
            owner_agent_id,
            created_at,
            updated_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&offering_code)
    .bind(&offering_type)
    .bind(&name)
    .bind(&unit_of_measure)
    .bind(payload.default_unit_price)
    .bind(&currency)
    .bind(active)
    .bind(&owner_agent_id)
    .bind(now)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(StrategyOfferingView {
        id: row.try_get("id").map_err(internal_error)?,
        offering_code: row.try_get("offering_code").map_err(internal_error)?,
        offering_type: row.try_get("offering_type").map_err(internal_error)?,
        name: row.try_get("name").map_err(internal_error)?,
        unit_of_measure: row.try_get("unit_of_measure").map_err(internal_error)?,
        default_unit_price: row.try_get("default_unit_price").map_err(internal_error)?,
        currency: row.try_get("currency").map_err(internal_error)?,
        active: row.try_get("active").map_err(internal_error)?,
        owner_agent_id: row.try_get("owner_agent_id").map_err(internal_error)?,
        created_at: row.try_get("created_at").map_err(internal_error)?,
        updated_at: row.try_get("updated_at").map_err(internal_error)?,
    }))
}

async fn list_strategy_offerings(
    State(state): State<AppState>,
    Query(query): Query<ListStrategyOfferingsQuery>,
) -> Result<Json<ListStrategyOfferingsResponse>, (StatusCode, String)> {
    let offering_type = query
        .offering_type
        .as_deref()
        .map(normalize_offering_type)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            offering_code,
            offering_type,
            name,
            unit_of_measure,
            default_unit_price,
            currency,
            active,
            owner_agent_id,
            created_at,
            updated_at
        FROM strategy_offerings
        WHERE ($1::text IS NULL OR offering_type = $1)
          AND ($2::boolean IS NULL OR active = $2)
        ORDER BY updated_at DESC, offering_code ASC
        LIMIT $3
        "#,
    )
    .bind(offering_type)
    .bind(query.active)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(StrategyOfferingView {
            id: row.try_get("id").map_err(internal_error)?,
            offering_code: row.try_get("offering_code").map_err(internal_error)?,
            offering_type: row.try_get("offering_type").map_err(internal_error)?,
            name: row.try_get("name").map_err(internal_error)?,
            unit_of_measure: row.try_get("unit_of_measure").map_err(internal_error)?,
            default_unit_price: row.try_get("default_unit_price").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            active: row.try_get("active").map_err(internal_error)?,
            owner_agent_id: row.try_get("owner_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListStrategyOfferingsResponse { items }))
}

async fn upsert_kpi_target(
    State(state): State<AppState>,
    Json(payload): Json<UpsertKpiTargetRequest>,
) -> Result<Json<KpiTargetView>, (StatusCode, String)> {
    let updated_by_agent_id = validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    validate_period_range(payload.period_start, payload.period_end)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let business_unit = normalize_strategy_key(&payload.business_unit, "business_unit")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = normalize_strategy_key(&payload.mandate, "mandate")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = normalize_metric_name(&payload.metric_name)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    if payload.target_value < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "target_value must be non-negative".to_string(),
        ));
    }

    let warning_threshold_pct = payload
        .warning_threshold_pct
        .unwrap_or_else(default_warning_threshold_pct);
    let critical_threshold_pct = payload
        .critical_threshold_pct
        .unwrap_or_else(default_critical_threshold_pct);

    if warning_threshold_pct < Decimal::ZERO || critical_threshold_pct < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "threshold percentages must be non-negative".to_string(),
        ));
    }
    if critical_threshold_pct < warning_threshold_pct {
        return Err((
            StatusCode::BAD_REQUEST,
            "critical_threshold_pct must be greater than or equal to warning_threshold_pct"
                .to_string(),
        ));
    }

    let currency = payload
        .currency
        .as_deref()
        .map(normalize_currency)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?
        .unwrap_or_else(|| "USD".to_string());
    let now = Utc::now();

    let row = sqlx::query(
        r#"
        INSERT INTO strategy_kpi_targets (
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            target_value,
            warning_threshold_pct,
            critical_threshold_pct,
            currency,
            updated_by_agent_id,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $12
        )
        ON CONFLICT (period_start, period_end, business_unit, mandate, metric_name)
        DO UPDATE SET
            target_value = EXCLUDED.target_value,
            warning_threshold_pct = EXCLUDED.warning_threshold_pct,
            critical_threshold_pct = EXCLUDED.critical_threshold_pct,
            currency = EXCLUDED.currency,
            updated_by_agent_id = EXCLUDED.updated_by_agent_id,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            target_value,
            warning_threshold_pct,
            critical_threshold_pct,
            currency,
            updated_by_agent_id,
            created_at,
            updated_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(&business_unit)
    .bind(&mandate)
    .bind(&metric_name)
    .bind(payload.target_value)
    .bind(warning_threshold_pct)
    .bind(critical_threshold_pct)
    .bind(&currency)
    .bind(&updated_by_agent_id)
    .bind(now)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(KpiTargetView {
        id: row.try_get("id").map_err(internal_error)?,
        period_start: row.try_get("period_start").map_err(internal_error)?,
        period_end: row.try_get("period_end").map_err(internal_error)?,
        business_unit: row.try_get("business_unit").map_err(internal_error)?,
        mandate: row.try_get("mandate").map_err(internal_error)?,
        metric_name: row.try_get("metric_name").map_err(internal_error)?,
        target_value: row.try_get("target_value").map_err(internal_error)?,
        warning_threshold_pct: row
            .try_get("warning_threshold_pct")
            .map_err(internal_error)?,
        critical_threshold_pct: row
            .try_get("critical_threshold_pct")
            .map_err(internal_error)?,
        currency: row.try_get("currency").map_err(internal_error)?,
        updated_by_agent_id: row.try_get("updated_by_agent_id").map_err(internal_error)?,
        created_at: row.try_get("created_at").map_err(internal_error)?,
        updated_at: row.try_get("updated_at").map_err(internal_error)?,
    }))
}

async fn list_kpi_targets(
    State(state): State<AppState>,
    Query(query): Query<ListKpiTargetsQuery>,
) -> Result<Json<ListKpiTargetsResponse>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let business_unit = query
        .business_unit
        .as_deref()
        .map(|value| normalize_strategy_key(value, "business_unit"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = query
        .mandate
        .as_deref()
        .map(|value| normalize_strategy_key(value, "mandate"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = query
        .metric_name
        .as_deref()
        .map(normalize_metric_name)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            target_value,
            warning_threshold_pct,
            critical_threshold_pct,
            currency,
            updated_by_agent_id,
            created_at,
            updated_at
        FROM strategy_kpi_targets
        WHERE ($1::date IS NULL OR period_start >= $1)
          AND ($2::date IS NULL OR period_end <= $2)
          AND ($3::text IS NULL OR business_unit = $3)
          AND ($4::text IS NULL OR mandate = $4)
          AND ($5::text IS NULL OR metric_name = $5)
        ORDER BY period_start DESC, business_unit ASC, mandate ASC, metric_name ASC
        LIMIT $6
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .bind(business_unit)
    .bind(mandate)
    .bind(metric_name)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(KpiTargetView {
            id: row.try_get("id").map_err(internal_error)?,
            period_start: row.try_get("period_start").map_err(internal_error)?,
            period_end: row.try_get("period_end").map_err(internal_error)?,
            business_unit: row.try_get("business_unit").map_err(internal_error)?,
            mandate: row.try_get("mandate").map_err(internal_error)?,
            metric_name: row.try_get("metric_name").map_err(internal_error)?,
            target_value: row.try_get("target_value").map_err(internal_error)?,
            warning_threshold_pct: row
                .try_get("warning_threshold_pct")
                .map_err(internal_error)?,
            critical_threshold_pct: row
                .try_get("critical_threshold_pct")
                .map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            updated_by_agent_id: row.try_get("updated_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListKpiTargetsResponse { items }))
}

async fn upsert_strategy_forecast(
    State(state): State<AppState>,
    Json(payload): Json<UpsertForecastRequest>,
) -> Result<Json<ForecastView>, (StatusCode, String)> {
    let generated_by_agent_id = validate_governance_actor(&payload.generated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    validate_period_range(payload.period_start, payload.period_end)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let business_unit = normalize_strategy_key(&payload.business_unit, "business_unit")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = normalize_strategy_key(&payload.mandate, "mandate")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = normalize_metric_name(&payload.metric_name)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    if payload.forecast_value < Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            "forecast_value must be non-negative".to_string(),
        ));
    }

    let confidence_pct = payload.confidence_pct.map(|value| value.round_dp(4));
    if let Some(value) = confidence_pct {
        if value < Decimal::ZERO || value > Decimal::new(100, 0) {
            return Err((
                StatusCode::BAD_REQUEST,
                "confidence_pct must be between 0 and 100".to_string(),
            ));
        }
    }

    let assumptions_json = payload.assumptions_json.unwrap_or_else(|| json!({}));
    let currency = payload
        .currency
        .as_deref()
        .map(normalize_currency)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?
        .unwrap_or_else(|| "USD".to_string());
    let now = Utc::now();

    let row = sqlx::query(
        r#"
        INSERT INTO strategy_forecasts (
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            forecast_value,
            confidence_pct,
            assumptions_json,
            currency,
            generated_by_agent_id,
            generated_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $12)
        ON CONFLICT (period_start, period_end, business_unit, mandate, metric_name)
        DO UPDATE SET
            forecast_value = EXCLUDED.forecast_value,
            confidence_pct = EXCLUDED.confidence_pct,
            assumptions_json = EXCLUDED.assumptions_json,
            currency = EXCLUDED.currency,
            generated_by_agent_id = EXCLUDED.generated_by_agent_id,
            generated_at = EXCLUDED.generated_at,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            forecast_value,
            confidence_pct,
            assumptions_json,
            currency,
            generated_by_agent_id,
            generated_at,
            updated_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(&business_unit)
    .bind(&mandate)
    .bind(&metric_name)
    .bind(payload.forecast_value)
    .bind(confidence_pct)
    .bind(assumptions_json)
    .bind(&currency)
    .bind(&generated_by_agent_id)
    .bind(now)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(ForecastView {
        id: row.try_get("id").map_err(internal_error)?,
        period_start: row.try_get("period_start").map_err(internal_error)?,
        period_end: row.try_get("period_end").map_err(internal_error)?,
        business_unit: row.try_get("business_unit").map_err(internal_error)?,
        mandate: row.try_get("mandate").map_err(internal_error)?,
        metric_name: row.try_get("metric_name").map_err(internal_error)?,
        forecast_value: row.try_get("forecast_value").map_err(internal_error)?,
        confidence_pct: row.try_get("confidence_pct").map_err(internal_error)?,
        assumptions_json: row.try_get("assumptions_json").map_err(internal_error)?,
        currency: row.try_get("currency").map_err(internal_error)?,
        generated_by_agent_id: row
            .try_get("generated_by_agent_id")
            .map_err(internal_error)?,
        generated_at: row.try_get("generated_at").map_err(internal_error)?,
        updated_at: row.try_get("updated_at").map_err(internal_error)?,
    }))
}

async fn list_strategy_forecasts(
    State(state): State<AppState>,
    Query(query): Query<ListForecastsQuery>,
) -> Result<Json<ListForecastsResponse>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let business_unit = query
        .business_unit
        .as_deref()
        .map(|value| normalize_strategy_key(value, "business_unit"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = query
        .mandate
        .as_deref()
        .map(|value| normalize_strategy_key(value, "mandate"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = query
        .metric_name
        .as_deref()
        .map(normalize_metric_name)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            forecast_value,
            confidence_pct,
            assumptions_json,
            currency,
            generated_by_agent_id,
            generated_at,
            updated_at
        FROM strategy_forecasts
        WHERE ($1::date IS NULL OR period_start >= $1)
          AND ($2::date IS NULL OR period_end <= $2)
          AND ($3::text IS NULL OR business_unit = $3)
          AND ($4::text IS NULL OR mandate = $4)
          AND ($5::text IS NULL OR metric_name = $5)
        ORDER BY generated_at DESC, business_unit ASC, mandate ASC, metric_name ASC
        LIMIT $6
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .bind(business_unit)
    .bind(mandate)
    .bind(metric_name)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(ForecastView {
            id: row.try_get("id").map_err(internal_error)?,
            period_start: row.try_get("period_start").map_err(internal_error)?,
            period_end: row.try_get("period_end").map_err(internal_error)?,
            business_unit: row.try_get("business_unit").map_err(internal_error)?,
            mandate: row.try_get("mandate").map_err(internal_error)?,
            metric_name: row.try_get("metric_name").map_err(internal_error)?,
            forecast_value: row.try_get("forecast_value").map_err(internal_error)?,
            confidence_pct: row.try_get("confidence_pct").map_err(internal_error)?,
            assumptions_json: row.try_get("assumptions_json").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            generated_by_agent_id: row
                .try_get("generated_by_agent_id")
                .map_err(internal_error)?,
            generated_at: row.try_get("generated_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListForecastsResponse { items }))
}

async fn evaluate_strategy_variance(
    State(state): State<AppState>,
    Json(payload): Json<EvaluateVarianceRequest>,
) -> Result<Json<EvaluateVarianceResponse>, (StatusCode, String)> {
    let requested_by_agent_id = validate_governance_actor(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    validate_period_range(payload.period_start, payload.period_end)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let business_unit = normalize_strategy_key(&payload.business_unit, "business_unit")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = normalize_strategy_key(&payload.mandate, "mandate")
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = normalize_metric_name(&payload.metric_name)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let notes = payload
        .notes
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let (period_start_at, period_end_exclusive) =
        period_bounds(payload.period_start, payload.period_end)
            .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let mut tx = state.pool.begin().await.map_err(internal_error)?;

    let target_row = sqlx::query(
        r#"
        SELECT target_value, warning_threshold_pct, critical_threshold_pct, currency
        FROM strategy_kpi_targets
        WHERE period_start = $1
          AND period_end = $2
          AND business_unit = $3
          AND mandate = $4
          AND metric_name = $5
        LIMIT 1
        "#,
    )
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(&business_unit)
    .bind(&mandate)
    .bind(&metric_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_error)?;

    let Some(target_row) = target_row else {
        return Err((
            StatusCode::NOT_FOUND,
            "kpi target not found for requested key".to_string(),
        ));
    };

    let target_value: Decimal = target_row.try_get("target_value").map_err(internal_error)?;
    let warning_threshold_pct: Decimal = target_row
        .try_get("warning_threshold_pct")
        .map_err(internal_error)?;
    let critical_threshold_pct: Decimal = target_row
        .try_get("critical_threshold_pct")
        .map_err(internal_error)?;
    let currency: String = target_row.try_get("currency").map_err(internal_error)?;

    let forecast_value = sqlx::query_scalar::<_, Option<Decimal>>(
        r#"
        SELECT forecast_value
        FROM strategy_forecasts
        WHERE period_start = $1
          AND period_end = $2
          AND business_unit = $3
          AND mandate = $4
          AND metric_name = $5
        ORDER BY generated_at DESC
        LIMIT 1
        "#,
    )
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(&business_unit)
    .bind(&mandate)
    .bind(&metric_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_error)?
    .flatten();

    let actual_value = if let Some(actual_value) = payload.actual_value {
        if actual_value < Decimal::ZERO {
            return Err((
                StatusCode::BAD_REQUEST,
                "actual_value must be non-negative".to_string(),
            ));
        }
        actual_value.round_dp(4)
    } else {
        derive_actual_metric_from_ledger(
            &mut tx,
            &metric_name,
            period_start_at,
            period_end_exclusive,
        )
        .await
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?
    };

    let variance_amount = (actual_value - target_value).abs().round_dp(4);
    let variance_pct = if target_value > Decimal::ZERO {
        (variance_amount / target_value * Decimal::new(100, 0)).round_dp(4)
    } else {
        Decimal::ZERO
    };
    let severity =
        classify_variance_severity(variance_pct, warning_threshold_pct, critical_threshold_pct);

    let variance_id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO strategy_variances (
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            target_value,
            actual_value,
            forecast_value,
            variance_amount,
            variance_pct,
            severity,
            evaluated_by_agent_id,
            evaluated_at,
            notes
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
        "#,
    )
    .bind(variance_id)
    .bind(payload.period_start)
    .bind(payload.period_end)
    .bind(&business_unit)
    .bind(&mandate)
    .bind(&metric_name)
    .bind(target_value)
    .bind(actual_value)
    .bind(forecast_value)
    .bind(variance_amount)
    .bind(variance_pct)
    .bind(&severity)
    .bind(&requested_by_agent_id)
    .bind(now)
    .bind(notes.as_deref())
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    let mut corrective_action_id = None;
    let mut escalation_id = None;

    if severity == "BREACH" {
        let created_escalation_id = Uuid::new_v4();
        let breach_reason = format!(
            "{} variance breach for {} {}",
            metric_name, business_unit, mandate
        );
        sqlx::query(
            r#"
            INSERT INTO governance_escalations (
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
                decision_note
            )
            VALUES (
                $1, 'STRATEGY_VARIANCE_BREACH', 'STRATEGY_VARIANCE', $2, 'PENDING', 'VARIANCE_BREACH', $3, $4, $5, $6, $7
            )
            "#,
        )
        .bind(created_escalation_id)
        .bind(variance_id)
        .bind(variance_amount)
        .bind(&currency)
        .bind(&requested_by_agent_id)
        .bind(now)
        .bind(&breach_reason)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

        let created_action_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO strategy_corrective_actions (
                id,
                variance_id,
                status,
                reason_code,
                action_note,
                linked_escalation_id,
                created_by_agent_id,
                created_at
            )
            VALUES ($1, $2, 'OPEN', 'VARIANCE_BREACH', $3, $4, $5, $6)
            "#,
        )
        .bind(created_action_id)
        .bind(variance_id)
        .bind(notes.as_deref())
        .bind(created_escalation_id)
        .bind(&requested_by_agent_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

        corrective_action_id = Some(created_action_id);
        escalation_id = Some(created_escalation_id);
    }

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(EvaluateVarianceResponse {
        variance_id,
        period_start: payload.period_start,
        period_end: payload.period_end,
        business_unit,
        mandate,
        metric_name,
        target_value,
        actual_value,
        forecast_value,
        variance_amount,
        variance_pct,
        severity,
        corrective_action_id,
        escalation_id,
        evaluated_at: now,
    }))
}

async fn list_strategy_variances(
    State(state): State<AppState>,
    Query(query): Query<ListVariancesQuery>,
) -> Result<Json<ListVariancesResponse>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let business_unit = query
        .business_unit
        .as_deref()
        .map(|value| normalize_strategy_key(value, "business_unit"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let mandate = query
        .mandate
        .as_deref()
        .map(|value| normalize_strategy_key(value, "mandate"))
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let metric_name = query
        .metric_name
        .as_deref()
        .map(normalize_metric_name)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let severity = query
        .severity
        .as_deref()
        .map(normalize_variance_severity)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            period_start,
            period_end,
            business_unit,
            mandate,
            metric_name,
            target_value,
            actual_value,
            forecast_value,
            variance_amount,
            variance_pct,
            severity,
            evaluated_by_agent_id,
            evaluated_at,
            notes
        FROM strategy_variances
        WHERE ($1::date IS NULL OR period_start >= $1)
          AND ($2::date IS NULL OR period_end <= $2)
          AND ($3::text IS NULL OR business_unit = $3)
          AND ($4::text IS NULL OR mandate = $4)
          AND ($5::text IS NULL OR metric_name = $5)
          AND ($6::text IS NULL OR severity = $6)
        ORDER BY evaluated_at DESC
        LIMIT $7
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .bind(business_unit)
    .bind(mandate)
    .bind(metric_name)
    .bind(severity)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(StrategyVarianceView {
            id: row.try_get("id").map_err(internal_error)?,
            period_start: row.try_get("period_start").map_err(internal_error)?,
            period_end: row.try_get("period_end").map_err(internal_error)?,
            business_unit: row.try_get("business_unit").map_err(internal_error)?,
            mandate: row.try_get("mandate").map_err(internal_error)?,
            metric_name: row.try_get("metric_name").map_err(internal_error)?,
            target_value: row.try_get("target_value").map_err(internal_error)?,
            actual_value: row.try_get("actual_value").map_err(internal_error)?,
            forecast_value: row.try_get("forecast_value").map_err(internal_error)?,
            variance_amount: row.try_get("variance_amount").map_err(internal_error)?,
            variance_pct: row.try_get("variance_pct").map_err(internal_error)?,
            severity: row.try_get("severity").map_err(internal_error)?,
            evaluated_by_agent_id: row
                .try_get("evaluated_by_agent_id")
                .map_err(internal_error)?,
            evaluated_at: row.try_get("evaluated_at").map_err(internal_error)?,
            notes: row.try_get("notes").map_err(internal_error)?,
        });
    }

    Ok(Json(ListVariancesResponse { items }))
}

async fn list_strategy_corrective_actions(
    State(state): State<AppState>,
    Query(query): Query<ListCorrectiveActionsQuery>,
) -> Result<Json<ListCorrectiveActionsResponse>, (StatusCode, String)> {
    let status = query
        .status
        .as_deref()
        .map(normalize_corrective_action_status)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            variance_id,
            status,
            reason_code,
            action_note,
            linked_escalation_id,
            created_by_agent_id,
            created_at,
            closed_at
        FROM strategy_corrective_actions
        WHERE ($1::text IS NULL OR status = $1)
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(status)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(StrategyCorrectiveActionView {
            id: row.try_get("id").map_err(internal_error)?,
            variance_id: row.try_get("variance_id").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            reason_code: row.try_get("reason_code").map_err(internal_error)?,
            action_note: row.try_get("action_note").map_err(internal_error)?,
            linked_escalation_id: row
                .try_get("linked_escalation_id")
                .map_err(internal_error)?,
            created_by_agent_id: row.try_get("created_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            closed_at: row.try_get("closed_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListCorrectiveActionsResponse { items }))
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

async fn ingest_email_origination_proof(
    State(state): State<AppState>,
    Json(payload): Json<IngestEmailProofRequest>,
) -> Result<(StatusCode, Json<OriginationProofResponse>), (StatusCode, String)> {
    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let message_id = payload.message_id.trim();
    if message_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "message_id is required".to_string(),
        ));
    }

    let from_email = payload.from_email.trim();
    if from_email.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "from_email is required".to_string(),
        ));
    }

    let to_email = payload
        .to_email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let subject = payload
        .subject
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let body_excerpt = payload
        .body_excerpt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let lead_note = payload
        .lead_note
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| subject.clone())
        .or_else(|| body_excerpt.clone());
    let contact_email = payload
        .contact_email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(from_email)
        .to_string();
    let auto_create_lead = payload.auto_create_lead.unwrap_or(true);

    if let Some(existing) = lookup_origination_proof(&state.pool, "EMAIL", message_id).await? {
        return Ok((StatusCode::OK, Json(existing)));
    }

    let mut tx = state.pool.begin().await.map_err(internal_error)?;
    let links = validate_origination_links(
        &mut tx,
        payload.lead_id,
        payload.opportunity_id,
        payload.quote_id,
        payload.acceptance_id,
    )
    .await?;
    let lead_id = match links.lead_id {
        Some(lead_id) => Some(lead_id),
        None if auto_create_lead => Some(
            create_linked_lead(
                &mut tx,
                &contact_email,
                "EMAIL",
                lead_note.as_deref(),
                &requested_by_agent_id,
            )
            .await
            .map_err(internal_error)?,
        ),
        _ => None,
    };

    let now = Utc::now();
    let proof_id = Uuid::new_v4();
    let proof_ref = format!("origination-proof:{proof_id}");
    let received_at = payload.received_at.unwrap_or(now);

    sqlx::query(
        r#"
        INSERT INTO origination_channel_proofs (
            id,
            proof_ref,
            channel_type,
            message_id,
            contact_email,
            subject,
            source_ref,
            payload_json,
            lead_id,
            opportunity_id,
            quote_id,
            acceptance_id,
            captured_by_agent_id,
            received_at,
            captured_at
        )
        VALUES ($1, $2, 'EMAIL', $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13, $14)
        "#,
    )
    .bind(proof_id)
    .bind(&proof_ref)
    .bind(message_id)
    .bind(&contact_email)
    .bind(subject.as_deref())
    .bind(format!("email:{message_id}"))
    .bind(json!({
        "from_email": from_email,
        "to_email": to_email,
        "subject": subject,
        "body_excerpt": body_excerpt,
        "metadata": payload.metadata.unwrap_or_else(|| json!({})),
    }))
    .bind(lead_id)
    .bind(links.opportunity_id)
    .bind(links.quote_id)
    .bind(links.acceptance_id)
    .bind(&requested_by_agent_id)
    .bind(received_at)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(OriginationProofResponse {
            proof_id,
            proof_ref,
            channel_type: "EMAIL".to_string(),
            message_id: message_id.to_string(),
            lead_id,
            opportunity_id: links.opportunity_id,
            quote_id: links.quote_id,
            acceptance_id: links.acceptance_id,
            contact_email: Some(contact_email),
            captured_at: now,
            deduplicated: false,
        }),
    ))
}

async fn ingest_webhook_origination_proof(
    State(state): State<AppState>,
    Json(payload): Json<IngestWebhookProofRequest>,
) -> Result<(StatusCode, Json<OriginationProofResponse>), (StatusCode, String)> {
    let requested_by_agent_id = validate_agent_id(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let event_id = payload.event_id.trim();
    if event_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "event_id is required".to_string()));
    }
    let source_system = payload.source_system.trim();
    if source_system.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "source_system is required".to_string(),
        ));
    }
    let event_type = payload.event_type.trim();
    if event_type.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "event_type is required".to_string(),
        ));
    }
    let contact_email = payload
        .contact_email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let lead_note = payload
        .lead_note
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| Some(format!("Webhook {} event {}", source_system, event_type)));
    let auto_create_lead = payload.auto_create_lead.unwrap_or(true);

    if let Some(existing) = lookup_origination_proof(&state.pool, "WEBHOOK", event_id).await? {
        return Ok((StatusCode::OK, Json(existing)));
    }

    let mut tx = state.pool.begin().await.map_err(internal_error)?;
    let links = validate_origination_links(
        &mut tx,
        payload.lead_id,
        payload.opportunity_id,
        payload.quote_id,
        payload.acceptance_id,
    )
    .await?;
    let lead_id = match (links.lead_id, auto_create_lead, contact_email.as_deref()) {
        (Some(lead_id), _, _) => Some(lead_id),
        (None, true, Some(contact_email)) => Some(
            create_linked_lead(
                &mut tx,
                contact_email,
                "WEBHOOK",
                lead_note.as_deref(),
                &requested_by_agent_id,
            )
            .await
            .map_err(internal_error)?,
        ),
        _ => None,
    };

    let now = Utc::now();
    let proof_id = Uuid::new_v4();
    let proof_ref = format!("origination-proof:{proof_id}");
    let received_at = payload.received_at.unwrap_or(now);

    sqlx::query(
        r#"
        INSERT INTO origination_channel_proofs (
            id,
            proof_ref,
            channel_type,
            message_id,
            contact_email,
            subject,
            source_ref,
            payload_json,
            lead_id,
            opportunity_id,
            quote_id,
            acceptance_id,
            captured_by_agent_id,
            received_at,
            captured_at
        )
        VALUES ($1, $2, 'WEBHOOK', $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13, $14)
        "#,
    )
    .bind(proof_id)
    .bind(&proof_ref)
    .bind(event_id)
    .bind(contact_email.as_deref())
    .bind(format!("{source_system}:{event_type}"))
    .bind(format!("webhook:{source_system}:{event_id}"))
    .bind(json!({
        "source_system": source_system,
        "event_type": event_type,
        "payload": payload.payload.unwrap_or_else(|| json!({})),
    }))
    .bind(lead_id)
    .bind(links.opportunity_id)
    .bind(links.quote_id)
    .bind(links.acceptance_id)
    .bind(&requested_by_agent_id)
    .bind(received_at)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(OriginationProofResponse {
            proof_id,
            proof_ref,
            channel_type: "WEBHOOK".to_string(),
            message_id: event_id.to_string(),
            lead_id,
            opportunity_id: links.opportunity_id,
            quote_id: links.quote_id,
            acceptance_id: links.acceptance_id,
            contact_email,
            captured_at: now,
            deduplicated: false,
        }),
    ))
}

async fn list_origination_proofs(
    State(state): State<AppState>,
    Query(query): Query<ListOriginationProofsQuery>,
) -> Result<Json<ListOriginationProofsResponse>, (StatusCode, String)> {
    let channel_type = query
        .channel_type
        .as_deref()
        .map(normalize_origination_channel_type)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            proof_ref,
            channel_type,
            message_id,
            contact_email,
            subject,
            source_ref,
            payload_json,
            lead_id,
            opportunity_id,
            quote_id,
            acceptance_id,
            captured_by_agent_id,
            received_at,
            captured_at
        FROM origination_channel_proofs
        WHERE ($1::text IS NULL OR channel_type = $1)
          AND ($2::uuid IS NULL OR lead_id = $2)
          AND ($3::uuid IS NULL OR opportunity_id = $3)
          AND ($4::uuid IS NULL OR quote_id = $4)
          AND ($5::uuid IS NULL OR acceptance_id = $5)
        ORDER BY captured_at DESC
        LIMIT $6
        "#,
    )
    .bind(channel_type)
    .bind(query.lead_id)
    .bind(query.opportunity_id)
    .bind(query.quote_id)
    .bind(query.acceptance_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(OriginationProofView {
            proof_id: row.try_get("id").map_err(internal_error)?,
            proof_ref: row.try_get("proof_ref").map_err(internal_error)?,
            channel_type: row.try_get("channel_type").map_err(internal_error)?,
            message_id: row.try_get("message_id").map_err(internal_error)?,
            contact_email: row.try_get("contact_email").map_err(internal_error)?,
            subject: row.try_get("subject").map_err(internal_error)?,
            source_ref: row.try_get("source_ref").map_err(internal_error)?,
            payload_json: row.try_get("payload_json").map_err(internal_error)?,
            lead_id: row.try_get("lead_id").map_err(internal_error)?,
            opportunity_id: row.try_get("opportunity_id").map_err(internal_error)?,
            quote_id: row.try_get("quote_id").map_err(internal_error)?,
            acceptance_id: row.try_get("acceptance_id").map_err(internal_error)?,
            captured_by_agent_id: row
                .try_get("captured_by_agent_id")
                .map_err(internal_error)?,
            received_at: row.try_get("received_at").map_err(internal_error)?,
            captured_at: row.try_get("captured_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListOriginationProofsResponse { items }))
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

async fn upsert_skill_registry(
    State(state): State<AppState>,
    Json(payload): Json<UpsertSkillRegistryRequest>,
) -> Result<Json<SkillRegistryView>, (StatusCode, String)> {
    let actor = validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let owner_agent_id = validate_agent_id(&payload.owner_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let skill_id = payload.skill_id.trim();
    if skill_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "skill_id is required".to_string()));
    }
    let skill_version = payload.skill_version.trim();
    if skill_version.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "skill_version is required".to_string(),
        ));
    }
    let capability = payload.capability.trim();
    if capability.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "capability is required".to_string(),
        ));
    }

    let approval_status = normalize_skill_approval_status(&payload.approval_status)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let required_input_fields = normalize_required_fields(&payload.required_input_fields)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let required_output_fields = normalize_required_fields(&payload.required_output_fields)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO skill_registry (
            id,
            skill_id,
            skill_version,
            capability,
            owner_agent_id,
            approval_status,
            required_input_fields,
            required_output_fields,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
        ON CONFLICT (skill_id, skill_version)
        DO UPDATE SET
            capability = EXCLUDED.capability,
            owner_agent_id = EXCLUDED.owner_agent_id,
            approval_status = EXCLUDED.approval_status,
            required_input_fields = EXCLUDED.required_input_fields,
            required_output_fields = EXCLUDED.required_output_fields,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(skill_id)
    .bind(skill_version)
    .bind(capability)
    .bind(&owner_agent_id)
    .bind(&approval_status)
    .bind(&required_input_fields)
    .bind(&required_output_fields)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    let row = sqlx::query(
        r#"
        SELECT
            skill_id,
            skill_version,
            capability,
            owner_agent_id,
            approval_status,
            required_input_fields,
            required_output_fields,
            created_at,
            updated_at
        FROM skill_registry
        WHERE skill_id = $1 AND skill_version = $2
        "#,
    )
    .bind(skill_id)
    .bind(skill_version)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let view = SkillRegistryView {
        skill_id: row.try_get("skill_id").map_err(internal_error)?,
        skill_version: row.try_get("skill_version").map_err(internal_error)?,
        capability: row.try_get("capability").map_err(internal_error)?,
        owner_agent_id: row.try_get("owner_agent_id").map_err(internal_error)?,
        approval_status: row.try_get("approval_status").map_err(internal_error)?,
        required_input_fields: row
            .try_get("required_input_fields")
            .map_err(internal_error)?,
        required_output_fields: row
            .try_get("required_output_fields")
            .map_err(internal_error)?,
        created_at: row.try_get("created_at").map_err(internal_error)?,
        updated_at: row.try_get("updated_at").map_err(internal_error)?,
    };

    info!(
        "skill registry upserted skill={} version={} by {}",
        view.skill_id, view.skill_version, actor
    );
    Ok(Json(view))
}

async fn list_skill_registry(
    State(state): State<AppState>,
    Query(query): Query<ListSkillRegistryQuery>,
) -> Result<Json<ListSkillRegistryResponse>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let approval_status = query
        .approval_status
        .as_deref()
        .map(normalize_skill_approval_status)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let capability = query
        .capability
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let rows = sqlx::query(
        r#"
        SELECT
            skill_id,
            skill_version,
            capability,
            owner_agent_id,
            approval_status,
            required_input_fields,
            required_output_fields,
            created_at,
            updated_at
        FROM skill_registry
        WHERE ($1::text IS NULL OR capability = $1)
          AND ($2::text IS NULL OR approval_status = $2)
        ORDER BY updated_at DESC, skill_id ASC, skill_version ASC
        LIMIT $3
        "#,
    )
    .bind(capability)
    .bind(approval_status)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(SkillRegistryView {
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
            skill_version: row.try_get("skill_version").map_err(internal_error)?,
            capability: row.try_get("capability").map_err(internal_error)?,
            owner_agent_id: row.try_get("owner_agent_id").map_err(internal_error)?,
            approval_status: row.try_get("approval_status").map_err(internal_error)?,
            required_input_fields: row
                .try_get("required_input_fields")
                .map_err(internal_error)?,
            required_output_fields: row
                .try_get("required_output_fields")
                .map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListSkillRegistryResponse { items }))
}

async fn upsert_skill_routing(
    State(state): State<AppState>,
    Json(payload): Json<UpsertSkillRoutingRequest>,
) -> Result<Json<SkillRoutingPolicyView>, (StatusCode, String)> {
    let actor = validate_governance_actor(&payload.updated_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let intent = payload.intent.trim().to_ascii_uppercase();
    if intent.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "intent is required".to_string()));
    }
    let transaction_type = normalize_routing_transaction_type(&payload.transaction_type)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let capability = payload.capability.trim().to_string();
    if capability.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "capability is required".to_string(),
        ));
    }

    let primary_skill_id = payload.primary_skill_id.trim().to_string();
    let primary_skill_version = payload.primary_skill_version.trim().to_string();
    if primary_skill_id.is_empty() || primary_skill_version.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "primary_skill_id and primary_skill_version are required".to_string(),
        ));
    }

    let fallback_skill_id = payload
        .fallback_skill_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let fallback_skill_version = payload
        .fallback_skill_version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if fallback_skill_id.is_some() ^ fallback_skill_version.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "fallback_skill_id and fallback_skill_version must be provided together".to_string(),
        ));
    }
    if !(0..=5).contains(&payload.max_retries) {
        return Err((
            StatusCode::BAD_REQUEST,
            "max_retries must be between 0 and 5".to_string(),
        ));
    }

    let escalation_action_type = payload
        .escalation_action_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("SKILL_EXECUTION")
        .to_ascii_uppercase();

    let primary_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM skill_registry
            WHERE skill_id = $1
              AND skill_version = $2
              AND approval_status = 'APPROVED'
        )
        "#,
    )
    .bind(&primary_skill_id)
    .bind(&primary_skill_version)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;
    if !primary_exists {
        return Err((
            StatusCode::BAD_REQUEST,
            "primary skill must exist in APPROVED status".to_string(),
        ));
    }

    if let (Some(fallback_id), Some(fallback_version)) = (
        fallback_skill_id.as_deref(),
        fallback_skill_version.as_deref(),
    ) {
        let fallback_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM skill_registry
                WHERE skill_id = $1
                  AND skill_version = $2
                  AND approval_status = 'APPROVED'
            )
            "#,
        )
        .bind(fallback_id)
        .bind(fallback_version)
        .fetch_one(&state.pool)
        .await
        .map_err(internal_error)?;
        if !fallback_exists {
            return Err((
                StatusCode::BAD_REQUEST,
                "fallback skill must exist in APPROVED status".to_string(),
            ));
        }
    }

    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO skill_routing_policies (
            intent,
            transaction_type,
            capability,
            primary_skill_id,
            primary_skill_version,
            fallback_skill_id,
            fallback_skill_version,
            max_retries,
            escalation_action_type,
            updated_by_agent_id,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (intent, transaction_type)
        DO UPDATE SET
            capability = EXCLUDED.capability,
            primary_skill_id = EXCLUDED.primary_skill_id,
            primary_skill_version = EXCLUDED.primary_skill_version,
            fallback_skill_id = EXCLUDED.fallback_skill_id,
            fallback_skill_version = EXCLUDED.fallback_skill_version,
            max_retries = EXCLUDED.max_retries,
            escalation_action_type = EXCLUDED.escalation_action_type,
            updated_by_agent_id = EXCLUDED.updated_by_agent_id,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&intent)
    .bind(&transaction_type)
    .bind(&capability)
    .bind(&primary_skill_id)
    .bind(&primary_skill_version)
    .bind(&fallback_skill_id)
    .bind(&fallback_skill_version)
    .bind(payload.max_retries)
    .bind(&escalation_action_type)
    .bind(&actor)
    .bind(now)
    .execute(&state.pool)
    .await
    .map_err(internal_error)?;

    let row = sqlx::query(
        r#"
        SELECT
            intent,
            transaction_type,
            capability,
            primary_skill_id,
            primary_skill_version,
            fallback_skill_id,
            fallback_skill_version,
            max_retries,
            escalation_action_type,
            updated_by_agent_id,
            updated_at
        FROM skill_routing_policies
        WHERE intent = $1 AND transaction_type = $2
        "#,
    )
    .bind(&intent)
    .bind(&transaction_type)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(SkillRoutingPolicyView {
        intent: row.try_get("intent").map_err(internal_error)?,
        transaction_type: row.try_get("transaction_type").map_err(internal_error)?,
        capability: row.try_get("capability").map_err(internal_error)?,
        primary_skill_id: row.try_get("primary_skill_id").map_err(internal_error)?,
        primary_skill_version: row
            .try_get("primary_skill_version")
            .map_err(internal_error)?,
        fallback_skill_id: row.try_get("fallback_skill_id").map_err(internal_error)?,
        fallback_skill_version: row
            .try_get("fallback_skill_version")
            .map_err(internal_error)?,
        max_retries: row.try_get("max_retries").map_err(internal_error)?,
        escalation_action_type: row
            .try_get("escalation_action_type")
            .map_err(internal_error)?,
        updated_by_agent_id: row.try_get("updated_by_agent_id").map_err(internal_error)?,
        updated_at: row.try_get("updated_at").map_err(internal_error)?,
    }))
}

async fn list_skill_routing(
    State(state): State<AppState>,
    Query(query): Query<ListSkillRoutingQuery>,
) -> Result<Json<ListSkillRoutingResponse>, (StatusCode, String)> {
    let intent = query
        .intent
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase);
    let transaction_type = query
        .transaction_type
        .as_deref()
        .map(normalize_routing_transaction_type)
        .transpose()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        SELECT
            intent,
            transaction_type,
            capability,
            primary_skill_id,
            primary_skill_version,
            fallback_skill_id,
            fallback_skill_version,
            max_retries,
            escalation_action_type,
            updated_by_agent_id,
            updated_at
        FROM skill_routing_policies
        WHERE ($1::text IS NULL OR intent = $1)
          AND ($2::text IS NULL OR transaction_type = $2)
        ORDER BY updated_at DESC, intent ASC, transaction_type ASC
        LIMIT $3
        "#,
    )
    .bind(intent)
    .bind(transaction_type)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(SkillRoutingPolicyView {
            intent: row.try_get("intent").map_err(internal_error)?,
            transaction_type: row.try_get("transaction_type").map_err(internal_error)?,
            capability: row.try_get("capability").map_err(internal_error)?,
            primary_skill_id: row.try_get("primary_skill_id").map_err(internal_error)?,
            primary_skill_version: row
                .try_get("primary_skill_version")
                .map_err(internal_error)?,
            fallback_skill_id: row.try_get("fallback_skill_id").map_err(internal_error)?,
            fallback_skill_version: row
                .try_get("fallback_skill_version")
                .map_err(internal_error)?,
            max_retries: row.try_get("max_retries").map_err(internal_error)?,
            escalation_action_type: row
                .try_get("escalation_action_type")
                .map_err(internal_error)?,
            updated_by_agent_id: row.try_get("updated_by_agent_id").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(ListSkillRoutingResponse { items }))
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
    let skill_id = payload
        .skill_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

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
    .bind(skill_id)
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
    let settle_payroll_ap = payload.settle_payroll_ap.unwrap_or(true);
    let period_key = format!("{}|{}", period_start.to_rfc3339(), period_end.to_rfc3339());
    let order_ids: Vec<Uuid> = orders.iter().map(|order| order.order_id).collect();
    let delete_memo_pattern = format!("PAYROLL_ALLOC|{period_key}|%");
    let payroll_counterparty = format!("autonomy-payroll:auto:{period_key}");

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

    clear_period_payroll_ap_obligations(&mut tx, &order_ids, &payroll_counterparty)
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
            PAYROLL_AP_ACCOUNT,
            Decimal::ZERO,
            rounded_cost,
            &format!("{memo_prefix}|CREDIT"),
        )
        .await
        .map_err(internal_error)?;
        create_and_settle_payroll_ap_obligation(
            &mut tx,
            order_id,
            rounded_cost,
            &currency,
            &requested_by_agent_id,
            &payroll_counterparty,
            &memo_prefix,
            completed_at,
            settle_payroll_ap,
        )
        .await
        .map_err(internal_error)?;
        journal_total += rounded_cost;

        let memory_id = Uuid::new_v4();
        let memory_source_ref = format!("finops-period:{period_key}");
        sqlx::query(
            r#"
            INSERT INTO agent_semantic_memory (
                id, agent_name, scope, entity_id, content, keywords, source_ref, created_at
            )
            VALUES ($1, 'payroll-agent', 'ORDER_COST_ALLOCATION', $2, $3, $4, $5, $6)
            "#,
        )
        .bind(memory_id)
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
        .bind(&memory_source_ref)
        .bind(completed_at)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

        sqlx::query(
            r#"
            INSERT INTO agent_memory_provenance (
                id, memory_id, entity_id, action_type, actor_agent_id, source_ref, query_text, created_at
            )
            VALUES ($1, $2, $3, 'WRITE', $4, $5, NULL, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(memory_id)
        .bind(order_id)
        .bind("payroll-agent")
        .bind(&memory_source_ref)
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

        let skill_allocations = split_amount_by_skill(
            tx,
            period_start,
            period_end,
            order_id,
            amount,
            input.skill_id.as_deref(),
        )
        .await?;

        for (skill_id, skill_amount) in skill_allocations {
            if skill_amount <= Decimal::ZERO {
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
            .bind(skill_id.as_deref())
            .bind(basis)
            .bind(skill_amount.round_dp(4))
            .bind(input.currency.as_str())
            .bind(Utc::now())
            .execute(&mut **tx)
            .await?;

            allocated_total += skill_amount;
        }
    }

    Ok(allocated_total.round_dp(4))
}

async fn split_amount_by_skill(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    order_id: Uuid,
    amount: Decimal,
    explicit_skill_id: Option<&str>,
) -> AnyResult<Vec<(Option<String>, Decimal)>> {
    let amount = amount.round_dp(4);
    if amount <= Decimal::ZERO {
        return Ok(Vec::new());
    }

    if let Some(skill_id) = explicit_skill_id {
        let trimmed = skill_id.trim();
        if !trimmed.is_empty() {
            return Ok(vec![(Some(trimmed.to_string()), amount)]);
        }
    }

    let rows = sqlx::query(
        r#"
        SELECT
            skill_id,
            COALESCE(SUM(total_cost), 0) AS skill_cost
        FROM finops_token_usage
        WHERE order_id = $1
          AND occurred_at >= $2
          AND occurred_at < $3
          AND skill_id IS NOT NULL
          AND BTRIM(skill_id) <> ''
        GROUP BY skill_id
        HAVING COALESCE(SUM(total_cost), 0) > 0
        ORDER BY skill_id
        "#,
    )
    .bind(order_id)
    .bind(period_start)
    .bind(period_end)
    .fetch_all(&mut **tx)
    .await?;

    if rows.is_empty() {
        return Ok(vec![(None, amount)]);
    }

    let mut weighted_skills: Vec<(String, Decimal)> = Vec::with_capacity(rows.len());
    let mut total_weight = Decimal::ZERO;
    for row in rows {
        let skill_id: String = row.try_get("skill_id")?;
        let skill_cost: Decimal = row.try_get("skill_cost")?;
        let rounded_cost = skill_cost.round_dp(4);
        if rounded_cost > Decimal::ZERO {
            weighted_skills.push((skill_id, rounded_cost));
            total_weight += rounded_cost;
        }
    }

    if weighted_skills.is_empty() || total_weight <= Decimal::ZERO {
        return Ok(vec![(None, amount)]);
    }

    let mut distributed: Vec<(Option<String>, Decimal)> = Vec::with_capacity(weighted_skills.len());
    let mut remaining = amount;
    for (idx, (skill_id, weight)) in weighted_skills.iter().enumerate() {
        let skill_amount = if idx == weighted_skills.len() - 1 {
            remaining.round_dp(4)
        } else {
            let provisional = (amount * *weight / total_weight).round_dp(4);
            remaining = (remaining - provisional).round_dp(4);
            provisional
        };

        distributed.push((Some(skill_id.clone()), skill_amount));
    }

    Ok(distributed)
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

async fn clear_period_payroll_ap_obligations(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_ids: &[Uuid],
    counterparty: &str,
) -> AnyResult<()> {
    if order_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        DELETE FROM ap_obligations
        WHERE order_id = ANY($1)
          AND source_type = 'AUTONOMY_PAYROLL'
          AND counterparty = $2
        "#,
    )
    .bind(order_ids)
    .bind(counterparty)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn create_and_settle_payroll_ap_obligation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    amount: Decimal,
    currency: &str,
    actor_agent_id: &str,
    counterparty: &str,
    memo_prefix: &str,
    posted_at: DateTime<Utc>,
    settle_immediately: bool,
) -> AnyResult<()> {
    let rounded_amount = amount.round_dp(4);
    if rounded_amount <= Decimal::ZERO {
        return Ok(());
    }

    let obligation_id = Uuid::new_v4();
    let due_at = posted_at + Duration::days(AP_DEFAULT_TERMS_DAYS);

    sqlx::query(
        r#"
        INSERT INTO ap_obligations (
            id, order_id, source_type, counterparty, amount, currency, status,
            due_at, settled_at, created_by_agent_id, created_at, updated_at
        )
        VALUES ($1, $2, 'AUTONOMY_PAYROLL', $3, $4, $5, 'OPEN', $6, NULL, $7, $8, $8)
        "#,
    )
    .bind(obligation_id)
    .bind(order_id)
    .bind(counterparty)
    .bind(rounded_amount)
    .bind(currency)
    .bind(due_at)
    .bind(actor_agent_id)
    .bind(posted_at)
    .execute(&mut **tx)
    .await?;

    insert_ap_subledger_line(
        tx,
        obligation_id,
        order_id,
        "OBLIGATION_RECOGNIZED",
        Decimal::ZERO,
        rounded_amount,
        rounded_amount,
        currency,
        &format!("{memo_prefix}|AP_OBLIGATION"),
        actor_agent_id,
        posted_at,
    )
    .await?;

    if !settle_immediately {
        return Ok(());
    }

    insert_ap_subledger_line(
        tx,
        obligation_id,
        order_id,
        "PAYMENT_POSTED",
        rounded_amount,
        Decimal::ZERO,
        Decimal::ZERO,
        currency,
        &format!("{memo_prefix}|AP_PAYMENT"),
        actor_agent_id,
        posted_at,
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE ap_obligations
        SET status = 'SETTLED',
            settled_at = $2,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(obligation_id)
    .bind(posted_at)
    .execute(&mut **tx)
    .await?;

    insert_journal_line(
        tx,
        order_id,
        PAYROLL_AP_ACCOUNT,
        rounded_amount,
        Decimal::ZERO,
        &format!("{memo_prefix}|AP_SETTLE_DEBIT"),
    )
    .await?;
    insert_journal_line(
        tx,
        order_id,
        CASH_ACCOUNT,
        Decimal::ZERO,
        rounded_amount,
        &format!("{memo_prefix}|AP_SETTLE_CREDIT"),
    )
    .await?;

    Ok(())
}

async fn settle_payroll_ap(
    State(state): State<AppState>,
    Json(payload): Json<SettleApRequest>,
) -> Result<Json<SettleApResponse>, (StatusCode, String)> {
    settle_ap_internal(state, payload, Some("AUTONOMY_PAYROLL"), "PAYROLL_AP_RETRY").await
}

async fn settle_ap(
    State(state): State<AppState>,
    Json(payload): Json<SettleApRequest>,
) -> Result<Json<SettleApResponse>, (StatusCode, String)> {
    settle_ap_internal(state, payload, None, "AP_SETTLEMENT").await
}

async fn settle_ap_internal(
    state: AppState,
    payload: SettleApRequest,
    expected_source_type: Option<&str>,
    memo_namespace: &str,
) -> Result<Json<SettleApResponse>, (StatusCode, String)> {
    let requested_by_agent_id = validate_finops_actor(&payload.requested_by_agent_id)
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;

    let now = Utc::now();
    let memo_root = payload
        .settlement_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("{memo_namespace}|{value}"))
        .unwrap_or_else(|| format!("{memo_namespace}|{}", payload.ap_obligation_id));

    let mut tx = state.pool.begin().await.map_err(internal_error)?;
    let row = sqlx::query(
        r#"
        SELECT id, order_id, source_type, status, currency, settled_at
        FROM ap_obligations
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(payload.ap_obligation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_error)?;

    let Some(row) = row else {
        return Err((StatusCode::NOT_FOUND, "ap_obligation not found".to_string()));
    };

    let source_type: String = row.try_get("source_type").map_err(internal_error)?;
    if let Some(expected) = expected_source_type
        && source_type != expected
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("ap_obligation is not {expected}"),
        ));
    }

    let ap_obligation_id: Uuid = row.try_get("id").map_err(internal_error)?;
    let order_id: Uuid = row.try_get("order_id").map_err(internal_error)?;
    let previous_status: String = row.try_get("status").map_err(internal_error)?;
    let currency: String = row.try_get("currency").map_err(internal_error)?;
    let existing_settled_at: Option<DateTime<Utc>> =
        row.try_get("settled_at").map_err(internal_error)?;
    let liability_account =
        ap_liability_account_for_source_type(&source_type).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("unsupported ap source_type {source_type}"),
            )
        })?;

    if previous_status == "CANCELLED" {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot settle a CANCELLED ap_obligation".to_string(),
        ));
    }

    let outstanding_before = current_ap_obligation_balance(&mut tx, ap_obligation_id)
        .await
        .map_err(internal_error)?;

    let (settled_amount, outstanding_after, settled_at, already_settled) =
        if previous_status == "SETTLED" && outstanding_before <= Decimal::new(1, 4) {
            (
                Decimal::ZERO,
                outstanding_before.round_dp(4),
                existing_settled_at.unwrap_or(now),
                true,
            )
        } else {
            let settled_amount = outstanding_before.round_dp(4);
            if settled_amount > Decimal::new(1, 4) {
                insert_ap_subledger_line(
                    &mut tx,
                    ap_obligation_id,
                    order_id,
                    "PAYMENT_POSTED",
                    settled_amount,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    &currency,
                    &format!("{memo_root}|AP_PAYMENT"),
                    &requested_by_agent_id,
                    now,
                )
                .await
                .map_err(internal_error)?;

                insert_journal_line(
                    &mut tx,
                    order_id,
                    liability_account,
                    settled_amount,
                    Decimal::ZERO,
                    &format!("{memo_root}|AP_SETTLE_DEBIT"),
                )
                .await
                .map_err(internal_error)?;
                insert_journal_line(
                    &mut tx,
                    order_id,
                    CASH_ACCOUNT,
                    Decimal::ZERO,
                    settled_amount,
                    &format!("{memo_root}|AP_SETTLE_CREDIT"),
                )
                .await
                .map_err(internal_error)?;
            }

            sqlx::query(
                r#"
                UPDATE ap_obligations
                SET status = 'SETTLED',
                    settled_at = COALESCE(settled_at, $2),
                    updated_at = $2
                WHERE id = $1
                "#,
            )
            .bind(ap_obligation_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(internal_error)?;

            (
                settled_amount,
                current_ap_obligation_balance(&mut tx, ap_obligation_id)
                    .await
                    .map_err(internal_error)?,
                now,
                false,
            )
        };

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(SettleApResponse {
        ap_obligation_id,
        order_id,
        source_type,
        previous_status,
        status: "SETTLED".to_string(),
        settled_amount: settled_amount.round_dp(4),
        outstanding_before: outstanding_before.round_dp(4),
        outstanding_after: outstanding_after.round_dp(4),
        settled_at,
        already_settled,
    }))
}

fn ap_liability_account_for_source_type(source_type: &str) -> Option<&'static str> {
    match source_type {
        "PROCUREMENT" => Some(PROCUREMENT_AP_ACCOUNT),
        "SERVICE_DELIVERY" => Some(SERVICE_COST_CLEARING_ACCOUNT),
        "AUTONOMY_PAYROLL" => Some(PAYROLL_AP_ACCOUNT),
        _ => None,
    }
}

async fn current_ap_obligation_balance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ap_obligation_id: Uuid,
) -> AnyResult<Decimal> {
    let balance = sqlx::query_scalar::<_, Decimal>(
        r#"
        SELECT balance_after
        FROM ap_subledger_entries
        WHERE ap_obligation_id = $1
        ORDER BY posted_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(ap_obligation_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(balance.unwrap_or(Decimal::ZERO).round_dp(4))
}

#[allow(clippy::too_many_arguments)]
async fn insert_ap_subledger_line(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ap_obligation_id: Uuid,
    order_id: Uuid,
    entry_type: &str,
    debit: Decimal,
    credit: Decimal,
    balance_after: Decimal,
    currency: &str,
    memo: &str,
    actor_agent_id: &str,
    posted_at: DateTime<Utc>,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO ap_subledger_entries (
            id, ap_obligation_id, order_id, entry_type, debit, credit, balance_after,
            currency, memo, posted_by_agent_id, posted_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(ap_obligation_id)
    .bind(order_id)
    .bind(entry_type)
    .bind(debit.round_dp(4))
    .bind(credit.round_dp(4))
    .bind(balance_after.round_dp(4))
    .bind(currency)
    .bind(memo)
    .bind(actor_agent_id)
    .bind(posted_at)
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

async fn derive_actual_metric_from_ledger(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    metric_name: &str,
    period_start_at: DateTime<Utc>,
    period_end_exclusive: DateTime<Utc>,
) -> AnyResult<Decimal> {
    let value = match metric_name {
        "REVENUE" => {
            sqlx::query_scalar::<_, Decimal>(
                r#"
                SELECT COALESCE(SUM(credit - debit), 0)::numeric
                FROM journals
                WHERE account = '4000'
                  AND posted_at >= $1
                  AND posted_at < $2
                "#,
            )
            .bind(period_start_at)
            .bind(period_end_exclusive)
            .fetch_one(&mut **tx)
            .await?
        }
        "COST" => {
            sqlx::query_scalar::<_, Decimal>(
                r#"
                SELECT COALESCE(SUM(debit - credit), 0)::numeric
                FROM journals
                WHERE account IN ('5000', '5100')
                  AND posted_at >= $1
                  AND posted_at < $2
                "#,
            )
            .bind(period_start_at)
            .bind(period_end_exclusive)
            .fetch_one(&mut **tx)
            .await?
        }
        "CASH" => {
            sqlx::query_scalar::<_, Decimal>(
                r#"
                SELECT COALESCE(SUM(debit - credit), 0)::numeric
                FROM journals
                WHERE account = '1000'
                  AND posted_at >= $1
                  AND posted_at < $2
                "#,
            )
            .bind(period_start_at)
            .bind(period_end_exclusive)
            .fetch_one(&mut **tx)
            .await?
        }
        _ => {
            anyhow::bail!(
                "actual_value is required for metric '{}'; automatic derivation supports REVENUE, COST, CASH",
                metric_name
            );
        }
    };

    Ok(value.round_dp(4))
}

#[derive(Debug, Clone, Copy)]
struct OriginationLinks {
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
}

async fn lookup_origination_proof(
    pool: &PgPool,
    channel_type: &str,
    message_id: &str,
) -> Result<Option<OriginationProofResponse>, (StatusCode, String)> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            proof_ref,
            channel_type,
            message_id,
            lead_id,
            opportunity_id,
            quote_id,
            acceptance_id,
            contact_email,
            captured_at
        FROM origination_channel_proofs
        WHERE channel_type = $1
          AND message_id = $2
        LIMIT 1
        "#,
    )
    .bind(channel_type)
    .bind(message_id)
    .fetch_optional(pool)
    .await
    .map_err(internal_error)?;

    if let Some(row) = row {
        return Ok(Some(OriginationProofResponse {
            proof_id: row.try_get("id").map_err(internal_error)?,
            proof_ref: row.try_get("proof_ref").map_err(internal_error)?,
            channel_type: row.try_get("channel_type").map_err(internal_error)?,
            message_id: row.try_get("message_id").map_err(internal_error)?,
            lead_id: row.try_get("lead_id").map_err(internal_error)?,
            opportunity_id: row.try_get("opportunity_id").map_err(internal_error)?,
            quote_id: row.try_get("quote_id").map_err(internal_error)?,
            acceptance_id: row.try_get("acceptance_id").map_err(internal_error)?,
            contact_email: row.try_get("contact_email").map_err(internal_error)?,
            captured_at: row.try_get("captured_at").map_err(internal_error)?,
            deduplicated: true,
        }));
    }

    Ok(None)
}

async fn validate_origination_links(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
) -> Result<OriginationLinks, (StatusCode, String)> {
    let mut resolved = OriginationLinks {
        lead_id,
        opportunity_id,
        quote_id,
        acceptance_id,
    };

    if let Some(lead_id) = resolved.lead_id {
        let lead_exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM leads WHERE id = $1)")
                .bind(lead_id)
                .fetch_one(&mut **tx)
                .await
                .map_err(internal_error)?;
        if !lead_exists {
            return Err((StatusCode::NOT_FOUND, "lead not found".to_string()));
        }
    }

    if let Some(opportunity_id) = resolved.opportunity_id {
        let row = sqlx::query("SELECT lead_id FROM opportunities WHERE id = $1")
            .bind(opportunity_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(internal_error)?;
        let Some(row) = row else {
            return Err((StatusCode::NOT_FOUND, "opportunity not found".to_string()));
        };
        let derived_lead_id: Uuid = row.try_get("lead_id").map_err(internal_error)?;
        if let Some(lead_id) = resolved.lead_id {
            if lead_id != derived_lead_id {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "opportunity_id does not belong to lead_id".to_string(),
                ));
            }
        } else {
            resolved.lead_id = Some(derived_lead_id);
        }
    }

    if let Some(quote_id) = resolved.quote_id {
        let row = sqlx::query("SELECT opportunity_id FROM quotes WHERE id = $1")
            .bind(quote_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(internal_error)?;
        let Some(row) = row else {
            return Err((StatusCode::NOT_FOUND, "quote not found".to_string()));
        };
        let derived_opportunity_id: Uuid = row.try_get("opportunity_id").map_err(internal_error)?;
        if let Some(opportunity_id) = resolved.opportunity_id {
            if opportunity_id != derived_opportunity_id {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "quote_id does not belong to opportunity_id".to_string(),
                ));
            }
        } else {
            resolved.opportunity_id = Some(derived_opportunity_id);
        }
    }

    if let Some(acceptance_id) = resolved.acceptance_id {
        let row = sqlx::query(
            r#"
            SELECT
                qa.quote_id,
                qa.opportunity_id,
                o.lead_id
            FROM quote_acceptances qa
            INNER JOIN opportunities o ON o.id = qa.opportunity_id
            WHERE qa.id = $1
            "#,
        )
        .bind(acceptance_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal_error)?;
        let Some(row) = row else {
            return Err((StatusCode::NOT_FOUND, "acceptance not found".to_string()));
        };
        let derived_quote_id: Uuid = row.try_get("quote_id").map_err(internal_error)?;
        let derived_opportunity_id: Uuid = row.try_get("opportunity_id").map_err(internal_error)?;
        let derived_lead_id: Uuid = row.try_get("lead_id").map_err(internal_error)?;

        if let Some(quote_id) = resolved.quote_id {
            if quote_id != derived_quote_id {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "acceptance_id does not belong to quote_id".to_string(),
                ));
            }
        } else {
            resolved.quote_id = Some(derived_quote_id);
        }

        if let Some(opportunity_id) = resolved.opportunity_id {
            if opportunity_id != derived_opportunity_id {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "acceptance_id does not belong to opportunity_id".to_string(),
                ));
            }
        } else {
            resolved.opportunity_id = Some(derived_opportunity_id);
        }

        if let Some(lead_id) = resolved.lead_id {
            if lead_id != derived_lead_id {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "acceptance_id does not belong to lead_id".to_string(),
                ));
            }
        } else {
            resolved.lead_id = Some(derived_lead_id);
        }
    }

    Ok(resolved)
}

async fn create_linked_lead(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    contact_email: &str,
    source_channel: &str,
    note: Option<&str>,
    requested_by_agent_id: &str,
) -> AnyResult<Uuid> {
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
    .bind(contact_email)
    .bind(source_channel)
    .bind(note)
    .bind(requested_by_agent_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    Ok(lead_id)
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

fn normalize_origination_channel_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "EMAIL" | "WEBHOOK" => Ok(normalized),
        _ => anyhow::bail!("channel_type must be EMAIL or WEBHOOK"),
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

fn normalize_skill_approval_status(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "APPROVED" | "DRAFT" | "REVOKED" => Ok(normalized),
        _ => anyhow::bail!("approval_status must be APPROVED, DRAFT, or REVOKED"),
    }
}

fn normalize_required_fields(fields: &[String]) -> AnyResult<Vec<String>> {
    let mut normalized: Vec<String> = fields
        .iter()
        .map(|field| field.trim())
        .filter(|field| !field.is_empty())
        .map(str::to_string)
        .collect();
    normalized.sort();
    normalized.dedup();

    if normalized.is_empty() {
        anyhow::bail!("at least one required field must be provided");
    }

    Ok(normalized)
}

fn normalize_routing_transaction_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ANY" | "PRODUCT" | "SERVICE" => Ok(normalized),
        _ => anyhow::bail!("transaction_type must be ANY, PRODUCT, or SERVICE"),
    }
}

fn normalize_offering_type(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "PRODUCT" | "SERVICE" => Ok(normalized),
        _ => anyhow::bail!("offering_type must be PRODUCT or SERVICE"),
    }
}

fn normalize_strategy_key(value: &str, field_name: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        anyhow::bail!("{field_name} is required");
    }

    Ok(normalized)
}

fn normalize_metric_name(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        anyhow::bail!("metric_name is required");
    }
    if !normalized.chars().all(|character| {
        character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
    }) {
        anyhow::bail!("metric_name must contain only uppercase letters, digits, and underscores");
    }

    Ok(normalized)
}

fn normalize_variance_severity(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ON_TRACK" | "WARNING" | "BREACH" => Ok(normalized),
        _ => anyhow::bail!("severity must be ON_TRACK, WARNING, or BREACH"),
    }
}

fn normalize_corrective_action_status(value: &str) -> AnyResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "OPEN" | "CLOSED" => Ok(normalized),
        _ => anyhow::bail!("status must be OPEN or CLOSED"),
    }
}

fn validate_period_range(period_start: NaiveDate, period_end: NaiveDate) -> AnyResult<()> {
    if period_end < period_start {
        anyhow::bail!("period_end must be greater than or equal to period_start");
    }

    Ok(())
}

fn period_bounds(
    period_start: NaiveDate,
    period_end: NaiveDate,
) -> AnyResult<(DateTime<Utc>, DateTime<Utc>)> {
    validate_period_range(period_start, period_end)?;

    let start_naive = period_start
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid period_start"))?;
    let end_day = period_end
        .succ_opt()
        .ok_or_else(|| anyhow::anyhow!("invalid period_end"))?;
    let end_naive = end_day
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid period_end"))?;

    Ok((
        DateTime::<Utc>::from_naive_utc_and_offset(start_naive, Utc),
        DateTime::<Utc>::from_naive_utc_and_offset(end_naive, Utc),
    ))
}

fn classify_variance_severity(
    variance_pct: Decimal,
    warning_threshold_pct: Decimal,
    critical_threshold_pct: Decimal,
) -> String {
    if variance_pct >= critical_threshold_pct {
        "BREACH".to_string()
    } else if variance_pct >= warning_threshold_pct {
        "WARNING".to_string()
    } else {
        "ON_TRACK".to_string()
    }
}

fn default_warning_threshold_pct() -> Decimal {
    Decimal::new(500, 2) // 5.00
}

fn default_critical_threshold_pct() -> Decimal {
    Decimal::new(1000, 2) // 10.00
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
