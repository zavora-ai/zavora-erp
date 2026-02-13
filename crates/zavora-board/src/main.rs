use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use tracing::info;
use uuid::Uuid;
use zavora_platform::{BoardPack, ServiceConfig, connect_database};

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Clone, Deserialize)]
struct SkillUnitEconomicsQuery {
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SkillTelemetryQuery {
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct FinancePeriodQuery {
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct BalanceSheetQuery {
    as_of: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct AgingQuery {
    as_of: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct LedgerQuery {
    order_id: Option<Uuid>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SkillUnitEconomicsResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    items: Vec<SkillUnitEconomicsRow>,
}

#[derive(Debug, Serialize)]
struct SkillTelemetryResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    items: Vec<SkillTelemetryRow>,
}

#[derive(Debug, Serialize)]
struct TrialBalanceRow {
    account: String,
    total_debit: Decimal,
    total_credit: Decimal,
    balance: Decimal,
}

#[derive(Debug, Serialize)]
struct TrialBalanceResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    total_debit: Decimal,
    total_credit: Decimal,
    net_balance: Decimal,
    is_balanced: bool,
    items: Vec<TrialBalanceRow>,
}

#[derive(Debug, Serialize)]
struct ProfitAndLossResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    revenue: Decimal,
    cogs: Decimal,
    gross_profit: Decimal,
    operating_expense: Decimal,
    operating_profit: Decimal,
    autonomy_cost: Decimal,
    profit_after_autonomy_cost: Decimal,
}

#[derive(Debug, Serialize)]
struct BalanceSheetRow {
    account: String,
    category: String,
    amount: Decimal,
}

#[derive(Debug, Serialize)]
struct BalanceSheetResponse {
    generated_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
    assets_total: Decimal,
    liabilities_total: Decimal,
    equity_accounts_total: Decimal,
    net_income_unclosed: Decimal,
    equity_total: Decimal,
    is_balanced: bool,
    items: Vec<BalanceSheetRow>,
}

#[derive(Debug, Serialize)]
struct CashFlowResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    cash_opening_balance: Decimal,
    cash_inflows: Decimal,
    cash_outflows: Decimal,
    net_cash_change: Decimal,
    cash_closing_balance: Decimal,
}

#[derive(Debug, Serialize)]
struct RevenueTrackingResponse {
    generated_at: DateTime<Utc>,
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
    accepted_quotes: i64,
    fulfilled_orders: i64,
    settled_orders: i64,
    booked_revenue: Decimal,
    billed_revenue: Decimal,
    collected_revenue: Decimal,
    unbilled_revenue: Decimal,
    billed_not_collected: Decimal,
    collection_rate_pct: Decimal,
}

#[derive(Debug, Serialize, Default)]
struct AgingBucketTotals {
    current: Decimal,
    days_1_30: Decimal,
    days_31_60: Decimal,
    days_61_90: Decimal,
    days_90_plus: Decimal,
}

#[derive(Debug, Serialize)]
struct ArAgingRow {
    order_id: Uuid,
    customer_email: String,
    due_at: DateTime<Utc>,
    age_days: i64,
    outstanding_ar: Decimal,
    bucket: String,
}

#[derive(Debug, Serialize)]
struct ArAgingResponse {
    generated_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
    total_outstanding_ar: Decimal,
    buckets: AgingBucketTotals,
    items: Vec<ArAgingRow>,
}

#[derive(Debug, Serialize)]
struct ApAgingRow {
    order_id: Uuid,
    account: String,
    due_at: DateTime<Utc>,
    age_days: i64,
    outstanding_ap: Decimal,
    bucket: String,
}

#[derive(Debug, Serialize)]
struct ApAgingResponse {
    generated_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
    total_outstanding_ap: Decimal,
    buckets: AgingBucketTotals,
    items: Vec<ApAgingRow>,
}

#[derive(Debug, Serialize)]
struct FinanceInvoicesResponse {
    generated_at: DateTime<Utc>,
    items: Vec<AuditInvoiceRecord>,
}

#[derive(Debug, Serialize)]
struct FinanceArSubledgerResponse {
    generated_at: DateTime<Utc>,
    items: Vec<AuditArSubledgerEntryRecord>,
}

#[derive(Debug, Serialize)]
struct FinanceApObligationsResponse {
    generated_at: DateTime<Utc>,
    items: Vec<AuditApObligationRecord>,
}

#[derive(Debug, Serialize)]
struct FinanceApSubledgerResponse {
    generated_at: DateTime<Utc>,
    items: Vec<AuditApSubledgerEntryRecord>,
}

#[derive(Debug, Serialize)]
struct SkillUnitEconomicsRow {
    skill_id: String,
    orders_touched: i64,
    token_cost: Decimal,
    cloud_cost: Decimal,
    subscription_cost: Decimal,
    autonomy_cost: Decimal,
    attributed_revenue: Decimal,
    margin_after_autonomy_cost: Decimal,
    revenue_to_cost_ratio: Decimal,
}

#[derive(Debug, Serialize)]
struct SkillTelemetryRow {
    skill_id: String,
    skill_version: String,
    total_invocations: i64,
    success_count: i64,
    failed_count: i64,
    escalated_count: i64,
    fallback_count: i64,
    success_rate_pct: Decimal,
    escalation_rate_pct: Decimal,
    avg_latency_ms: Decimal,
    p95_latency_ms: Decimal,
}

#[derive(Debug, Serialize)]
struct OrderEvidencePackage {
    generated_at: DateTime<Utc>,
    order: AuditOrderRecord,
    lead: Option<AuditLeadRecord>,
    opportunity: Option<AuditOpportunityRecord>,
    quote: Option<AuditQuoteRecord>,
    acceptance: Option<AuditAcceptanceRecord>,
    origination_proofs: Vec<AuditOriginationProofRecord>,
    escalations: Vec<AuditEscalationRecord>,
    inventory_movements: Vec<AuditInventoryMovementRecord>,
    journals: Vec<AuditJournalRecord>,
    invoice: Option<AuditInvoiceRecord>,
    ar_subledger_entries: Vec<AuditArSubledgerEntryRecord>,
    ap_obligations: Vec<AuditApObligationRecord>,
    ap_subledger_entries: Vec<AuditApSubledgerEntryRecord>,
    settlements: Vec<AuditSettlementRecord>,
    payroll_allocations: Vec<AuditPayrollAllocationRecord>,
    skill_invocations: Vec<AuditSkillInvocationRecord>,
    memories: Vec<AuditMemoryRecord>,
    memory_provenance: Vec<AuditMemoryProvenanceRecord>,
    timeline: Vec<AuditTimelineEvent>,
    totals: AuditTotals,
}

#[derive(Debug, Serialize)]
struct AuditOrderRecord {
    id: Uuid,
    customer_email: String,
    transaction_type: String,
    requested_by_agent_id: String,
    item_code: String,
    quantity: Decimal,
    unit_price: Decimal,
    currency: String,
    status: String,
    failure_reason: Option<String>,
    created_at: DateTime<Utc>,
    fulfilled_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditLeadRecord {
    id: Uuid,
    contact_email: String,
    source_channel: String,
    note: Option<String>,
    status: String,
    requested_by_agent_id: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditOpportunityRecord {
    id: Uuid,
    lead_id: Uuid,
    customer_email: String,
    transaction_type: String,
    item_code: String,
    quantity: Decimal,
    target_unit_price: Decimal,
    currency: String,
    risk_class: String,
    stage: String,
    requested_by_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditQuoteRecord {
    id: Uuid,
    opportunity_id: Uuid,
    unit_price: Decimal,
    quantity: Decimal,
    currency: String,
    payment_terms_days: i32,
    valid_until: DateTime<Utc>,
    status: String,
    requested_by_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditAcceptanceRecord {
    id: Uuid,
    quote_id: Uuid,
    opportunity_id: Uuid,
    order_id: Uuid,
    accepted_by: String,
    acceptance_channel: String,
    proof_ref: String,
    requested_by_agent_id: String,
    accepted_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditOriginationProofRecord {
    id: Uuid,
    proof_ref: String,
    channel_type: String,
    message_id: String,
    contact_email: Option<String>,
    subject: Option<String>,
    source_ref: Option<String>,
    payload_json: serde_json::Value,
    lead_id: Option<Uuid>,
    opportunity_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    acceptance_id: Option<Uuid>,
    captured_by_agent_id: String,
    received_at: DateTime<Utc>,
    captured_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditEscalationRecord {
    id: Uuid,
    action_type: String,
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

#[derive(Debug, Serialize)]
struct AuditInventoryMovementRecord {
    id: Uuid,
    movement_type: String,
    item_code: String,
    quantity: Decimal,
    unit_cost: Decimal,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditJournalRecord {
    id: Uuid,
    account: String,
    debit: Decimal,
    credit: Decimal,
    memo: String,
    posted_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditSettlementRecord {
    id: Uuid,
    amount: Decimal,
    currency: String,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditInvoiceRecord {
    id: Uuid,
    order_id: Uuid,
    invoice_number: String,
    customer_email: String,
    amount: Decimal,
    currency: String,
    status: String,
    issued_at: DateTime<Utc>,
    due_at: DateTime<Utc>,
    settled_at: Option<DateTime<Utc>>,
    created_by_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditArSubledgerEntryRecord {
    id: Uuid,
    invoice_id: Uuid,
    order_id: Uuid,
    entry_type: String,
    debit: Decimal,
    credit: Decimal,
    balance_after: Decimal,
    currency: String,
    memo: String,
    posted_by_agent_id: String,
    posted_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditApObligationRecord {
    id: Uuid,
    order_id: Uuid,
    source_type: String,
    counterparty: String,
    amount: Decimal,
    currency: String,
    status: String,
    due_at: DateTime<Utc>,
    settled_at: Option<DateTime<Utc>>,
    created_by_agent_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditApSubledgerEntryRecord {
    id: Uuid,
    ap_obligation_id: Uuid,
    order_id: Uuid,
    entry_type: String,
    debit: Decimal,
    credit: Decimal,
    balance_after: Decimal,
    currency: String,
    memo: String,
    posted_by_agent_id: String,
    posted_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditPayrollAllocationRecord {
    id: Uuid,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    source_type: String,
    source_id: Uuid,
    agent_id: Option<String>,
    skill_id: Option<String>,
    allocation_basis: String,
    allocated_cost: Decimal,
    currency: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditSkillInvocationRecord {
    id: Uuid,
    intent: String,
    capability: String,
    skill_id: String,
    skill_version: String,
    actor_agent_id: String,
    attempt_no: i32,
    status: String,
    failure_reason: Option<String>,
    fallback_used: bool,
    input_hash: String,
    output_hash: Option<String>,
    latency_ms: i64,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditMemoryRecord {
    id: Uuid,
    agent_name: String,
    scope: String,
    content: String,
    keywords: Vec<String>,
    source_ref: Option<String>,
    created_at: DateTime<Utc>,
    access_count: i64,
    last_accessed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct AuditMemoryProvenanceRecord {
    id: Uuid,
    memory_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    action_type: String,
    actor_agent_id: String,
    source_ref: String,
    query_text: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuditTimelineEvent {
    occurred_at: DateTime<Utc>,
    event_type: String,
    source: String,
    details: String,
}

#[derive(Debug, Serialize)]
struct AuditTotals {
    line_value_total: Decimal,
    invoice_total: Decimal,
    ar_open_balance: Decimal,
    ap_open_balance: Decimal,
    journal_debit_total: Decimal,
    journal_credit_total: Decimal,
    cogs_total: Decimal,
    settlement_total: Decimal,
    autonomy_cost_total: Decimal,
    margin_after_autonomy_cost: Decimal,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "zavora_board=info".to_string()),
        )
        .init();

    let config = ServiceConfig::from_env("0.0.0.0:8090")?;
    let pool = connect_database(&config.database_url).await?;

    let state = AppState { pool };
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/board/pack", get(board_pack))
        .route("/finance/trial-balance", get(trial_balance))
        .route("/finance/pnl", get(profit_and_loss))
        .route("/finance/balance-sheet", get(balance_sheet))
        .route("/finance/cash-flow", get(cash_flow))
        .route("/revenue/tracking", get(revenue_tracking))
        .route("/finance/ar-aging", get(ar_aging))
        .route("/finance/ap-aging", get(ap_aging))
        .route("/finance/invoices", get(finance_invoices))
        .route("/finance/ar-subledger", get(finance_ar_subledger))
        .route("/finance/ap-obligations", get(finance_ap_obligations))
        .route("/finance/ap-subledger", get(finance_ap_subledger))
        .route("/board/skills/unit-economics", get(skill_unit_economics))
        .route("/board/skills/telemetry", get(skill_telemetry))
        .route("/audit/orders/{order_id}/evidence", get(order_evidence))
        .with_state(state);

    let addr: SocketAddr = config.http_addr.parse()?;
    info!("board service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn board_pack(
    State(state): State<AppState>,
) -> std::result::Result<Json<BoardPack>, (axum::http::StatusCode, String)> {
    let totals = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS orders_total,
            COUNT(*) FILTER (WHERE status = 'FULFILLED')::BIGINT AS orders_fulfilled,
            COUNT(*) FILTER (WHERE status <> 'FULFILLED')::BIGINT AS orders_open,
            COUNT(*) FILTER (WHERE status = 'PENDING_APPROVAL')::BIGINT AS orders_pending_approval,
            COALESCE(SUM(CASE WHEN status = 'FULFILLED' THEN quantity * unit_price ELSE 0 END), 0) AS revenue
        FROM orders
        "#,
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let settlements =
        sqlx::query("SELECT COALESCE(SUM(amount), 0) AS cash_collected FROM settlements")
            .fetch_one(&state.pool)
            .await
            .map_err(internal_error)?;

    let inventory = sqlx::query(
        "SELECT COALESCE(SUM(on_hand * avg_cost), 0) AS inventory_value FROM inventory_positions",
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let autonomy_cost_row = sqlx::query(
        "SELECT COALESCE(SUM(allocated_cost), 0) AS autonomy_operating_cost FROM finops_cost_allocations",
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let cogs_row = sqlx::query(
        "SELECT COALESCE(SUM(debit), 0) AS cogs_total FROM journals WHERE account = '5000'",
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let pipeline = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT FROM leads) AS leads_total,
            (SELECT COUNT(*)::BIGINT FROM opportunities WHERE stage <> 'ACCEPTED' AND stage <> 'LOST') AS opportunities_open,
            (SELECT COUNT(*)::BIGINT FROM quotes WHERE status = 'ISSUED') AS quotes_issued,
            (SELECT COUNT(*)::BIGINT FROM quotes WHERE status = 'ACCEPTED') AS quotes_accepted,
            (SELECT COUNT(*)::BIGINT FROM governance_escalations WHERE status = 'PENDING') AS governance_escalations_pending
        "#,
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let latest_reconciliation = sqlx::query(
        r#"
        SELECT status, variance_pct, completed_at
        FROM finops_period_reconciliations
        ORDER BY completed_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_error)?;

    let revenue = totals
        .try_get::<Decimal, _>("revenue")
        .map_err(internal_error)?;
    let autonomy_operating_cost = autonomy_cost_row
        .try_get::<Decimal, _>("autonomy_operating_cost")
        .map_err(internal_error)?;
    let cogs_total = cogs_row
        .try_get::<Decimal, _>("cogs_total")
        .map_err(internal_error)?;
    let margin_after_autonomy_cost = (revenue - cogs_total - autonomy_operating_cost).round_dp(4);
    let revenue_to_agent_payroll_ratio = if autonomy_operating_cost > Decimal::ZERO {
        (revenue / autonomy_operating_cost).round_dp(4)
    } else {
        Decimal::ZERO
    };
    let (
        finops_reconciliation_status,
        finops_reconciliation_variance_pct,
        finops_last_reconciled_at,
    ) = if let Some(row) = latest_reconciliation {
        (
            row.try_get::<String, _>("status").map_err(internal_error)?,
            row.try_get::<Decimal, _>("variance_pct")
                .map_err(internal_error)?,
            row.try_get::<Option<DateTime<Utc>>, _>("completed_at")
                .map_err(internal_error)?,
        )
    } else {
        ("NOT_RUN".to_string(), Decimal::ZERO, None)
    };

    let pack = BoardPack {
        generated_at: Utc::now(),
        orders_total: totals
            .try_get::<i64, _>("orders_total")
            .map_err(internal_error)?,
        orders_fulfilled: totals
            .try_get::<i64, _>("orders_fulfilled")
            .map_err(internal_error)?,
        orders_open: totals
            .try_get::<i64, _>("orders_open")
            .map_err(internal_error)?,
        orders_pending_approval: totals
            .try_get::<i64, _>("orders_pending_approval")
            .map_err(internal_error)?,
        leads_total: pipeline
            .try_get::<i64, _>("leads_total")
            .map_err(internal_error)?,
        opportunities_open: pipeline
            .try_get::<i64, _>("opportunities_open")
            .map_err(internal_error)?,
        quotes_issued: pipeline
            .try_get::<i64, _>("quotes_issued")
            .map_err(internal_error)?,
        quotes_accepted: pipeline
            .try_get::<i64, _>("quotes_accepted")
            .map_err(internal_error)?,
        governance_escalations_pending: pipeline
            .try_get::<i64, _>("governance_escalations_pending")
            .map_err(internal_error)?,
        revenue,
        cash_collected: settlements
            .try_get::<Decimal, _>("cash_collected")
            .map_err(internal_error)?,
        inventory_value: inventory
            .try_get::<Decimal, _>("inventory_value")
            .map_err(internal_error)?,
        autonomy_operating_cost,
        margin_after_autonomy_cost,
        revenue_to_agent_payroll_ratio,
        finops_reconciliation_status,
        finops_reconciliation_variance_pct,
        finops_last_reconciled_at,
    };

    Ok(Json(pack))
}

async fn trial_balance(
    State(state): State<AppState>,
    Query(query): Query<FinancePeriodQuery>,
) -> std::result::Result<Json<TrialBalanceResponse>, (axum::http::StatusCode, String)> {
    validate_period_bounds(query.period_start, query.period_end)?;

    let rows = sqlx::query(
        r#"
        SELECT
            account,
            COALESCE(SUM(debit), 0) AS total_debit,
            COALESCE(SUM(credit), 0) AS total_credit
        FROM journals
        WHERE ($1::timestamptz IS NULL OR posted_at >= $1)
          AND ($2::timestamptz IS NULL OR posted_at < $2)
        GROUP BY account
        ORDER BY account
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut total_debit = Decimal::ZERO;
    let mut total_credit = Decimal::ZERO;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let debit: Decimal = row.try_get("total_debit").map_err(internal_error)?;
        let credit: Decimal = row.try_get("total_credit").map_err(internal_error)?;
        total_debit += debit;
        total_credit += credit;
        items.push(TrialBalanceRow {
            account: row.try_get("account").map_err(internal_error)?,
            total_debit: debit.round_dp(4),
            total_credit: credit.round_dp(4),
            balance: (debit - credit).round_dp(4),
        });
    }

    let net_balance = (total_debit - total_credit).round_dp(4);
    let is_balanced = net_balance.abs() <= Decimal::new(1, 2);

    Ok(Json(TrialBalanceResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        total_debit: total_debit.round_dp(4),
        total_credit: total_credit.round_dp(4),
        net_balance,
        is_balanced,
        items,
    }))
}

async fn profit_and_loss(
    State(state): State<AppState>,
    Query(query): Query<FinancePeriodQuery>,
) -> std::result::Result<Json<ProfitAndLossResponse>, (axum::http::StatusCode, String)> {
    validate_period_bounds(query.period_start, query.period_end)?;

    let pnl_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN account LIKE '4%' THEN credit - debit ELSE 0 END), 0) AS revenue,
            COALESCE(SUM(CASE WHEN account = '5000' THEN debit - credit ELSE 0 END), 0) AS cogs,
            COALESCE(SUM(CASE WHEN account LIKE '5%' AND account <> '5000' THEN debit - credit ELSE 0 END), 0) AS operating_expense
        FROM journals
        WHERE ($1::timestamptz IS NULL OR posted_at >= $1)
          AND ($2::timestamptz IS NULL OR posted_at < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let autonomy_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(allocated_cost), 0) AS autonomy_cost
        FROM finops_cost_allocations
        WHERE ($1::timestamptz IS NULL OR period_end > $1)
          AND ($2::timestamptz IS NULL OR period_start < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let revenue: Decimal = pnl_row.try_get("revenue").map_err(internal_error)?;
    let cogs: Decimal = pnl_row.try_get("cogs").map_err(internal_error)?;
    let operating_expense: Decimal = pnl_row
        .try_get("operating_expense")
        .map_err(internal_error)?;
    let autonomy_cost: Decimal = autonomy_row
        .try_get("autonomy_cost")
        .map_err(internal_error)?;

    let gross_profit = (revenue - cogs).round_dp(4);
    let operating_profit = (gross_profit - operating_expense).round_dp(4);
    let profit_after_autonomy_cost = (operating_profit - autonomy_cost).round_dp(4);

    Ok(Json(ProfitAndLossResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        revenue: revenue.round_dp(4),
        cogs: cogs.round_dp(4),
        gross_profit,
        operating_expense: operating_expense.round_dp(4),
        operating_profit,
        autonomy_cost: autonomy_cost.round_dp(4),
        profit_after_autonomy_cost,
    }))
}

async fn balance_sheet(
    State(state): State<AppState>,
    Query(query): Query<BalanceSheetQuery>,
) -> std::result::Result<Json<BalanceSheetResponse>, (axum::http::StatusCode, String)> {
    let as_of = query.as_of.unwrap_or_else(Utc::now);

    let rows = sqlx::query(
        r#"
        SELECT
            account,
            COALESCE(SUM(debit), 0) AS total_debit,
            COALESCE(SUM(credit), 0) AS total_credit
        FROM journals
        WHERE posted_at <= $1
        GROUP BY account
        ORDER BY account
        "#,
    )
    .bind(as_of)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let income_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN account LIKE '4%' THEN credit - debit ELSE 0 END), 0) AS revenue,
            COALESCE(SUM(CASE WHEN account LIKE '5%' THEN debit - credit ELSE 0 END), 0) AS expenses
        FROM journals
        WHERE posted_at <= $1
        "#,
    )
    .bind(as_of)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut assets_total = Decimal::ZERO;
    let mut liabilities_total = Decimal::ZERO;
    let mut equity_accounts_total = Decimal::ZERO;
    let mut items = Vec::new();

    for row in rows {
        let account: String = row.try_get("account").map_err(internal_error)?;
        let debit: Decimal = row.try_get("total_debit").map_err(internal_error)?;
        let credit: Decimal = row.try_get("total_credit").map_err(internal_error)?;
        match account_category(&account) {
            Some("ASSET") => {
                let amount = (debit - credit).round_dp(4);
                assets_total += amount;
                if amount.abs() > Decimal::ZERO {
                    items.push(BalanceSheetRow {
                        account,
                        category: "ASSET".to_string(),
                        amount,
                    });
                }
            }
            Some("LIABILITY") => {
                let amount = (credit - debit).round_dp(4);
                liabilities_total += amount;
                if amount.abs() > Decimal::ZERO {
                    items.push(BalanceSheetRow {
                        account,
                        category: "LIABILITY".to_string(),
                        amount,
                    });
                }
            }
            Some("EQUITY") => {
                let amount = (credit - debit).round_dp(4);
                equity_accounts_total += amount;
                if amount.abs() > Decimal::ZERO {
                    items.push(BalanceSheetRow {
                        account,
                        category: "EQUITY".to_string(),
                        amount,
                    });
                }
            }
            _ => {}
        }
    }

    let revenue: Decimal = income_row.try_get("revenue").map_err(internal_error)?;
    let expenses: Decimal = income_row.try_get("expenses").map_err(internal_error)?;
    let net_income_unclosed = (revenue - expenses).round_dp(4);
    if net_income_unclosed.abs() > Decimal::ZERO {
        items.push(BalanceSheetRow {
            account: "UNREALIZED_PNL".to_string(),
            category: "EQUITY_ADJUSTMENT".to_string(),
            amount: net_income_unclosed,
        });
    }

    let assets_total = assets_total.round_dp(4);
    let liabilities_total = liabilities_total.round_dp(4);
    let equity_accounts_total = equity_accounts_total.round_dp(4);
    let equity_total = (equity_accounts_total + net_income_unclosed).round_dp(4);
    let is_balanced = (assets_total - liabilities_total - equity_total).abs() <= Decimal::new(1, 2);

    Ok(Json(BalanceSheetResponse {
        generated_at: Utc::now(),
        as_of,
        assets_total,
        liabilities_total,
        equity_accounts_total,
        net_income_unclosed,
        equity_total,
        is_balanced,
        items,
    }))
}

async fn cash_flow(
    State(state): State<AppState>,
    Query(query): Query<FinancePeriodQuery>,
) -> std::result::Result<Json<CashFlowResponse>, (axum::http::StatusCode, String)> {
    validate_period_bounds(query.period_start, query.period_end)?;

    let opening_balance = if let Some(period_start) = query.period_start {
        sqlx::query_scalar::<_, Decimal>(
            r#"
            SELECT COALESCE(SUM(debit - credit), 0)
            FROM journals
            WHERE account = '1000'
              AND posted_at < $1
            "#,
        )
        .bind(period_start)
        .fetch_one(&state.pool)
        .await
        .map_err(internal_error)?
    } else {
        Decimal::ZERO
    };

    let period_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN account = '1000' THEN debit ELSE 0 END), 0) AS cash_inflows,
            COALESCE(SUM(CASE WHEN account = '1000' THEN credit ELSE 0 END), 0) AS cash_outflows
        FROM journals
        WHERE ($1::timestamptz IS NULL OR posted_at >= $1)
          AND ($2::timestamptz IS NULL OR posted_at < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let cash_inflows: Decimal = period_row.try_get("cash_inflows").map_err(internal_error)?;
    let cash_outflows: Decimal = period_row
        .try_get("cash_outflows")
        .map_err(internal_error)?;
    let net_cash_change = (cash_inflows - cash_outflows).round_dp(4);
    let cash_closing_balance = (opening_balance + net_cash_change).round_dp(4);

    Ok(Json(CashFlowResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        cash_opening_balance: opening_balance.round_dp(4),
        cash_inflows: cash_inflows.round_dp(4),
        cash_outflows: cash_outflows.round_dp(4),
        net_cash_change,
        cash_closing_balance,
    }))
}

async fn revenue_tracking(
    State(state): State<AppState>,
    Query(query): Query<FinancePeriodQuery>,
) -> std::result::Result<Json<RevenueTrackingResponse>, (axum::http::StatusCode, String)> {
    validate_period_bounds(query.period_start, query.period_end)?;

    let booked_row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS accepted_quotes,
            COALESCE(SUM(q.quantity * q.unit_price), 0) AS booked_revenue
        FROM quote_acceptances qa
        INNER JOIN quotes q ON q.id = qa.quote_id
        WHERE ($1::timestamptz IS NULL OR qa.accepted_at >= $1)
          AND ($2::timestamptz IS NULL OR qa.accepted_at < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let billed_row = sqlx::query(
        r#"
        WITH invoice_orders AS (
            SELECT DISTINCT order_id
            FROM invoices
            WHERE status <> 'VOID'
              AND ($1::timestamptz IS NULL OR issued_at >= $1)
              AND ($2::timestamptz IS NULL OR issued_at < $2)
        ),
        invoice_billing AS (
            SELECT COALESCE(SUM(amount), 0) AS billed_revenue
            FROM invoices
            WHERE status <> 'VOID'
              AND ($1::timestamptz IS NULL OR issued_at >= $1)
              AND ($2::timestamptz IS NULL OR issued_at < $2)
        ),
        legacy_journal_billing AS (
            SELECT COALESCE(SUM(j.credit - j.debit), 0) AS billed_revenue
            FROM journals j
            WHERE j.account = '4000'
              AND ($1::timestamptz IS NULL OR j.posted_at >= $1)
              AND ($2::timestamptz IS NULL OR j.posted_at < $2)
              AND NOT EXISTS (
                    SELECT 1
                    FROM invoice_orders io
                    WHERE io.order_id = j.order_id
              )
        )
        SELECT
            (SELECT billed_revenue FROM invoice_billing)
            + (SELECT billed_revenue FROM legacy_journal_billing)
            AS billed_revenue
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let settlement_row = sqlx::query(
        r#"
        SELECT
            COUNT(DISTINCT order_id)::BIGINT AS settled_orders,
            COALESCE(SUM(amount), 0) AS collected_revenue
        FROM settlements
        WHERE ($1::timestamptz IS NULL OR received_at >= $1)
          AND ($2::timestamptz IS NULL OR received_at < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let fulfilled_orders = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM orders
        WHERE status = 'FULFILLED'
          AND ($1::timestamptz IS NULL OR fulfilled_at >= $1)
          AND ($2::timestamptz IS NULL OR fulfilled_at < $2)
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

    let accepted_quotes: i64 = booked_row
        .try_get("accepted_quotes")
        .map_err(internal_error)?;
    let booked_revenue: Decimal = booked_row
        .try_get("booked_revenue")
        .map_err(internal_error)?;
    let billed_revenue: Decimal = billed_row
        .try_get("billed_revenue")
        .map_err(internal_error)?;
    let settled_orders: i64 = settlement_row
        .try_get("settled_orders")
        .map_err(internal_error)?;
    let collected_revenue: Decimal = settlement_row
        .try_get("collected_revenue")
        .map_err(internal_error)?;

    let unbilled_revenue = (booked_revenue - billed_revenue).round_dp(4);
    let billed_not_collected = (billed_revenue - collected_revenue).round_dp(4);
    let collection_rate_pct = if billed_revenue > Decimal::ZERO {
        ((collected_revenue / billed_revenue) * Decimal::new(100, 0)).round_dp(4)
    } else {
        Decimal::ZERO
    };

    Ok(Json(RevenueTrackingResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        accepted_quotes,
        fulfilled_orders,
        settled_orders,
        booked_revenue: booked_revenue.round_dp(4),
        billed_revenue: billed_revenue.round_dp(4),
        collected_revenue: collected_revenue.round_dp(4),
        unbilled_revenue,
        billed_not_collected,
        collection_rate_pct,
    }))
}

async fn ar_aging(
    State(state): State<AppState>,
    Query(query): Query<AgingQuery>,
) -> std::result::Result<Json<ArAgingResponse>, (axum::http::StatusCode, String)> {
    let as_of = query.as_of.unwrap_or_else(Utc::now);
    let limit = query.limit.unwrap_or(200).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        WITH ar_balances AS (
            SELECT
                i.id AS invoice_id,
                i.order_id,
                i.customer_email,
                i.due_at,
                COALESCE(SUM(ase.debit - ase.credit), 0) AS outstanding_ar
            FROM invoices i
            LEFT JOIN ar_subledger_entries ase
                ON ase.invoice_id = i.id
               AND ase.posted_at <= $1
            WHERE i.status <> 'VOID'
              AND i.issued_at <= $1
            GROUP BY i.id, i.order_id, i.customer_email, i.due_at
        )
        SELECT
            order_id,
            customer_email,
            due_at,
            (EXTRACT(EPOCH FROM ($1::timestamptz - due_at)) / 86400)::BIGINT AS age_days,
            outstanding_ar
        FROM ar_balances
        WHERE outstanding_ar > 0
        ORDER BY due_at ASC, order_id ASC
        LIMIT $2
        "#,
    )
    .bind(as_of)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut buckets = AgingBucketTotals::default();
    let mut total_outstanding_ar = Decimal::ZERO;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let age_days: i64 = row.try_get("age_days").map_err(internal_error)?;
        let outstanding_ar: Decimal = row.try_get("outstanding_ar").map_err(internal_error)?;
        let bucket = aging_bucket_label(age_days);
        accumulate_aging_bucket(&mut buckets, bucket, outstanding_ar);
        total_outstanding_ar += outstanding_ar;
        items.push(ArAgingRow {
            order_id: row.try_get("order_id").map_err(internal_error)?,
            customer_email: row.try_get("customer_email").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            age_days,
            outstanding_ar: outstanding_ar.round_dp(4),
            bucket: bucket.to_string(),
        });
    }

    Ok(Json(ArAgingResponse {
        generated_at: Utc::now(),
        as_of,
        total_outstanding_ar: total_outstanding_ar.round_dp(4),
        buckets,
        items,
    }))
}

async fn ap_aging(
    State(state): State<AppState>,
    Query(query): Query<AgingQuery>,
) -> std::result::Result<Json<ApAgingResponse>, (axum::http::StatusCode, String)> {
    let as_of = query.as_of.unwrap_or_else(Utc::now);
    let limit = query.limit.unwrap_or(200).clamp(1, 500);

    let rows = sqlx::query(
        r#"
        WITH ap_balances AS (
            SELECT
                apo.id AS ap_obligation_id,
                apo.order_id,
                CASE
                    WHEN apo.source_type = 'PROCUREMENT' THEN '2100'
                    ELSE '2200'
                END AS account,
                apo.due_at,
                COALESCE(SUM(ase.credit - ase.debit), 0) AS outstanding_ap
            FROM ap_obligations apo
            LEFT JOIN ap_subledger_entries ase
                ON ase.ap_obligation_id = apo.id
               AND ase.posted_at <= $1
            WHERE apo.status <> 'CANCELLED'
              AND apo.created_at <= $1
            GROUP BY apo.id, apo.order_id, apo.source_type, apo.due_at
        )
        SELECT
            order_id,
            account,
            due_at,
            (EXTRACT(EPOCH FROM ($1::timestamptz - due_at)) / 86400)::BIGINT AS age_days,
            outstanding_ap
        FROM ap_balances
        WHERE outstanding_ap > 0
        ORDER BY due_at ASC, order_id ASC, account ASC
        LIMIT $2
        "#,
    )
    .bind(as_of)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut buckets = AgingBucketTotals::default();
    let mut total_outstanding_ap = Decimal::ZERO;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let age_days: i64 = row.try_get("age_days").map_err(internal_error)?;
        let outstanding_ap: Decimal = row.try_get("outstanding_ap").map_err(internal_error)?;
        let bucket = aging_bucket_label(age_days);
        accumulate_aging_bucket(&mut buckets, bucket, outstanding_ap);
        total_outstanding_ap += outstanding_ap;
        items.push(ApAgingRow {
            order_id: row.try_get("order_id").map_err(internal_error)?,
            account: row.try_get("account").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            age_days,
            outstanding_ap: outstanding_ap.round_dp(4),
            bucket: bucket.to_string(),
        });
    }

    Ok(Json(ApAgingResponse {
        generated_at: Utc::now(),
        as_of,
        total_outstanding_ap: total_outstanding_ap.round_dp(4),
        buckets,
        items,
    }))
}

async fn finance_invoices(
    State(state): State<AppState>,
    Query(query): Query<LedgerQuery>,
) -> std::result::Result<Json<FinanceInvoicesResponse>, (axum::http::StatusCode, String)> {
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            order_id,
            invoice_number,
            customer_email,
            amount,
            currency,
            status,
            issued_at,
            due_at,
            settled_at,
            created_by_agent_id,
            created_at,
            updated_at
        FROM invoices
        WHERE ($1::uuid IS NULL OR order_id = $1)
        ORDER BY issued_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(query.order_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(AuditInvoiceRecord {
            id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            invoice_number: row.try_get("invoice_number").map_err(internal_error)?,
            customer_email: row.try_get("customer_email").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            issued_at: row.try_get("issued_at").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            settled_at: row.try_get("settled_at").map_err(internal_error)?,
            created_by_agent_id: row.try_get("created_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(FinanceInvoicesResponse {
        generated_at: Utc::now(),
        items,
    }))
}

async fn finance_ar_subledger(
    State(state): State<AppState>,
    Query(query): Query<LedgerQuery>,
) -> std::result::Result<Json<FinanceArSubledgerResponse>, (axum::http::StatusCode, String)> {
    let limit = query.limit.unwrap_or(500).clamp(1, 2000);
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            invoice_id,
            order_id,
            entry_type,
            debit,
            credit,
            balance_after,
            currency,
            memo,
            posted_by_agent_id,
            posted_at
        FROM ar_subledger_entries
        WHERE ($1::uuid IS NULL OR order_id = $1)
        ORDER BY posted_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(query.order_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(AuditArSubledgerEntryRecord {
            id: row.try_get("id").map_err(internal_error)?,
            invoice_id: row.try_get("invoice_id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            entry_type: row.try_get("entry_type").map_err(internal_error)?,
            debit: row.try_get("debit").map_err(internal_error)?,
            credit: row.try_get("credit").map_err(internal_error)?,
            balance_after: row.try_get("balance_after").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            memo: row.try_get("memo").map_err(internal_error)?,
            posted_by_agent_id: row.try_get("posted_by_agent_id").map_err(internal_error)?,
            posted_at: row.try_get("posted_at").map_err(internal_error)?,
        });
    }

    Ok(Json(FinanceArSubledgerResponse {
        generated_at: Utc::now(),
        items,
    }))
}

async fn finance_ap_obligations(
    State(state): State<AppState>,
    Query(query): Query<LedgerQuery>,
) -> std::result::Result<Json<FinanceApObligationsResponse>, (axum::http::StatusCode, String)> {
    let limit = query.limit.unwrap_or(500).clamp(1, 2000);
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            order_id,
            source_type,
            counterparty,
            amount,
            currency,
            status,
            due_at,
            settled_at,
            created_by_agent_id,
            created_at,
            updated_at
        FROM ap_obligations
        WHERE ($1::uuid IS NULL OR order_id = $1)
        ORDER BY created_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(query.order_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(AuditApObligationRecord {
            id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            source_type: row.try_get("source_type").map_err(internal_error)?,
            counterparty: row.try_get("counterparty").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            settled_at: row.try_get("settled_at").map_err(internal_error)?,
            created_by_agent_id: row.try_get("created_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    Ok(Json(FinanceApObligationsResponse {
        generated_at: Utc::now(),
        items,
    }))
}

async fn finance_ap_subledger(
    State(state): State<AppState>,
    Query(query): Query<LedgerQuery>,
) -> std::result::Result<Json<FinanceApSubledgerResponse>, (axum::http::StatusCode, String)> {
    let limit = query.limit.unwrap_or(500).clamp(1, 2000);
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            ap_obligation_id,
            order_id,
            entry_type,
            debit,
            credit,
            balance_after,
            currency,
            memo,
            posted_by_agent_id,
            posted_at
        FROM ap_subledger_entries
        WHERE ($1::uuid IS NULL OR order_id = $1)
        ORDER BY posted_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(query.order_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(AuditApSubledgerEntryRecord {
            id: row.try_get("id").map_err(internal_error)?,
            ap_obligation_id: row.try_get("ap_obligation_id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            entry_type: row.try_get("entry_type").map_err(internal_error)?,
            debit: row.try_get("debit").map_err(internal_error)?,
            credit: row.try_get("credit").map_err(internal_error)?,
            balance_after: row.try_get("balance_after").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            memo: row.try_get("memo").map_err(internal_error)?,
            posted_by_agent_id: row.try_get("posted_by_agent_id").map_err(internal_error)?,
            posted_at: row.try_get("posted_at").map_err(internal_error)?,
        });
    }

    Ok(Json(FinanceApSubledgerResponse {
        generated_at: Utc::now(),
        items,
    }))
}

async fn skill_unit_economics(
    State(state): State<AppState>,
    Query(query): Query<SkillUnitEconomicsQuery>,
) -> std::result::Result<Json<SkillUnitEconomicsResponse>, (axum::http::StatusCode, String)> {
    if let (Some(period_start), Some(period_end)) = (query.period_start, query.period_end) {
        if period_end <= period_start {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "period_end must be greater than period_start".to_string(),
            ));
        }
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query(
        r#"
        WITH filtered_alloc AS (
            SELECT
                order_id,
                COALESCE(NULLIF(BTRIM(skill_id), ''), 'UNSPECIFIED') AS skill_id,
                source_type,
                allocated_cost
            FROM finops_cost_allocations
            WHERE ($1::timestamptz IS NULL OR period_end > $1)
              AND ($2::timestamptz IS NULL OR period_start < $2)
        ),
        order_skill_cost AS (
            SELECT
                order_id,
                skill_id,
                SUM(allocated_cost) AS skill_cost
            FROM filtered_alloc
            GROUP BY order_id, skill_id
        ),
        order_total_skill_cost AS (
            SELECT
                order_id,
                SUM(skill_cost) AS total_skill_cost
            FROM order_skill_cost
            GROUP BY order_id
        ),
        skill_revenue AS (
            SELECT
                osc.skill_id,
                SUM(
                    (o.quantity * o.unit_price)
                    * CASE
                        WHEN otsc.total_skill_cost > 0
                            THEN osc.skill_cost / otsc.total_skill_cost
                        ELSE 0
                      END
                ) AS attributed_revenue
            FROM order_skill_cost osc
            INNER JOIN order_total_skill_cost otsc ON otsc.order_id = osc.order_id
            INNER JOIN orders o ON o.id = osc.order_id AND o.status = 'FULFILLED'
            GROUP BY osc.skill_id
        ),
        skill_costs AS (
            SELECT
                skill_id,
                COUNT(DISTINCT order_id)::BIGINT AS orders_touched,
                COALESCE(SUM(CASE WHEN source_type = 'TOKEN' THEN allocated_cost ELSE 0 END), 0) AS token_cost,
                COALESCE(SUM(CASE WHEN source_type = 'CLOUD' THEN allocated_cost ELSE 0 END), 0) AS cloud_cost,
                COALESCE(SUM(CASE WHEN source_type = 'SUBSCRIPTION' THEN allocated_cost ELSE 0 END), 0) AS subscription_cost,
                COALESCE(SUM(allocated_cost), 0) AS autonomy_cost
            FROM filtered_alloc
            GROUP BY skill_id
        )
        SELECT
            sc.skill_id,
            sc.orders_touched,
            sc.token_cost,
            sc.cloud_cost,
            sc.subscription_cost,
            sc.autonomy_cost,
            COALESCE(sr.attributed_revenue, 0) AS attributed_revenue
        FROM skill_costs sc
        LEFT JOIN skill_revenue sr ON sr.skill_id = sc.skill_id
        ORDER BY sc.autonomy_cost DESC, sc.skill_id ASC
        LIMIT $3
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let autonomy_cost: Decimal = row.try_get("autonomy_cost").map_err(internal_error)?;
        let attributed_revenue: Decimal =
            row.try_get("attributed_revenue").map_err(internal_error)?;
        let margin_after_autonomy_cost = (attributed_revenue - autonomy_cost).round_dp(4);
        let revenue_to_cost_ratio = if autonomy_cost > Decimal::ZERO {
            (attributed_revenue / autonomy_cost).round_dp(4)
        } else {
            Decimal::ZERO
        };

        items.push(SkillUnitEconomicsRow {
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
            orders_touched: row.try_get("orders_touched").map_err(internal_error)?,
            token_cost: row.try_get("token_cost").map_err(internal_error)?,
            cloud_cost: row.try_get("cloud_cost").map_err(internal_error)?,
            subscription_cost: row.try_get("subscription_cost").map_err(internal_error)?,
            autonomy_cost,
            attributed_revenue,
            margin_after_autonomy_cost,
            revenue_to_cost_ratio,
        });
    }

    Ok(Json(SkillUnitEconomicsResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        items,
    }))
}

async fn skill_telemetry(
    State(state): State<AppState>,
    Query(query): Query<SkillTelemetryQuery>,
) -> std::result::Result<Json<SkillTelemetryResponse>, (axum::http::StatusCode, String)> {
    if let (Some(period_start), Some(period_end)) = (query.period_start, query.period_end) {
        if period_end <= period_start {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "period_end must be greater than period_start".to_string(),
            ));
        }
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query(
        r#"
        SELECT
            skill_id,
            skill_version,
            COUNT(*)::BIGINT AS total_invocations,
            COUNT(*) FILTER (WHERE status = 'SUCCESS')::BIGINT AS success_count,
            COUNT(*) FILTER (WHERE status = 'FAILED')::BIGINT AS failed_count,
            COUNT(*) FILTER (WHERE status = 'ESCALATED')::BIGINT AS escalated_count,
            COUNT(*) FILTER (WHERE fallback_used = TRUE)::BIGINT AS fallback_count,
            COALESCE(AVG(latency_ms)::numeric, 0) AS avg_latency_ms,
            COALESCE((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms))::numeric, 0::numeric) AS p95_latency_ms
        FROM skill_invocations
        WHERE ($1::timestamptz IS NULL OR created_at >= $1)
          AND ($2::timestamptz IS NULL OR created_at < $2)
        GROUP BY skill_id, skill_version
        ORDER BY total_invocations DESC, skill_id ASC, skill_version ASC
        LIMIT $3
        "#,
    )
    .bind(query.period_start)
    .bind(query.period_end)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let total_invocations: i64 = row.try_get("total_invocations").map_err(internal_error)?;
        let success_count: i64 = row.try_get("success_count").map_err(internal_error)?;
        let failed_count: i64 = row.try_get("failed_count").map_err(internal_error)?;
        let escalated_count: i64 = row.try_get("escalated_count").map_err(internal_error)?;
        let fallback_count: i64 = row.try_get("fallback_count").map_err(internal_error)?;
        let avg_latency_ms: Decimal = row.try_get("avg_latency_ms").map_err(internal_error)?;
        let p95_latency_ms: Decimal = row.try_get("p95_latency_ms").map_err(internal_error)?;

        let success_rate_pct = if total_invocations > 0 {
            (Decimal::from(success_count) / Decimal::from(total_invocations) * Decimal::new(100, 0))
                .round_dp(4)
        } else {
            Decimal::ZERO
        };
        let escalation_rate_pct = if total_invocations > 0 {
            (Decimal::from(escalated_count) / Decimal::from(total_invocations)
                * Decimal::new(100, 0))
            .round_dp(4)
        } else {
            Decimal::ZERO
        };

        items.push(SkillTelemetryRow {
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
            skill_version: row.try_get("skill_version").map_err(internal_error)?,
            total_invocations,
            success_count,
            failed_count,
            escalated_count,
            fallback_count,
            success_rate_pct,
            escalation_rate_pct,
            avg_latency_ms: avg_latency_ms.round_dp(4),
            p95_latency_ms: p95_latency_ms.round_dp(4),
        });
    }

    Ok(Json(SkillTelemetryResponse {
        generated_at: Utc::now(),
        period_start: query.period_start,
        period_end: query.period_end,
        items,
    }))
}

async fn order_evidence(
    Path(order_id): Path<Uuid>,
    State(state): State<AppState>,
) -> std::result::Result<Json<OrderEvidencePackage>, (axum::http::StatusCode, String)> {
    let order_row = sqlx::query(
        r#"
        SELECT
            id,
            customer_email,
            transaction_type,
            requested_by_agent_id,
            item_code,
            quantity,
            unit_price,
            currency,
            status,
            failure_reason,
            created_at,
            fulfilled_at,
            updated_at
        FROM orders
        WHERE id = $1
        "#,
    )
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_error)?;

    let Some(order_row) = order_row else {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            "order not found".to_string(),
        ));
    };

    let order = AuditOrderRecord {
        id: order_row.try_get("id").map_err(internal_error)?,
        customer_email: order_row
            .try_get("customer_email")
            .map_err(internal_error)?,
        transaction_type: order_row
            .try_get("transaction_type")
            .map_err(internal_error)?,
        requested_by_agent_id: order_row
            .try_get("requested_by_agent_id")
            .map_err(internal_error)?,
        item_code: order_row.try_get("item_code").map_err(internal_error)?,
        quantity: order_row.try_get("quantity").map_err(internal_error)?,
        unit_price: order_row.try_get("unit_price").map_err(internal_error)?,
        currency: order_row.try_get("currency").map_err(internal_error)?,
        status: order_row.try_get("status").map_err(internal_error)?,
        failure_reason: order_row
            .try_get("failure_reason")
            .map_err(internal_error)?,
        created_at: order_row.try_get("created_at").map_err(internal_error)?,
        fulfilled_at: order_row.try_get("fulfilled_at").map_err(internal_error)?,
        updated_at: order_row.try_get("updated_at").map_err(internal_error)?,
    };

    let acceptance_row = sqlx::query(
        r#"
        SELECT
            id,
            quote_id,
            opportunity_id,
            order_id,
            accepted_by,
            acceptance_channel,
            proof_ref,
            requested_by_agent_id,
            accepted_at
        FROM quote_acceptances
        WHERE order_id = $1
        LIMIT 1
        "#,
    )
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut acceptance: Option<AuditAcceptanceRecord> = None;
    let mut quote: Option<AuditQuoteRecord> = None;
    let mut opportunity: Option<AuditOpportunityRecord> = None;
    let mut lead: Option<AuditLeadRecord> = None;

    if let Some(row) = acceptance_row {
        let acceptance_record = AuditAcceptanceRecord {
            id: row.try_get("id").map_err(internal_error)?,
            quote_id: row.try_get("quote_id").map_err(internal_error)?,
            opportunity_id: row.try_get("opportunity_id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            accepted_by: row.try_get("accepted_by").map_err(internal_error)?,
            acceptance_channel: row.try_get("acceptance_channel").map_err(internal_error)?,
            proof_ref: row.try_get("proof_ref").map_err(internal_error)?,
            requested_by_agent_id: row
                .try_get("requested_by_agent_id")
                .map_err(internal_error)?,
            accepted_at: row.try_get("accepted_at").map_err(internal_error)?,
        };

        let quote_row = sqlx::query(
            r#"
            SELECT
                id,
                opportunity_id,
                unit_price,
                quantity,
                currency,
                payment_terms_days,
                valid_until,
                status,
                requested_by_agent_id,
                created_at,
                updated_at
            FROM quotes
            WHERE id = $1
            "#,
        )
        .bind(acceptance_record.quote_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(internal_error)?;

        if let Some(qrow) = quote_row {
            quote = Some(AuditQuoteRecord {
                id: qrow.try_get("id").map_err(internal_error)?,
                opportunity_id: qrow.try_get("opportunity_id").map_err(internal_error)?,
                unit_price: qrow.try_get("unit_price").map_err(internal_error)?,
                quantity: qrow.try_get("quantity").map_err(internal_error)?,
                currency: qrow.try_get("currency").map_err(internal_error)?,
                payment_terms_days: qrow.try_get("payment_terms_days").map_err(internal_error)?,
                valid_until: qrow.try_get("valid_until").map_err(internal_error)?,
                status: qrow.try_get("status").map_err(internal_error)?,
                requested_by_agent_id: qrow
                    .try_get("requested_by_agent_id")
                    .map_err(internal_error)?,
                created_at: qrow.try_get("created_at").map_err(internal_error)?,
                updated_at: qrow.try_get("updated_at").map_err(internal_error)?,
            });
        }

        let opportunity_row = sqlx::query(
            r#"
            SELECT
                id,
                lead_id,
                customer_email,
                transaction_type,
                item_code,
                quantity,
                target_unit_price,
                currency,
                risk_class,
                stage,
                requested_by_agent_id,
                created_at,
                updated_at
            FROM opportunities
            WHERE id = $1
            "#,
        )
        .bind(acceptance_record.opportunity_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(internal_error)?;

        if let Some(orow) = opportunity_row {
            let lead_id: Uuid = orow.try_get("lead_id").map_err(internal_error)?;
            opportunity = Some(AuditOpportunityRecord {
                id: orow.try_get("id").map_err(internal_error)?,
                lead_id,
                customer_email: orow.try_get("customer_email").map_err(internal_error)?,
                transaction_type: orow.try_get("transaction_type").map_err(internal_error)?,
                item_code: orow.try_get("item_code").map_err(internal_error)?,
                quantity: orow.try_get("quantity").map_err(internal_error)?,
                target_unit_price: orow.try_get("target_unit_price").map_err(internal_error)?,
                currency: orow.try_get("currency").map_err(internal_error)?,
                risk_class: orow.try_get("risk_class").map_err(internal_error)?,
                stage: orow.try_get("stage").map_err(internal_error)?,
                requested_by_agent_id: orow
                    .try_get("requested_by_agent_id")
                    .map_err(internal_error)?,
                created_at: orow.try_get("created_at").map_err(internal_error)?,
                updated_at: orow.try_get("updated_at").map_err(internal_error)?,
            });

            let lead_row = sqlx::query(
                r#"
                SELECT
                    id,
                    contact_email,
                    source_channel,
                    note,
                    status,
                    requested_by_agent_id,
                    created_at
                FROM leads
                WHERE id = $1
                "#,
            )
            .bind(lead_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(internal_error)?;

            if let Some(lrow) = lead_row {
                lead = Some(AuditLeadRecord {
                    id: lrow.try_get("id").map_err(internal_error)?,
                    contact_email: lrow.try_get("contact_email").map_err(internal_error)?,
                    source_channel: lrow.try_get("source_channel").map_err(internal_error)?,
                    note: lrow.try_get("note").map_err(internal_error)?,
                    status: lrow.try_get("status").map_err(internal_error)?,
                    requested_by_agent_id: lrow
                        .try_get("requested_by_agent_id")
                        .map_err(internal_error)?,
                    created_at: lrow.try_get("created_at").map_err(internal_error)?,
                });
            }
        }

        acceptance = Some(acceptance_record);
    }

    let acceptance_id = acceptance.as_ref().map(|record| record.id);
    let acceptance_proof_ref = acceptance.as_ref().map(|record| record.proof_ref.clone());
    let quote_id = quote.as_ref().map(|record| record.id);
    let opportunity_id = opportunity.as_ref().map(|record| record.id);
    let lead_id = lead.as_ref().map(|record| record.id);

    let origination_proof_rows = sqlx::query(
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
        WHERE
            ($1::uuid IS NOT NULL AND lead_id = $1)
            OR ($2::uuid IS NOT NULL AND opportunity_id = $2)
            OR ($3::uuid IS NOT NULL AND quote_id = $3)
            OR ($4::uuid IS NOT NULL AND acceptance_id = $4)
            OR ($5::text IS NOT NULL AND proof_ref = $5)
        ORDER BY captured_at
        "#,
    )
    .bind(lead_id)
    .bind(opportunity_id)
    .bind(quote_id)
    .bind(acceptance_id)
    .bind(acceptance_proof_ref.as_deref())
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut origination_proofs = Vec::with_capacity(origination_proof_rows.len());
    for row in origination_proof_rows {
        origination_proofs.push(AuditOriginationProofRecord {
            id: row.try_get("id").map_err(internal_error)?,
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

    let escalation_rows = sqlx::query(
        r#"
        SELECT
            id,
            action_type,
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
        WHERE reference_type = 'ORDER'
          AND reference_id = $1
        ORDER BY created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut escalations = Vec::with_capacity(escalation_rows.len());
    for row in escalation_rows {
        escalations.push(AuditEscalationRecord {
            id: row.try_get("id").map_err(internal_error)?,
            action_type: row.try_get("action_type").map_err(internal_error)?,
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

    let movement_rows = sqlx::query(
        r#"
        SELECT id, movement_type, item_code, quantity, unit_cost, created_at
        FROM inventory_movements
        WHERE order_id = $1
        ORDER BY created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut inventory_movements = Vec::with_capacity(movement_rows.len());
    for row in movement_rows {
        inventory_movements.push(AuditInventoryMovementRecord {
            id: row.try_get("id").map_err(internal_error)?,
            movement_type: row.try_get("movement_type").map_err(internal_error)?,
            item_code: row.try_get("item_code").map_err(internal_error)?,
            quantity: row.try_get("quantity").map_err(internal_error)?,
            unit_cost: row.try_get("unit_cost").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
        });
    }

    let journal_rows = sqlx::query(
        r#"
        SELECT id, account, debit, credit, memo, posted_at
        FROM journals
        WHERE order_id = $1
        ORDER BY posted_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut journals = Vec::with_capacity(journal_rows.len());
    for row in journal_rows {
        journals.push(AuditJournalRecord {
            id: row.try_get("id").map_err(internal_error)?,
            account: row.try_get("account").map_err(internal_error)?,
            debit: row.try_get("debit").map_err(internal_error)?,
            credit: row.try_get("credit").map_err(internal_error)?,
            memo: row.try_get("memo").map_err(internal_error)?,
            posted_at: row.try_get("posted_at").map_err(internal_error)?,
        });
    }

    let invoice_row = sqlx::query(
        r#"
        SELECT
            id,
            order_id,
            invoice_number,
            customer_email,
            amount,
            currency,
            status,
            issued_at,
            due_at,
            settled_at,
            created_by_agent_id,
            created_at,
            updated_at
        FROM invoices
        WHERE order_id = $1
        LIMIT 1
        "#,
    )
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_error)?;

    let invoice = if let Some(row) = invoice_row {
        Some(AuditInvoiceRecord {
            id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            invoice_number: row.try_get("invoice_number").map_err(internal_error)?,
            customer_email: row.try_get("customer_email").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            issued_at: row.try_get("issued_at").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            settled_at: row.try_get("settled_at").map_err(internal_error)?,
            created_by_agent_id: row.try_get("created_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        })
    } else {
        None
    };

    let ar_subledger_rows = sqlx::query(
        r#"
        SELECT
            id,
            invoice_id,
            order_id,
            entry_type,
            debit,
            credit,
            balance_after,
            currency,
            memo,
            posted_by_agent_id,
            posted_at
        FROM ar_subledger_entries
        WHERE order_id = $1
        ORDER BY
            posted_at,
            CASE entry_type
                WHEN 'INVOICE_ISSUED' THEN 0
                WHEN 'PAYMENT_RECEIVED' THEN 1
                ELSE 2
            END,
            id
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut ar_subledger_entries = Vec::with_capacity(ar_subledger_rows.len());
    for row in ar_subledger_rows {
        ar_subledger_entries.push(AuditArSubledgerEntryRecord {
            id: row.try_get("id").map_err(internal_error)?,
            invoice_id: row.try_get("invoice_id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            entry_type: row.try_get("entry_type").map_err(internal_error)?,
            debit: row.try_get("debit").map_err(internal_error)?,
            credit: row.try_get("credit").map_err(internal_error)?,
            balance_after: row.try_get("balance_after").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            memo: row.try_get("memo").map_err(internal_error)?,
            posted_by_agent_id: row.try_get("posted_by_agent_id").map_err(internal_error)?,
            posted_at: row.try_get("posted_at").map_err(internal_error)?,
        });
    }

    let ap_obligation_rows = sqlx::query(
        r#"
        SELECT
            id,
            order_id,
            source_type,
            counterparty,
            amount,
            currency,
            status,
            due_at,
            settled_at,
            created_by_agent_id,
            created_at,
            updated_at
        FROM ap_obligations
        WHERE order_id = $1
        ORDER BY created_at, id
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut ap_obligations = Vec::with_capacity(ap_obligation_rows.len());
    for row in ap_obligation_rows {
        ap_obligations.push(AuditApObligationRecord {
            id: row.try_get("id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            source_type: row.try_get("source_type").map_err(internal_error)?,
            counterparty: row.try_get("counterparty").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            due_at: row.try_get("due_at").map_err(internal_error)?,
            settled_at: row.try_get("settled_at").map_err(internal_error)?,
            created_by_agent_id: row.try_get("created_by_agent_id").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            updated_at: row.try_get("updated_at").map_err(internal_error)?,
        });
    }

    let ap_subledger_rows = sqlx::query(
        r#"
        SELECT
            id,
            ap_obligation_id,
            order_id,
            entry_type,
            debit,
            credit,
            balance_after,
            currency,
            memo,
            posted_by_agent_id,
            posted_at
        FROM ap_subledger_entries
        WHERE order_id = $1
        ORDER BY posted_at, id
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut ap_subledger_entries = Vec::with_capacity(ap_subledger_rows.len());
    for row in ap_subledger_rows {
        ap_subledger_entries.push(AuditApSubledgerEntryRecord {
            id: row.try_get("id").map_err(internal_error)?,
            ap_obligation_id: row.try_get("ap_obligation_id").map_err(internal_error)?,
            order_id: row.try_get("order_id").map_err(internal_error)?,
            entry_type: row.try_get("entry_type").map_err(internal_error)?,
            debit: row.try_get("debit").map_err(internal_error)?,
            credit: row.try_get("credit").map_err(internal_error)?,
            balance_after: row.try_get("balance_after").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            memo: row.try_get("memo").map_err(internal_error)?,
            posted_by_agent_id: row.try_get("posted_by_agent_id").map_err(internal_error)?,
            posted_at: row.try_get("posted_at").map_err(internal_error)?,
        });
    }

    let settlement_rows = sqlx::query(
        r#"
        SELECT id, amount, currency, received_at
        FROM settlements
        WHERE order_id = $1
        ORDER BY received_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut settlements = Vec::with_capacity(settlement_rows.len());
    for row in settlement_rows {
        settlements.push(AuditSettlementRecord {
            id: row.try_get("id").map_err(internal_error)?,
            amount: row.try_get("amount").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            received_at: row.try_get("received_at").map_err(internal_error)?,
        });
    }

    let payroll_rows = sqlx::query(
        r#"
        SELECT
            id,
            period_start,
            period_end,
            source_type,
            source_id,
            agent_id,
            skill_id,
            allocation_basis,
            allocated_cost,
            currency,
            created_at
        FROM finops_cost_allocations
        WHERE order_id = $1
        ORDER BY created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut payroll_allocations = Vec::with_capacity(payroll_rows.len());
    for row in payroll_rows {
        payroll_allocations.push(AuditPayrollAllocationRecord {
            id: row.try_get("id").map_err(internal_error)?,
            period_start: row.try_get("period_start").map_err(internal_error)?,
            period_end: row.try_get("period_end").map_err(internal_error)?,
            source_type: row.try_get("source_type").map_err(internal_error)?,
            source_id: row.try_get("source_id").map_err(internal_error)?,
            agent_id: row.try_get("agent_id").map_err(internal_error)?,
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
            allocation_basis: row.try_get("allocation_basis").map_err(internal_error)?,
            allocated_cost: row.try_get("allocated_cost").map_err(internal_error)?,
            currency: row.try_get("currency").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
        });
    }

    let skill_rows = sqlx::query(
        r#"
        SELECT
            id,
            intent,
            capability,
            skill_id,
            skill_version,
            actor_agent_id,
            attempt_no,
            status,
            failure_reason,
            fallback_used,
            input_hash,
            output_hash,
            latency_ms,
            started_at,
            completed_at
        FROM skill_invocations
        WHERE order_id = $1
        ORDER BY started_at, created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut skill_invocations = Vec::with_capacity(skill_rows.len());
    for row in skill_rows {
        skill_invocations.push(AuditSkillInvocationRecord {
            id: row.try_get("id").map_err(internal_error)?,
            intent: row.try_get("intent").map_err(internal_error)?,
            capability: row.try_get("capability").map_err(internal_error)?,
            skill_id: row.try_get("skill_id").map_err(internal_error)?,
            skill_version: row.try_get("skill_version").map_err(internal_error)?,
            actor_agent_id: row.try_get("actor_agent_id").map_err(internal_error)?,
            attempt_no: row.try_get("attempt_no").map_err(internal_error)?,
            status: row.try_get("status").map_err(internal_error)?,
            failure_reason: row.try_get("failure_reason").map_err(internal_error)?,
            fallback_used: row.try_get("fallback_used").map_err(internal_error)?,
            input_hash: row.try_get("input_hash").map_err(internal_error)?,
            output_hash: row.try_get("output_hash").map_err(internal_error)?,
            latency_ms: row.try_get("latency_ms").map_err(internal_error)?,
            started_at: row.try_get("started_at").map_err(internal_error)?,
            completed_at: row.try_get("completed_at").map_err(internal_error)?,
        });
    }

    let memory_rows = sqlx::query(
        r#"
        SELECT
            id,
            agent_name,
            scope,
            content,
            keywords,
            source_ref,
            created_at,
            access_count,
            last_accessed_at
        FROM agent_semantic_memory
        WHERE entity_id = $1
        ORDER BY created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut memories = Vec::with_capacity(memory_rows.len());
    for row in memory_rows {
        memories.push(AuditMemoryRecord {
            id: row.try_get("id").map_err(internal_error)?,
            agent_name: row.try_get("agent_name").map_err(internal_error)?,
            scope: row.try_get("scope").map_err(internal_error)?,
            content: row.try_get("content").map_err(internal_error)?,
            keywords: row.try_get("keywords").map_err(internal_error)?,
            source_ref: row.try_get("source_ref").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
            access_count: row.try_get("access_count").map_err(internal_error)?,
            last_accessed_at: row.try_get("last_accessed_at").map_err(internal_error)?,
        });
    }

    let memory_provenance_rows = sqlx::query(
        r#"
        SELECT
            id,
            memory_id,
            entity_id,
            action_type,
            actor_agent_id,
            source_ref,
            query_text,
            created_at
        FROM agent_memory_provenance
        WHERE entity_id = $1
        ORDER BY created_at
        "#,
    )
    .bind(order_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut memory_provenance = Vec::with_capacity(memory_provenance_rows.len());
    for row in memory_provenance_rows {
        memory_provenance.push(AuditMemoryProvenanceRecord {
            id: row.try_get("id").map_err(internal_error)?,
            memory_id: row.try_get("memory_id").map_err(internal_error)?,
            entity_id: row.try_get("entity_id").map_err(internal_error)?,
            action_type: row.try_get("action_type").map_err(internal_error)?,
            actor_agent_id: row.try_get("actor_agent_id").map_err(internal_error)?,
            source_ref: row.try_get("source_ref").map_err(internal_error)?,
            query_text: row.try_get("query_text").map_err(internal_error)?,
            created_at: row.try_get("created_at").map_err(internal_error)?,
        });
    }

    let mut timeline = Vec::new();
    timeline.push(AuditTimelineEvent {
        occurred_at: order.created_at,
        event_type: "ORDER_CREATED".to_string(),
        source: "orders".to_string(),
        details: format!(
            "{} {} x {} at {}",
            order.transaction_type, order.item_code, order.quantity, order.unit_price
        ),
    });

    if let Some(ref acc) = acceptance {
        timeline.push(AuditTimelineEvent {
            occurred_at: acc.accepted_at,
            event_type: "QUOTE_ACCEPTED".to_string(),
            source: "quote_acceptances".to_string(),
            details: format!(
                "accepted_by={} channel={} proof={}",
                acc.accepted_by, acc.acceptance_channel, acc.proof_ref
            ),
        });
    }

    for proof in &origination_proofs {
        timeline.push(AuditTimelineEvent {
            occurred_at: proof.captured_at,
            event_type: "ORIGINATION_PROOF_CAPTURED".to_string(),
            source: "origination_channel_proofs".to_string(),
            details: format!(
                "channel={} message_id={} proof_ref={}",
                proof.channel_type, proof.message_id, proof.proof_ref
            ),
        });
    }

    for escalation in &escalations {
        timeline.push(AuditTimelineEvent {
            occurred_at: escalation.created_at,
            event_type: "GOVERNANCE_ESCALATED".to_string(),
            source: "governance_escalations".to_string(),
            details: format!(
                "{} amount={} status={} reason={}",
                escalation.action_type,
                escalation.amount,
                escalation.status,
                escalation.reason_code
            ),
        });

        if let Some(decided_at) = escalation.decided_at {
            timeline.push(AuditTimelineEvent {
                occurred_at: decided_at,
                event_type: "GOVERNANCE_DECIDED".to_string(),
                source: "governance_escalations".to_string(),
                details: format!(
                    "status={} decided_by={} note={}",
                    escalation.status,
                    escalation
                        .decided_by_agent_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    escalation.decision_note.clone().unwrap_or_default()
                ),
            });
        }
    }

    for movement in &inventory_movements {
        timeline.push(AuditTimelineEvent {
            occurred_at: movement.created_at,
            event_type: format!("INVENTORY_{}", movement.movement_type),
            source: "inventory_movements".to_string(),
            details: format!(
                "item={} qty={} unit_cost={}",
                movement.item_code, movement.quantity, movement.unit_cost
            ),
        });
    }

    if let Some(ref invoice_record) = invoice {
        timeline.push(AuditTimelineEvent {
            occurred_at: invoice_record.issued_at,
            event_type: "INVOICE_ISSUED".to_string(),
            source: "invoices".to_string(),
            details: format!(
                "invoice={} amount={} {} due_at={} status={}",
                invoice_record.invoice_number,
                invoice_record.amount,
                invoice_record.currency,
                invoice_record.due_at,
                invoice_record.status
            ),
        });
    }

    for entry in &ar_subledger_entries {
        timeline.push(AuditTimelineEvent {
            occurred_at: entry.posted_at,
            event_type: format!("AR_{}", entry.entry_type),
            source: "ar_subledger_entries".to_string(),
            details: format!(
                "debit={} credit={} balance_after={} memo={}",
                entry.debit, entry.credit, entry.balance_after, entry.memo
            ),
        });
    }

    for obligation in &ap_obligations {
        timeline.push(AuditTimelineEvent {
            occurred_at: obligation.created_at,
            event_type: "AP_OBLIGATION_RECORDED".to_string(),
            source: "ap_obligations".to_string(),
            details: format!(
                "source_type={} counterparty={} amount={} {} status={} due_at={}",
                obligation.source_type,
                obligation.counterparty,
                obligation.amount,
                obligation.currency,
                obligation.status,
                obligation.due_at
            ),
        });
    }

    for entry in &ap_subledger_entries {
        timeline.push(AuditTimelineEvent {
            occurred_at: entry.posted_at,
            event_type: format!("AP_{}", entry.entry_type),
            source: "ap_subledger_entries".to_string(),
            details: format!(
                "debit={} credit={} balance_after={} memo={}",
                entry.debit, entry.credit, entry.balance_after, entry.memo
            ),
        });
    }

    for journal in &journals {
        timeline.push(AuditTimelineEvent {
            occurred_at: journal.posted_at,
            event_type: "JOURNAL_POSTED".to_string(),
            source: "journals".to_string(),
            details: format!(
                "account={} debit={} credit={} memo={}",
                journal.account, journal.debit, journal.credit, journal.memo
            ),
        });
    }

    for allocation in &payroll_allocations {
        timeline.push(AuditTimelineEvent {
            occurred_at: allocation.created_at,
            event_type: "PAYROLL_COST_ALLOCATED".to_string(),
            source: "finops_cost_allocations".to_string(),
            details: format!(
                "source_type={} basis={} skill={} allocated_cost={} {}",
                allocation.source_type,
                allocation.allocation_basis,
                allocation
                    .skill_id
                    .clone()
                    .unwrap_or_else(|| "UNSPECIFIED".to_string()),
                allocation.allocated_cost,
                allocation.currency
            ),
        });
    }

    for invocation in &skill_invocations {
        timeline.push(AuditTimelineEvent {
            occurred_at: invocation.started_at,
            event_type: "SKILL_INVOKED".to_string(),
            source: "skill_invocations".to_string(),
            details: format!(
                "intent={} skill={}@{} attempt={} fallback_used={} status={} failure={}",
                invocation.intent,
                invocation.skill_id,
                invocation.skill_version,
                invocation.attempt_no,
                invocation.fallback_used,
                invocation.status,
                invocation.failure_reason.clone().unwrap_or_default()
            ),
        });
    }

    for settlement in &settlements {
        timeline.push(AuditTimelineEvent {
            occurred_at: settlement.received_at,
            event_type: "SETTLEMENT_RECEIVED".to_string(),
            source: "settlements".to_string(),
            details: format!("amount={} {}", settlement.amount, settlement.currency),
        });
    }

    for memory in &memories {
        timeline.push(AuditTimelineEvent {
            occurred_at: memory.created_at,
            event_type: "MEMORY_STORED".to_string(),
            source: "agent_semantic_memory".to_string(),
            details: format!(
                "agent={} scope={} source_ref={}",
                memory.agent_name,
                memory.scope,
                memory.source_ref.clone().unwrap_or_default()
            ),
        });
    }

    for provenance in &memory_provenance {
        let event_type = match provenance.action_type.as_str() {
            "READ" => "MEMORY_RECALLED",
            "WRITE" => "MEMORY_PROVENANCE_WRITE",
            "RETENTION_PRUNE" => "MEMORY_RETENTION_PRUNE",
            _ => "MEMORY_PROVENANCE",
        };
        timeline.push(AuditTimelineEvent {
            occurred_at: provenance.created_at,
            event_type: event_type.to_string(),
            source: "agent_memory_provenance".to_string(),
            details: format!(
                "action={} actor={} source_ref={} query={}",
                provenance.action_type,
                provenance.actor_agent_id,
                provenance.source_ref,
                provenance.query_text.clone().unwrap_or_default()
            ),
        });
    }

    if let Some(fulfilled_at) = order.fulfilled_at {
        timeline.push(AuditTimelineEvent {
            occurred_at: fulfilled_at,
            event_type: "ORDER_FULFILLED".to_string(),
            source: "orders".to_string(),
            details: format!("status={}", order.status),
        });
    }

    if order.status == "FAILED" {
        timeline.push(AuditTimelineEvent {
            occurred_at: order.updated_at,
            event_type: "ORDER_FAILED".to_string(),
            source: "orders".to_string(),
            details: format!(
                "failure_reason={}",
                order.failure_reason.clone().unwrap_or_default()
            ),
        });
    }

    timeline.sort_by(|a, b| a.occurred_at.cmp(&b.occurred_at));

    let line_value_total = (order.quantity * order.unit_price).round_dp(4);
    let invoice_total = invoice
        .as_ref()
        .map(|record| record.amount)
        .unwrap_or(Decimal::ZERO)
        .round_dp(4);
    let ar_open_balance = ar_subledger_entries
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.debit - line.credit)
        .round_dp(4);
    let ap_open_balance = ap_subledger_entries
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.credit - line.debit)
        .round_dp(4);
    let journal_debit_total = journals
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.debit)
        .round_dp(4);
    let journal_credit_total = journals
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.credit)
        .round_dp(4);
    let cogs_total = journals
        .iter()
        .filter(|line| line.account == "5000")
        .fold(Decimal::ZERO, |acc, line| acc + line.debit)
        .round_dp(4);
    let settlement_total = settlements
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.amount)
        .round_dp(4);
    let autonomy_cost_total = payroll_allocations
        .iter()
        .fold(Decimal::ZERO, |acc, line| acc + line.allocated_cost)
        .round_dp(4);
    let margin_after_autonomy_cost =
        (line_value_total - cogs_total - autonomy_cost_total).round_dp(4);

    let package = OrderEvidencePackage {
        generated_at: Utc::now(),
        order,
        lead,
        opportunity,
        quote,
        acceptance,
        origination_proofs,
        escalations,
        inventory_movements,
        journals,
        invoice,
        ar_subledger_entries,
        ap_obligations,
        ap_subledger_entries,
        settlements,
        payroll_allocations,
        skill_invocations,
        memories,
        memory_provenance,
        timeline,
        totals: AuditTotals {
            line_value_total,
            invoice_total,
            ar_open_balance,
            ap_open_balance,
            journal_debit_total,
            journal_credit_total,
            cogs_total,
            settlement_total,
            autonomy_cost_total,
            margin_after_autonomy_cost,
        },
    };

    Ok(Json(package))
}

fn validate_period_bounds(
    period_start: Option<DateTime<Utc>>,
    period_end: Option<DateTime<Utc>>,
) -> std::result::Result<(), (axum::http::StatusCode, String)> {
    if let (Some(period_start), Some(period_end)) = (period_start, period_end) {
        if period_end <= period_start {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "period_end must be greater than period_start".to_string(),
            ));
        }
    }

    Ok(())
}

fn account_category(account: &str) -> Option<&'static str> {
    match account.chars().next() {
        Some('1') => Some("ASSET"),
        Some('2') => Some("LIABILITY"),
        Some('3') => Some("EQUITY"),
        _ => None,
    }
}

fn aging_bucket_label(age_days: i64) -> &'static str {
    if age_days <= 0 {
        "CURRENT"
    } else if age_days <= 30 {
        "1_30"
    } else if age_days <= 60 {
        "31_60"
    } else if age_days <= 90 {
        "61_90"
    } else {
        "90_PLUS"
    }
}

fn accumulate_aging_bucket(totals: &mut AgingBucketTotals, bucket: &str, amount: Decimal) {
    match bucket {
        "CURRENT" => totals.current += amount,
        "1_30" => totals.days_1_30 += amount,
        "31_60" => totals.days_31_60 += amount,
        "61_90" => totals.days_61_90 += amount,
        _ => totals.days_90_plus += amount,
    }

    totals.current = totals.current.round_dp(4);
    totals.days_1_30 = totals.days_1_30.round_dp(4);
    totals.days_31_60 = totals.days_31_60.round_dp(4);
    totals.days_61_90 = totals.days_61_90.round_dp(4);
    totals.days_90_plus = totals.days_90_plus.round_dp(4);
}

fn internal_error<E: std::fmt::Display>(err: E) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.to_string(),
    )
}
