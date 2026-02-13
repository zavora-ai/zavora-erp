use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{PgPool, Row};
use tracing::info;
use uuid::Uuid;
use zavora_platform::{BoardPack, ServiceConfig, connect_database};

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Serialize)]
struct OrderEvidencePackage {
    generated_at: DateTime<Utc>,
    order: AuditOrderRecord,
    lead: Option<AuditLeadRecord>,
    opportunity: Option<AuditOpportunityRecord>,
    quote: Option<AuditQuoteRecord>,
    acceptance: Option<AuditAcceptanceRecord>,
    escalations: Vec<AuditEscalationRecord>,
    inventory_movements: Vec<AuditInventoryMovementRecord>,
    journals: Vec<AuditJournalRecord>,
    settlements: Vec<AuditSettlementRecord>,
    payroll_allocations: Vec<AuditPayrollAllocationRecord>,
    memories: Vec<AuditMemoryRecord>,
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
struct AuditTimelineEvent {
    occurred_at: DateTime<Utc>,
    event_type: String,
    source: String,
    details: String,
}

#[derive(Debug, Serialize)]
struct AuditTotals {
    line_value_total: Decimal,
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
                "source_type={} basis={} allocated_cost={} {}",
                allocation.source_type,
                allocation.allocation_basis,
                allocation.allocated_cost,
                allocation.currency
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
        escalations,
        inventory_movements,
        journals,
        settlements,
        payroll_allocations,
        memories,
        timeline,
        totals: AuditTotals {
            line_value_total,
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

fn internal_error<E: std::fmt::Display>(err: E) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.to_string(),
    )
}
