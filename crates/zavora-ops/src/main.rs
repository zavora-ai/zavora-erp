use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use redis::Msg;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{error::Error as StdError, fmt};
use tracing::{error, info};
use uuid::Uuid;
use zavora_platform::{
    OrderCreatedEvent, OrderFulfilledEvent, RedisBus, ServiceConfig, connect_database,
};

const AR_ACCOUNT: &str = "1100";
const INVENTORY_ACCOUNT: &str = "1300";
const REVENUE_ACCOUNT: &str = "4000";
const COGS_ACCOUNT: &str = "5000";
const CASH_ACCOUNT: &str = "1000";
const SERVICE_COST_CLEARING_ACCOUNT: &str = "2100";
const OPS_AGENT_ID: &str = "ops-orchestrator-agent";
const MEMORY_SCOPE_ORDER_EXECUTION: &str = "ORDER_EXECUTION";
const MEMORY_SCOPE_PRODUCT_EXECUTION: &str = "PRODUCT_EXECUTION";
const MEMORY_SCOPE_SERVICE_EXECUTION: &str = "SERVICE_EXECUTION";

const SKILL_STATUS_SUCCESS: &str = "SUCCESS";
const SKILL_STATUS_FAILED: &str = "FAILED";
const SKILL_STATUS_ESCALATED: &str = "ESCALATED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionType {
    Product,
    Service,
}

impl TransactionType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Product => "PRODUCT",
            Self::Service => "SERVICE",
        }
    }
}

#[derive(Debug, Clone)]
struct SkillExecutionContext {
    order_id: Uuid,
    item_code: String,
    quantity: Decimal,
    unit_price: Decimal,
    currency: String,
    transaction_type: TransactionType,
}

#[derive(Debug, Clone)]
struct SkillRoutingPolicy {
    intent: String,
    capability: String,
    primary_skill_id: String,
    primary_skill_version: String,
    fallback_skill_id: Option<String>,
    fallback_skill_version: Option<String>,
    max_retries: i32,
    escalation_action_type: String,
}

#[derive(Debug, Clone)]
struct SkillInvocationOutcome {
    success: bool,
    failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct SkillEscalatedError {
    escalation_id: Uuid,
    reason: String,
}

impl fmt::Display for SkillEscalatedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "skill execution failed and escalated with id {}: {}",
            self.escalation_id, self.reason
        )
    }
}

impl StdError for SkillEscalatedError {}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "zavora_ops=info".to_string()),
        )
        .init();

    let config = ServiceConfig::worker_from_env()?;
    let pool = connect_database(&config.database_url).await?;
    let redis = RedisBus::connect(&config.redis_url)?;

    let mut pubsub = redis.client().get_async_pubsub().await?;
    pubsub.subscribe("orders.created").await?;
    let mut messages = pubsub.on_message();

    info!("ops worker subscribed to orders.created");

    loop {
        let msg = messages
            .next()
            .await
            .context("orders.created stream ended unexpectedly")?;
        if let Err(err) = handle_message(&pool, &redis, msg).await {
            error!("failed to process message: {err:#}");
        }
    }
}

async fn handle_message(pool: &PgPool, redis: &RedisBus, msg: Msg) -> Result<()> {
    let payload: String = msg.get_payload()?;
    let event: OrderCreatedEvent = serde_json::from_str(&payload)?;

    match process_order(pool, event.order_id).await {
        Ok(done) => {
            redis.publish_json("orders.fulfilled", &done).await?;
            info!("order {} fulfilled", done.order_id);
            Ok(())
        }
        Err(err) => {
            mark_order_failed(pool, event.order_id, &err.to_string()).await?;
            let _ = write_failure_memory(pool, event.order_id, &err.to_string()).await;
            Err(err)
        }
    }
}

async fn process_order(pool: &PgPool, order_id: Uuid) -> Result<OrderFulfilledEvent> {
    let mut tx = pool.begin().await?;

    let order_row = sqlx::query(
        "SELECT COALESCE(transaction_type, 'PRODUCT') AS transaction_type, customer_email, item_code, quantity, unit_price, currency FROM orders WHERE id = $1 FOR UPDATE",
    )
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await?
    .context("order not found")?;

    let item_code: String = order_row.try_get("item_code")?;
    let quantity: Decimal = order_row.try_get("quantity")?;
    let unit_price: Decimal = order_row.try_get("unit_price")?;
    let currency: String = order_row.try_get("currency")?;
    let customer_email: String = order_row.try_get("customer_email")?;
    let transaction_type_raw: String = order_row.try_get("transaction_type")?;
    let transaction_type = parse_transaction_type(&transaction_type_raw)?;

    if quantity <= Decimal::ZERO || unit_price <= Decimal::ZERO {
        anyhow::bail!("order has invalid quantity or price");
    }

    let recalled_memories = recall_memories_for_execution(
        &mut tx,
        order_id,
        &item_code,
        &customer_email,
        transaction_type,
    )
    .await?;

    let skill_context = SkillExecutionContext {
        order_id,
        item_code: item_code.clone(),
        quantity,
        unit_price,
        currency: currency.clone(),
        transaction_type,
    };
    if let Err(err) = execute_skill_plan(&mut tx, &skill_context).await {
        if let Some(escalation) = err.downcast_ref::<SkillEscalatedError>() {
            let now = Utc::now();
            sqlx::query(
                "UPDATE orders SET status = 'FAILED', failure_reason = $2, updated_at = $3 WHERE id = $1",
            )
            .bind(order_id)
            .bind(escalation.to_string())
            .bind(now)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            anyhow::bail!(escalation.to_string());
        }

        return Err(err);
    }

    let cogs = if transaction_type == TransactionType::Product {
        let (on_hand, average_cost) =
            ensure_inventory_for_order(&mut tx, &item_code, quantity, unit_price, order_id).await?;

        if on_hand < quantity {
            anyhow::bail!("inventory still insufficient after procurement");
        }

        let remaining_qty = on_hand - quantity;
        let product_cogs = (quantity * average_cost).round_dp(4);

        sqlx::query(
            "UPDATE inventory_positions SET on_hand = $2, updated_at = $3 WHERE item_code = $1",
        )
        .bind(&item_code)
        .bind(remaining_qty)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO inventory_movements (
                id, order_id, item_code, movement_type, quantity, unit_cost, created_at
            )
            VALUES ($1, $2, $3, 'ISSUE', $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order_id)
        .bind(&item_code)
        .bind(quantity)
        .bind(average_cost)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;

        product_cogs
    } else {
        (quantity * unit_price * service_delivery_cost_ratio()).round_dp(4)
    };

    let revenue = (quantity * unit_price).round_dp(4);

    insert_journal(
        &mut tx,
        order_id,
        AR_ACCOUNT,
        revenue,
        Decimal::ZERO,
        "Invoice posted",
    )
    .await?;
    insert_journal(
        &mut tx,
        order_id,
        REVENUE_ACCOUNT,
        Decimal::ZERO,
        revenue,
        "Revenue recognized",
    )
    .await?;
    insert_journal(
        &mut tx,
        order_id,
        COGS_ACCOUNT,
        cogs,
        Decimal::ZERO,
        "COGS recognized",
    )
    .await?;
    if transaction_type == TransactionType::Product {
        insert_journal(
            &mut tx,
            order_id,
            INVENTORY_ACCOUNT,
            Decimal::ZERO,
            cogs,
            "Inventory relieved",
        )
        .await?;
    } else {
        insert_journal(
            &mut tx,
            order_id,
            SERVICE_COST_CLEARING_ACCOUNT,
            Decimal::ZERO,
            cogs,
            "Service delivery cost recognized",
        )
        .await?;
    }
    insert_journal(
        &mut tx,
        order_id,
        CASH_ACCOUNT,
        revenue,
        Decimal::ZERO,
        "Cash receipt",
    )
    .await?;
    insert_journal(
        &mut tx,
        order_id,
        AR_ACCOUNT,
        Decimal::ZERO,
        revenue,
        "AR settled",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO settlements (id, order_id, amount, currency, received_at)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(revenue)
    .bind(&currency)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE orders SET status = 'FULFILLED', fulfilled_at = $2, updated_at = $2 WHERE id = $1",
    )
    .bind(order_id)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await?;

    write_execution_memory(
        &mut tx,
        order_id,
        &item_code,
        transaction_type,
        revenue,
        cogs,
        recalled_memories,
    )
    .await?;

    tx.commit().await?;

    Ok(OrderFulfilledEvent {
        order_id,
        settled_amount: revenue,
        currency,
    })
}

async fn ensure_inventory_for_order(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    item_code: &str,
    requested_qty: Decimal,
    unit_price: Decimal,
    order_id: Uuid,
) -> Result<(Decimal, Decimal)> {
    let maybe_row = sqlx::query(
        "SELECT on_hand, avg_cost FROM inventory_positions WHERE item_code = $1 FOR UPDATE",
    )
    .bind(item_code)
    .fetch_optional(&mut **tx)
    .await?;

    let (mut on_hand, mut avg_cost) = if let Some(row) = maybe_row {
        (
            row.try_get::<Decimal, _>("on_hand")?,
            row.try_get::<Decimal, _>("avg_cost")?,
        )
    } else {
        sqlx::query(
            "INSERT INTO inventory_positions (item_code, on_hand, avg_cost, updated_at) VALUES ($1, 0, 0, $2)",
        )
        .bind(item_code)
        .bind(Utc::now())
        .execute(&mut **tx)
        .await?;
        (Decimal::ZERO, Decimal::ZERO)
    };

    if on_hand < requested_qty {
        let shortage = requested_qty - on_hand;
        let procurement_unit_cost = (unit_price * Decimal::new(60, 2)).round_dp(4);
        let current_value = on_hand * avg_cost;
        let incoming_value = shortage * procurement_unit_cost;
        let new_qty = on_hand + shortage;
        let new_avg = if new_qty.is_zero() {
            Decimal::ZERO
        } else {
            ((current_value + incoming_value) / new_qty).round_dp(4)
        };

        sqlx::query(
            "UPDATE inventory_positions SET on_hand = $2, avg_cost = $3, updated_at = $4 WHERE item_code = $1",
        )
        .bind(item_code)
        .bind(new_qty)
        .bind(new_avg)
        .bind(Utc::now())
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO inventory_movements (
                id, order_id, item_code, movement_type, quantity, unit_cost, created_at
            )
            VALUES ($1, $2, $3, 'RECEIPT', $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(order_id)
        .bind(item_code)
        .bind(shortage)
        .bind(procurement_unit_cost)
        .bind(Utc::now())
        .execute(&mut **tx)
        .await?;

        on_hand = new_qty;
        avg_cost = new_avg;
    }

    Ok((on_hand, avg_cost))
}

async fn execute_skill_plan(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &SkillExecutionContext,
) -> Result<()> {
    let policy = resolve_skill_routing_policy(tx, context.transaction_type).await?;

    let mut attempt_no = 1_i32;
    let mut last_failure_reason = "unknown skill runtime failure".to_string();
    let mut last_skill_id = policy.primary_skill_id.clone();
    let mut last_skill_version = policy.primary_skill_version.clone();
    let mut last_fallback_used = false;

    for _ in 0..=policy.max_retries {
        let outcome = invoke_skill(
            tx,
            context,
            &policy,
            &policy.primary_skill_id,
            &policy.primary_skill_version,
            attempt_no,
            false,
        )
        .await?;
        if outcome.success {
            return Ok(());
        }

        last_failure_reason = outcome
            .failure_reason
            .unwrap_or_else(|| "primary skill failed".to_string());
        last_skill_id = policy.primary_skill_id.clone();
        last_skill_version = policy.primary_skill_version.clone();
        last_fallback_used = false;
        attempt_no += 1;
    }

    if let (Some(fallback_id), Some(fallback_version)) = (
        policy.fallback_skill_id.as_deref(),
        policy.fallback_skill_version.as_deref(),
    ) {
        let outcome = invoke_skill(
            tx,
            context,
            &policy,
            fallback_id,
            fallback_version,
            attempt_no,
            true,
        )
        .await?;
        if outcome.success {
            return Ok(());
        }

        last_failure_reason = outcome
            .failure_reason
            .unwrap_or_else(|| "fallback skill failed".to_string());
        last_skill_id = fallback_id.to_string();
        last_skill_version = fallback_version.to_string();
        last_fallback_used = true;
    }

    let escalation_id = insert_skill_escalation(tx, context, &policy, &last_failure_reason).await?;
    insert_skill_invocation(
        tx,
        context,
        &policy,
        &last_skill_id,
        &last_skill_version,
        attempt_no,
        SKILL_STATUS_ESCALATED,
        Some(format!(
            "{}; escalation_id={}",
            last_failure_reason, escalation_id
        )),
        last_fallback_used,
        stable_payload_hash(&build_skill_input_payload(context, &policy)),
        None,
        Utc::now(),
        Utc::now(),
    )
    .await?;

    Err(SkillEscalatedError {
        escalation_id,
        reason: last_failure_reason,
    }
    .into())
}

async fn resolve_skill_routing_policy(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_type: TransactionType,
) -> Result<SkillRoutingPolicy> {
    let (intent, default_capability, tx_type) = match transaction_type {
        TransactionType::Product => (
            "ORDER_EXECUTION_PRODUCT",
            "fulfillment-execution",
            "PRODUCT",
        ),
        TransactionType::Service => ("ORDER_EXECUTION_SERVICE", "service-delivery", "SERVICE"),
    };

    let row = sqlx::query(
        r#"
        SELECT
            intent,
            capability,
            primary_skill_id,
            primary_skill_version,
            fallback_skill_id,
            fallback_skill_version,
            max_retries,
            escalation_action_type
        FROM skill_routing_policies
        WHERE intent = $1
          AND transaction_type = $2
        LIMIT 1
        "#,
    )
    .bind(intent)
    .bind(tx_type)
    .fetch_optional(&mut **tx)
    .await?;

    let row = if let Some(row) = row {
        row
    } else {
        sqlx::query(
            r#"
            SELECT
                intent,
                capability,
                primary_skill_id,
                primary_skill_version,
                fallback_skill_id,
                fallback_skill_version,
                max_retries,
                escalation_action_type
            FROM skill_routing_policies
            WHERE intent = $1
              AND transaction_type = 'ANY'
            LIMIT 1
            "#,
        )
        .bind(intent)
        .fetch_optional(&mut **tx)
        .await?
        .context("skill routing policy not configured")?
    };

    Ok(SkillRoutingPolicy {
        intent: row.try_get("intent")?,
        capability: row
            .try_get::<Option<String>, _>("capability")?
            .unwrap_or_else(|| default_capability.to_string()),
        primary_skill_id: row.try_get("primary_skill_id")?,
        primary_skill_version: row.try_get("primary_skill_version")?,
        fallback_skill_id: row.try_get("fallback_skill_id")?,
        fallback_skill_version: row.try_get("fallback_skill_version")?,
        max_retries: row.try_get("max_retries")?,
        escalation_action_type: row.try_get("escalation_action_type")?,
    })
}

async fn invoke_skill(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &SkillExecutionContext,
    policy: &SkillRoutingPolicy,
    skill_id: &str,
    skill_version: &str,
    attempt_no: i32,
    fallback_used: bool,
) -> Result<SkillInvocationOutcome> {
    let input_payload = build_skill_input_payload(context, policy);
    let input_hash = stable_payload_hash(&input_payload);
    let started_at = Utc::now();

    let skill_row = sqlx::query(
        r#"
        SELECT capability, approval_status, required_input_fields, required_output_fields
        FROM skill_registry
        WHERE skill_id = $1
          AND skill_version = $2
        LIMIT 1
        "#,
    )
    .bind(skill_id)
    .bind(skill_version)
    .fetch_optional(&mut **tx)
    .await?;

    let mut failure_reason: Option<String> = None;
    let mut output_payload: Option<Value> = None;

    if let Some(row) = skill_row {
        let skill_capability: String = row.try_get("capability")?;
        let approval_status: String = row.try_get("approval_status")?;
        let required_input_fields: Vec<String> = row.try_get("required_input_fields")?;
        let required_output_fields: Vec<String> = row.try_get("required_output_fields")?;

        if approval_status != "APPROVED" {
            failure_reason = Some(format!(
                "skill {}@{} is not approved (status={})",
                skill_id, skill_version, approval_status
            ));
        } else if skill_capability != policy.capability {
            failure_reason = Some(format!(
                "capability mismatch for {}@{}: expected {}, got {}",
                skill_id, skill_version, policy.capability, skill_capability
            ));
        } else if let Err(err) =
            validate_required_fields(&input_payload, &required_input_fields, "input")
        {
            failure_reason = Some(err.to_string());
        } else {
            match simulate_skill_execution(context, skill_id, fallback_used) {
                Ok(output) => {
                    if let Err(err) =
                        validate_required_fields(&output, &required_output_fields, "output")
                    {
                        failure_reason = Some(err.to_string());
                    } else {
                        output_payload = Some(output);
                    }
                }
                Err(err) => {
                    failure_reason = Some(err.to_string());
                }
            }
        }
    } else {
        failure_reason = Some(format!(
            "skill {}@{} is not registered",
            skill_id, skill_version
        ));
    }

    let completed_at = Utc::now();
    let output_hash = output_payload.as_ref().map(stable_payload_hash);
    let status = if failure_reason.is_none() {
        SKILL_STATUS_SUCCESS
    } else {
        SKILL_STATUS_FAILED
    };

    insert_skill_invocation(
        tx,
        context,
        policy,
        skill_id,
        skill_version,
        attempt_no,
        status,
        failure_reason.clone(),
        fallback_used,
        input_hash,
        output_hash,
        started_at,
        completed_at,
    )
    .await?;

    Ok(SkillInvocationOutcome {
        success: failure_reason.is_none(),
        failure_reason,
    })
}

async fn insert_skill_invocation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &SkillExecutionContext,
    policy: &SkillRoutingPolicy,
    skill_id: &str,
    skill_version: &str,
    attempt_no: i32,
    status: &str,
    failure_reason: Option<String>,
    fallback_used: bool,
    input_hash: String,
    output_hash: Option<String>,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let latency_ms = (completed_at - started_at).num_milliseconds().max(0);

    sqlx::query(
        r#"
        INSERT INTO skill_invocations (
            id,
            order_id,
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
            completed_at,
            created_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(context.order_id)
    .bind(&policy.intent)
    .bind(&policy.capability)
    .bind(skill_id)
    .bind(skill_version)
    .bind(OPS_AGENT_ID)
    .bind(attempt_no)
    .bind(status)
    .bind(failure_reason)
    .bind(fallback_used)
    .bind(input_hash)
    .bind(output_hash)
    .bind(latency_ms)
    .bind(started_at)
    .bind(completed_at)
    .bind(completed_at)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn insert_skill_escalation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &SkillExecutionContext,
    policy: &SkillRoutingPolicy,
    reason: &str,
) -> Result<Uuid> {
    let escalation_id = Uuid::new_v4();
    let now = Utc::now();
    let amount = (context.quantity * context.unit_price).round_dp(4);

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
        VALUES ($1, $2, 'ORDER', $3, 'PENDING', 'SKILL_RUNTIME_FAILURE', $4, $5, $6, $7, $8)
        "#,
    )
    .bind(escalation_id)
    .bind(&policy.escalation_action_type)
    .bind(context.order_id)
    .bind(amount)
    .bind(&context.currency)
    .bind(OPS_AGENT_ID)
    .bind(now)
    .bind(reason)
    .execute(&mut **tx)
    .await?;

    Ok(escalation_id)
}

fn build_skill_input_payload(
    context: &SkillExecutionContext,
    policy: &SkillRoutingPolicy,
) -> Value {
    json!({
        "order_id": context.order_id,
        "intent": policy.intent.as_str(),
        "capability": policy.capability.as_str(),
        "transaction_type": context.transaction_type.as_str(),
        "item_code": context.item_code.as_str(),
        "quantity": context.quantity.to_string(),
        "unit_price": context.unit_price.to_string(),
        "currency": context.currency.as_str(),
    })
}

fn simulate_skill_execution(
    context: &SkillExecutionContext,
    skill_id: &str,
    fallback_used: bool,
) -> Result<Value> {
    let marker = context.item_code.to_ascii_uppercase();
    if marker.contains("FAILALL") {
        anyhow::bail!("forced skill failure marker FAILALL encountered");
    }

    if marker.contains("FAILPRIMARY") {
        let is_primary_product = context.transaction_type == TransactionType::Product
            && skill_id == "product-fulfillment"
            && !fallback_used;
        let is_primary_service = context.transaction_type == TransactionType::Service
            && skill_id == "service-delivery"
            && !fallback_used;
        if is_primary_product || is_primary_service {
            anyhow::bail!("forced primary skill failure marker FAILPRIMARY encountered");
        }
    }

    let mode = if fallback_used { "SAFE" } else { "AUTO" };
    let payload = if context.transaction_type == TransactionType::Product {
        json!({
            "execution_status": "SUCCESS",
            "fulfillment_mode": mode,
            "inventory_strategy": "AVCO"
        })
    } else {
        json!({
            "execution_status": "SUCCESS",
            "delivery_mode": mode,
            "service_strategy": "STANDARD"
        })
    };

    Ok(payload)
}

fn validate_required_fields(payload: &Value, required_fields: &[String], kind: &str) -> Result<()> {
    for field in required_fields {
        let Some(value) = payload.get(field) else {
            anyhow::bail!("{kind} contract missing required field '{field}'");
        };

        if value.is_null() {
            anyhow::bail!("{kind} contract field '{field}' is null");
        }

        if value
            .as_str()
            .map(|text| text.trim().is_empty())
            .unwrap_or(false)
        {
            anyhow::bail!("{kind} contract field '{field}' is empty");
        }
    }

    Ok(())
}

fn stable_payload_hash(payload: &Value) -> String {
    let serialized = serde_json::to_vec(payload).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(serialized);
    format!("{:x}", hasher.finalize())
}

async fn insert_journal(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    account: &str,
    debit: Decimal,
    credit: Decimal,
    memo: &str,
) -> Result<()> {
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

async fn mark_order_failed(pool: &PgPool, order_id: Uuid, reason: &str) -> Result<()> {
    sqlx::query(
        "UPDATE orders SET status = 'FAILED', failure_reason = $2, updated_at = $3 WHERE id = $1",
    )
    .bind(order_id)
    .bind(reason)
    .bind(Utc::now())
    .execute(pool)
    .await?;

    Ok(())
}

async fn recall_memories_for_execution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    item_code: &str,
    customer_email: &str,
    transaction_type: TransactionType,
) -> Result<i64> {
    let execution_scope = execution_scope_for(transaction_type);
    let query_text = format!("{item_code} {customer_email}");

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            entity_id,
            ts_rank_cd(
                to_tsvector('simple', content),
                plainto_tsquery('simple', $4)
            ) AS score
        FROM agent_semantic_memory
        WHERE agent_name = $1
          AND scope IN ($2, $3)
          AND (
                to_tsvector('simple', content) @@ plainto_tsquery('simple', $4)
                OR EXISTS (
                    SELECT 1 FROM unnest(keywords) AS kw
                    WHERE kw ILIKE ('%' || $5 || '%')
                )
                OR entity_id = $6
          )
        ORDER BY score DESC NULLS LAST, created_at DESC
        LIMIT 5
        "#,
    )
    .bind(OPS_AGENT_ID)
    .bind(execution_scope)
    .bind(MEMORY_SCOPE_ORDER_EXECUTION)
    .bind(&query_text)
    .bind(item_code)
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await?;

    let recalled = rows.len() as i64;
    if recalled == 0 {
        return Ok(0);
    }

    let now = Utc::now();
    let source_ref = format!("order:{order_id}:pre-execution");

    for row in rows {
        let memory_id: Uuid = row.try_get("id")?;
        let entity_id: Option<Uuid> = row.try_get("entity_id")?;
        insert_memory_provenance(
            tx,
            Some(memory_id),
            entity_id.or(Some(order_id)),
            "READ",
            OPS_AGENT_ID,
            &source_ref,
            Some(query_text.as_str()),
            now,
        )
        .await?;
    }

    Ok(recalled)
}

async fn write_execution_memory(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    item_code: &str,
    transaction_type: TransactionType,
    revenue: Decimal,
    cogs: Decimal,
    recalled_memories: i64,
) -> Result<()> {
    let scope = execution_scope_for(transaction_type);
    let now = Utc::now();
    let source_ref = format!("order:{order_id}:execution-result");
    let content = format!(
        "Executed order {} for {} flow with revenue {} and cogs {}. Recalled {} prior memories before execution.",
        order_id,
        transaction_type.as_str(),
        revenue,
        cogs,
        recalled_memories
    );
    let keywords = vec![
        transaction_type.as_str().to_ascii_lowercase(),
        item_code.to_ascii_lowercase(),
        "execution".to_string(),
        "autonomy".to_string(),
    ];

    let memory_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO agent_semantic_memory (
            id, agent_name, scope, entity_id, content, keywords, source_ref, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(memory_id)
    .bind(OPS_AGENT_ID)
    .bind(scope)
    .bind(order_id)
    .bind(content)
    .bind(keywords)
    .bind(&source_ref)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    insert_memory_provenance(
        tx,
        Some(memory_id),
        Some(order_id),
        "WRITE",
        OPS_AGENT_ID,
        &source_ref,
        None,
        now,
    )
    .await?;

    Ok(())
}

async fn write_failure_memory(pool: &PgPool, order_id: Uuid, reason: &str) -> Result<()> {
    let mut tx = pool.begin().await?;

    let order = sqlx::query(
        "SELECT COALESCE(transaction_type, 'PRODUCT') AS transaction_type, item_code FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (transaction_type, item_code) = if let Some(row) = order {
        (
            parse_transaction_type(&row.try_get::<String, _>("transaction_type")?)?,
            row.try_get::<String, _>("item_code")?,
        )
    } else {
        (TransactionType::Product, "unknown-item".to_string())
    };

    let scope = execution_scope_for(transaction_type);
    let now = Utc::now();
    let source_ref = format!("order:{order_id}:execution-failure");
    let content = format!(
        "Execution failed for order {} in {} flow: {}",
        order_id,
        transaction_type.as_str(),
        reason
    );
    let keywords = vec![
        transaction_type.as_str().to_ascii_lowercase(),
        item_code.to_ascii_lowercase(),
        "failure".to_string(),
        "exception".to_string(),
    ];

    let memory_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO agent_semantic_memory (
            id, agent_name, scope, entity_id, content, keywords, source_ref, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(memory_id)
    .bind(OPS_AGENT_ID)
    .bind(scope)
    .bind(order_id)
    .bind(content)
    .bind(keywords)
    .bind(&source_ref)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    insert_memory_provenance(
        &mut tx,
        Some(memory_id),
        Some(order_id),
        "WRITE",
        OPS_AGENT_ID,
        &source_ref,
        Some(reason),
        now,
    )
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn insert_memory_provenance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    memory_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    action_type: &str,
    actor_agent_id: &str,
    source_ref: &str,
    query_text: Option<&str>,
    created_at: chrono::DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO agent_memory_provenance (
            id, memory_id, entity_id, action_type, actor_agent_id, source_ref, query_text, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(memory_id)
    .bind(entity_id)
    .bind(action_type)
    .bind(actor_agent_id)
    .bind(source_ref)
    .bind(query_text)
    .bind(created_at)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

fn execution_scope_for(transaction_type: TransactionType) -> &'static str {
    match transaction_type {
        TransactionType::Product => MEMORY_SCOPE_PRODUCT_EXECUTION,
        TransactionType::Service => MEMORY_SCOPE_SERVICE_EXECUTION,
    }
}

fn parse_transaction_type(value: &str) -> Result<TransactionType> {
    match value.trim().to_ascii_uppercase().as_str() {
        "PRODUCT" => Ok(TransactionType::Product),
        "SERVICE" => Ok(TransactionType::Service),
        other => anyhow::bail!("unsupported transaction_type: {other}"),
    }
}

fn service_delivery_cost_ratio() -> Decimal {
    Decimal::new(3000, 4) // 30.00%
}
