use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::StreamExt;
use redis::Msg;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionType {
    Product,
    Service,
}

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
            Err(err)
        }
    }
}

async fn process_order(pool: &PgPool, order_id: Uuid) -> Result<OrderFulfilledEvent> {
    let mut tx = pool.begin().await?;

    let order_row = sqlx::query(
        "SELECT COALESCE(transaction_type, 'PRODUCT') AS transaction_type, item_code, quantity, unit_price, currency FROM orders WHERE id = $1 FOR UPDATE",
    )
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await?
    .context("order not found")?;

    let item_code: String = order_row.try_get("item_code")?;
    let quantity: Decimal = order_row.try_get("quantity")?;
    let unit_price: Decimal = order_row.try_get("unit_price")?;
    let currency: String = order_row.try_get("currency")?;
    let transaction_type_raw: String = order_row.try_get("transaction_type")?;
    let transaction_type = parse_transaction_type(&transaction_type_raw)?;

    if quantity <= Decimal::ZERO || unit_price <= Decimal::ZERO {
        anyhow::bail!("order has invalid quantity or price");
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
