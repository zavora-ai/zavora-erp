use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{Json, Router, extract::State, routing::get};
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use tracing::info;
use zavora_platform::{BoardPack, ServiceConfig, connect_database};

#[derive(Clone)]
struct AppState {
    pool: PgPool,
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

    let pipeline = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT FROM leads) AS leads_total,
            (SELECT COUNT(*)::BIGINT FROM opportunities WHERE stage <> 'ACCEPTED' AND stage <> 'LOST') AS opportunities_open,
            (SELECT COUNT(*)::BIGINT FROM quotes WHERE status = 'ISSUED') AS quotes_issued,
            (SELECT COUNT(*)::BIGINT FROM quotes WHERE status = 'ACCEPTED') AS quotes_accepted
        "#,
    )
    .fetch_one(&state.pool)
    .await
    .map_err(internal_error)?;

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
        revenue: totals
            .try_get::<Decimal, _>("revenue")
            .map_err(internal_error)?,
        cash_collected: settlements
            .try_get::<Decimal, _>("cash_collected")
            .map_err(internal_error)?,
        inventory_value: inventory
            .try_get::<Decimal, _>("inventory_value")
            .map_err(internal_error)?,
    };

    Ok(Json(pack))
}

fn internal_error<E: std::fmt::Display>(err: E) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.to_string(),
    )
}
