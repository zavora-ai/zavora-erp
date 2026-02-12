use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use chrono::Utc;
use sqlx::{PgPool, Row};
use tracing::{error, info};
use uuid::Uuid;
use zavora_platform::{
    MemorySearchHit, MemorySearchRequest, MemorySearchResponse, MemoryWriteRequest,
    MemoryWriteResponse, ServiceConfig, connect_database,
};

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "zavora_memory=info".to_string()),
        )
        .init();

    let config = ServiceConfig::from_env("0.0.0.0:8100")?;
    let pool = connect_database(&config.database_url).await?;

    let state = AppState { pool };
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/memory/entries", post(write_memory))
        .route("/memory/search", post(search_memory))
        .with_state(state);

    let addr: SocketAddr = config.http_addr.parse()?;
    info!("memory service listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn write_memory(
    State(state): State<AppState>,
    Json(payload): Json<MemoryWriteRequest>,
) -> Result<(StatusCode, Json<MemoryWriteResponse>), (StatusCode, String)> {
    validate_write_request(&payload).map_err(invalid_request)?;

    let memory_id = Uuid::new_v4();
    let now = Utc::now();

    if let Err(err) = sqlx::query(
        r#"
        INSERT INTO agent_semantic_memory (
            id, agent_name, scope, entity_id, content, keywords, source_ref, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(memory_id)
    .bind(payload.agent_name.trim())
    .bind(payload.scope.trim())
    .bind(payload.entity_id)
    .bind(payload.content.trim())
    .bind(payload.keywords)
    .bind(payload.source_ref)
    .bind(now)
    .execute(&state.pool)
    .await
    {
        error!("failed to persist semantic memory: {err}");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist semantic memory".to_string(),
        ));
    }

    let response = MemoryWriteResponse {
        memory_id,
        stored_at: now,
    };
    Ok((StatusCode::CREATED, Json(response)))
}

async fn search_memory(
    State(state): State<AppState>,
    Json(payload): Json<MemorySearchRequest>,
) -> Result<Json<MemorySearchResponse>, (StatusCode, String)> {
    validate_search_request(&payload).map_err(invalid_request)?;
    let limit = payload.limit.unwrap_or(10).clamp(1, 50);

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            agent_name,
            scope,
            entity_id,
            content,
            keywords,
            source_ref,
            created_at,
            ts_rank_cd(
                to_tsvector('simple', content),
                plainto_tsquery('simple', $2)
            ) AS score
        FROM agent_semantic_memory
        WHERE agent_name = $1
          AND ($3::text IS NULL OR scope = $3)
          AND ($4::uuid IS NULL OR entity_id = $4)
          AND (
                to_tsvector('simple', content) @@ plainto_tsquery('simple', $2)
                OR content ILIKE ('%' || $2 || '%')
                OR EXISTS (
                    SELECT 1 FROM unnest(keywords) AS kw
                    WHERE kw ILIKE ('%' || $2 || '%')
                )
          )
        ORDER BY score DESC NULLS LAST, created_at DESC
        LIMIT $5
        "#,
    )
    .bind(payload.agent_name.trim())
    .bind(payload.query.trim())
    .bind(payload.scope.as_deref().map(str::trim))
    .bind(payload.entity_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut memory_ids: Vec<Uuid> = Vec::with_capacity(rows.len());
    let mut hits: Vec<MemorySearchHit> = Vec::with_capacity(rows.len());

    for row in rows {
        let memory_id = row.try_get::<Uuid, _>("id").map_err(internal_error)?;
        memory_ids.push(memory_id);

        hits.push(MemorySearchHit {
            memory_id,
            agent_name: row
                .try_get::<String, _>("agent_name")
                .map_err(internal_error)?,
            scope: row.try_get::<String, _>("scope").map_err(internal_error)?,
            entity_id: row
                .try_get::<Option<Uuid>, _>("entity_id")
                .map_err(internal_error)?,
            content: row
                .try_get::<String, _>("content")
                .map_err(internal_error)?,
            keywords: row
                .try_get::<Vec<String>, _>("keywords")
                .map_err(internal_error)?,
            source_ref: row
                .try_get::<Option<String>, _>("source_ref")
                .map_err(internal_error)?,
            score: row.try_get::<f64, _>("score").unwrap_or(0.0),
            created_at: row
                .try_get::<chrono::DateTime<Utc>, _>("created_at")
                .map_err(internal_error)?,
        });
    }

    if !memory_ids.is_empty() {
        let _ = sqlx::query(
            r#"
            UPDATE agent_semantic_memory
            SET
                access_count = access_count + 1,
                last_accessed_at = $2
            WHERE id = ANY($1)
            "#,
        )
        .bind(&memory_ids)
        .bind(Utc::now())
        .execute(&state.pool)
        .await;
    }

    Ok(Json(MemorySearchResponse { hits }))
}

fn validate_write_request(payload: &MemoryWriteRequest) -> AnyResult<()> {
    if payload.agent_name.trim().is_empty() {
        anyhow::bail!("agent_name is required");
    }
    if payload.scope.trim().is_empty() {
        anyhow::bail!("scope is required");
    }
    if payload.content.trim().is_empty() {
        anyhow::bail!("content is required");
    }

    Ok(())
}

fn validate_search_request(payload: &MemorySearchRequest) -> AnyResult<()> {
    if payload.agent_name.trim().is_empty() {
        anyhow::bail!("agent_name is required");
    }
    if payload.query.trim().is_empty() {
        anyhow::bail!("query is required");
    }

    Ok(())
}

fn invalid_request(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
