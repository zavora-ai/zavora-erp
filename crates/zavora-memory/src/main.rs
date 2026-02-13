use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tracing::{error, info};
use uuid::Uuid;
use zavora_platform::{
    MemorySearchHit, MemorySearchRequest, MemorySearchResponse, MemoryWriteRequest,
    MemoryWriteResponse, ServiceConfig, connect_database,
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
}

#[derive(Debug, Clone, Deserialize)]
struct RunRetentionRequest {
    requested_by_agent_id: String,
    dry_run: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionScopeSummary {
    scope: String,
    retention_days: i32,
    pruned_count: i64,
}

#[derive(Debug, Clone, Serialize)]
struct RunRetentionResponse {
    run_at: DateTime<Utc>,
    requested_by_agent_id: String,
    dry_run: bool,
    scopes_processed: i64,
    total_pruned: i64,
    details: Vec<RetentionScopeSummary>,
}

#[derive(Debug, Clone, Deserialize)]
struct McpToolCallRequest {
    tool: String,
    input: Value,
}

#[derive(Debug, Clone, Serialize)]
struct McpToolCallResponse {
    tool: String,
    output: Value,
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
        .route("/memory/retention/run", post(run_retention))
        .route("/memory/mcp/call", post(mcp_call))
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
    write_memory_inner(&state, payload)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
}

async fn search_memory(
    State(state): State<AppState>,
    Json(payload): Json<MemorySearchRequest>,
) -> Result<Json<MemorySearchResponse>, (StatusCode, String)> {
    search_memory_inner(&state, payload).await.map(Json)
}

async fn run_retention(
    State(state): State<AppState>,
    Json(payload): Json<RunRetentionRequest>,
) -> Result<Json<RunRetentionResponse>, (StatusCode, String)> {
    run_retention_inner(&state, payload).await.map(Json)
}

async fn mcp_call(
    State(state): State<AppState>,
    Json(payload): Json<McpToolCallRequest>,
) -> Result<Json<McpToolCallResponse>, (StatusCode, String)> {
    let tool = payload.tool.trim().to_ascii_lowercase();

    let output = match tool.as_str() {
        "memory.write" => {
            let request: MemoryWriteRequest =
                serde_json::from_value(payload.input).map_err(invalid_request)?;
            let response = write_memory_inner(&state, request).await?;
            json!(response)
        }
        "memory.search" => {
            let request: MemorySearchRequest =
                serde_json::from_value(payload.input).map_err(invalid_request)?;
            let response = search_memory_inner(&state, request).await?;
            json!(response)
        }
        "memory.retention.run" => {
            let request: RunRetentionRequest =
                serde_json::from_value(payload.input).map_err(invalid_request)?;
            let response = run_retention_inner(&state, request).await?;
            json!(response)
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unsupported MCP tool '{}'", payload.tool),
            ));
        }
    };

    Ok(Json(McpToolCallResponse { tool, output }))
}

async fn write_memory_inner(
    state: &AppState,
    payload: MemoryWriteRequest,
) -> Result<MemoryWriteResponse, (StatusCode, String)> {
    validate_write_request(&payload).map_err(invalid_request)?;

    let memory_id = Uuid::new_v4();
    let now = Utc::now();

    let mut tx = state.pool.begin().await.map_err(internal_error)?;

    sqlx::query(
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
    .bind(payload.source_ref.trim())
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|err| {
        error!("failed to persist semantic memory: {err}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to persist semantic memory".to_string(),
        )
    })?;

    insert_memory_provenance(
        &mut tx,
        Some(memory_id),
        payload.entity_id,
        "WRITE",
        payload.actor_agent_id.trim(),
        payload.source_ref.trim(),
        None,
        now,
    )
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    Ok(MemoryWriteResponse {
        memory_id,
        stored_at: now,
    })
}

async fn search_memory_inner(
    state: &AppState,
    payload: MemorySearchRequest,
) -> Result<MemorySearchResponse, (StatusCode, String)> {
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

    if memory_ids.is_empty() {
        return Ok(MemorySearchResponse { hits });
    }

    let now = Utc::now();
    let mut tx = state.pool.begin().await.map_err(internal_error)?;

    sqlx::query(
        r#"
        UPDATE agent_semantic_memory
        SET
            access_count = access_count + 1,
            last_accessed_at = $2
        WHERE id = ANY($1)
        "#,
    )
    .bind(&memory_ids)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;

    let actor = payload.requested_by_agent_id.trim();
    let source_ref = format!(
        "memory.search:{}:{}",
        payload.agent_name.trim(),
        payload.scope.as_deref().map(str::trim).unwrap_or("ANY")
    );
    let query_text = Some(payload.query.trim());

    for hit in &hits {
        insert_memory_provenance(
            &mut tx,
            Some(hit.memory_id),
            hit.entity_id,
            "READ",
            actor,
            &source_ref,
            query_text,
            now,
        )
        .await
        .map_err(internal_error)?;
    }

    tx.commit().await.map_err(internal_error)?;

    Ok(MemorySearchResponse { hits })
}

async fn run_retention_inner(
    state: &AppState,
    payload: RunRetentionRequest,
) -> Result<RunRetentionResponse, (StatusCode, String)> {
    let actor = payload.requested_by_agent_id.trim();
    validate_registered_agent(actor).map_err(invalid_request)?;

    let run_at = Utc::now();
    let dry_run = payload.dry_run.unwrap_or(false);

    let policy_rows = sqlx::query(
        r#"
        SELECT scope, retention_days
        FROM memory_retention_policies
        WHERE enabled = TRUE
        ORDER BY scope
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(internal_error)?;

    let mut details: Vec<RetentionScopeSummary> = Vec::with_capacity(policy_rows.len());
    let mut total_pruned = 0_i64;

    for row in policy_rows {
        let scope: String = row.try_get("scope").map_err(internal_error)?;
        let retention_days: i32 = row.try_get("retention_days").map_err(internal_error)?;
        let cutoff = run_at - Duration::days(i64::from(retention_days));

        if dry_run {
            let pruned_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM agent_semantic_memory
                WHERE scope = $1
                  AND created_at < $2
                "#,
            )
            .bind(&scope)
            .bind(cutoff)
            .fetch_one(&state.pool)
            .await
            .map_err(internal_error)?;

            total_pruned += pruned_count;
            details.push(RetentionScopeSummary {
                scope,
                retention_days,
                pruned_count,
            });
            continue;
        }

        let mut tx = state.pool.begin().await.map_err(internal_error)?;

        let removed_rows = sqlx::query(
            r#"
            DELETE FROM agent_semantic_memory
            WHERE scope = $1
              AND created_at < $2
            RETURNING id, entity_id
            "#,
        )
        .bind(&scope)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal_error)?;

        let pruned_count = removed_rows.len() as i64;
        if pruned_count > 0 {
            for row in removed_rows {
                let memory_id: Uuid = row.try_get("id").map_err(internal_error)?;
                let entity_id: Option<Uuid> = row.try_get("entity_id").map_err(internal_error)?;
                let source_ref = format!("memory.retention.pruned:{}", memory_id);

                insert_memory_provenance(
                    &mut tx,
                    None,
                    entity_id,
                    "RETENTION_PRUNE",
                    actor,
                    &source_ref,
                    Some(scope.as_str()),
                    run_at,
                )
                .await
                .map_err(internal_error)?;
            }
        }

        tx.commit().await.map_err(internal_error)?;

        total_pruned += pruned_count;
        details.push(RetentionScopeSummary {
            scope,
            retention_days,
            pruned_count,
        });
    }

    Ok(RunRetentionResponse {
        run_at,
        requested_by_agent_id: actor.to_string(),
        dry_run,
        scopes_processed: details.len() as i64,
        total_pruned,
        details,
    })
}

async fn insert_memory_provenance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    memory_id: Option<Uuid>,
    entity_id: Option<Uuid>,
    action_type: &str,
    actor_agent_id: &str,
    source_ref: &str,
    query_text: Option<&str>,
    created_at: DateTime<Utc>,
) -> AnyResult<()> {
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

fn validate_write_request(payload: &MemoryWriteRequest) -> AnyResult<()> {
    if payload.agent_name.trim().is_empty() {
        anyhow::bail!("agent_name is required");
    }
    if payload.actor_agent_id.trim().is_empty() {
        anyhow::bail!("actor_agent_id is required");
    }
    if payload.scope.trim().is_empty() {
        anyhow::bail!("scope is required");
    }
    if payload.content.trim().is_empty() {
        anyhow::bail!("content is required");
    }
    if payload.source_ref.trim().is_empty() {
        anyhow::bail!("source_ref is required");
    }

    validate_registered_agent(payload.agent_name.trim())?;
    validate_registered_agent(payload.actor_agent_id.trim())?;

    Ok(())
}

fn validate_search_request(payload: &MemorySearchRequest) -> AnyResult<()> {
    if payload.agent_name.trim().is_empty() {
        anyhow::bail!("agent_name is required");
    }
    if payload.requested_by_agent_id.trim().is_empty() {
        anyhow::bail!("requested_by_agent_id is required");
    }
    if payload.query.trim().is_empty() {
        anyhow::bail!("query is required");
    }

    validate_registered_agent(payload.agent_name.trim())?;
    validate_registered_agent(payload.requested_by_agent_id.trim())?;

    Ok(())
}

fn validate_registered_agent(agent_id: &str) -> AnyResult<()> {
    if REGISTERED_AGENT_IDS.contains(&agent_id) {
        return Ok(());
    }

    anyhow::bail!(
        "unknown agent_id '{}'; register it in docs/agents.md first",
        agent_id
    )
}

fn invalid_request(err: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
