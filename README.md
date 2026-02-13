# Zavora Agentic ERP

## 1) Project Purpose

Zavora Agentic ERP is intended to run a company as an autonomous operating system, not as a data-entry system. Zavora ERP is made up of AI Agents as first‑class citizens; they autonomously negotiate, execute and settle commitments. Humans interact primarily through channels (e‑mail/portal/chat/telegram/whatsapp/mobileapp) as stakeholders (customers, suppliers, directors, auditors) rather than operators. The system is event‑sourced, with every action recorded as a commitment, obligation, proof or settlement. A constitution defines citizens, mandates, policy enforcement, financial standards, and board governance.

The MVP goal is to prove that Zavora can execute a complete lead-order-to-cash cycle autonomously while producing auditable inventory and financial records.

Core principles:
- Agents are first-class operators.
- Humans intervene only when policy requires escalation.
- Every material action is traceable.
- Governance and financial standards are enforced, not optional.

The MVP focuses on features that deliver the largest value with minimum complexity while adhering to the constitution.

MVP Goals
Showcase autonomy: A customer places an order via a channel; agents negotiate terms, check stock, coordinate procurement if necessary, fulfill the order, generate an invoice and manage payment without human intervention.

Traceable commitments: Every step (quotes, acceptance, purchase commitments, shipment, invoicing, receipt) is stored as a commitment with proofs and can be replayed for audit.

Accounting & inventory: Inventory movements, COGS, AR, AP and GL postings are created deterministically based on events and a simple standards profile (IFRS‑lite). This meets small‑business ERP expectations that an ERP unify sales, finance and inventory into a single system.

Governance: The Board (humans + board agents) approves mandates, risk appetite and financial reports and can freeze actions. An example board pack is generated for period close.

Demonstrate mature AI Agent Framework integration: Use ADK‑Rust to build powerful AI agents (LLM, workflow - graph, sequential, parallel, loop and custom agents) and tools (function tools, session memory, file storage and mcp servers) to orchestrate the workflow in a highly performant and scalable way.

## 2) Constitutional Intent (What Must Be True)

The product is governed by a constitution-oriented model where business actions are represented as:
- Commitment: Signed agreement between parties; holds terms, status, risk class and signatures.
- Obligation: Unit of work created by commitments; includes due date, owner and dependencies.
- Proof: Evidence (messages, documents, tool logs) supporting commitments and obligations.
- Settlement: Records financial closure (e.g., payment received); linked to commitments and proofs.
Dispute: Structured conflict; basic case management

This means:
- Decisions must be explainable.
- Actions must be policy-bounded.
- Financial outcomes must be deterministic and auditable.
- Board oversight must be possible in real time.


## 3) MVP Business Scope

The MVP scope is order-to-cash for a single entity and a small product and service catalog.

Required functional units:
1. Strategy-to-execution layer with business model, KPI targets, and forecast/variance governance driving its execution.
2. Business origination (lead/opportunity/quote/acceptance)
3. Order processing (intake, policy check, fulfillment)
4. Inventory and procurement (availability, shortage response, AVCO)
5. Finance and accounting (AR/AP/GL, revenue, COGS, settlement)
6. Reporting and revenue tracking (board pack + financial views)
7. Governance and policy enforcement (thresholds, approvals, freeze)
8. Agent autonomy and exception handling
9. Audit and compliance evidence
10. Agent payroll and cost allocation (token usage, cloud costs, subscriptions)

Out of scope (MVP):
- Multi-entity consolidation
- Human payroll/HR
- Manufacturing/MRP
- Complex tax engines
- Deep external partner integrations beyond demo needs

## 4) Current Implementation Baseline

The implementation baseline is defined as full functional coverage of units `1..10` in this README.
A baseline is only considered complete when all 10 units are implemented and demonstrated with evidence.

Services:
- `zavora-gateway`: accepts orders and publishes workflow event
- `zavora-ops`: processes workflow (inventory movement, journals, settlement)
- `zavora-board`: exposes board pack KPI endpoint
- `zavora-memory`: provides long-term semantic memory APIs (MCP-facing for agent tooling)
- `postgres`: record system
- `redis`: event transport

Baseline functional coverage contract:
1. Strategy-to-execution layer
2. Business origination
3. Order processing
4. Inventory and procurement (for product flows)
5. Finance and accounting
6. Reporting and revenue tracking
7. Governance and policy enforcement
8. Agent autonomy and exception handling
9. Audit and compliance evidence
10. Agent payroll and cost allocation

Important:
- Microservices are an implementation design to deliver these 10 functional units.
- Agents execute work through approved skills and capability routing, not ad-hoc prompts.
- Technical service uptime alone does not satisfy baseline completion; functional evidence for `1..10` is required.

## 5) Run and Validate the Current Baseline

Start stack:

```bash
docker compose up --build
```

If your machine already uses `5432` or `6379`:

```bash
POSTGRES_PORT=55432 REDIS_PORT=56379 docker compose up --build
```

If you already had an older Postgres volume before this update, apply the latest schema once:

```bash
docker compose exec -T postgres psql -U zavora -d zavora -f /docker-entrypoint-initdb.d/001_schema.sql
```

Health checks:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8090/healthz
curl http://localhost:8100/healthz
```

Create a lead (business origination):

```bash
curl -X POST http://localhost:8080/origination/leads \
  -H 'content-type: application/json' \
  -d '{
    "contact_email": "procurement@acme.com",
    "source_channel": "EMAIL",
    "note": "Needs implementation services and starter SKU bundle",
    "requested_by_agent_id": "sales-agent"
  }'
```

Create an opportunity (replace `LEAD_ID` with the lead ID from previous response):

```bash
curl -X POST http://localhost:8080/origination/opportunities \
  -H 'content-type: application/json' \
  -d '{
    "lead_id": "LEAD_ID",
    "customer_email": "procurement@acme.com",
    "transaction_type": "SERVICE",
    "item_code": "SVC-IMPLEMENTATION",
    "quantity": "1",
    "target_unit_price": "1200.00",
    "currency": "USD",
    "risk_class": "STANDARD",
    "requested_by_agent_id": "sales-agent"
  }'
```

Create a quote (replace `OPPORTUNITY_ID` from previous response):

```bash
curl -X POST http://localhost:8080/origination/quotes \
  -H 'content-type: application/json' \
  -d '{
    "opportunity_id": "OPPORTUNITY_ID",
    "unit_price": "1100.00",
    "payment_terms_days": 14,
    "valid_for_days": 14,
    "risk_note": "Inside mandate",
    "requested_by_agent_id": "sales-agent"
  }'
```

Accept the quote and trigger executable demand (replace `QUOTE_ID` from previous response):

```bash
curl -X POST http://localhost:8080/origination/quotes/QUOTE_ID/accept \
  -H 'content-type: application/json' \
  -d '{
    "accepted_by": "procurement@acme.com",
    "acceptance_channel": "EMAIL",
    "proof_ref": "email:thread-2026-02-12-001",
    "requested_by_agent_id": "sales-agent"
  }'
```

Set governance threshold (example: service orders above 100 require approval):

```bash
curl -X POST http://localhost:8080/governance/thresholds \
  -H 'content-type: application/json' \
  -d '{
    "action_type": "ORDER_EXECUTION_SERVICE",
    "max_auto_amount": "100.00",
    "currency": "USD",
    "updated_by_agent_id": "board-agent"
  }'
```

Upsert strategy offering (FU-01):

```bash
curl -X POST http://localhost:8080/strategy/offerings \
  -H 'content-type: application/json' \
  -d '{
    "offering_code": "SVC-IMPLEMENTATION",
    "offering_type": "SERVICE",
    "name": "Implementation Service Package",
    "unit_of_measure": "ENGAGEMENT",
    "default_unit_price": "1100.00",
    "currency": "USD",
    "owner_agent_id": "strategy-agent",
    "updated_by_agent_id": "strategy-agent"
  }'
```

Upsert KPI target (FU-01):

```bash
curl -X POST http://localhost:8080/strategy/kpi-targets \
  -H 'content-type: application/json' \
  -d '{
    "period_start": "2026-02-01",
    "period_end": "2026-02-28",
    "business_unit": "GLOBAL",
    "mandate": "GROWTH",
    "metric_name": "REVENUE",
    "target_value": "15000.00",
    "warning_threshold_pct": "5.00",
    "critical_threshold_pct": "10.00",
    "currency": "USD",
    "updated_by_agent_id": "strategy-agent"
  }'
```

Upsert forecast baseline (FU-01):

```bash
curl -X POST http://localhost:8080/strategy/forecasts \
  -H 'content-type: application/json' \
  -d '{
    "period_start": "2026-02-01",
    "period_end": "2026-02-28",
    "business_unit": "GLOBAL",
    "mandate": "GROWTH",
    "metric_name": "REVENUE",
    "forecast_value": "14250.00",
    "confidence_pct": "82.50",
    "assumptions_json": {"driver":"pipeline-weighted"},
    "currency": "USD",
    "generated_by_agent_id": "strategy-agent"
  }'
```

Evaluate target-vs-actual variance and trigger corrective action on breach:

```bash
curl -X POST http://localhost:8080/strategy/variance/evaluate \
  -H 'content-type: application/json' \
  -d '{
    "period_start": "2026-02-01",
    "period_end": "2026-02-28",
    "business_unit": "GLOBAL",
    "mandate": "GROWTH",
    "metric_name": "REVENUE",
    "requested_by_agent_id": "strategy-agent"
  }'
```

Inspect variance history and open corrective actions:

```bash
curl "http://localhost:8080/strategy/variance?limit=20"
curl "http://localhost:8080/strategy/corrective-actions?status=OPEN&limit=20"
```

List pending governance escalations:

```bash
curl "http://localhost:8080/governance/escalations?status=PENDING&limit=20"
```

Approve an escalation (replace `ESCALATION_ID`):

```bash
curl -X POST http://localhost:8080/governance/escalations/ESCALATION_ID/decide \
  -H 'content-type: application/json' \
  -d '{
    "decision": "APPROVED",
    "decided_by_agent_id": "board-agent",
    "decision_note": "approved for execution"
  }'
```

Freeze or unfreeze an action type:

```bash
curl -X POST http://localhost:8080/governance/freeze \
  -H 'content-type: application/json' \
  -d '{
    "action_type": "ORDER_EXECUTION_PRODUCT",
    "is_frozen": true,
    "reason": "temporary board hold",
    "updated_by_agent_id": "board-agent"
  }'
```

Create a direct transaction (bypassing origination):

```bash
curl -X POST http://localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{
    "customer_email": "buyer@acme.com",
    "transaction_type": "PRODUCT",
    "item_code": "SKU-001",
    "quantity": "5",
    "unit_price": "49.99",
    "currency": "USD",
    "requested_by_agent_id": "sales-agent"
  }'
```

Create a service transaction:

```bash
curl -X POST http://localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{
    "customer_email": "client@acme.com",
    "transaction_type": "SERVICE",
    "item_code": "SVC-IMPLEMENTATION",
    "quantity": "1",
    "unit_price": "1200.00",
    "currency": "USD",
    "requested_by_agent_id": "sales-agent"
  }'
```

Ingest token usage cost (FU-10):

```bash
curl -X POST http://localhost:8080/finops/token-usage \
  -H 'content-type: application/json' \
  -d '{
    "order_id": "ORDER_ID",
    "agent_id": "sales-agent",
    "skill_id": "quote-negotiation:v1",
    "action_name": "draft_quote_terms",
    "input_tokens": 1200,
    "output_tokens": 800,
    "token_unit_cost": "0.000002",
    "currency": "USD",
    "source_ref": "llm-run:demo-001",
    "ingested_by_agent_id": "payroll-agent"
  }'
```

Ingest cloud infrastructure cost (FU-10):

```bash
curl -X POST http://localhost:8080/finops/cloud-costs \
  -H 'content-type: application/json' \
  -d '{
    "order_id": "ORDER_ID",
    "provider": "aws",
    "cost_type": "COMPUTE",
    "usage_quantity": "2.50",
    "unit_cost": "0.0400",
    "currency": "USD",
    "source_ref": "cloud-bill:demo-001",
    "ingested_by_agent_id": "payroll-agent"
  }'
```

Ingest subscription/tool cost (FU-10):

```bash
curl -X POST http://localhost:8080/finops/subscriptions \
  -H 'content-type: application/json' \
  -d '{
    "tool_name": "adk-rust-runtime",
    "subscription_name": "team-plan",
    "period_start": "2026-02-01T00:00:00Z",
    "period_end": "2026-03-01T00:00:00Z",
    "total_cost": "300.00",
    "currency": "USD",
    "source_ref": "vendor-invoice:2026-02",
    "ingested_by_agent_id": "payroll-agent"
  }'
```

Run deterministic cost allocation + payroll journal posting for a period:

```bash
curl -X POST http://localhost:8080/finops/allocate \
  -H 'content-type: application/json' \
  -d '{
    "period_start": "2026-02-01T00:00:00Z",
    "period_end": "2026-03-01T00:00:00Z",
    "requested_by_agent_id": "payroll-agent"
  }'
```

Read board pack:

```bash
curl http://localhost:8090/board/pack
```

Read skill unit economics view (FR-056):

```bash
curl "http://localhost:8090/board/skills/unit-economics?period_start=2026-02-01T00:00:00Z&period_end=2026-03-01T00:00:00Z&limit=20"
```

Read skill runtime telemetry by skill/version:

```bash
curl "http://localhost:8090/board/skills/telemetry?limit=20"
```

List approved skill registry:

```bash
curl "http://localhost:8080/skills/registry?approval_status=APPROVED&limit=50"
```

List intent-to-skill routing policies:

```bash
curl "http://localhost:8080/skills/routing?limit=50"
```

Read audit evidence package for an order (replace `ORDER_ID`):

```bash
curl http://localhost:8090/audit/orders/ORDER_ID/evidence
```

Write semantic memory example:

```bash
curl -X POST http://localhost:8100/memory/entries \
  -H 'content-type: application/json' \
  -d '{
    "agent_name": "sales-agent",
    "actor_agent_id": "sales-agent",
    "scope": "ORDER",
    "content": "Customer accepted quote only after 2% discount and 14-day payment terms.",
    "keywords": ["discount", "payment-terms", "negotiation"],
    "source_ref": "proof:email:thread-2026-02-12-001"
  }'
```

Search semantic memory example:

```bash
curl -X POST http://localhost:8100/memory/search \
  -H 'content-type: application/json' \
  -d '{
    "agent_name": "sales-agent",
    "requested_by_agent_id": "sales-agent",
    "query": "discount negotiation",
    "scope": "ORDER",
    "limit": 5
  }'
```

Run semantic memory retention policy worker:

```bash
curl -X POST http://localhost:8100/memory/retention/run \
  -H 'content-type: application/json' \
  -d '{
    "requested_by_agent_id": "audit-agent",
    "dry_run": true
  }'
```

Call memory tools via MCP-style endpoint:

```bash
curl -X POST http://localhost:8100/memory/mcp/call \
  -H 'content-type: application/json' \
  -d '{
    "tool": "memory.search",
    "input": {
      "agent_name": "ops-orchestrator-agent",
      "requested_by_agent_id": "ops-orchestrator-agent",
      "query": "SKU-001",
      "scope": "PRODUCT_EXECUTION",
      "limit": 3
    }
  }'
```

Note:
- Current baseline supports both product and service transactions.
- Business origination (`lead -> opportunity -> quote -> acceptance`) now creates executable demand via order creation and workflow dispatch.
- Board pack includes pipeline and governance counters (`leads_total`, `opportunities_open`, `quotes_issued`, `quotes_accepted`, `orders_pending_approval`, `governance_escalations_pending`) in addition to fulfillment and finance metrics.
- Board pack now includes autonomy economics (`autonomy_operating_cost`, `margin_after_autonomy_cost`, `revenue_to_agent_payroll_ratio`, reconciliation status/variance).
- Skill unit economics is available at `/board/skills/unit-economics`, including skill-level token/cloud/subscription costs, attributed revenue, margin, and revenue-to-cost ratio.
- Skill runtime telemetry is available at `/board/skills/telemetry` with success/failure/escalation/fallback rates and latency by skill version.
- Audit evidence API returns linked order/origination/governance/finance/inventory/memory artifacts plus a replayable timeline for each order.
- Audit evidence now includes `payroll_allocations` and margin-after-autonomy totals per order.
- Audit evidence now includes `skill_invocations` so each autonomous skill attempt is traceable with status, retries, fallback, and hashes.
- Audit evidence now includes semantic-memory provenance (`agent_memory_provenance`) for read/write/retention actions linked to order timelines.

## 6) Functional Verification Evidence

For each run, verify:
1. state transitions from intake to closure,
2. policy checks and exception paths,
3. financial postings and settlement records,
4. product/service cost capture,
5. board KPI and variance movement,
6. skill selection, skill execution outcomes, and skill-level evidence capture,
7. semantic memory retrieval and learning write-back evidence for each agent.

## 7) Documentation Map

Primary documents:
1. `README.md`
2. `docs/functional-units.md`
3. `docs/roadmap.md`
4. `docs/spec-requirements.md`
5. `docs/spec-design-and-tasks.md`
6. `docs/sequence-diagrams-user-journeys.md`
7. `docs/organization.md`
8. `docs/agents.md`

## 8) Delivery Standard

All work is spec-driven and traceable:
- Requirements: `FR-*`, `NFR-*`
- Design: `DS-*`
- Tasks: `TSK-*`

No feature is done without requirement coverage, acceptance evidence, and traceability.
