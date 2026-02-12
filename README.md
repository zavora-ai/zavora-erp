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

Health checks:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8090/healthz
```

Create a transaction (current baseline API):

```bash
curl -X POST http://localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{
    "customer_email": "buyer@acme.com",
    "item_code": "SKU-001",
    "quantity": "5",
    "unit_price": "49.99",
    "currency": "USD"
  }'
```

Read board pack:

```bash
curl http://localhost:8090/board/pack
```

Note:
- The current API models a product-style transaction.
- The spec now requires hybrid support where a business can sell a product or a service.
- Service-specific workflows are tracked in the docs below and will be implemented against the same governance/accounting model.

## 6) Functional Verification Evidence

For each run, verify:
1. state transitions from intake to closure,
2. policy checks and exception paths,
3. financial postings and settlement records,
4. product/service cost capture,
5. board KPI and variance movement,
6. skill selection, skill execution outcomes, and skill-level evidence capture.

## 7) Documentation Map

Primary documents:
1. `README.md`
2. `docs/functional-units.md`
3. `docs/roadmap.md`
4. `docs/spec-requirements.md`
5. `docs/spec-design-and-tasks.md`
6. `docs/sequence-diagrams-user-journeys.md`

## 8) Delivery Standard

All work is spec-driven and traceable:
- Requirements: `FR-*`, `NFR-*`
- Design: `DS-*`
- Tasks: `TSK-*`

No feature is done without requirement coverage, acceptance evidence, and traceability.
