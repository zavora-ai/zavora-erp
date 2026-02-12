# Spec Requirements (Functional and Non-Functional)

## 1. Requirement ID Standard

- Functional requirements use `FR-###`.
- Non-functional requirements use `NFR-###`.
- Each requirement is mapped to one functional unit (`FU-01` ... `FU-10`).
- Priority classes: `P0` (must), `P1` (should), `P2` (could).

## 2. Functional Requirements

### 2.1 FU-01 Strategy-to-Execution Layer

- `FR-001 (P0)` Define business model entities for product and service offerings.
- `FR-002 (P0)` Define KPI target registry by period, business unit, and mandate.
- `FR-003 (P0)` Generate forecast values for revenue, cost, and cash by period.
- `FR-004 (P0)` Compute target-vs-actual variance and classify variance severity.
- `FR-005 (P0)` Trigger corrective-action workflow when variance breaches threshold.

### 2.2 FU-02 Business Origination

- `FR-006 (P0)` Capture lead with channel source and timestamp.
- `FR-007 (P0)` Maintain opportunity lifecycle states.
- `FR-008 (P0)` Generate quote with terms, validity, and risk metadata.
- `FR-009 (P0)` Capture acceptance proof and bind to quote/opportunity.
- `FR-010 (P0)` Convert accepted quote into executable demand intent.

### 2.3 FU-03 Order Processing

- `FR-011 (P0)` Validate intake payload for product and service transaction types.
- `FR-012 (P0)` Apply policy gate before execution.
- `FR-013 (P0)` Orchestrate execution state transitions (`NEW`, `IN_PROGRESS`, `FULFILLED`, `FAILED`).
- `FR-014 (P0)` Persist failure reason and policy reason codes.
- `FR-015 (P1)` Support partial completion handling where workflow policy allows.

### 2.4 FU-04 Inventory and Procurement (Product Flows)

- `FR-016 (P0)` Maintain product stock positions (`on_hand`, `reserved`, `available`).
- `FR-017 (P0)` Reserve stock for approved product transactions.
- `FR-018 (P0)` Trigger procurement when product stock is insufficient.
- `FR-019 (P0)` Record receipt/issue/adjustment movements.
- `FR-020 (P0)` Apply AVCO valuation updates on receipts.
- `FR-021 (P1)` Compute product COGS on issue.

### 2.5 FU-05 Finance and Accounting

- `FR-022 (P0)` Issue invoice from completed transaction.
- `FR-023 (P0)` Post balanced double-entry journals for revenue/cost/settlement.
- `FR-024 (P0)` Maintain AR lifecycle through settlement.
- `FR-025 (P0)` Maintain AP obligations from procurement and external service commitments.
- `FR-026 (P0)` Support both product COGS and service-delivery cost recognition.

### 2.6 FU-06 Reporting and Revenue Tracking

- `FR-027 (P0)` Produce board pack with operational and financial KPIs.
- `FR-028 (P0)` Report target vs actual and forecast variance.
- `FR-029 (P1)` Produce Trial Balance, P&L, Balance Sheet, and Cash Flow views.
- `FR-030 (P1)` Produce revenue tracking views (booked, billed, collected).
- `FR-031 (P1)` Produce AR/AP aging views.

### 2.7 FU-07 Governance and Policy Enforcement

- `FR-032 (P0)` Enforce configurable approval thresholds.
- `FR-033 (P0)` Route workflow escalations to authorized human stakeholders.
- `FR-034 (P0)` Support freeze controls blocking high-risk actions.
- `FR-035 (P1)` Persist governance decisions with actor, reason, and timestamp.

### 2.8 FU-08 Agent Autonomy and Exception Handling

- `FR-036 (P0)` Execute eligible workflows autonomously within mandate.
- `FR-037 (P0)` Require human intervention only when policy triggers exception.
- `FR-038 (P1)` Persist decision rationale for autonomous actions.

### 2.9 FU-09 Audit and Compliance Evidence

- `FR-039 (P0)` Persist immutable event history for all material business actions.
- `FR-040 (P0)` Provide order-level timeline from origination to settlement.
- `FR-041 (P0)` Link commitment, obligation, proof, and settlement artifacts.
- `FR-042 (P1)` Export audit evidence package per transaction or period.

### 2.10 FU-10 Agent Payroll and Cost Allocation

- `FR-043 (P0)` Capture token usage and token-cost by agent action.
- `FR-044 (P0)` Capture cloud costs (compute/storage/network) for workflows.
- `FR-045 (P0)` Capture subscription/tool costs by period.
- `FR-046 (P0)` Allocate autonomous operating costs to workflow/order/period.
- `FR-047 (P0)` Post payroll-cost accounting entries and reconcile with source costs.
- `FR-048 (P1)` Report margin after autonomous operating costs.

## 3. Non-Functional Requirements

### 3.1 Reliability and Integrity

- `NFR-001 (P0)` No committed business transaction may be lost on restart.
- `NFR-002 (P0)` Workflow processing must be idempotent for duplicate event deliveries.
- `NFR-003 (P0)` Financial posting and status update must be atomic.
- `NFR-004 (P1)` Event replay must reconstruct prior state deterministically.

### 3.2 Performance

- `NFR-005 (P1)` Intake API median latency under 500 ms at nominal load.
- `NFR-006 (P1)` End-to-end demo workflow completion under 5 seconds at nominal load.
- `NFR-007 (P2)` Board pack response under 1 second for demo dataset size.

### 3.3 Security and Access

- `NFR-008 (P0)` Non-local deployments require authenticated access.
- `NFR-009 (P0)` Sensitive data must be encrypted in transit.
- `NFR-010 (P1)` Governance and finance actions must be role-restricted.

### 3.4 Operability

- `NFR-011 (P0)` All services must expose health endpoints.
- `NFR-012 (P1)` Critical workflow steps must emit structured correlation logs.
- `NFR-013 (P1)` Local stack must start with one command.

### 3.5 Quality and Delivery

- `NFR-014 (P0)` Every P0 functional requirement must have automated acceptance coverage.
- `NFR-015 (P0)` Every implemented requirement must be traceable to design/task IDs.
- `NFR-016 (P1)` Release candidate must have zero open P0 defects.

### 3.6 FinOps Integrity

- `NFR-017 (P0)` Cost ingestion pipelines must complete at least daily with no silent loss.
- `NFR-018 (P0)` Cost allocation must be deterministic for identical source inputs.
- `NFR-019 (P1)` At least 99% of autonomous operating cost must be attributable.
- `NFR-020 (P1)` Reconciliation variance between cost sources and payroll ledger must be below 0.5%.

## 4. Acceptance Rule

A requirement is done only when all conditions are met:
1. implementation complete,
2. acceptance evidence captured,
3. traceability linked to design and tasks,
4. business-owner signoff recorded.
