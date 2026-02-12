# Functional Units (Official)

This document defines the official business-service functional units for Zavora ERP.

## FU-01 Strategy-to-Execution Layer

Objective:
Drive operating decisions from business model, KPI targets, and forecast/variance governance.

In scope:
- Business model definition (product lines, service lines, pricing posture)
- KPI targets by period, business unit, and agent mandate
- Forecasting and variance tracking
- Corrective action triggers when variance exceeds threshold

Outputs:
- Strategy model
- Target registry
- Forecast and variance decisions

KPIs:
- Target attainment
- Forecast accuracy
- Variance response time

## FU-02 Business Origination

Objective:
Convert market demand into commercial commitments.

In scope:
- Lead capture
- Opportunity progression
- Quote generation and acceptance proof
- Conversion into executable order intent

Outputs:
- Qualified opportunity
- Accepted quote
- Commitment-ready intake

KPIs:
- Lead-to-opportunity rate
- Quote-to-win rate
- Time-to-conversion

## FU-03 Order Processing

Objective:
Execute customer demand through policy-validated workflows.

In scope:
- Intake validation
- Policy and risk gating
- Fulfillment orchestration
- Closure state management (`FULFILLED`/`FAILED`)

Outputs:
- Order timeline
- Fulfillment status
- Failure classifications

KPIs:
- Order cycle time
- Fulfillment success rate
- Exception ratio

## FU-04 Inventory and Procurement

Objective:
Support product businesses with stock and replenishment integrity.

In scope:
- Availability and reservation
- Shortage detection and procurement response
- Inventory movements and AVCO valuation
- COGS basis for product fulfillment

Outputs:
- On-hand/reserved/available positions
- Receipt/issue/adjustment logs
- Updated valuation layers

KPIs:
- Fill rate
- Stockout frequency
- Inventory turns

Note:
- For service-only businesses, this unit is optional and replaced by capacity/resource planning in FU-03 and FU-05.

## FU-05 Finance and Accounting

Objective:
Produce deterministic economic truth for product and service transactions.

In scope:
- AR/AP/GL subledgers
- Revenue and cost recognition
- Invoicing and settlement
- Reconciliation and period integrity checks

Outputs:
- Balanced journals
- Subledger balances
- Settlement records

KPIs:
- Journal integrity
- DSO/DPO
- Close readiness

## FU-06 Reporting and Revenue Tracking

Objective:
Provide commercial and financial transparency for board and operations.

In scope:
- Board pack and financial statements
- Revenue tracking (booked/billed/collected)
- Target vs actual tracking
- Forecast and variance reporting

Outputs:
- Board KPI pack
- Period reporting suite
- Variance reports

KPIs:
- Revenue attainment
- Gross margin
- Cash conversion

## FU-07 Governance and Policy Enforcement

Objective:
Ensure all actions stay within constitutional mandates.

In scope:
- Approval thresholds
- Escalation routing
- Freeze controls
- Governance decision audit trail

Outputs:
- Approval records
- Freeze actions
- Policy decision logs

KPIs:
- Policy breach rate
- Approval turnaround

## FU-08 Agent Autonomy and Exception Handling

Objective:
Maximize autonomous execution while preserving safe escalation.

In scope:
- Autonomous decisions within mandate
- Human-in-the-loop only for policy-triggered exceptions
- Decision rationale persistence

Outputs:
- Autonomous execution trail
- Exception queue outcomes

KPIs:
- Autonomous completion rate
- Human intervention ratio

## FU-09 Audit and Compliance Evidence

Objective:
Provide replayable and complete evidence for all material actions.

In scope:
- Immutable event history
- Commitment-obligation-proof-settlement linkage
- Transaction timeline and evidence packaging

Outputs:
- Replayable event trails
- Audit evidence packages

KPIs:
- Evidence completeness
- Evidence retrieval latency

## FU-10 Agent Payroll and Cost Allocation

Objective:
Account for autonomous operating costs as agent payroll economics.

In scope:
- Token usage and token cost metering
- Cloud cost ingestion (compute/storage/network)
- Subscription cost ingestion
- Cost allocation to workflow/order/period
- Cost accounting and unit-economics reporting

Outputs:
- Agent payroll ledger
- Cost-to-serve views
- Margin-after-autonomy-cost reports

KPIs:
- Revenue-to-agent-payroll ratio
- Cost-to-serve trend
- Allocation coverage rate

## Handoff Model

- FU-01 sets targets and guardrails for FU-02 to FU-10.
- FU-02 feeds FU-03 with accepted demand.
- FU-03 invokes FU-04 (for product flows) and FU-05.
- FU-05 and FU-10 feed FU-06 reporting.
- FU-07 and FU-08 constrain execution across all units.
- FU-09 spans all units as the evidence layer.
