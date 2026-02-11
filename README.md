Zavora Agentic ERP – Minimum Viable Product (MVP) Plan
Overview

Zavora Agentic ERP aims to operate like a virtual company rather than a traditional data‑entry ERP. Agents are first‑class citizens; they autonomously negotiate, execute and settle commitments. Humans interact primarily through channels (e‑mail/portal/chat) as stakeholders (customers, suppliers, directors, auditors) rather than operators. The system is event‑sourced, with every action recorded as a commitment, obligation, proof or settlement. A constitution (previously drafted) defines citizens, mandates, policy enforcement, financial standards, and board governance.

The MVP demonstrates that Zavora can run a full order‑to‑cash cycle autonomously while producing auditable financial and inventory records. It leverages the ADK‑Rust framework to orchestrate agents and tools and uses Rust’s performance and type safety to build a modular, production‑ready system. This plan focuses on features that deliver the largest value with minimum complexity while adhering to the constitution.

Goals

Showcase autonomy: A customer places an order via a channel; agents negotiate terms, check stock, coordinate procurement if necessary, fulfill the order, generate an invoice and manage payment without human intervention.

Traceable commitments: Every step (quotes, acceptance, purchase commitments, shipment, invoicing, receipt) is stored as a commitment with proofs and can be replayed for audit.

Accounting & inventory: Inventory movements, COGS, AR, AP and GL postings are created deterministically based on events and a simple standards profile (IFRS‑lite). This meets small‑business ERP expectations that an ERP unify sales, finance and inventory into a single system.

Governance: The Board (humans + board agents) approves mandates, risk appetite and financial reports and can freeze actions. An example board pack is generated for period close.

Demonstrate ADK‑Rust integration: Use ADK‑Rust agents (LLM, workflow and custom agents) and tools (function tools, session memory, file storage) to orchestrate the workflow.

Scope

The MVP focuses on the Order‑to‑Cash cycle for a single legal entity and a few products. It includes:

Agents / Internal Companies
Company / Agent	Role in MVP
Sales Co.	Receives orders via a channel; negotiates price and terms; creates a Sales Commitment once accepted; raises obligations for fulfillment and invoicing.
Procurement Co.	Ensures sufficient stock by raising purchase commitments; negotiates with suppliers when inventory falls below threshold.
Warehouse Co.	Manages inventory movements; performs pick/pack/ship; issues proof of delivery.
Accounts Receivable (AR) Co.	Issues invoices; applies receipts; manages allocations and aging.
Controller Co.	Posts events to subledgers and GL under the selected standards profile; validates evidence before posting.
Board Co. / Board Agents	Monitors risk and KPIs; ratifies financials; can suspend actions over thresholds.
External Stakeholders

Customer (human) interacts via e‑mail or a web portal.

Supplier (agent or human) provides goods based on procurement commitments.

Data Model

Implement the key objects defined in the constitution:

Commitment: Signed agreement between parties; holds terms, status, risk class and signatures.

Obligation: Unit of work created by commitments; includes due date, owner and dependencies.

Proof: Evidence (messages, documents, tool logs) supporting commitments and obligations.

Settlement: Records financial closure (e.g., payment received); linked to commitments and proofs.

Dispute: Structured conflict; basic case management included but not automated in MVP.

Event Store & Projections

Event store: PostgreSQL (or SQLite for early development) storing append‑only events. Each event corresponds to creation, update or settlement of a commitment/obligation/proof/settlement.

Projections: Materialized views for (a) current inventory levels by item/location; (b) AR/AP subledger balances; (c) General Ledger; and (d) KPI dashboards for the Board.

Accounting & Inventory

Inventory: Weighted‑average cost (AVCO). Inventory events (stock received, issued, transfer, adjustment) update both the operational stock and valuation layers. Real‑time stock and cost-of-goods sold are computed.

Financial: Implement AR and AP subledgers. Recognize revenue and COGS upon shipment. Controller Co. posts double‑entry journals to a Chart of Accounts defined by a simple IFRS‑lite profile. Reports: Trial Balance, P&L, Balance Sheet, Cash Flow, Inventory Valuation and AR/AP Aging.

Standards Profile: One profile (IFRS‑lite) defines recognition rules, account mappings, and AVCO for inventory.

Channels

Email (via ADK‑Rust adk-browser or adk-tool integration): Sales Co. monitors a dedicated inbox, parses requests, and replies. Proof objects store all messages.

Webhook / API (optional): For simulation, a REST endpoint accepts orders and returns commitments.

Architecture & Technology
ADK‑Rust integration

Agent Definitions: Use adk-core and adk-agent to define custom agents. Each company is implemented as a long‑lived loop agent (e.g., SalesAgent, ProcurementAgent). Agents subscribe to new events and channel messages and then invoke tools or submit intents.

Tool Layer: Implement key functions as FunctionTools: inventory query, procurement creation, posting to ledger, sending e‑mails, updating subledgers. Use ADK‑Rust’s built‑in Google Search and streaming capabilities where appropriate.

Workflow Coordination: Use SequentialAgent, ParallelAgent and LoopAgent to orchestrate multi‑step processes (e.g., fulfilment → invoicing → settlement). ADK‑Rust’s event streaming and session management provide stateful conversations.

Observability & Guardrails: Adopt ADK‑Rust’s guardrails for JSON schema validation, content filtering and OpenTelemetry tracing.

Rust & Project Structure

Create a Cargo workspace with the following crates:

zavora-core: Defines data models (commitment, obligation, proof, settlement), event types, standards profile and basic storage traits.

zavora-eventstore: Implements event storage and projection logic (using Postgres/SQLite). Exposes functions to append events and build projections.

zavora-finance: Implements subledgers, GL posting rules, Chart of Accounts and reporting functions. Uses zavora-eventstore for event reads.

zavora-inventory: Implements inventory movements and valuation logic (AVCO). Provides stock and cost queries.

zavora-agents: Contains agent definitions using ADK‑Rust: SalesAgent, ProcurementAgent, WarehouseAgent, ControllerAgent, ARAgent, BoardAgent. Each agent uses tools defined in zavora-tools.

zavora-tools: Implements function tools for sending/receiving messages, generating documents, performing database operations, etc. Exposes them to ADK‑Rust.

zavora-server (optional): Provides a REST or GraphQL API and channel integration endpoints for simulation/testing. Could serve a minimal UI for board/stakeholder views.

External Dependencies

ADK‑Rust (adk-rust crate) – provides the agent runtime, tool interfaces and multi‑provider LLM support.

Tokio / Async Runtime – concurrency for agents and event store.

PostgreSQL or SQLite – data storage; start with SQLite for local development.

Serde & JSON Schema – for message/commitment serialization and policy validation.

Any mail library (e.g., lettre) – to send and receive e‑mails in simulation.

MVP Workflow (Order‑to‑Cash)

Below is the end‑to‑end flow for the MVP with the main interactions:

Customer Inquiry – Customer emails or posts an order request. SalesAgent captures the message as a proof and opens a new Conversation. SalesAgent queries inventory via InventoryAgent; if sufficient stock, proceeds; else triggers ProcurementAgent to replenish.

Negotiation & Quote – SalesAgent uses an LLM via ADK‑Rust to generate a draft quote and terms. A deterministic policy checks risk (credit limit, product availability). If within mandate, SalesAgent sends the quote to the customer; else escalates to BoardAgent for approval.

Commitment Creation – Upon customer acceptance, SalesAgent creates a Sales Commitment with terms (quantity, price, delivery date). The commitment includes both parties’ signatures. Events: commitment_created and obligations_assigned (fulfillment and invoicing).

Procurement (if needed) – ProcurementAgent monitors stock levels; if stock is below threshold or reserved for the commitment, it creates a Purchase Commitment with suppliers. It negotiates price and lead time using agent capabilities. Once a supplier accepts, events for purchase commitment and obligations (receipt, payment) are created.

Fulfillment – WarehouseAgent picks, packs and ships the goods. It records a stock_issued inventory event and attaches proof of delivery (scan, shipping tracking number). Inventory valuation is updated, and COGS is calculated.

Invoicing & Accounts – ARAgent generates an invoice based on the commitment; a settlement_proposed event is emitted. ControllerAgent posts accounting entries to the AR subledger and GL: credit revenue, debit AR; simultaneously debit COGS and credit inventory.

Payment & Settlement – When payment is received (e.g., via simulated bank confirmation), ARAgent matches the receipt to the invoice. It creates a settlement_confirmed event; ControllerAgent posts the journal (debit cash, credit AR). The commitment status is set to fulfilled.

Reporting & Board Oversight – ControllerAgent periodically runs period close: computes Trial Balance, P&L, Balance Sheet, Cash Flow, AR/AP Aging and Inventory Valuation. BoardAgent compiles a Board Pack summarizing financials, commitments at risk, and inventory positions. Human directors can review and sign the reports via portal or email. BoardAgent enforces risk appetite limits (e.g., maximum open AR exposure) and can freeze actions above thresholds.

MVP Out of Scope

Multi‑entity consolidation, payroll, HR, manufacturing/MRP, complex tax engines.

Voice channels and real‐time voice agents (supported by ADK‑Rust but not needed in MVP).

External partner integrations beyond basic email/webhook.

Milestones & Timeline (Suggested)

Week 1–2: Setup & Data Model

Create the Cargo workspace and core crates (zavora-core, zavora-eventstore, zavora-finance, zavora-inventory).

Define schemas and serialization for commitments, obligations, proofs, settlements and events.

Implement event store with basic append and projection APIs.

Week 3–4: Inventory & Finance Modules

Implement inventory movement functions and AVCO valuation; create projections for current stock levels.

Define Chart of Accounts and posting rules for IFRS‑lite profile. Implement GL posting for inventory receipt, shipment, invoice and payment.

Implement reporting functions (Trial Balance, P&L, Balance Sheet, Inventory Valuation).

Week 5–6: Agent Foundations

Set up ADK‑Rust dependencies and define custom SalesAgent, ProcurementAgent, WarehouseAgent, ARAgent and ControllerAgent skeletons.

Implement essential function tools: send e‑mail, query inventory, create purchase commitment, post events.

Build event listeners and loops for each agent.

Week 7–8: Workflow Implementation & Channel Integration

Implement the order‑to‑cash workflow in zavora-agents using sequential and parallel agents.

Set up a local e‑mail simulation or simple REST interface to simulate customer orders and responses.

Implement procurement workflow triggered when inventory is low.

Week 9–10: Governance & Reporting

Implement BoardAgent to compile board packs and enforce risk appetites. Add a simple stakeholder portal (e.g., CLI or minimal web UI) for board and directors to review reports.

Implement basic user management for human directors to sign commitments and board resolutions.

Finalize dispute and escalation logic (simple logging for MVP).

Week 11–12: Testing & Hardening

Write integration tests simulating typical order‑to‑cash flows and edge cases (out-of-stock, price thresholds).

Perform performance testing on the event store and agents; optimize as needed.

Conduct a dry run of period close and board review.

References

Agentic ERP definition and capabilities: Agentic AI ERP integrates AI agents into ERP to enable data analysis, prediction and autonomous actions; an agent might detect a shipping delay and autonomously reroute deliveries and update inventory. Leading platforms use agents to schedule technicians, replenish inventory and predict failures.

Essential ERP features: Modern ERPs unify sales, finance and inventory management into a single system to eliminate data silos. Core modules include finance, CRM and inventory; the ERP should track every cent, manage accounts payable and provide real‑time inventory. Integrated finance and CRM can deliver higher efficiency and lower inventory costs.

ADK‑Rust features: ADK‑Rust is a production‑ready Rust framework for building AI agents. It offers type‑safe abstractions, workflow agents (sequential, parallel, loop), real‑time voice agents, tool ecosystems, session management, memory systems and production features such as REST/A2A APIs and guardrails.
