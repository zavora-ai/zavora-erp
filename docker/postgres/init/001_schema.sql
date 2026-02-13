CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_email TEXT NOT NULL,
    transaction_type TEXT NOT NULL DEFAULT 'PRODUCT' CHECK (transaction_type IN ('PRODUCT', 'SERVICE')),
    requested_by_agent_id TEXT NOT NULL DEFAULT 'sales-agent',
    item_code TEXT NOT NULL,
    quantity NUMERIC(20, 4) NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(20, 4) NOT NULL CHECK (unit_price > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    fulfilled_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_transaction_type ON orders(transaction_type);
CREATE INDEX IF NOT EXISTS idx_orders_requested_by_agent_id ON orders(requested_by_agent_id);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS transaction_type TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS requested_by_agent_id TEXT;

UPDATE orders
SET transaction_type = 'PRODUCT'
WHERE transaction_type IS NULL;

UPDATE orders
SET requested_by_agent_id = 'sales-agent'
WHERE requested_by_agent_id IS NULL;

ALTER TABLE orders ALTER COLUMN transaction_type SET DEFAULT 'PRODUCT';
ALTER TABLE orders ALTER COLUMN requested_by_agent_id SET DEFAULT 'sales-agent';
ALTER TABLE orders ALTER COLUMN transaction_type SET NOT NULL;
ALTER TABLE orders ALTER COLUMN requested_by_agent_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'orders_transaction_type_check'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_transaction_type_check
            CHECK (transaction_type IN ('PRODUCT', 'SERVICE'));
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS leads (
    id UUID PRIMARY KEY,
    contact_email TEXT NOT NULL,
    source_channel TEXT NOT NULL,
    note TEXT,
    status TEXT NOT NULL CHECK (status IN ('NEW', 'QUALIFIED', 'CONVERTED', 'DROPPED')),
    requested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);

CREATE TABLE IF NOT EXISTS opportunities (
    id UUID PRIMARY KEY,
    lead_id UUID NOT NULL REFERENCES leads(id),
    customer_email TEXT NOT NULL,
    transaction_type TEXT NOT NULL DEFAULT 'PRODUCT' CHECK (transaction_type IN ('PRODUCT', 'SERVICE')),
    item_code TEXT NOT NULL,
    quantity NUMERIC(20, 4) NOT NULL CHECK (quantity > 0),
    target_unit_price NUMERIC(20, 4) NOT NULL CHECK (target_unit_price > 0),
    currency TEXT NOT NULL,
    risk_class TEXT NOT NULL DEFAULT 'STANDARD',
    stage TEXT NOT NULL CHECK (stage IN ('QUALIFIED', 'PROPOSAL', 'ACCEPTED', 'LOST')),
    requested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opportunities_lead_id ON opportunities(lead_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_stage ON opportunities(stage);
CREATE INDEX IF NOT EXISTS idx_opportunities_created_at ON opportunities(created_at);

CREATE TABLE IF NOT EXISTS quotes (
    id UUID PRIMARY KEY,
    opportunity_id UUID NOT NULL REFERENCES opportunities(id),
    unit_price NUMERIC(20, 4) NOT NULL CHECK (unit_price > 0),
    quantity NUMERIC(20, 4) NOT NULL CHECK (quantity > 0),
    currency TEXT NOT NULL,
    payment_terms_days INTEGER NOT NULL CHECK (payment_terms_days >= 0),
    valid_until TIMESTAMPTZ NOT NULL,
    terms_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('ISSUED', 'ACCEPTED', 'EXPIRED', 'REJECTED')),
    requested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_quotes_opportunity_id ON quotes(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_quotes_status ON quotes(status);
CREATE INDEX IF NOT EXISTS idx_quotes_valid_until ON quotes(valid_until);

CREATE TABLE IF NOT EXISTS quote_acceptances (
    id UUID PRIMARY KEY,
    quote_id UUID NOT NULL UNIQUE REFERENCES quotes(id),
    opportunity_id UUID NOT NULL REFERENCES opportunities(id),
    order_id UUID NOT NULL UNIQUE REFERENCES orders(id),
    accepted_by TEXT NOT NULL,
    acceptance_channel TEXT NOT NULL,
    proof_ref TEXT NOT NULL,
    requested_by_agent_id TEXT NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_quote_acceptances_quote_id ON quote_acceptances(quote_id);
CREATE INDEX IF NOT EXISTS idx_quote_acceptances_order_id ON quote_acceptances(order_id);
CREATE INDEX IF NOT EXISTS idx_quote_acceptances_accepted_at ON quote_acceptances(accepted_at);

CREATE TABLE IF NOT EXISTS origination_channel_proofs (
    id UUID PRIMARY KEY,
    proof_ref TEXT NOT NULL UNIQUE,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('EMAIL', 'WEBHOOK')),
    message_id TEXT NOT NULL,
    contact_email TEXT,
    subject TEXT,
    source_ref TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    lead_id UUID REFERENCES leads(id),
    opportunity_id UUID REFERENCES opportunities(id),
    quote_id UUID REFERENCES quotes(id),
    acceptance_id UUID REFERENCES quote_acceptances(id),
    captured_by_agent_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    UNIQUE (channel_type, message_id)
);

CREATE INDEX IF NOT EXISTS idx_origination_channel_proofs_lead_id
    ON origination_channel_proofs(lead_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_origination_channel_proofs_opportunity_id
    ON origination_channel_proofs(opportunity_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_origination_channel_proofs_quote_id
    ON origination_channel_proofs(quote_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_origination_channel_proofs_acceptance_id
    ON origination_channel_proofs(acceptance_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_origination_channel_proofs_channel_captured
    ON origination_channel_proofs(channel_type, captured_at DESC);

CREATE TABLE IF NOT EXISTS governance_thresholds (
    action_type TEXT PRIMARY KEY,
    max_auto_amount NUMERIC(20, 4) NOT NULL CHECK (max_auto_amount > 0),
    currency TEXT NOT NULL DEFAULT 'USD',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_by_agent_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS governance_freeze_controls (
    action_type TEXT PRIMARY KEY,
    is_frozen BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT,
    updated_by_agent_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS governance_escalations (
    id UUID PRIMARY KEY,
    action_type TEXT NOT NULL,
    reference_type TEXT NOT NULL,
    reference_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'FROZEN')),
    reason_code TEXT NOT NULL,
    amount NUMERIC(20, 4) NOT NULL CHECK (amount >= 0),
    currency TEXT NOT NULL,
    requested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ,
    decided_by_agent_id TEXT,
    decision_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_governance_escalations_status_created
    ON governance_escalations(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_governance_escalations_reference
    ON governance_escalations(reference_type, reference_id);

CREATE TABLE IF NOT EXISTS strategy_offerings (
    id UUID PRIMARY KEY,
    offering_code TEXT NOT NULL UNIQUE,
    offering_type TEXT NOT NULL CHECK (offering_type IN ('PRODUCT', 'SERVICE')),
    name TEXT NOT NULL,
    unit_of_measure TEXT NOT NULL,
    default_unit_price NUMERIC(20, 4),
    currency TEXT NOT NULL DEFAULT 'USD',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    owner_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (default_unit_price IS NULL OR default_unit_price >= 0)
);

CREATE INDEX IF NOT EXISTS idx_strategy_offerings_type_active
    ON strategy_offerings(offering_type, active, updated_at DESC);

CREATE TABLE IF NOT EXISTS strategy_kpi_targets (
    id UUID PRIMARY KEY,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    business_unit TEXT NOT NULL,
    mandate TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    target_value NUMERIC(20, 4) NOT NULL CHECK (target_value >= 0),
    warning_threshold_pct NUMERIC(20, 4) NOT NULL CHECK (warning_threshold_pct >= 0),
    critical_threshold_pct NUMERIC(20, 4) NOT NULL CHECK (critical_threshold_pct >= warning_threshold_pct),
    currency TEXT NOT NULL DEFAULT 'USD',
    updated_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (period_end >= period_start),
    UNIQUE (period_start, period_end, business_unit, mandate, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_strategy_kpi_targets_period
    ON strategy_kpi_targets(period_start DESC, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_kpi_targets_dimension
    ON strategy_kpi_targets(business_unit, mandate, metric_name);

CREATE TABLE IF NOT EXISTS strategy_forecasts (
    id UUID PRIMARY KEY,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    business_unit TEXT NOT NULL,
    mandate TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    forecast_value NUMERIC(20, 4) NOT NULL CHECK (forecast_value >= 0),
    confidence_pct NUMERIC(20, 4) CHECK (confidence_pct >= 0 AND confidence_pct <= 100),
    assumptions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    currency TEXT NOT NULL DEFAULT 'USD',
    generated_by_agent_id TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (period_end >= period_start),
    UNIQUE (period_start, period_end, business_unit, mandate, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_strategy_forecasts_period
    ON strategy_forecasts(period_start DESC, period_end DESC, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_forecasts_dimension
    ON strategy_forecasts(business_unit, mandate, metric_name);

CREATE TABLE IF NOT EXISTS strategy_variances (
    id UUID PRIMARY KEY,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    business_unit TEXT NOT NULL,
    mandate TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    target_value NUMERIC(20, 4) NOT NULL CHECK (target_value >= 0),
    actual_value NUMERIC(20, 4) NOT NULL CHECK (actual_value >= 0),
    forecast_value NUMERIC(20, 4),
    variance_amount NUMERIC(20, 4) NOT NULL CHECK (variance_amount >= 0),
    variance_pct NUMERIC(20, 4) NOT NULL CHECK (variance_pct >= 0),
    severity TEXT NOT NULL CHECK (severity IN ('ON_TRACK', 'WARNING', 'BREACH')),
    evaluated_by_agent_id TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    notes TEXT,
    CHECK (period_end >= period_start)
);

CREATE INDEX IF NOT EXISTS idx_strategy_variances_period
    ON strategy_variances(period_start DESC, period_end DESC, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_variances_dimension
    ON strategy_variances(business_unit, mandate, metric_name, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_variances_severity
    ON strategy_variances(severity, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS strategy_corrective_actions (
    id UUID PRIMARY KEY,
    variance_id UUID NOT NULL REFERENCES strategy_variances(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    reason_code TEXT NOT NULL,
    action_note TEXT,
    linked_escalation_id UUID REFERENCES governance_escalations(id),
    created_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_strategy_corrective_actions_status_created
    ON strategy_corrective_actions(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_corrective_actions_variance
    ON strategy_corrective_actions(variance_id, created_at DESC);

CREATE TABLE IF NOT EXISTS inventory_positions (
    item_code TEXT PRIMARY KEY,
    on_hand NUMERIC(20, 4) NOT NULL,
    avg_cost NUMERIC(20, 4) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_movements (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES orders(id),
    item_code TEXT NOT NULL REFERENCES inventory_positions(item_code),
    movement_type TEXT NOT NULL,
    quantity NUMERIC(20, 4) NOT NULL,
    unit_cost NUMERIC(20, 4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_movements_order_id ON inventory_movements(order_id);
CREATE INDEX IF NOT EXISTS idx_inventory_movements_item_code ON inventory_movements(item_code);

CREATE TABLE IF NOT EXISTS journals (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    account TEXT NOT NULL,
    debit NUMERIC(20, 4) NOT NULL,
    credit NUMERIC(20, 4) NOT NULL,
    memo TEXT NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL,
    CHECK (debit >= 0),
    CHECK (credit >= 0)
);

CREATE INDEX IF NOT EXISTS idx_journals_order_id ON journals(order_id);
CREATE INDEX IF NOT EXISTS idx_journals_account ON journals(account);

CREATE TABLE IF NOT EXISTS settlements (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    amount NUMERIC(20, 4) NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_settlements_order_id ON settlements(order_id);

CREATE TABLE IF NOT EXISTS invoices (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders(id),
    invoice_number TEXT NOT NULL UNIQUE,
    customer_email TEXT NOT NULL,
    amount NUMERIC(20, 4) NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ISSUED', 'PARTIALLY_PAID', 'PAID', 'VOID')),
    issued_at TIMESTAMPTZ NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ,
    created_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (due_at >= issued_at)
);

CREATE INDEX IF NOT EXISTS idx_invoices_status_due_at
    ON invoices(status, due_at);
CREATE INDEX IF NOT EXISTS idx_invoices_issued_at
    ON invoices(issued_at DESC);

CREATE TABLE IF NOT EXISTS ar_subledger_entries (
    id UUID PRIMARY KEY,
    invoice_id UUID NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    order_id UUID NOT NULL REFERENCES orders(id),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('INVOICE_ISSUED', 'PAYMENT_RECEIVED', 'ADJUSTMENT')),
    debit NUMERIC(20, 4) NOT NULL CHECK (debit >= 0),
    credit NUMERIC(20, 4) NOT NULL CHECK (credit >= 0),
    balance_after NUMERIC(20, 4) NOT NULL,
    currency TEXT NOT NULL,
    memo TEXT NOT NULL,
    posted_by_agent_id TEXT NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ar_subledger_invoice_posted_at
    ON ar_subledger_entries(invoice_id, posted_at);
CREATE INDEX IF NOT EXISTS idx_ar_subledger_order_posted_at
    ON ar_subledger_entries(order_id, posted_at);

CREATE TABLE IF NOT EXISTS ap_obligations (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    source_type TEXT NOT NULL CHECK (source_type IN ('PROCUREMENT', 'SERVICE_DELIVERY', 'AUTONOMY_PAYROLL')),
    counterparty TEXT NOT NULL,
    amount NUMERIC(20, 4) NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'SETTLED', 'CANCELLED')),
    due_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ,
    created_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ap_obligations_status_due_at
    ON ap_obligations(status, due_at);
CREATE INDEX IF NOT EXISTS idx_ap_obligations_order_id
    ON ap_obligations(order_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ap_subledger_entries (
    id UUID PRIMARY KEY,
    ap_obligation_id UUID NOT NULL REFERENCES ap_obligations(id) ON DELETE CASCADE,
    order_id UUID NOT NULL REFERENCES orders(id),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('OBLIGATION_RECOGNIZED', 'PAYMENT_POSTED', 'ADJUSTMENT')),
    debit NUMERIC(20, 4) NOT NULL CHECK (debit >= 0),
    credit NUMERIC(20, 4) NOT NULL CHECK (credit >= 0),
    balance_after NUMERIC(20, 4) NOT NULL,
    currency TEXT NOT NULL,
    memo TEXT NOT NULL,
    posted_by_agent_id TEXT NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ap_subledger_obligation_posted_at
    ON ap_subledger_entries(ap_obligation_id, posted_at);
CREATE INDEX IF NOT EXISTS idx_ap_subledger_order_posted_at
    ON ap_subledger_entries(order_id, posted_at);

CREATE TABLE IF NOT EXISTS agent_semantic_memory (
    id UUID PRIMARY KEY,
    agent_name TEXT NOT NULL,
    scope TEXT NOT NULL,
    entity_id UUID,
    content TEXT NOT NULL,
    keywords TEXT[] NOT NULL DEFAULT '{}',
    source_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    last_accessed_at TIMESTAMPTZ,
    access_count BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_agent_semantic_memory_agent_scope
    ON agent_semantic_memory(agent_name, scope, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_semantic_memory_entity_id
    ON agent_semantic_memory(entity_id);
CREATE INDEX IF NOT EXISTS idx_agent_semantic_memory_keywords
    ON agent_semantic_memory USING GIN (keywords);
CREATE INDEX IF NOT EXISTS idx_agent_semantic_memory_fts
    ON agent_semantic_memory USING GIN (to_tsvector('simple', content));

CREATE TABLE IF NOT EXISTS agent_memory_provenance (
    id UUID PRIMARY KEY,
    memory_id UUID REFERENCES agent_semantic_memory(id) ON DELETE CASCADE,
    entity_id UUID,
    action_type TEXT NOT NULL CHECK (action_type IN ('WRITE', 'READ', 'RETENTION_PRUNE')),
    actor_agent_id TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    query_text TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_memory_provenance_memory_id
    ON agent_memory_provenance(memory_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_memory_provenance_entity_id
    ON agent_memory_provenance(entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_memory_provenance_actor
    ON agent_memory_provenance(actor_agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS memory_retention_policies (
    scope TEXT PRIMARY KEY,
    retention_days INTEGER NOT NULL CHECK (retention_days > 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_by_agent_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO memory_retention_policies(scope, retention_days, enabled, updated_by_agent_id, updated_at)
VALUES
    ('ORDER', 365, TRUE, 'audit-agent', NOW()),
    ('ORDER_EXECUTION', 365, TRUE, 'audit-agent', NOW()),
    ('ORDER_COST_ALLOCATION', 365, TRUE, 'audit-agent', NOW()),
    ('PRODUCT_EXECUTION', 365, TRUE, 'audit-agent', NOW()),
    ('SERVICE_EXECUTION', 365, TRUE, 'audit-agent', NOW())
ON CONFLICT (scope) DO NOTHING;

INSERT INTO inventory_positions(item_code, on_hand, avg_cost, updated_at)
VALUES
    ('SKU-001', 25.0000, 18.5000, NOW()),
    ('SKU-002', 10.0000, 42.0000, NOW())
ON CONFLICT (item_code) DO NOTHING;

INSERT INTO governance_thresholds(
    action_type, max_auto_amount, currency, active, updated_by_agent_id, updated_at
)
VALUES
    ('ORDER_EXECUTION_PRODUCT', 5000.0000, 'USD', TRUE, 'board-agent', NOW()),
    ('ORDER_EXECUTION_SERVICE', 5000.0000, 'USD', TRUE, 'board-agent', NOW())
ON CONFLICT (action_type) DO NOTHING;

INSERT INTO governance_freeze_controls(
    action_type, is_frozen, reason, updated_by_agent_id, updated_at
)
VALUES
    ('ORDER_EXECUTION_PRODUCT', FALSE, NULL, 'board-agent', NOW()),
    ('ORDER_EXECUTION_SERVICE', FALSE, NULL, 'board-agent', NOW())
ON CONFLICT (action_type) DO NOTHING;

INSERT INTO strategy_offerings(
    id,
    offering_code,
    offering_type,
    name,
    unit_of_measure,
    default_unit_price,
    currency,
    active,
    owner_agent_id,
    created_at,
    updated_at
)
VALUES
    (
        uuid_generate_v4(),
        'SKU-001',
        'PRODUCT',
        'Starter Product Bundle',
        'UNIT',
        55.0000,
        'USD',
        TRUE,
        'strategy-agent',
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        'SVC-IMPLEMENTATION',
        'SERVICE',
        'Implementation Service Package',
        'ENGAGEMENT',
        1100.0000,
        'USD',
        TRUE,
        'strategy-agent',
        NOW(),
        NOW()
    )
ON CONFLICT (offering_code) DO NOTHING;

INSERT INTO strategy_kpi_targets(
    id,
    period_start,
    period_end,
    business_unit,
    mandate,
    metric_name,
    target_value,
    warning_threshold_pct,
    critical_threshold_pct,
    currency,
    updated_by_agent_id,
    created_at,
    updated_at
)
VALUES
    (
        uuid_generate_v4(),
        date_trunc('month', NOW())::date,
        (date_trunc('month', NOW()) + interval '1 month - 1 day')::date,
        'GLOBAL',
        'GROWTH',
        'REVENUE',
        10000.0000,
        5.0000,
        10.0000,
        'USD',
        'strategy-agent',
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        date_trunc('month', NOW())::date,
        (date_trunc('month', NOW()) + interval '1 month - 1 day')::date,
        'GLOBAL',
        'GROWTH',
        'CASH',
        8000.0000,
        5.0000,
        10.0000,
        'USD',
        'strategy-agent',
        NOW(),
        NOW()
    )
ON CONFLICT (period_start, period_end, business_unit, mandate, metric_name) DO NOTHING;

INSERT INTO strategy_forecasts(
    id,
    period_start,
    period_end,
    business_unit,
    mandate,
    metric_name,
    forecast_value,
    confidence_pct,
    assumptions_json,
    currency,
    generated_by_agent_id,
    generated_at,
    updated_at
)
VALUES
    (
        uuid_generate_v4(),
        date_trunc('month', NOW())::date,
        (date_trunc('month', NOW()) + interval '1 month - 1 day')::date,
        'GLOBAL',
        'GROWTH',
        'REVENUE',
        9500.0000,
        82.5000,
        '{"driver":"pipeline-weighted", "seasonality":"neutral"}'::jsonb,
        'USD',
        'strategy-agent',
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        date_trunc('month', NOW())::date,
        (date_trunc('month', NOW()) + interval '1 month - 1 day')::date,
        'GLOBAL',
        'GROWTH',
        'CASH',
        7800.0000,
        80.0000,
        '{"driver":"collections-curve"}'::jsonb,
        'USD',
        'strategy-agent',
        NOW(),
        NOW()
    )
ON CONFLICT (period_start, period_end, business_unit, mandate, metric_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS finops_token_usage (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES orders(id),
    agent_id TEXT NOT NULL,
    skill_id TEXT,
    action_name TEXT NOT NULL,
    input_tokens BIGINT NOT NULL CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL CHECK (output_tokens >= 0),
    total_tokens BIGINT NOT NULL CHECK (total_tokens >= 0),
    token_unit_cost NUMERIC(20, 8) NOT NULL CHECK (token_unit_cost >= 0),
    total_cost NUMERIC(20, 4) NOT NULL CHECK (total_cost >= 0),
    currency TEXT NOT NULL,
    source_ref TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_finops_token_usage_occurred_at
    ON finops_token_usage(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_finops_token_usage_order_id
    ON finops_token_usage(order_id);
CREATE INDEX IF NOT EXISTS idx_finops_token_usage_agent_id
    ON finops_token_usage(agent_id);

CREATE TABLE IF NOT EXISTS finops_cloud_costs (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES orders(id),
    provider TEXT NOT NULL,
    cost_type TEXT NOT NULL CHECK (cost_type IN ('COMPUTE', 'STORAGE', 'NETWORK')),
    usage_quantity NUMERIC(20, 6) NOT NULL CHECK (usage_quantity >= 0),
    unit_cost NUMERIC(20, 6) NOT NULL CHECK (unit_cost >= 0),
    total_cost NUMERIC(20, 4) NOT NULL CHECK (total_cost >= 0),
    currency TEXT NOT NULL,
    source_ref TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_finops_cloud_costs_occurred_at
    ON finops_cloud_costs(occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_finops_cloud_costs_order_id
    ON finops_cloud_costs(order_id);

CREATE TABLE IF NOT EXISTS finops_subscription_costs (
    id UUID PRIMARY KEY,
    tool_name TEXT NOT NULL,
    subscription_name TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    total_cost NUMERIC(20, 4) NOT NULL CHECK (total_cost >= 0),
    currency TEXT NOT NULL,
    source_ref TEXT,
    ingested_by_agent_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (period_end > period_start)
);

CREATE INDEX IF NOT EXISTS idx_finops_subscription_costs_period
    ON finops_subscription_costs(period_start, period_end);

CREATE TABLE IF NOT EXISTS finops_cost_allocations (
    id UUID PRIMARY KEY,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    order_id UUID NOT NULL REFERENCES orders(id),
    source_type TEXT NOT NULL CHECK (source_type IN ('TOKEN', 'CLOUD', 'SUBSCRIPTION')),
    source_id UUID NOT NULL,
    agent_id TEXT,
    skill_id TEXT,
    allocation_basis TEXT NOT NULL CHECK (allocation_basis IN ('DIRECT_ORDER', 'REVENUE_SHARE')),
    allocated_cost NUMERIC(20, 4) NOT NULL CHECK (allocated_cost >= 0),
    currency TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

DROP INDEX IF EXISTS idx_finops_cost_allocations_uniqueness;
CREATE UNIQUE INDEX IF NOT EXISTS idx_finops_cost_allocations_uniqueness
    ON finops_cost_allocations(
        period_start,
        period_end,
        source_type,
        source_id,
        order_id,
        COALESCE(skill_id, 'UNSPECIFIED')
    );
CREATE INDEX IF NOT EXISTS idx_finops_cost_allocations_period
    ON finops_cost_allocations(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_finops_cost_allocations_order_id
    ON finops_cost_allocations(order_id);

CREATE TABLE IF NOT EXISTS finops_period_reconciliations (
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    source_total NUMERIC(20, 4) NOT NULL CHECK (source_total >= 0),
    allocated_total NUMERIC(20, 4) NOT NULL CHECK (allocated_total >= 0),
    journal_total NUMERIC(20, 4) NOT NULL CHECK (journal_total >= 0),
    variance_amount NUMERIC(20, 4) NOT NULL CHECK (variance_amount >= 0),
    variance_pct NUMERIC(20, 4) NOT NULL CHECK (variance_pct >= 0),
    orders_allocated BIGINT NOT NULL CHECK (orders_allocated >= 0),
    status TEXT NOT NULL CHECK (status IN ('BALANCED', 'OUT_OF_TOLERANCE', 'NO_SOURCE_COSTS')),
    completed_by_agent_id TEXT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (period_start, period_end),
    CHECK (period_end > period_start)
);

CREATE INDEX IF NOT EXISTS idx_finops_period_reconciliations_completed_at
    ON finops_period_reconciliations(completed_at DESC);

CREATE TABLE IF NOT EXISTS skill_registry (
    id UUID PRIMARY KEY,
    skill_id TEXT NOT NULL,
    skill_version TEXT NOT NULL,
    capability TEXT NOT NULL,
    owner_agent_id TEXT NOT NULL,
    approval_status TEXT NOT NULL CHECK (approval_status IN ('APPROVED', 'DRAFT', 'REVOKED')),
    required_input_fields TEXT[] NOT NULL DEFAULT '{}',
    required_output_fields TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (skill_id, skill_version)
);

CREATE INDEX IF NOT EXISTS idx_skill_registry_capability_status
    ON skill_registry(capability, approval_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS skill_routing_policies (
    intent TEXT NOT NULL,
    transaction_type TEXT NOT NULL DEFAULT 'ANY' CHECK (transaction_type IN ('ANY', 'PRODUCT', 'SERVICE')),
    capability TEXT NOT NULL,
    primary_skill_id TEXT NOT NULL,
    primary_skill_version TEXT NOT NULL,
    fallback_skill_id TEXT,
    fallback_skill_version TEXT,
    max_retries INTEGER NOT NULL DEFAULT 1 CHECK (max_retries >= 0 AND max_retries <= 5),
    escalation_action_type TEXT NOT NULL DEFAULT 'SKILL_EXECUTION',
    updated_by_agent_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (intent, transaction_type)
);

CREATE TABLE IF NOT EXISTS skill_invocations (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    intent TEXT NOT NULL,
    capability TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    skill_version TEXT NOT NULL,
    actor_agent_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'ESCALATED')),
    failure_reason TEXT,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    input_hash TEXT NOT NULL,
    output_hash TEXT,
    latency_ms BIGINT NOT NULL CHECK (latency_ms >= 0),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_skill_invocations_order_id
    ON skill_invocations(order_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_skill_invocations_skill_version
    ON skill_invocations(skill_id, skill_version, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_skill_invocations_status_created
    ON skill_invocations(status, created_at DESC);

INSERT INTO skill_registry (
    id,
    skill_id,
    skill_version,
    capability,
    owner_agent_id,
    approval_status,
    required_input_fields,
    required_output_fields,
    created_at,
    updated_at
)
VALUES
    (
        uuid_generate_v4(),
        'product-fulfillment',
        '1.0.0',
        'fulfillment-execution',
        'ops-orchestrator-agent',
        'APPROVED',
        ARRAY['order_id', 'transaction_type', 'item_code', 'quantity', 'unit_price'],
        ARRAY['execution_status', 'fulfillment_mode'],
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        'product-fulfillment-safe',
        '1.0.0',
        'fulfillment-execution',
        'ops-orchestrator-agent',
        'APPROVED',
        ARRAY['order_id', 'transaction_type', 'item_code', 'quantity', 'unit_price'],
        ARRAY['execution_status', 'fulfillment_mode'],
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        'service-delivery',
        '1.0.0',
        'service-delivery',
        'ops-orchestrator-agent',
        'APPROVED',
        ARRAY['order_id', 'transaction_type', 'item_code', 'quantity', 'unit_price'],
        ARRAY['execution_status', 'delivery_mode'],
        NOW(),
        NOW()
    ),
    (
        uuid_generate_v4(),
        'service-delivery-safe',
        '1.0.0',
        'service-delivery',
        'ops-orchestrator-agent',
        'APPROVED',
        ARRAY['order_id', 'transaction_type', 'item_code', 'quantity', 'unit_price'],
        ARRAY['execution_status', 'delivery_mode'],
        NOW(),
        NOW()
    )
ON CONFLICT (skill_id, skill_version) DO NOTHING;

INSERT INTO skill_routing_policies (
    intent,
    transaction_type,
    capability,
    primary_skill_id,
    primary_skill_version,
    fallback_skill_id,
    fallback_skill_version,
    max_retries,
    escalation_action_type,
    updated_by_agent_id,
    updated_at
)
VALUES
    (
        'ORDER_EXECUTION_PRODUCT',
        'PRODUCT',
        'fulfillment-execution',
        'product-fulfillment',
        '1.0.0',
        'product-fulfillment-safe',
        '1.0.0',
        1,
        'SKILL_EXECUTION_PRODUCT',
        'board-agent',
        NOW()
    ),
    (
        'ORDER_EXECUTION_SERVICE',
        'SERVICE',
        'service-delivery',
        'service-delivery',
        '1.0.0',
        'service-delivery-safe',
        '1.0.0',
        1,
        'SKILL_EXECUTION_SERVICE',
        'board-agent',
        NOW()
    )
ON CONFLICT (intent, transaction_type) DO NOTHING;
