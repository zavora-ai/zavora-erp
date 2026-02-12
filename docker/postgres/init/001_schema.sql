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
