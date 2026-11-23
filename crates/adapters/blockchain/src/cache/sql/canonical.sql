-- Raw branch identity is never reconstructed from a mutable height-to-hash lookup.
CREATE TABLE IF NOT EXISTS blockchain_canonical_state (
    chain_id INTEGER PRIMARY KEY REFERENCES chain(chain_id) ON DELETE CASCADE,
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    anchor_number BIGINT NOT NULL CHECK (anchor_number >= 0),
    anchor_hash TEXT NOT NULL,
    recovery_in_progress BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Only the canonical owner can create these headers. Legacy height rows are never backfilled.
CREATE TABLE IF NOT EXISTS canonical_block (
    chain_id INTEGER NOT NULL REFERENCES chain(chain_id) ON DELETE CASCADE,
    number BIGINT NOT NULL CHECK (number >= 0),
    hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    observed_timestamp TEXT NOT NULL,
    PRIMARY KEY (chain_id, number)
);

CREATE TABLE IF NOT EXISTS pool_event_identity (
    chain_id INTEGER NOT NULL REFERENCES chain(chain_id) ON DELETE CASCADE,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL CHECK (log_index >= 0),
    event_family TEXT NOT NULL,
    pool_identifier TEXT NOT NULL,
    block BIGINT NOT NULL CHECK (block >= 0),
    transaction_index INTEGER NOT NULL CHECK (transaction_index >= 0),
    observed_block_hash TEXT NOT NULL,
    observed_timestamp TEXT NOT NULL,
    PRIMARY KEY (chain_id, transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_pool_event_identity_branch
    ON pool_event_identity(chain_id, block);

ALTER TABLE pool_event_block ADD COLUMN IF NOT EXISTS hash TEXT;
ALTER TABLE pool_event_block ADD COLUMN IF NOT EXISTS parent_hash TEXT;
ALTER TABLE pool_snapshot ADD COLUMN IF NOT EXISTS observed_block_hash TEXT;
ALTER TABLE pool_snapshot ADD COLUMN IF NOT EXISTS observed_timestamp TEXT;
ALTER TABLE pool_snapshot ADD COLUMN IF NOT EXISTS content_digest TEXT;
ALTER TABLE pool_snapshot ADD COLUMN IF NOT EXISTS validated_content_digest TEXT;
ALTER TABLE pool_snapshot ADD COLUMN IF NOT EXISTS complete BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pool_event_sync ADD COLUMN IF NOT EXISTS block_hash TEXT;
ALTER TABLE pool ADD COLUMN IF NOT EXISTS last_full_sync_block_hash TEXT;
