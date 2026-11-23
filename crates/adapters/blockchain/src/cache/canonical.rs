// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Hash-bound market-data persistence and transactionally fenced canonical recovery.

use std::{collections::BTreeMap, str::FromStr};

use alloy::primitives::B256;
use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::defi::{Block, DexType, PoolIdentifier, data::canonical::CanonicalAnchor};
use sqlx::{AssertSqlSafe, Postgres, Transaction};

use super::database::BlockchainCacheDatabase;

const CHAIN_LOCK_NAMESPACE: i32 = 0x4e54_4443;

const EVENT_TABLES: [(&str, &str); 6] = [
    ("pool_swap_event", "swap"),
    ("pool_liquidity_event", "liquidity"),
    ("pool_collect_event", "collect"),
    ("pool_flash_event", "flash"),
    ("pool_fee_protocol_update_event", "fee_protocol_update"),
    ("pool_fee_protocol_collect_event", "fee_protocol_collect"),
];

/// Persisted recovery state, which remains incomplete across a process restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalRecoveryState {
    /// Last complete anchor, or the trusted ancestor while recovery is in progress.
    pub anchor: CanonicalAnchor,
    /// True until every required pool and event family has reached the same target.
    pub recovering: bool,
}

/// Event families which must be complete before a pool can participate in a ready view.
#[derive(Debug, Clone)]
pub struct CanonicalPoolCoverage {
    /// DEX which owns the pool.
    pub dex: DexType,
    /// Stable native pool identifier.
    pub pool: PoolIdentifier,
    /// The source's required event-family names, including zero-event families.
    pub families: Vec<String>,
}

pub(super) struct PoolEventIdentity<'a> {
    pub pool: PoolIdentifier,
    pub block: u64,
    pub block_hash: Option<&'a str>,
    pub transaction_hash: &'a str,
    pub transaction_index: u32,
    pub log_index: u32,
    pub timestamp: UnixNanos,
}

impl BlockchainCacheDatabase {
    /// Installs the canonical metadata schema without authenticating any legacy market rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the base blockchain schema is absent or migration fails.
    pub async fn ensure_canonical_schema(&self) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::raw_sql(include_str!("sql/canonical.sql"))
            .execute(&mut *transaction)
            .await
            .context("Failed to migrate canonical market-data storage")?;
        transaction.commit().await?;
        Ok(())
    }

    /// Returns the durable canonical owner state, or `None` for unmanaged historical storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable or the retained identity is malformed.
    pub async fn canonical_state(
        &self,
        chain_id: u32,
    ) -> anyhow::Result<Option<CanonicalRecoveryState>> {
        let row = sqlx::query_as::<_, (i64, i64, String, bool)>(
            "SELECT epoch, anchor_number, anchor_hash, recovery_in_progress
             FROM blockchain_canonical_state WHERE chain_id = $1",
        )
        .bind(chain_id_sql(chain_id)?)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(epoch, number, hash, recovering)| {
            Ok(CanonicalRecoveryState {
                anchor: CanonicalAnchor {
                    chain_id,
                    epoch: u64::try_from(epoch).context("Negative canonical epoch")?,
                    number: u64::try_from(number).context("Negative canonical anchor")?,
                    block_hash: parse_hash(&hash)?,
                },
                recovering,
            })
        })
        .transpose()
    }

    /// Returns an immutable header accepted by the canonical owner at this height.
    ///
    /// Historical height-only rows are intentionally excluded. A caller comparing a provider's
    /// branch against retained history must never authenticate that history using the provider.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable or the retained hash is malformed.
    pub async fn canonical_block_hash(
        &self,
        chain_id: u32,
        number: u64,
    ) -> anyhow::Result<Option<B256>> {
        let hash = sqlx::query_scalar::<_, String>(
            "SELECT hash FROM canonical_block WHERE chain_id = $1 AND number = $2",
        )
        .bind(chain_id_sql(chain_id)?)
        .bind(block_sql(number)?)
        .fetch_optional(&self.pool)
        .await?;
        hash.as_deref().map(parse_hash).transpose()
    }

    /// Captures an immutable epoch for an asynchronous producer before it starts external IO.
    ///
    /// Every market write checks this captured epoch under the database's chain lock. Cloning
    /// this handle never refreshes its epoch, so a late writer cannot revive after recovery.
    #[must_use]
    pub fn canonical_writer(&self, anchor: CanonicalAnchor) -> Self {
        Self {
            pool: self.pool.clone(),
            canonical_epoch: Some(anchor),
        }
    }

    pub(super) async fn begin_chain_read(
        &self,
        chain_id: u32,
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        let mut transaction = self.pool.begin().await?;
        lock_chain(&mut transaction, chain_id).await?;
        Ok(transaction)
    }

    pub(super) async fn begin_chain_write(
        &self,
        chain_id: u32,
    ) -> anyhow::Result<Transaction<'static, Postgres>> {
        let mut transaction = self.pool.begin().await?;
        lock_chain(&mut transaction, chain_id).await?;
        let epoch = sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM blockchain_canonical_state WHERE chain_id = $1",
        )
        .bind(chain_id_sql(chain_id)?)
        .fetch_optional(&mut *transaction)
        .await?;

        match (epoch, self.canonical_epoch) {
            (Some(stored), Some(captured)) => anyhow::ensure!(
                captured.chain_id == chain_id && epoch_sql(captured.epoch)? == stored,
                "Canonical market writer is fenced: captured {captured:?}, stored epoch {stored}"
            ),
            (Some(_), None) => anyhow::bail!(
                "Chain {chain_id} requires a writer captured before the producer started"
            ),
            (None, Some(_)) => anyhow::bail!("Canonical chain {chain_id} is not initialized"),
            (None, None) => {}
        }

        // A historical bulk-loader setting on a pooled connection cannot weaken recovery durability
        sqlx::query("SET LOCAL synchronous_commit = ON")
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }

    pub(super) async fn require_unmanaged_progress(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        chain_id: u32,
    ) -> anyhow::Result<()> {
        let managed: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM blockchain_canonical_state WHERE chain_id = $1)",
        )
        .bind(chain_id_sql(chain_id)?)
        .fetch_one(&mut **transaction)
        .await?;
        anyhow::ensure!(
            !managed,
            "Canonical completeness requires complete_canonical_pool_batch, not a height-only update"
        );
        Ok(())
    }

    pub(super) async fn validate_block_identities(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        chain_id: u32,
        blocks: &[Block],
    ) -> anyhow::Result<()> {
        let mut observed = BTreeMap::new();
        let mut numbers = Vec::with_capacity(blocks.len());
        let mut hashes = Vec::with_capacity(blocks.len());
        let mut parents = Vec::with_capacity(blocks.len());
        let mut timestamps = Vec::with_capacity(blocks.len());
        for block in blocks {
            let hash = parse_hash(&block.hash)?;
            if let Some(previous) = observed.insert(block.number, hash) {
                anyhow::ensure!(previous == hash, "Conflicting block hashes in one batch");
            }
            numbers.push(block_sql(block.number)?);
            hashes.push(hash.to_string());
            parents.push(block.parent_hash.clone());
            timestamps.push(block.timestamp.to_string());
        }
        let conflict: bool = sqlx::query_scalar(
            "WITH input AS (
                SELECT * FROM UNNEST($2::BIGINT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])
                    AS observed(number, hash, parent_hash, timestamp)
             ) SELECT EXISTS (
                SELECT 1 FROM input i JOIN block b ON b.chain_id = $1 AND b.number = i.number
                    WHERE b.hash IS NOT NULL AND LOWER(b.hash) <> i.hash
                UNION ALL
                SELECT 1 FROM input i JOIN pool_event_block b ON b.chain_id = $1 AND b.number = i.number
                    WHERE b.hash IS NOT NULL AND LOWER(b.hash) <> i.hash
                UNION ALL
                SELECT 1 FROM input i JOIN canonical_block b ON b.chain_id = $1 AND b.number = i.number
                    WHERE LOWER(b.hash) <> i.hash OR LOWER(b.parent_hash) <> LOWER(i.parent_hash)
                        OR b.observed_timestamp <> i.timestamp
             )",
        )
        .bind(chain_id_sql(chain_id)?)
        .bind(numbers)
        .bind(hashes)
        .bind(parents)
        .bind(timestamps)
        .fetch_one(&mut **transaction)
        .await?;
        anyhow::ensure!(
            !conflict,
            "Observed header conflicts with retained branch identity; recover first"
        );
        Ok(())
    }

    /// Authenticates a header which the canonical source independently confirmed by block hash.
    ///
    /// Historical providers may persist immutable observations using the same captured writer,
    /// but ordinary header insertion never grants common-ancestor or completeness authority.
    /// The source calls this method only after its hash-pinned RPC confirmation has succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the epoch is stale or the anchor, observation, or parent conflicts.
    pub async fn authenticate_canonical_block(
        &self,
        anchor: CanonicalAnchor,
        block: &Block,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.canonical_epoch.is_some_and(
                |lease| lease.chain_id == anchor.chain_id && lease.epoch == anchor.epoch
            ),
            "Canonical authentication requires the captured writer"
        );
        anyhow::ensure!(
            anchor.number == block.number && anchor.block_hash == parse_hash(&block.hash)?,
            "Authenticated header differs from its canonical anchor"
        );
        let mut transaction = self.begin_chain_write(anchor.chain_id).await?;
        verify_stored_hash(
            &mut transaction,
            anchor.chain_id,
            anchor.number,
            anchor.block_hash,
            false,
        )
        .await?;
        record_canonical_header(&mut transaction, anchor.chain_id, block).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn record_pool_event_identities(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        chain_id: u32,
        family: &str,
        events: &[PoolEventIdentity<'_>],
    ) -> anyhow::Result<()> {
        let mut anchors = BTreeMap::new();
        let mut pools = Vec::with_capacity(events.len());
        let mut blocks = Vec::with_capacity(events.len());
        let mut hashes = Vec::with_capacity(events.len());
        let mut transaction_hashes = Vec::with_capacity(events.len());
        let mut transaction_indices = Vec::with_capacity(events.len());
        let mut log_indices = Vec::with_capacity(events.len());
        let mut timestamps = Vec::with_capacity(events.len());
        for event in events {
            let hash = parse_hash(event.block_hash.ok_or_else(|| {
                anyhow::anyhow!(
                    "Pool event lacks its original block hash; refetch before persisting"
                )
            })?)?;

            if let Some(previous) = anchors.insert(event.block, hash) {
                anyhow::ensure!(
                    previous == hash,
                    "Conflicting event block hashes in one batch"
                );
            }
            pools.push(event.pool.to_string());
            blocks.push(block_sql(event.block)?);
            hashes.push(hash.to_string());
            transaction_hashes.push(event.transaction_hash.to_string());
            transaction_indices.push(i32::try_from(event.transaction_index)?);
            log_indices.push(i32::try_from(event.log_index)?);
            timestamps.push(event.timestamp.to_string());
        }

        for (number, hash) in anchors {
            verify_stored_hash(transaction, chain_id, number, hash, true).await?;
        }
        let table = EVENT_TABLES
            .iter()
            .find_map(|(table, name)| (*name == family).then_some(*table))
            .context("Unknown pool event family")?;
        let unauthenticated: bool = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {table} e
             JOIN UNNEST($2::TEXT[], $3::INTEGER[]) input(transaction_hash, log_index)
               ON input.transaction_hash = e.transaction_hash AND input.log_index = e.log_index
             LEFT JOIN pool_event_identity i ON i.chain_id = e.chain_id
               AND i.transaction_hash = e.transaction_hash AND i.log_index = e.log_index
             WHERE e.chain_id = $1 AND i.chain_id IS NULL)"
        )))
        .bind(chain_id_sql(chain_id)?)
        .bind(&transaction_hashes)
        .bind(&log_indices)
        .fetch_one(&mut **transaction)
        .await?;
        anyhow::ensure!(
            !unauthenticated,
            "Legacy pool event requires canonical recovery before replacement"
        );
        let result = sqlx::query(
            "INSERT INTO pool_event_identity (
                chain_id, event_family, pool_identifier, block, observed_block_hash,
                transaction_hash, transaction_index, log_index, observed_timestamp
             ) SELECT $1, $2, * FROM UNNEST(
                $3::TEXT[], $4::BIGINT[], $5::TEXT[], $6::TEXT[],
                $7::INTEGER[], $8::INTEGER[], $9::TEXT[]
             ) ON CONFLICT (chain_id, transaction_hash, log_index)
             DO UPDATE SET observed_block_hash = EXCLUDED.observed_block_hash
             WHERE pool_event_identity.event_family = EXCLUDED.event_family
               AND pool_event_identity.pool_identifier = EXCLUDED.pool_identifier
               AND pool_event_identity.block = EXCLUDED.block
               AND pool_event_identity.transaction_index = EXCLUDED.transaction_index
               AND pool_event_identity.observed_block_hash = EXCLUDED.observed_block_hash
               AND pool_event_identity.observed_timestamp = EXCLUDED.observed_timestamp",
        )
        .bind(chain_id_sql(chain_id)?)
        .bind(family)
        .bind(pools)
        .bind(blocks)
        .bind(hashes)
        .bind(transaction_hashes)
        .bind(transaction_indices)
        .bind(log_indices)
        .bind(timestamps)
        .execute(&mut **transaction)
        .await?;
        anyhow::ensure!(
            result.rows_affected() == u64::try_from(events.len())?,
            "A retained pool event has a different immutable branch identity"
        );
        Ok(())
    }

    /// Fences old writers and atomically rewinds all market state to a verified common ancestor.
    ///
    /// The owner must stop new work before calling this method. The transaction waits for
    /// in-flight writes, increments the durable epoch, clears every invalid suffix and rolls
    /// back every family watermark. Its committed state remains recovering after a crash.
    /// Legacy events without original branch identity and incomplete snapshots are discarded;
    /// their old watermarks cannot authenticate their contents.
    ///
    /// # Errors
    ///
    /// Returns an error if ownership is stale, the ancestor conflicts, arithmetic overflows, or the database fails.
    pub async fn begin_canonical_recovery(
        &self,
        chain_id: u32,
        expected: Option<CanonicalAnchor>,
        ancestor: &Block,
    ) -> anyhow::Result<CanonicalAnchor> {
        if let Some(captured) = self.canonical_epoch {
            anyhow::ensure!(
                captured.chain_id == chain_id
                    && expected.is_some_and(
                        |owner| owner.chain_id == chain_id && owner.epoch == captured.epoch
                    ),
                "A fenced canonical writer cannot acquire a newer recovery epoch"
            );
        }
        let chain = chain_id_sql(chain_id)?;
        let number = block_sql(ancestor.number)?;
        let hash = parse_hash(&ancestor.hash)?;
        let mut transaction = self.pool.begin().await?;
        lock_chain(&mut transaction, chain_id).await?;
        sqlx::query("SET LOCAL synchronous_commit = ON")
            .execute(&mut *transaction)
            .await?;
        let retained = sqlx::query_as::<_, (i64, i64, String)>(
            "SELECT epoch, anchor_number, anchor_hash FROM blockchain_canonical_state
             WHERE chain_id = $1 FOR UPDATE",
        )
        .bind(chain)
        .fetch_optional(&mut *transaction)
        .await?;
        let epoch = match (retained, expected) {
            (None, None) => 1,
            (Some((epoch, block, retained_hash)), Some(expected)) => {
                anyhow::ensure!(
                    expected.chain_id == chain_id
                        && epoch_sql(expected.epoch)? == epoch
                        && block_sql(expected.number)? == block
                        && expected.block_hash == parse_hash(&retained_hash)?,
                    "Canonical recovery owner is stale"
                );
                anyhow::ensure!(
                    ancestor.number <= expected.number,
                    "Ancestor is ahead of the retained anchor"
                );
                let trusted = sqlx::query_scalar::<_, String>(
                    "SELECT hash FROM canonical_block WHERE chain_id = $1 AND number = $2",
                )
                .bind(chain)
                .bind(number)
                .fetch_optional(&mut *transaction)
                .await?
                .context("Recovery ancestor lacks an immutable canonical header")?;
                anyhow::ensure!(
                    parse_hash(&trusted)? == hash,
                    "Recovery ancestor differs from retained history"
                );
                epoch.checked_add(1).context("Canonical epoch overflow")?
            }
            _ => {
                anyhow::bail!("Canonical recovery state changed before the owner acquired its lock")
            }
        };
        verify_stored_hash(&mut transaction, chain_id, ancestor.number, hash, false).await?;

        let mut legacy = false;

        for (table, family) in EVENT_TABLES {
            let predicate = format!(
                "NOT EXISTS (SELECT 1 FROM pool_event_identity i
                 WHERE i.chain_id = e.chain_id AND i.transaction_hash = e.transaction_hash
                   AND i.log_index = e.log_index AND i.event_family = '{family}'
                   AND i.pool_identifier = e.pool_identifier AND i.block = e.block
                   AND i.transaction_index = e.transaction_index)"
            );
            let found: bool = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT EXISTS (SELECT 1 FROM {table} e WHERE e.chain_id = $1 AND {predicate})"
            )))
            .bind(chain)
            .fetch_one(&mut *transaction)
            .await?;
            legacy |= found;
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} e WHERE e.chain_id = $1 AND (e.block > $2 OR {predicate})"
            )))
            .bind(chain)
            .bind(number)
            .execute(&mut *transaction)
            .await?;
        }

        // Explicit child deletion also covers databases predating the cascading snapshot FK
        for table in ["pool_position", "pool_tick"] {
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} c WHERE c.chain_id = $1 AND (
                    c.snapshot_block > $2 OR NOT EXISTS (
                        SELECT 1 FROM pool_snapshot s
                        WHERE s.chain_id = c.chain_id AND s.pool_identifier = c.pool_identifier
                          AND s.block = c.snapshot_block
                          AND s.transaction_index = c.snapshot_transaction_index
                          AND s.log_index = c.snapshot_log_index
                          AND s.complete AND s.observed_block_hash IS NOT NULL))"
            )))
            .bind(chain)
            .bind(number)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "DELETE FROM pool_snapshot WHERE chain_id = $1
             AND (block > $2 OR observed_block_hash IS NULL OR NOT complete)",
        )
        .bind(chain)
        .bind(number)
        .execute(&mut *transaction)
        .await?;

        for (table, column) in [
            ("pool_event_identity", "block"),
            ("canonical_block", "number"),
            ("pool_event_block", "number"),
            ("block", "number"),
        ] {
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE chain_id = $1 AND {column} > $2"
            )))
            .bind(chain)
            .bind(number)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query("DELETE FROM pool WHERE chain_id = $1 AND creation_block > $2")
            .bind(chain)
            .bind(number)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE pool_event_sync SET
                 block_hash = CASE WHEN last_full_sync_block_number > $2 THEN $3 ELSE block_hash END,
                 last_full_sync_block_number = LEAST(last_full_sync_block_number, $2)
             WHERE chain_id = $1",
        )
        .bind(chain)
        .bind(number)
        .bind(hash.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM pool_event_sync WHERE chain_id = $1 AND ($2 OR block_hash IS NULL)",
        )
        .bind(chain)
        .bind(legacy)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE pool SET
                last_full_sync_block_hash = CASE
                    WHEN $4 OR last_full_sync_block_hash IS NULL THEN NULL
                    WHEN last_full_sync_block_number > $2 THEN $3
                    ELSE last_full_sync_block_hash END,
                last_full_sync_block_number = CASE
                    WHEN $4 OR last_full_sync_block_hash IS NULL THEN NULL
                    ELSE LEAST(last_full_sync_block_number, $2) END,
                event_sync_version = CASE WHEN $4 OR last_full_sync_block_hash IS NULL THEN 0 ELSE event_sync_version END,
                initial_tick = NULL, initial_sqrt_price_x96 = NULL
             WHERE chain_id = $1",
        )
        .bind(chain)
        .bind(number)
        .bind(hash.to_string())
        .bind(legacy)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE dex SET last_full_sync_pools_block_number =
                CASE WHEN $3 OR last_full_sync_pools_block_number IS NULL THEN NULL ELSE LEAST(last_full_sync_pools_block_number, $2) END
             WHERE chain_id = $1",
        )
        .bind(chain)
        .bind(number)
        .bind(legacy)
        .execute(&mut *transaction)
        .await?;
        record_canonical_header(&mut transaction, chain_id, ancestor).await?;
        sqlx::query(
            "INSERT INTO pool_event_block (chain_id, number, hash, parent_hash, timestamp)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (chain_id, number)
             DO UPDATE SET hash = EXCLUDED.hash, parent_hash = EXCLUDED.parent_hash, timestamp = EXCLUDED.timestamp",
        )
        .bind(chain)
        .bind(number)
        .bind(hash.to_string())
        .bind(&ancestor.parent_hash)
        .bind(ancestor.timestamp.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO blockchain_canonical_state
                (chain_id, epoch, anchor_number, anchor_hash, recovery_in_progress)
             VALUES ($1, $2, $3, $4, TRUE) ON CONFLICT (chain_id) DO UPDATE SET
                epoch = EXCLUDED.epoch, anchor_number = EXCLUDED.anchor_number,
                anchor_hash = EXCLUDED.anchor_hash, recovery_in_progress = TRUE,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(chain)
        .bind(epoch)
        .bind(number)
        .bind(hash.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(CanonicalAnchor {
            chain_id,
            epoch: u64::try_from(epoch)?,
            number: ancestor.number,
            block_hash: hash,
        })
    }

    /// Advances hash-bound family coverage after the source has persisted one complete block.
    ///
    /// A zero-event family is advanced explicitly. A bootstrap at `previous == target` requires
    /// an atomic complete snapshot independently validated on chain at that exact hash.
    ///
    /// # Errors
    ///
    /// Returns an error if event families or proofs are missing, coverage has a gap, or the epoch is stale.
    pub async fn complete_canonical_pool_batch(
        &self,
        previous: CanonicalAnchor,
        target: CanonicalAnchor,
        coverage: &CanonicalPoolCoverage,
        version: u32,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            previous.chain_id == target.chain_id && previous.epoch == target.epoch,
            "Canonical batch crosses chain or epoch"
        );
        anyhow::ensure!(
            self.canonical_epoch
                .is_some_and(|a| a.chain_id == target.chain_id && a.epoch == target.epoch),
            "Canonical completion requires the captured writer"
        );
        anyhow::ensure!(
            !coverage.families.is_empty(),
            "Canonical pool coverage is empty"
        );
        let mut transaction = self.begin_chain_write(target.chain_id).await?;
        verify_canonical_anchor(&mut transaction, target).await?;
        verify_stored_hash(
            &mut transaction,
            target.chain_id,
            target.number,
            target.block_hash,
            true,
        )
        .await?;
        verify_stored_hash(
            &mut transaction,
            previous.chain_id,
            previous.number,
            previous.block_hash,
            true,
        )
        .await?;
        verify_canonical_anchor(&mut transaction, previous).await?;
        let retained = sqlx::query_as::<_, (i64, String)>(
            "SELECT anchor_number, anchor_hash FROM blockchain_canonical_state WHERE chain_id = $1",
        )
        .bind(chain_id_sql(target.chain_id)?)
        .fetch_one(&mut *transaction)
        .await?;
        anyhow::ensure!(
            block_sql(target.number)? >= retained.0,
            "Canonical pool completion cannot rewind the owner's complete anchor"
        );
        let bootstrap = previous == target;
        if bootstrap {
            anyhow::ensure!(
                block_sql(target.number)? == retained.0
                    && parse_hash(&retained.1)? == target.block_hash,
                "Canonical bootstrap must use the owner's retained anchor"
            );
            let valid: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pool_snapshot
                 WHERE chain_id = $1 AND dex_name = $2 AND pool_identifier = $3
                   AND block = $4 AND observed_block_hash = $5
                   AND complete AND validation_state = 'on_chain'
                   AND validated_content_digest = content_digest)",
            )
            .bind(chain_id_sql(target.chain_id)?)
            .bind(coverage.dex.to_string())
            .bind(coverage.pool.as_ref())
            .bind(block_sql(target.number)?)
            .bind(target.block_hash.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            anyhow::ensure!(
                valid,
                "Canonical bootstrap requires a complete on-chain snapshot at the exact anchor"
            );
        } else {
            anyhow::ensure!(
                previous.number.checked_add(1) == Some(target.number),
                "Canonical batch is not the immediate successor"
            );
            let parents: Vec<Option<String>> = sqlx::query_scalar(
                "SELECT parent_hash FROM canonical_block WHERE chain_id = $1 AND number = $2",
            )
            .bind(chain_id_sql(target.chain_id)?)
            .bind(block_sql(target.number)?)
            .fetch_all(&mut *transaction)
            .await?;
            let mut proven = false;

            for parent in parents.into_iter().flatten() {
                anyhow::ensure!(
                    parse_hash(&parent)? == previous.block_hash,
                    "Canonical batch parent hash differs"
                );
                proven = true;
            }
            anyhow::ensure!(proven, "Canonical batch lacks its observed parent hash");
        }

        for family in &coverage.families {
            let checkpoint = sqlx::query_as::<_, (i64, Option<String>)>(
                "SELECT last_full_sync_block_number, block_hash FROM pool_event_sync
                 WHERE chain_id = $1 AND dex_name = $2 AND pool_identifier = $3 AND event_family = $4",
            )
            .bind(chain_id_sql(target.chain_id)?)
            .bind(coverage.dex.to_string())
            .bind(coverage.pool.as_ref())
            .bind(family)
            .fetch_optional(&mut *transaction)
            .await?;

            if !bootstrap {
                let (number, hash) =
                    checkpoint.context("Canonical event-family coverage is missing")?;
                let hash = hash.context("Canonical event-family coverage has no original hash")?;
                anyhow::ensure!(
                    (number == block_sql(previous.number)?
                        && parse_hash(&hash)? == previous.block_hash)
                        || (number == block_sql(target.number)?
                            && parse_hash(&hash)? == target.block_hash),
                    "Canonical event-family coverage has a gap or different branch"
                );
            }
            sqlx::query(
                "INSERT INTO pool_event_sync (chain_id, dex_name, pool_identifier, event_family,
                    last_full_sync_block_number, block_hash) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (chain_id, dex_name, pool_identifier, event_family) DO UPDATE SET
                    last_full_sync_block_number = EXCLUDED.last_full_sync_block_number,
                    block_hash = EXCLUDED.block_hash",
            )
            .bind(chain_id_sql(target.chain_id)?)
            .bind(coverage.dex.to_string())
            .bind(coverage.pool.as_ref())
            .bind(family)
            .bind(block_sql(target.number)?)
            .bind(target.block_hash.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        let updated = sqlx::query(
            "UPDATE pool SET last_full_sync_block_number = $4, last_full_sync_block_hash = $5,
                event_sync_version = $6 WHERE chain_id = $1 AND dex_name = $2 AND pool_identifier = $3",
        )
        .bind(chain_id_sql(target.chain_id)?)
        .bind(coverage.dex.to_string())
        .bind(coverage.pool.as_ref())
        .bind(block_sql(target.number)?)
        .bind(target.block_hash.to_string())
        .bind(i32::try_from(version)?)
        .execute(&mut *transaction)
        .await?;
        anyhow::ensure!(
            updated.rows_affected() == 1,
            "Canonical pool metadata is missing"
        );
        transaction.commit().await?;
        Ok(())
    }

    /// Marks a common target complete only after every required pool family has reached it.
    ///
    /// # Errors
    ///
    /// Returns an error if coverage is incomplete, writers are stale, or the target is not canonical.
    pub async fn finish_canonical_recovery(
        &self,
        target: CanonicalAnchor,
        required: &[CanonicalPoolCoverage],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !required.is_empty(),
            "Canonical recovery has no required pools"
        );
        anyhow::ensure!(
            self.canonical_epoch
                .is_some_and(|a| a.chain_id == target.chain_id && a.epoch == target.epoch),
            "Canonical recovery requires its captured writer"
        );
        let mut transaction = self.begin_chain_write(target.chain_id).await?;
        verify_canonical_anchor(&mut transaction, target).await?;
        verify_stored_hash(
            &mut transaction,
            target.chain_id,
            target.number,
            target.block_hash,
            true,
        )
        .await?;

        for coverage in required {
            anyhow::ensure!(
                !coverage.families.is_empty(),
                "Canonical pool has no required families"
            );

            for family in &coverage.families {
                let valid: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pool_event_sync
                     WHERE chain_id = $1 AND dex_name = $2 AND pool_identifier = $3
                       AND event_family = $4 AND last_full_sync_block_number = $5 AND block_hash = $6)",
                )
                .bind(chain_id_sql(target.chain_id)?)
                .bind(coverage.dex.to_string())
                .bind(coverage.pool.as_ref())
                .bind(family)
                .bind(block_sql(target.number)?)
                .bind(target.block_hash.to_string())
                .fetch_one(&mut *transaction)
                .await?;
                anyhow::ensure!(
                    valid,
                    "Canonical pool {} family {family} is incomplete",
                    coverage.pool
                );
            }
        }
        let updated = sqlx::query(
            "UPDATE blockchain_canonical_state SET anchor_number = $3, anchor_hash = $4,
                recovery_in_progress = FALSE, updated_at = CURRENT_TIMESTAMP
             WHERE chain_id = $1 AND epoch = $2 AND anchor_number <= $3",
        )
        .bind(chain_id_sql(target.chain_id)?)
        .bind(epoch_sql(target.epoch)?)
        .bind(block_sql(target.number)?)
        .bind(target.block_hash.to_string())
        .execute(&mut *transaction)
        .await?;
        anyhow::ensure!(
            updated.rows_affected() == 1,
            "Canonical completion owner or target is stale"
        );
        transaction.commit().await?;
        Ok(())
    }
}

pub(super) async fn verify_stored_hash(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: u32,
    number: u64,
    expected: B256,
    require_present: bool,
) -> anyhow::Result<()> {
    let hashes: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT hash FROM block WHERE chain_id = $1 AND number = $2
         UNION ALL SELECT hash FROM pool_event_block WHERE chain_id = $1 AND number = $2
         UNION ALL SELECT hash FROM canonical_block WHERE chain_id = $1 AND number = $2",
    )
    .bind(chain_id_sql(chain_id)?)
    .bind(block_sql(number)?)
    .fetch_all(&mut **transaction)
    .await?;
    let mut present = false;

    for hash in hashes.into_iter().flatten() {
        anyhow::ensure!(
            parse_hash(&hash)? == expected,
            "Block {number} has a different retained hash; recover its canonical suffix first"
        );
        present = true;
    }
    anyhow::ensure!(
        !require_present || present,
        "Block {number} has no verified retained header"
    );

    Ok(())
}

async fn verify_canonical_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    anchor: CanonicalAnchor,
) -> anyhow::Result<()> {
    let trusted: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM canonical_block
         WHERE chain_id = $1 AND number = $2 AND hash = $3)",
    )
    .bind(chain_id_sql(anchor.chain_id)?)
    .bind(block_sql(anchor.number)?)
    .bind(anchor.block_hash.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    anyhow::ensure!(
        trusted,
        "Canonical anchor lacks an independently authenticated header"
    );
    Ok(())
}

async fn record_canonical_header(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: u32,
    block: &Block,
) -> anyhow::Result<()> {
    let hash = parse_hash(&block.hash)?;
    let parent = parse_hash(&block.parent_hash)?;
    let chain = chain_id_sql(chain_id)?;
    let number = block_sql(block.number)?;
    let preceding = sqlx::query_scalar::<_, String>(
        "SELECT hash FROM canonical_block WHERE chain_id = $1 AND number = $2 - 1",
    )
    .bind(chain)
    .bind(number)
    .fetch_optional(&mut **transaction)
    .await?;

    if let Some(preceding) = preceding {
        anyhow::ensure!(
            parse_hash(&preceding)? == parent,
            "Canonical header has a different retained parent"
        );
    }

    if let Some(following_number) = number.checked_add(1) {
        let following = sqlx::query_scalar::<_, String>(
            "SELECT parent_hash FROM canonical_block WHERE chain_id = $1 AND number = $2",
        )
        .bind(chain)
        .bind(following_number)
        .fetch_optional(&mut **transaction)
        .await?;

        if let Some(following) = following {
            anyhow::ensure!(
                parse_hash(&following)? == hash,
                "Canonical header has a different retained child"
            );
        }
    }
    let inserted = sqlx::query(
        "INSERT INTO canonical_block (chain_id, number, hash, parent_hash, observed_timestamp)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (chain_id, number)
         DO UPDATE SET hash = EXCLUDED.hash
         WHERE canonical_block.hash = EXCLUDED.hash
           AND canonical_block.parent_hash = EXCLUDED.parent_hash
           AND canonical_block.observed_timestamp = EXCLUDED.observed_timestamp",
    )
    .bind(chain)
    .bind(number)
    .bind(hash.to_string())
    .bind(parent.to_string())
    .bind(block.timestamp.to_string())
    .execute(&mut **transaction)
    .await?;
    anyhow::ensure!(
        inserted.rows_affected() == 1,
        "Canonical header has different immutable content"
    );
    Ok(())
}

pub(super) fn parse_hash(value: &str) -> anyhow::Result<B256> {
    B256::from_str(value).context("Invalid original block hash")
}

fn chain_id_sql(chain_id: u32) -> anyhow::Result<i32> {
    i32::try_from(chain_id).context("Chain ID exceeds PostgreSQL INTEGER")
}

fn block_sql(number: u64) -> anyhow::Result<i64> {
    i64::try_from(number).context("Block number exceeds PostgreSQL BIGINT")
}

fn epoch_sql(epoch: u64) -> anyhow::Result<i64> {
    i64::try_from(epoch).context("Canonical epoch exceeds PostgreSQL BIGINT")
}

async fn lock_chain(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: u32,
) -> anyhow::Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(CHAIN_LOCK_NAMESPACE)
        .bind(chain_id_sql(chain_id)?)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}
