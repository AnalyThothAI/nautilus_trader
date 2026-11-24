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

//! DeFi-specific cache functionality.

use std::{cell::RefCell, fmt::Debug, sync::Arc};

use ahash::{AHashMap, AHashSet};
use nautilus_core::UUID4;
use nautilus_model::{
    defi::{
        Blockchain, Chain, Pool, PoolProfiler, SharedPool, currency,
        data::{
            block::{BLOCK_SCOPED_SNAPSHOT_INDEX, Block, BlockPosition},
            canonical::CanonicalAnchor,
        },
        pool_analysis::{PoolSnapshot, validation::validate_pool_profiler},
    },
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    types::Currency,
};
use ustr::Ustr;

use crate::{cache::Cache, observation::ObservationKey};

/// Identifies a DeFi currency conflict that prevents dependent state from being published.
#[derive(Debug, thiserror::Error)]
#[error("DeFi currency integrity: {0}")]
pub struct IntegrityError(pub String);

/// DeFi-specific cache state.
#[derive(Clone, Debug, Default)]
pub(crate) struct DefiCache {
    currencies: AHashSet<Ustr>,
    execution_checks: RefCell<AHashMap<ClientId, ExecutionCheck>>,
    pools: AHashMap<InstrumentId, Pool>,
    pool_profilers: AHashMap<InstrumentId, PoolProfiler>,
    pool_health: AHashMap<InstrumentId, PoolStateHealth>,
    canonical_epochs: AHashMap<u32, u64>,
    observed_blocks: AHashMap<Blockchain, (Block, ObservationKey)>,
}

/// Trust state of a pool managed by complete canonical batches.
///
/// A profiler's last event is independent of `complete_through`: a complete block can have no
/// events for this pool. Legacy profilers have no canonical health entry and cannot prove readiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolStateHealth {
    /// The old profiler has been removed while a hash-bound snapshot is requested.
    Hydrating {
        /// The requested canonical state.
        anchor: CanonicalAnchor,
        /// The only snapshot response allowed to install the state.
        request_id: UUID4,
    },
    /// All events through this block have been applied successfully.
    Ready {
        /// The last complete canonical block, including zero-event blocks.
        complete_through: CanonicalAnchor,
    },
    /// No profiler is exposed until explicit hydration succeeds.
    Dirty {
        /// The recovery epoch and last known block, which is not a usable state.
        anchor: CanonicalAnchor,
        /// The first failure which invalidated this state.
        reason: String,
    },
}

impl PoolStateHealth {
    /// Returns the identity governing this pool's current hydration or completeness state.
    #[must_use]
    pub fn anchor(&self) -> &CanonicalAnchor {
        match self {
            Self::Hydrating { anchor, .. } | Self::Dirty { anchor, .. } => anchor,
            Self::Ready { complete_through } => complete_through,
        }
    }
}

#[derive(Clone)]
struct ExecutionCheck {
    chain_id: u32,
    check: Arc<dyn Fn(InstrumentId) -> anyhow::Result<()> + Send + Sync>,
}

impl Debug for ExecutionCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ExecutionCheck))
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

/// Checks the live state and authoritative intent stores before replacing a legacy instrument.
///
/// The cache creates this check for one instrument. Backends invoke it only for legacy replacement.
#[derive(Clone)]
pub struct InstrumentMigration {
    check: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
}

impl Debug for InstrumentMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InstrumentMigration")
    }
}

impl InstrumentMigration {
    /// Checks that replacement preserves live state and has no active swap intent.
    ///
    /// # Errors
    ///
    /// Returns an error if position history, nonterminal orders, or unavailable or active execution
    /// state blocks replacement.
    pub fn validate(&self) -> anyhow::Result<()> {
        (self.check)()
    }
}

impl Cache {
    /// Records the native consuming observation of the latest block for its chain.
    ///
    /// The current head's first observation and complete original header survive duplicate inputs.
    /// A different hash replaces the head, including a reorg to a previously observed hash. This
    /// bounded cache is an ingress fact; it does not assert canonical pool readiness.
    ///
    /// # Errors
    ///
    /// Returns an error if the block has no chain or the observation sequence is zero.
    pub fn add_observed_block(&mut self, block: Block, at: ObservationKey) -> anyhow::Result<()> {
        anyhow::ensure!(
            at.sequence > 0,
            "Observed block requires a positive ingress sequence"
        );
        let chain = block
            .chain
            .ok_or_else(|| anyhow::anyhow!("Observed block requires a chain"))?;

        if self
            .defi
            .observed_blocks
            .get(&chain)
            .is_some_and(|(head, _)| head.hash == block.hash)
        {
            return Ok(());
        }
        self.defi.observed_blocks.insert(chain, (block, at));
        Ok(())
    }

    /// Returns the original latest head and its first native consuming observation for a chain.
    ///
    /// Unobserved historical data does not populate this entry. Returns `None` for an unknown
    /// chain or before any observed block has been consumed. Cache reset clears the entry.
    #[must_use]
    pub fn latest_observed_block(&self, chain_id: u32) -> Option<(Block, ObservationKey)> {
        let chain = Chain::from_chain_id(chain_id)?;
        self.defi.observed_blocks.get(&chain.name).cloned()
    }

    /// Registers the authoritative execution-store check for a blockchain client.
    ///
    /// The check covers active swap intents for an instrument, including when the cache uses another store.
    pub fn register_defi_execution_check(
        &self,
        client_id: ClientId,
        chain_id: u32,
        check: impl Fn(InstrumentId) -> anyhow::Result<()> + Send + Sync + 'static,
    ) {
        self.defi.execution_checks.borrow_mut().insert(
            client_id,
            ExecutionCheck {
                chain_id,
                check: Arc::new(check),
            },
        );
    }

    pub(crate) fn check_instrument_migration(
        &self,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Option<InstrumentMigration>> {
        let id = instrument.id();
        let previous = self.instrument(&id);

        let changed = previous.is_some_and(|previous| {
            previous.base_currency() != instrument.base_currency()
                || previous.quote_currency() != instrument.quote_currency()
                || previous.settlement_currency() != instrument.settlement_currency()
        });

        if let Some(previous) = previous
            && changed
            && !currency::instrument_codes(previous)?.is_empty()
        {
            return Err(IntegrityError(
                "Cannot change a token instrument's currency identities".into(),
            )
            .into());
        }

        if currency::instrument_codes(instrument)?.is_empty() {
            return Ok(None);
        }

        let migration = self.instrument_migration(instrument)?;
        if changed
            || (previous.is_none()
                && (self.has_position_history(&id)? || self.has_nonterminal_orders(&id)))
        {
            migration.validate()?;
        }

        Ok(Some(migration))
    }

    pub(crate) fn instrument_migration(
        &self,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<InstrumentMigration> {
        let id = instrument.id();
        let history = self.has_position_history(&id)?;
        let orders = self.has_nonterminal_orders(&id);
        let mut chains = Vec::new();

        for code in currency::instrument_codes(instrument)? {
            let (_, metadata) = currency::lookup(Ustr::from(&code))
                .ok_or_else(|| anyhow::anyhow!("Missing token definition for {code}"))?;
            chains.push(metadata.chain_id);
        }

        chains.sort_unstable();
        chains.dedup();
        let checks = self
            .defi
            .execution_checks
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        Ok(InstrumentMigration {
            check: Arc::new(move || {
                anyhow::ensure!(
                    !history,
                    "Instrument {id} has position history; retain its legacy currencies"
                );
                anyhow::ensure!(!orders, "Instrument {id} has nonterminal orders");

                for chain in &chains {
                    let matching = checks
                        .iter()
                        .filter(|check| check.chain_id == *chain)
                        .collect::<Vec<_>>();
                    anyhow::ensure!(
                        !matching.is_empty(),
                        "No authoritative execution-store check for chain {chain}; cannot replace {id}"
                    );

                    for check in matching {
                        (check.check)(id)?;
                    }
                }

                Ok(())
            }),
        })
    }

    fn has_nonterminal_orders(&self, id: &InstrumentId) -> bool {
        self.orders_refs(None, Some(id), None, None, None)
            .iter()
            .any(|order| !order.is_closed())
    }

    /// Adds a `Pool` to the cache.
    ///
    /// This function does not return errors; the result type matches the other add methods.
    #[allow(
        clippy::missing_errors_doc,
        reason = "this method only inserts into the in-memory cache"
    )]
    pub fn add_pool(&mut self, pool: Pool) -> anyhow::Result<()> {
        log::debug!("Adding `Pool` {}", pool.instrument_id);

        self.defi.pools.insert(pool.instrument_id, pool);
        Ok(())
    }

    /// Adds an ordinary historical `PoolProfiler` to the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool requires the explicit canonical hydration transition.
    pub fn add_pool_profiler(&mut self, pool_profiler: PoolProfiler) -> anyhow::Result<()> {
        let instrument_id = pool_profiler.pool.instrument_id;
        anyhow::ensure!(
            !self.defi.pool_health.contains_key(&instrument_id),
            "Canonical pool {instrument_id} requires explicit hydration"
        );
        log::debug!("Adding `PoolProfiler` {instrument_id}");

        self.defi
            .pool_profilers
            .insert(instrument_id, pool_profiler);
        Ok(())
    }

    /// Returns the canonical trust state, or `None` for an ordinary historical profiler.
    #[must_use]
    pub fn pool_state_health(&self, instrument_id: &InstrumentId) -> Option<&PoolStateHealth> {
        self.defi.pool_health.get(instrument_id)
    }

    /// Returns the latest recovery epoch observed for a chain.
    #[must_use]
    pub fn canonical_epoch(&self, chain_id: u32) -> Option<u64> {
        self.defi.canonical_epochs.get(&chain_id).copied()
    }

    /// Returns sorted chain IDs with an observed canonical recovery epoch.
    #[must_use]
    pub fn canonical_chain_ids(&self) -> Vec<u32> {
        let mut chains = self
            .defi
            .canonical_epochs
            .keys()
            .copied()
            .collect::<Vec<_>>();
        chains.sort_unstable();
        chains
    }

    /// Returns the sorted pool universe managed by canonical batches for a chain.
    #[must_use]
    pub fn canonical_pool_ids(&self, chain_id: u32) -> Vec<InstrumentId> {
        let mut ids = self
            .defi
            .pool_health
            .iter()
            .filter(|(_, health)| health.anchor().chain_id == chain_id)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Freezes the complete canonical pool universe with one snapshot extraction per pool.
    ///
    /// Every pool must have the same proven complete-through anchor. Only the derived checkpoint
    /// position is normalized; the live profiler's last event and original observations are unchanged.
    /// Pool, position and tick order are deterministic for repeatable research artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error if the universe is empty, the epoch is stale, a pool is unready, a profiler
    /// is missing, or native state is structurally inconsistent.
    pub fn canonical_pool_snapshots(
        &self,
        anchor: CanonicalAnchor,
    ) -> anyhow::Result<Vec<(SharedPool, PoolSnapshot)>> {
        let ids = self.canonical_pool_ids(anchor.chain_id);
        anyhow::ensure!(!ids.is_empty(), "Canonical snapshot universe is empty");
        anyhow::ensure!(
            self.canonical_epoch(anchor.chain_id) == Some(anchor.epoch),
            "Canonical snapshot epoch is stale"
        );

        for id in &ids {
            anyhow::ensure!(
                matches!(self.pool_state_health(id),
                    Some(PoolStateHealth::Ready { complete_through }) if *complete_through == anchor)
                    && self.pool_profiler(id).is_some(),
                "Canonical pool {id} is not complete through the requested anchor"
            );
        }
        ids.into_iter()
            .map(|id| {
                let profiler = self
                    .pool_profiler(&id)
                    .ok_or_else(|| anyhow::anyhow!("Missing canonical profiler"))?;
                validate_pool_profiler(profiler)?;
                let mut snapshot = profiler.extract_snapshot()?;
                snapshot.block_position = BlockPosition::new(
                    anchor.number,
                    anchor.block_hash.to_string(),
                    BLOCK_SCOPED_SNAPSHOT_INDEX,
                    BLOCK_SCOPED_SNAPSHOT_INDEX,
                )
                .with_block_hash(Some(anchor.block_hash.to_string()));
                snapshot.positions.sort_by_key(|position| {
                    (position.owner, position.tick_lower, position.tick_upper)
                });
                snapshot.ticks.sort_by_key(|tick| tick.value);
                Ok((Arc::clone(&profiler.pool), snapshot))
            })
            .collect()
    }

    /// Starts a hash-bound hydration and removes the previous profiler.
    ///
    /// # Errors
    ///
    /// Returns an error if the request belongs to an obsolete recovery epoch or another chain.
    pub fn begin_pool_hydration(
        &mut self,
        instrument_id: InstrumentId,
        anchor: CanonicalAnchor,
        request_id: UUID4,
    ) -> anyhow::Result<()> {
        if let Some(current) = self.canonical_epoch(anchor.chain_id) {
            anyhow::ensure!(
                anchor.epoch >= current,
                "Obsolete canonical hydration epoch"
            );
        }

        if let Some(pool) = self.pool(&instrument_id) {
            anyhow::ensure!(
                pool.chain.chain_id == anchor.chain_id,
                "Pool hydration chain mismatch"
            );
        }

        if self.canonical_epoch(anchor.chain_id) != Some(anchor.epoch) {
            self.invalidate_canonical_chain(
                anchor.chain_id,
                anchor.epoch,
                "Canonical hydration started",
            );
        }
        self.defi.pool_profilers.remove(&instrument_id);
        self.defi.pool_health.insert(
            instrument_id,
            PoolStateHealth::Hydrating { anchor, request_id },
        );
        Ok(())
    }

    /// Installs a fully validated snapshot for the currently pending request.
    ///
    /// Snapshot structure, block hash and completeness are validated by the data engine before
    /// this transition. This method refuses responses whose request or recovery identity changed.
    ///
    /// # Errors
    ///
    /// Returns an error if the request no longer owns the pool's hydration.
    pub fn install_hydrated_pool(
        &mut self,
        profiler: PoolProfiler,
        anchor: &CanonicalAnchor,
        request_id: UUID4,
    ) -> anyhow::Result<()> {
        let instrument_id = profiler.pool.instrument_id;
        anyhow::ensure!(
            matches!(self.pool_state_health(&instrument_id),
                Some(PoolStateHealth::Hydrating { anchor: pending, request_id: id })
                if pending == anchor && *id == request_id),
            "Snapshot no longer owns canonical hydration for {instrument_id}"
        );
        self.defi.pool_profilers.insert(instrument_id, profiler);
        self.defi.pool_health.insert(
            instrument_id,
            PoolStateHealth::Ready {
                complete_through: anchor.clone(),
            },
        );
        Ok(())
    }

    /// Advances all pool completeness watermarks after a whole canonical batch succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if any pool is missing, unready, or not immediately before the new block.
    pub fn complete_canonical_pool_batch(
        &mut self,
        anchor: &CanonicalAnchor,
        pools: &[InstrumentId],
    ) -> anyhow::Result<()> {
        for id in pools {
            let Some(PoolStateHealth::Ready { complete_through }) = self.pool_state_health(id)
            else {
                anyhow::bail!("Pool {id} is not ready for a canonical batch");
            };
            anyhow::ensure!(
                complete_through.chain_id == anchor.chain_id
                    && complete_through.epoch == anchor.epoch
                    && complete_through.number.checked_add(1) == Some(anchor.number)
                    && self.defi.pool_profilers.contains_key(id),
                "Noncontiguous canonical batch for {id}"
            );
        }

        for id in pools {
            self.defi.pool_health.insert(
                *id,
                PoolStateHealth::Ready {
                    complete_through: anchor.clone(),
                },
            );
        }

        Ok(())
    }

    /// Fences obsolete work and removes invalid canonical pool state.
    ///
    /// An older epoch cannot invalidate newer state. Repeated failures retain the first reason.
    pub fn invalidate_canonical_chain(&mut self, chain_id: u32, epoch: u64, reason: &str) {
        let previous = self.canonical_epoch(chain_id);

        if previous.is_some_and(|current| epoch < current) {
            return;
        }

        self.defi.canonical_epochs.insert(chain_id, epoch);

        for id in self.canonical_pool_ids(chain_id) {
            let Some(health) = self.defi.pool_health.get_mut(&id) else {
                continue;
            };

            if matches!(health, PoolStateHealth::Dirty { anchor, .. } if anchor.epoch == epoch) {
                continue;
            }

            let mut anchor = health.anchor().clone();
            anchor.epoch = epoch;
            *health = PoolStateHealth::Dirty {
                anchor,
                reason: reason.to_string(),
            };
            self.defi.pool_profilers.remove(&id);
        }
    }

    /// Gets a reference to the pool for the `instrument_id`.
    #[must_use]
    pub fn pool(&self, instrument_id: &InstrumentId) -> Option<&Pool> {
        self.defi.pools.get(instrument_id)
    }

    /// Gets a mutable reference to the pool for the `instrument_id`.
    #[must_use]
    pub fn pool_mut(&mut self, instrument_id: &InstrumentId) -> Option<&mut Pool> {
        self.defi.pools.get_mut(instrument_id)
    }

    /// Returns the instrument IDs of all pools in the cache, optionally filtered by `venue`.
    #[must_use]
    pub fn pool_ids(&self, venue: Option<&Venue>) -> Vec<InstrumentId> {
        match venue {
            Some(v) => self
                .defi
                .pools
                .keys()
                .filter(|id| &id.venue == v)
                .copied()
                .collect(),
            None => self.defi.pools.keys().copied().collect(),
        }
    }

    /// Returns references to all pools in the cache, optionally filtered by `venue`.
    #[must_use]
    pub fn pools(&self, venue: Option<&Venue>) -> Vec<&Pool> {
        match venue {
            Some(v) => self
                .defi
                .pools
                .values()
                .filter(|p| &p.instrument_id.venue == v)
                .collect(),
            None => self.defi.pools.values().collect(),
        }
    }

    /// Gets a reference to the pool profiler for the `instrument_id`.
    #[must_use]
    pub fn pool_profiler(&self, instrument_id: &InstrumentId) -> Option<&PoolProfiler> {
        self.defi.pool_profilers.get(instrument_id)
    }

    /// Gets a mutable reference to the pool profiler for the `instrument_id`.
    #[must_use]
    pub fn pool_profiler_mut(&mut self, instrument_id: &InstrumentId) -> Option<&mut PoolProfiler> {
        self.defi.pool_profilers.get_mut(instrument_id)
    }

    /// Returns the instrument IDs of all pool profilers in the cache, optionally filtered by `venue`.
    #[must_use]
    pub fn pool_profiler_ids(&self, venue: Option<&Venue>) -> Vec<InstrumentId> {
        match venue {
            Some(v) => self
                .defi
                .pool_profilers
                .keys()
                .filter(|id| &id.venue == v)
                .copied()
                .collect(),
            None => self.defi.pool_profilers.keys().copied().collect(),
        }
    }

    /// Returns references to all pool profilers in the cache, optionally filtered by `venue`.
    #[must_use]
    pub fn pool_profilers(&self, venue: Option<&Venue>) -> Vec<&PoolProfiler> {
        match venue {
            Some(v) => self
                .defi
                .pool_profilers
                .values()
                .filter(|p| &p.pool.instrument_id.venue == v)
                .collect(),
            None => self.defi.pool_profilers.values().collect(),
        }
    }
}

impl DefiCache {
    pub(crate) fn restore_currencies(&mut self, currencies: &AHashMap<Ustr, Currency>) {
        self.currencies = currencies
            .values()
            .filter(|currency| currency::lookup(currency.code).is_some())
            .map(|currency| currency.code)
            .collect();
    }

    pub(crate) fn contains_currency(&self, code: &Ustr) -> bool {
        self.currencies.contains(code)
    }

    pub(crate) fn add_currency(&mut self, code: Ustr) {
        self.currencies.insert(code);
    }

    pub(crate) fn clear_currencies(&mut self) {
        self.currencies.clear();
    }

    pub(crate) fn remove_instrument(&mut self, instrument_id: &InstrumentId) {
        self.pools.remove(instrument_id);
        self.pool_profilers.remove(instrument_id);
        self.pool_health.remove(instrument_id);
    }

    pub(crate) fn clear_pools(&mut self) {
        self.pools.clear();
        self.pool_profilers.clear();
        self.pool_health.clear();
        self.canonical_epochs.clear();
        self.observed_blocks.clear();
    }
}
