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

//! DeFi-specific data engine functionality.
//!
//! This module provides DeFi processing methods for the `DataEngine`.
//! All code in this module requires the `defi` feature flag.

use std::{collections::BTreeSet, rc::Rc, sync::Arc};

use alloy_primitives::B256;
use nautilus_common::{
    defi::{self, cache::PoolStateHealth},
    messages::defi::{
        DefiRequestCommand, DefiSubscribeCommand, DefiUnsubscribeCommand, PoolSnapshotResponse,
        RequestPoolSnapshot,
    },
    msgbus::{self, TypedHandler},
    observation::current_data_observation,
};
use nautilus_core::UUID4;
use nautilus_model::{
    defi::{
        Blockchain, DefiData, Pool, PoolProfiler,
        data::{
            DexPoolData,
            block::BlockPosition,
            canonical::{
                CanonicalAnchor, CanonicalPoolBatch, CanonicalPoolBootstrap,
                CanonicalStateInvalidated,
            },
        },
        pool_analysis::{
            PoolSnapshot,
            validation::{validate_canonical_snapshot, validate_pool_profiler},
        },
    },
    identifiers::{ClientId, InstrumentId},
    instruments::{CurrencyPair, InstrumentAny},
};

use crate::engine::{
    DataEngine,
    pool::{
        CanonicalBatchReceipt, PoolApplyOutcome, PoolCollectHandler, PoolFlashHandler,
        PoolLiquidityHandler, PoolSwapHandler, PoolUpdater,
    },
};

/// Extracts the block position tuple from a `DexPoolData` event.
fn get_event_block_position(event: &DexPoolData) -> (u64, u32, u32) {
    match event {
        DexPoolData::Swap(s) => (s.block, s.transaction_index, s.log_index),
        DexPoolData::LiquidityUpdate(u) => (u.block, u.transaction_index, u.log_index),
        DexPoolData::FeeCollect(c) => (c.block, c.transaction_index, c.log_index),
        DexPoolData::FeeProtocolUpdate(u) => (u.block, u.transaction_index, u.log_index),
        DexPoolData::FeeProtocolCollect(c) => (c.block, c.transaction_index, c.log_index),
        DexPoolData::Flash(f) => (f.block, f.transaction_index, f.log_index),
    }
}

/// Converts buffered `DefiData` events to `DexPoolData` and sorts by block position.
fn convert_and_sort_buffered_events(buffered_events: Vec<DefiData>) -> Vec<DexPoolData> {
    let mut events: Vec<DexPoolData> = buffered_events
        .into_iter()
        .filter_map(|event| match event {
            DefiData::PoolSwap(swap) => Some(DexPoolData::Swap(swap)),
            DefiData::PoolLiquidityUpdate(update) => Some(DexPoolData::LiquidityUpdate(update)),
            DefiData::PoolFeeCollect(collect) => Some(DexPoolData::FeeCollect(collect)),
            DefiData::PoolFeeProtocolUpdate(update) => Some(DexPoolData::FeeProtocolUpdate(update)),
            DefiData::PoolFeeProtocolCollect(collect) => {
                Some(DexPoolData::FeeProtocolCollect(collect))
            }
            DefiData::PoolFlash(flash) => Some(DexPoolData::Flash(flash)),
            _ => None,
        })
        .collect();

    events.sort_by(|a, b| {
        let pos_a = get_event_block_position(a);
        let pos_b = get_event_block_position(b);
        pos_a.cmp(&pos_b)
    });

    events
}

/// Compares complete chain payloads without treating receive timestamps as chain identity.
fn same_canonical_batch_payload(left: &CanonicalPoolBatch, right: &CanonicalPoolBatch) -> bool {
    // Block's ordinary equality compares its hash only; a canonical duplicate must preserve
    // all header payload fields used by consumers, including transaction cost inputs
    let a = &left.block;
    let b = &right.block;
    if left.anchor != right.anchor
        || a.chain != b.chain
        || a.hash != b.hash
        || a.number != b.number
        || a.parent_hash != b.parent_hash
        || a.miner != b.miner
        || a.gas_limit != b.gas_limit
        || a.gas_used != b.gas_used
        || a.base_fee_per_gas != b.base_fee_per_gas
        || a.blob_gas_used != b.blob_gas_used
        || a.excess_blob_gas != b.excess_blob_gas
        || a.l1_gas_price != b.l1_gas_price
        || a.l1_gas_used != b.l1_gas_used
        || a.l1_fee_scalar != b.l1_fee_scalar
        || a.timestamp != b.timestamp
        || left.pools != right.pools
        || left.events.len() != right.events.len()
    {
        return false;
    }
    left.events
        .iter()
        .zip(&right.events)
        .all(|(previous, incoming)| {
            let mut normalized = incoming.clone();
            match (previous, &mut normalized) {
                (DexPoolData::Swap(a), DexPoolData::Swap(b)) => b.ts_init = a.ts_init,
                (DexPoolData::LiquidityUpdate(a), DexPoolData::LiquidityUpdate(b)) => {
                    b.ts_init = a.ts_init;
                }
                (DexPoolData::FeeCollect(a), DexPoolData::FeeCollect(b)) => b.ts_init = a.ts_init,
                (DexPoolData::FeeProtocolUpdate(a), DexPoolData::FeeProtocolUpdate(b)) => {
                    b.ts_init = a.ts_init;
                }
                (DexPoolData::FeeProtocolCollect(a), DexPoolData::FeeProtocolCollect(b)) => {
                    b.ts_init = a.ts_init;
                }
                (DexPoolData::Flash(a), DexPoolData::Flash(b)) => b.ts_init = a.ts_init,
                _ => return false,
            }
            previous == &normalized
        })
}

impl DataEngine {
    /// Replaces a pool's state through an explicitly hash-bound snapshot request.
    ///
    /// The old updater, pending response and buffered events are detached before the new request
    /// is dispatched. Canonical pools accept market changes only through complete native batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the epoch is stale, the pool belongs to another chain, or dispatch fails.
    pub fn request_pool_snapshot_at(
        &mut self,
        instrument_id: &InstrumentId,
        client_id: Option<&ClientId>,
        anchor: CanonicalAnchor,
    ) -> anyhow::Result<UUID4> {
        self.request_pool_snapshot_with_identity(instrument_id, client_id, anchor, UUID4::new())
    }

    fn request_pool_snapshot_with_identity(
        &mut self,
        instrument_id: &InstrumentId,
        client_id: Option<&ClientId>,
        anchor: CanonicalAnchor,
        request_id: UUID4,
    ) -> anyhow::Result<UUID4> {
        let request =
            self.prepare_pool_snapshot_with_identity(instrument_id, client_id, anchor, request_id)?;
        if let Err(e) = self.execute_defi_request(DefiRequestCommand::PoolSnapshot(request)) {
            self.fail_pool_hydration(instrument_id, &e.to_string());
            return Err(e);
        }
        Ok(request_id)
    }

    fn prepare_pool_snapshot_with_identity(
        &mut self,
        instrument_id: &InstrumentId,
        client_id: Option<&ClientId>,
        anchor: CanonicalAnchor,
        request_id: UUID4,
    ) -> anyhow::Result<RequestPoolSnapshot> {
        let current_epoch = {
            let cache = self.cache().borrow();
            let epoch = cache.canonical_epoch(anchor.chain_id);
            anyhow::ensure!(
                epoch.is_none_or(|current| anchor.epoch >= current),
                "Obsolete hydration epoch"
            );
            anyhow::ensure!(
                cache
                    .pool(instrument_id)
                    .is_none_or(|pool| pool.chain.chain_id == anchor.chain_id)
                    && cache
                        .pool_state_health(instrument_id)
                        .is_none_or(|health| health.anchor().chain_id == anchor.chain_id),
                "Pool hydration chain mismatch"
            );
            epoch
        };

        if current_epoch != Some(anchor.epoch) {
            let ts_init = self.clock().borrow().timestamp_ns();
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id: anchor.chain_id,
                epoch: anchor.epoch,
                reason: "Canonical hydration started".to_string(),
                ts_init,
            });
        }
        self.release_pool_updater(instrument_id);
        self.canonical_batches.remove(&anchor.chain_id);
        self.cache()
            .borrow_mut()
            .begin_pool_hydration(*instrument_id, anchor, request_id)?;
        let request = RequestPoolSnapshot::new(
            *instrument_id,
            client_id.copied(),
            request_id,
            self.clock().borrow().timestamp_ns(),
            None,
        )
        .with_canonical_anchor(anchor);
        self.pool_snapshot_pending
            .insert(*instrument_id, request.clone());
        Ok(request)
    }

    /// Begins native hydration from a recorded source bootstrap and its exact request identities.
    ///
    /// # Errors
    ///
    /// Returns an error if the epoch, pool universe, metadata, request IDs or dispatch are invalid.
    pub fn process_canonical_bootstrap(
        &mut self,
        bootstrap: &CanonicalPoolBootstrap,
    ) -> anyhow::Result<()> {
        let epoch = self
            .cache()
            .borrow()
            .canonical_epoch(bootstrap.anchor.chain_id);
        anyhow::ensure!(
            epoch.is_none_or(|current| bootstrap.anchor.epoch >= current),
            "Obsolete canonical bootstrap epoch"
        );
        let result = self.apply_canonical_bootstrap(bootstrap);

        if let Err(e) = &result {
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id: bootstrap.anchor.chain_id,
                epoch: bootstrap.anchor.epoch,
                reason: e.to_string(),
                ts_init: bootstrap.ts_init,
            });
        }

        result
    }

    fn apply_canonical_bootstrap(
        &mut self,
        bootstrap: &CanonicalPoolBootstrap,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !bootstrap.pools.is_empty(),
            "Canonical bootstrap has no managed pools"
        );
        anyhow::ensure!(
            bootstrap
                .pools
                .windows(2)
                .all(|pools| pools[0].pool.instrument_id < pools[1].pool.instrument_id),
            "Canonical bootstrap pools must be unique and sorted"
        );
        let requests = bootstrap
            .pools
            .iter()
            .map(|pool| pool.request_id.to_string())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            requests.len() == bootstrap.pools.len(),
            "Canonical bootstrap request IDs must be unique"
        );
        let pools = bootstrap
            .pools
            .iter()
            .map(|entry| entry.pool.instrument_id)
            .collect::<Vec<_>>();
        let current_pools = self
            .cache()
            .borrow()
            .canonical_pool_ids(bootstrap.anchor.chain_id);
        anyhow::ensure!(
            current_pools.is_empty() || current_pools == pools,
            "Canonical bootstrap cannot change the running pool universe"
        );

        {
            let cache = self.cache().borrow();

            for entry in &bootstrap.pools {
                anyhow::ensure!(
                    entry.pool.chain.chain_id == bootstrap.anchor.chain_id
                        && cache
                            .pool(&entry.pool.instrument_id)
                            .is_none_or(|pool| pool.chain.chain_id == bootstrap.anchor.chain_id)
                        && cache
                            .pool_state_health(&entry.pool.instrument_id)
                            .is_none_or(
                                |health| health.anchor().chain_id == bootstrap.anchor.chain_id
                            ),
                    "Canonical bootstrap pool belongs to another chain"
                );
            }
        }

        let needs_invalidation = {
            let cache = self.cache().borrow();
            cache.canonical_epoch(bootstrap.anchor.chain_id) != Some(bootstrap.anchor.epoch)
                || current_pools.iter().any(|id| {
                    !matches!(
                        cache.pool_state_health(id),
                        Some(PoolStateHealth::Dirty { .. })
                    )
                })
        };

        if needs_invalidation {
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id: bootstrap.anchor.chain_id,
                epoch: bootstrap.anchor.epoch,
                reason: "Canonical source bootstrap started".to_string(),
                ts_init: bootstrap.ts_init,
            });
        }

        // Register the entire universe before any metadata publication or request dispatch.
        // A failure must leave every declared pool Dirty, including pools not yet requested.
        let requests = bootstrap
            .pools
            .iter()
            .map(|entry| {
                self.prepare_pool_snapshot_with_identity(
                    &entry.pool.instrument_id,
                    Some(&bootstrap.client_id),
                    bootstrap.anchor,
                    entry.request_id,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        for entry in &bootstrap.pools {
            anyhow::ensure!(
                self.handle_pool(&entry.pool),
                "Canonical pool metadata failed integrity validation"
            );
        }

        for request in requests {
            self.execute_defi_request(DefiRequestCommand::PoolSnapshot(request))?;
        }
        Ok(())
    }

    /// Removes invalid canonical profilers and fences all old snapshot responses.
    pub fn invalidate_canonical_state(&mut self, invalidation: &CanonicalStateInvalidated) {
        if self
            .cache()
            .borrow()
            .canonical_epoch(invalidation.chain_id)
            .is_some_and(|epoch| invalidation.epoch < epoch)
        {
            return;
        }

        let pools = self
            .cache()
            .borrow()
            .canonical_pool_ids(invalidation.chain_id);

        for id in pools {
            self.release_pool_updater(&id);
        }

        self.cache().borrow_mut().invalidate_canonical_chain(
            invalidation.chain_id,
            invalidation.epoch,
            &invalidation.reason,
        );
        self.canonical_batches.remove(&invalidation.chain_id);
        let topic = defi::switchboard::get_defi_canonical_invalidated_topic(invalidation.chain_id);
        msgbus::publish_any(topic, invalidation);
    }

    /// Applies a complete canonical batch before exposing the resulting state to subscribers.
    ///
    /// All cache borrows have ended when the successful batch is published. Subscribers can freeze
    /// an owned view from their cache without reentering the data engine. A duplicate of the last
    /// batch produces a duplicate receipt and no second publication.
    ///
    /// # Errors
    ///
    /// Returns an error if identity, completeness, hydration or any pool event application fails.
    pub fn process_canonical_pool_batch(
        &mut self,
        batch: &CanonicalPoolBatch,
    ) -> anyhow::Result<CanonicalBatchReceipt> {
        let epoch = self.cache().borrow().canonical_epoch(batch.anchor.chain_id);
        anyhow::ensure!(
            epoch.is_none_or(|current| batch.anchor.epoch >= current),
            "Obsolete canonical batch epoch"
        );

        if let Err(e) = batch.validate() {
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id: batch.anchor.chain_id,
                epoch: batch.anchor.epoch,
                reason: e.to_string(),
                ts_init: batch.ts_init,
            });
            return Err(e);
        }

        if self
            .canonical_batches
            .get(&batch.anchor.chain_id)
            .is_some_and(|previous| same_canonical_batch_payload(previous, batch))
        {
            return Ok(CanonicalBatchReceipt {
                anchor: batch.anchor,
                applied: 0,
                duplicates: batch.events.len(),
                pools: batch.pools.clone(),
            });
        }

        let result = self.apply_canonical_pool_batch(batch);

        match result {
            Ok(receipt) => {
                self.canonical_batches
                    .insert(batch.anchor.chain_id, batch.clone());
                let topic =
                    defi::switchboard::get_defi_canonical_batch_topic(batch.anchor.chain_id);
                msgbus::publish_any(topic, batch);
                Ok(receipt)
            }
            Err(e) => {
                self.invalidate_canonical_state(&CanonicalStateInvalidated {
                    chain_id: batch.anchor.chain_id,
                    epoch: batch.anchor.epoch,
                    reason: e.to_string(),
                    ts_init: batch.ts_init,
                });
                Err(e)
            }
        }
    }

    fn apply_canonical_pool_batch(
        &self,
        batch: &CanonicalPoolBatch,
    ) -> anyhow::Result<CanonicalBatchReceipt> {
        let parent = batch.block.parent_hash.parse::<B256>()?;

        {
            let cache = self.cache().borrow();
            anyhow::ensure!(
                cache.canonical_pool_ids(batch.anchor.chain_id) == batch.pools,
                "Canonical batch does not cover the managed pool universe"
            );

            for id in &batch.pools {
                let Some(PoolStateHealth::Ready { complete_through }) = cache.pool_state_health(id)
                else {
                    anyhow::bail!("Canonical pool {id} is not ready");
                };
                anyhow::ensure!(
                    complete_through.chain_id == batch.anchor.chain_id
                        && complete_through.epoch == batch.anchor.epoch
                        && complete_through.number.checked_add(1) == Some(batch.anchor.number)
                        && complete_through.block_hash == parent
                        && cache.pool_profiler(id).is_some()
                        && self.pool_updaters.contains_key(id),
                    "Canonical batch does not extend the complete state of {id}"
                );
            }
        }

        let mut applied = 0;
        let mut duplicates = 0;

        for event in &batch.events {
            let updater = self
                .pool_updaters
                .get(&event.instrument_id())
                .ok_or_else(|| anyhow::anyhow!("Missing canonical pool updater"))?;

            match updater.apply(event) {
                PoolApplyOutcome::Applied => applied += 1,
                PoolApplyOutcome::Duplicate => duplicates += 1,
                PoolApplyOutcome::MissingProfiler => {
                    anyhow::bail!("Canonical profiler disappeared")
                }
                PoolApplyOutcome::Rejected(e) => {
                    anyhow::bail!("Canonical pool event rejected: {e}")
                }
            }
        }

        {
            let cache = self.cache().borrow();

            for id in &batch.pools {
                let profiler = cache.pool_profiler(id).ok_or_else(|| {
                    anyhow::anyhow!("Canonical profiler disappeared before completion")
                })?;
                validate_pool_profiler(profiler)?;
            }
        }

        self.cache()
            .borrow_mut()
            .complete_canonical_pool_batch(&batch.anchor, &batch.pools)?;
        Ok(CanonicalBatchReceipt {
            anchor: batch.anchor,
            applied,
            duplicates,
            pools: batch.pools.clone(),
        })
    }

    /// Returns all blockchains for which blocks subscriptions exist.
    #[must_use]
    pub fn subscribed_blocks(&self) -> Vec<Blockchain> {
        self.collect_subscriptions(|client| &client.subscriptions_blocks)
    }

    /// Returns all instrument IDs for which pool subscriptions exist.
    #[must_use]
    pub fn subscribed_pools(&self) -> Vec<InstrumentId> {
        self.collect_subscriptions(|client| &client.subscriptions_pools)
    }

    /// Returns all instrument IDs for which swap subscriptions exist.
    #[must_use]
    pub fn subscribed_pool_swaps(&self) -> Vec<InstrumentId> {
        self.collect_subscriptions(|client| &client.subscriptions_pool_swaps)
    }

    /// Returns all instrument IDs for which liquidity update subscriptions exist.
    #[must_use]
    pub fn subscribed_pool_liquidity_updates(&self) -> Vec<InstrumentId> {
        self.collect_subscriptions(|client| &client.subscriptions_pool_liquidity_updates)
    }

    /// Returns all instrument IDs for which fee collect subscriptions exist.
    #[must_use]
    pub fn subscribed_pool_fee_collects(&self) -> Vec<InstrumentId> {
        self.collect_subscriptions(|client| &client.subscriptions_pool_fee_collects)
    }

    /// Returns all instrument IDs for which flash loan subscriptions exist.
    #[must_use]
    pub fn subscribed_pool_flash(&self) -> Vec<InstrumentId> {
        self.collect_subscriptions(|client| &client.subscriptions_pool_flash)
    }

    /// Handles a subscribe command, updating internal state and forwarding to the client.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription is invalid (e.g., synthetic instrument for book data),
    /// or if the underlying client operation fails.
    pub fn execute_defi_subscribe(&mut self, cmd: DefiSubscribeCommand) -> anyhow::Result<()> {
        if let Some(client_id) = cmd.client_id()
            && self.is_external_client(*client_id)
        {
            if self.config().debug {
                log::debug!("Skipping defi subscribe for external client {client_id}: {cmd:?}");
            }
            return Ok(());
        }

        let Some(client) = self.get_client(cmd.client_id(), cmd.venue()) else {
            log::error!(
                "Cannot handle command: no client found for client_id={:?}, venue={:?}",
                cmd.client_id(),
                cmd.venue(),
            );
            return Ok(());
        };

        #[rustfmt::skip]
        let pool = match &cmd {
            DefiSubscribeCommand::Pool(cmd) => Some((cmd.instrument_id, cmd.client_id)),
            DefiSubscribeCommand::PoolSwaps(cmd) => Some((cmd.instrument_id, cmd.client_id)),
            DefiSubscribeCommand::PoolLiquidityUpdates(cmd) => Some((cmd.instrument_id, cmd.client_id)),
            DefiSubscribeCommand::PoolFeeCollects(cmd) => Some((cmd.instrument_id, cmd.client_id)),
            DefiSubscribeCommand::PoolFlashEvents(cmd) => Some((cmd.instrument_id, cmd.client_id)),
            DefiSubscribeCommand::Blocks(_) => None, // No pool setup needed for blocks
        };

        log::info!("Forwarding subscription to client {}", client.client_id);
        client.execute_defi_subscribe_with_retained(cmd, true);

        if let Some((instrument_id, client_id)) = pool {
            self.setup_pool_updater(&instrument_id, client_id.as_ref());
        }

        Ok(())
    }

    /// Handles an unsubscribe command, updating internal state and forwarding to the client.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying client operation fails.
    pub fn execute_defi_unsubscribe(&mut self, cmd: &DefiUnsubscribeCommand) -> anyhow::Result<()> {
        if let Some(client_id) = cmd.client_id()
            && self.is_external_client(*client_id)
        {
            if self.config().debug {
                log::debug!("Skipping defi unsubscribe for external client {client_id}: {cmd:?}");
            }
            return Ok(());
        }

        let Some(client) = self.get_client(cmd.client_id(), cmd.venue()) else {
            log::error!(
                "Cannot handle command: no client found for client_id={:?}, venue={:?}",
                cmd.client_id(),
                cmd.venue(),
            );
            return Ok(());
        };

        client.execute_defi_unsubscribe(cmd);

        #[rustfmt::skip]
        let instrument_id = match cmd {
            DefiUnsubscribeCommand::Pool(cmd) => cmd.instrument_id,
            DefiUnsubscribeCommand::PoolSwaps(cmd) => cmd.instrument_id,
            DefiUnsubscribeCommand::PoolLiquidityUpdates(cmd) => cmd.instrument_id,
            DefiUnsubscribeCommand::PoolFeeCollects(cmd) => cmd.instrument_id,
            DefiUnsubscribeCommand::PoolFlashEvents(cmd) => cmd.instrument_id,
            DefiUnsubscribeCommand::Blocks(_) => return Ok(()),
        };

        // Canonical updaters belong to the recovery epoch, independently of raw subscriptions
        let canonical = self
            .cache()
            .borrow()
            .pool_state_health(&instrument_id)
            .is_some();

        if !canonical
            && !self
                .get_clients()
                .iter()
                .any(|client| client.has_defi_pool_demand(&instrument_id))
        {
            self.release_pool_updater(&instrument_id);
        }

        Ok(())
    }

    /// Sends a [`DefiRequestCommand`] to a suitable data client implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if no client is found for the given client ID or venue,
    /// or if the client fails to process the request.
    pub fn execute_defi_request(&mut self, req: DefiRequestCommand) -> anyhow::Result<()> {
        // Skip requests for external clients
        if let Some(cid) = req.client_id()
            && self.is_external_client(*cid)
        {
            if self.config().debug {
                log::debug!("Skipping defi data request for external client {cid}: {req:?}");
            }
            return Ok(());
        }

        if let Some(client) = self.get_client(req.client_id(), req.venue()) {
            let client_id = client.client_id();
            client
                .execute_defi_request(req)
                .map_err(|e| e.context(format!("Data client {client_id} request failed")))
        } else {
            anyhow::bail!(
                "Cannot handle request: no client found for {:?} {:?}",
                req.client_id(),
                req.venue()
            );
        }
    }

    /// Processes DeFi-specific data events.
    pub fn process_defi_data(&mut self, data: DefiData) {
        self.increment_data_count();

        // Raw websocket events are hints for canonical pools. Only the complete batch owner
        // can advance them; routing these through the legacy updater would apply each log twice.
        let is_pool_event = matches!(
            &data,
            DefiData::PoolSwap(_)
                | DefiData::PoolLiquidityUpdate(_)
                | DefiData::PoolFeeCollect(_)
                | DefiData::PoolFeeProtocolUpdate(_)
                | DefiData::PoolFeeProtocolCollect(_)
                | DefiData::PoolFlash(_)
        );

        if is_pool_event
            && self
                .cache()
                .borrow()
                .pool_state_health(&data.instrument_id())
                .is_some()
        {
            return;
        }

        match data {
            DefiData::CanonicalBootstrap(bootstrap) => {
                if let Err(e) = self.process_canonical_bootstrap(&bootstrap) {
                    log::error!("Canonical pool bootstrap rejected: {e}");
                }
            }
            DefiData::CanonicalBatch(batch) => {
                if let Err(e) = self.process_canonical_pool_batch(&batch) {
                    log::error!("Canonical pool batch rejected: {e}");
                }
            }
            DefiData::CanonicalInvalidation(invalidation) => {
                self.invalidate_canonical_state(&invalidation);
            }
            DefiData::Block(block) => {
                // Startup flushes native data before actors start. Retain the consuming identity
                // in the native cache before publishing, without inventing an observation for
                // ordinary historical replay or retaining a strategy-side head mirror.
                if let Some(at) = current_data_observation()
                    && let Err(e) = self
                        .cache()
                        .borrow_mut()
                        .add_observed_block(block.clone(), at)
                {
                    log::error!("Observed block rejected: {e}");
                    return;
                }
                let topic = defi::switchboard::get_defi_blocks_topic(block.chain());
                msgbus::publish_defi_block(topic, &block);
            }
            DefiData::Pool(pool) => {
                self.handle_pool(&pool);
            }
            DefiData::PoolSnapshot(snapshot) => {
                // Legacy replay snapshots have no request identity. A canonical request must
                // receive the explicit request and anchor envelope through the native response path.
                let Some(request) = self.pool_snapshot_pending.get(&snapshot.instrument_id) else {
                    log::warn!(
                        "Received unexpected pool snapshot for {}",
                        snapshot.instrument_id
                    );
                    return;
                };

                if request.canonical_anchor.is_some() {
                    log::warn!(
                        "Discarding unbound replay snapshot for canonical pool {}",
                        snapshot.instrument_id
                    );
                    return;
                }

                self.hydrate_pool_profiler(&snapshot);
            }
            DefiData::PoolSwap(swap) => {
                let instrument_id = swap.instrument_id;
                self.buffer_pool_event(instrument_id, "swap event", || {
                    DefiData::PoolSwap(swap.clone())
                });

                let topic = defi::switchboard::get_defi_pool_swaps_topic(instrument_id);
                msgbus::publish_defi_swap(topic, &swap);
            }
            DefiData::PoolLiquidityUpdate(update) => {
                let instrument_id = update.instrument_id;
                self.buffer_pool_event(instrument_id, "liquidity update event", || {
                    DefiData::PoolLiquidityUpdate(update.clone())
                });

                let topic = defi::switchboard::get_defi_liquidity_topic(instrument_id);
                msgbus::publish_defi_liquidity(topic, &update);
            }
            DefiData::PoolFeeCollect(collect) => {
                let instrument_id = collect.instrument_id;
                self.buffer_pool_event(instrument_id, "fee collect event", || {
                    DefiData::PoolFeeCollect(collect.clone())
                });

                let topic = defi::switchboard::get_defi_collect_topic(instrument_id);
                msgbus::publish_defi_collect(topic, &collect);
            }
            DefiData::PoolFeeProtocolUpdate(update) => {
                let instrument_id = update.instrument_id;
                // A protocol-fee change is profiler infrastructure, not a subscriber data stream:
                // it only needs to keep the profiler's `fee_protocol` consistent for swap and flash
                // fee splitting. Buffer until the snapshot lands, otherwise apply in place.
                if self.pool_snapshot_pending.contains_key(&instrument_id) {
                    log::debug!(
                        "Buffering fee protocol update for {instrument_id} (waiting for snapshot)"
                    );
                    self.pool_event_buffers
                        .entry(instrument_id)
                        .or_default()
                        .push(DefiData::PoolFeeProtocolUpdate(update));
                } else if let Some(profiler) =
                    self.cache().borrow_mut().pool_profiler_mut(&instrument_id)
                    && let Err(e) = profiler.process_fee_protocol_update(&update)
                {
                    log::error!("Failed to process pool fee protocol update: {e}");
                }
            }
            DefiData::PoolFeeProtocolCollect(collect) => {
                let instrument_id = collect.instrument_id;
                // A protocol-fee withdrawal is profiler infrastructure, not a subscriber data
                // stream: it only decrements the profiler's accrued protocol-fee balances. Buffer
                // until the snapshot lands, otherwise apply in place.
                if self.pool_snapshot_pending.contains_key(&instrument_id) {
                    log::debug!(
                        "Buffering fee protocol collect event for {instrument_id} (waiting for snapshot)"
                    );
                    self.pool_event_buffers
                        .entry(instrument_id)
                        .or_default()
                        .push(DefiData::PoolFeeProtocolCollect(collect));
                } else if let Some(profiler) =
                    self.cache().borrow_mut().pool_profiler_mut(&instrument_id)
                    && let Err(e) = profiler.process_fee_protocol_collect(&collect)
                {
                    log::error!("Failed to process pool fee protocol collect event: {e}");
                }
            }
            DefiData::PoolFlash(flash) => {
                let instrument_id = flash.instrument_id;
                self.buffer_pool_event(instrument_id, "flash event", || {
                    DefiData::PoolFlash(flash.clone())
                });

                let topic = defi::switchboard::get_defi_flash_topic(instrument_id);
                msgbus::publish_defi_flash(topic, &flash);
            }
        }
    }

    fn buffer_pool_event(
        &mut self,
        instrument_id: InstrumentId,
        event_name: &str,
        event: impl FnOnce() -> DefiData,
    ) {
        // Subscribers receive events immediately; only hydration needs the buffered copy
        if self.pool_snapshot_pending.contains_key(&instrument_id) {
            log::debug!("Buffering {event_name} for {instrument_id} (waiting for snapshot)");
            self.pool_event_buffers
                .entry(instrument_id)
                .or_default()
                .push(event());
        }
    }

    fn handle_pool(&mut self, pool: &Pool) -> bool {
        let instrument = match CurrencyPair::try_from(pool) {
            Ok(instrument) => InstrumentAny::CurrencyPair(instrument),
            Err(e) => {
                log::error!(
                    "Failed to create instrument for Pool {}: {e}",
                    pool.instrument_id
                );
                return false;
            }
        };

        if !self.cache_instrument(&instrument) {
            return false;
        }

        if let Err(e) = self.cache().borrow_mut().add_pool(pool.clone()) {
            log::error!("Failed to add Pool to cache: {e}");
            return false;
        }

        self.publish_instrument(&instrument);

        let topic = defi::switchboard::get_defi_pool_topic(pool.instrument_id);
        msgbus::publish_defi_pool(topic, pool);
        true
    }

    pub(crate) fn handle_pool_snapshot_response(&mut self, response: &PoolSnapshotResponse) {
        let instrument_id = response.data.instrument_id;

        let Some(request) = self.pool_snapshot_pending.get(&instrument_id) else {
            return;
        };

        if request.request_id != response.correlation_id {
            log::warn!("Discarding stale pool snapshot response for {instrument_id}");
            return;
        }

        if request.canonical_anchor != response.canonical_anchor {
            self.fail_pool_hydration(
                &instrument_id,
                "Snapshot response canonical anchor mismatch",
            );
            return;
        }

        self.hydrate_pool_profiler(&response.data);
    }

    fn hydrate_pool_profiler(&mut self, snapshot: &PoolSnapshot) {
        let instrument_id = snapshot.instrument_id;
        let Some(request) = self.pool_snapshot_pending.get(&instrument_id).cloned() else {
            return;
        };
        log::info!(
            "Received pool snapshot for {instrument_id} at block {} with {} positions and {} ticks",
            snapshot.block_position.number,
            snapshot.positions.len(),
            snapshot.ticks.len()
        );

        // Get pool from cache
        let pool = self
            .cache()
            .borrow()
            .pool(&instrument_id)
            .map(|pool| Arc::new(pool.clone()));

        let Some(pool) = pool else {
            log::error!("Pool {instrument_id} not found in cache when processing snapshot");
            self.fail_pool_hydration(&instrument_id, "Pool snapshot hydration failed");
            return;
        };

        // Defensive: refuse stub snapshots that slipped past the bootstrap-side
        // guards. Installing one would leave Python actors observing an initialized
        // profiler with zero liquidity; better to leave the pool without a profiler
        // so the bad state is visible.
        if snapshot.positions.is_empty()
            && snapshot.ticks.is_empty()
            && snapshot.block_position.number == pool.creation_block
        {
            log::warn!(
                "Refusing empty stub snapshot for {instrument_id} at pool creation block {}; pool will remain without profiler",
                snapshot.block_position.number,
            );
            self.fail_pool_hydration(&instrument_id, "Pool snapshot hydration failed");
            return;
        }

        if let Some(anchor) = &request.canonical_anchor
            && let Err(e) = validate_canonical_snapshot(&pool, snapshot, anchor)
        {
            log::error!("Invalid canonical snapshot for {instrument_id}: {e}");
            self.fail_pool_hydration(&instrument_id, &e.to_string());
            return;
        }

        // Create profiler and restore from snapshot
        let mut profiler = PoolProfiler::new(pool);
        if let Err(e) = profiler.restore_from_snapshot(snapshot.clone()) {
            log::error!("Failed to restore profiler from snapshot for {instrument_id}: {e}");
            self.fail_pool_hydration(&instrument_id, "Pool snapshot hydration failed");
            return;
        }

        log::debug!("Restored pool profiler for {instrument_id} from snapshot");

        // Process buffered events
        let buffered_events = self
            .pool_event_buffers
            .remove(&instrument_id)
            .unwrap_or_default();

        if !buffered_events.is_empty() {
            log::info!(
                "Processing {} buffered events for {instrument_id}",
                buffered_events.len()
            );

            let events_to_apply = convert_and_sort_buffered_events(buffered_events);
            let applied_count = match Self::apply_buffered_events_to_profiler(
                &mut profiler,
                events_to_apply,
                &snapshot.block_position,
                instrument_id,
            ) {
                Ok(count) => count,
                Err(e) => {
                    log::error!("Failed to hydrate pool profiler for {instrument_id}: {e}");
                    self.fail_pool_hydration(&instrument_id, "Pool snapshot hydration failed");
                    return;
                }
            };

            log::info!("Applied {applied_count} buffered events to profiler for {instrument_id}");
        }

        // Add profiler to cache
        let result = if let Some(anchor) = &request.canonical_anchor {
            self.cache()
                .borrow_mut()
                .install_hydrated_pool(profiler, anchor, request.request_id)
        } else {
            self.cache().borrow_mut().add_pool_profiler(profiler)
        };

        if let Err(e) = result {
            log::error!("Failed to add pool profiler to cache for {instrument_id}: {e}");
            self.fail_pool_hydration(&instrument_id, "Pool snapshot hydration failed");
            return;
        }

        // Create updater and subscribe to topics
        self.pool_snapshot_pending.remove(&instrument_id);
        let updater = Rc::new(PoolUpdater::new(&instrument_id, Rc::clone(self.cache())));

        if request.canonical_anchor.is_none() {
            self.subscribe_pool_updater_topics(instrument_id, Rc::clone(&updater));
        }
        self.pool_updaters.insert(instrument_id, updater);

        if let Some(anchor) = request.canonical_anchor {
            let all_ready = {
                let cache = self.cache().borrow();
                let pools = cache.canonical_pool_ids(anchor.chain_id);
                !pools.is_empty() && pools.iter().all(|id| {
                    matches!(
                        cache.pool_state_health(id),
                        Some(PoolStateHealth::Ready { complete_through }) if *complete_through == anchor
                    ) && self.pool_updaters.contains_key(id)
                })
            };

            if all_ready {
                // This is derived during the final qualified native SnapshotResponse dispatch.
                // Actor callbacks may freeze Cache here; no Cache borrow or engine reentry is held.
                let topic = defi::switchboard::get_defi_canonical_ready_topic(anchor.chain_id);
                msgbus::publish_any(topic, &anchor);
            }
        }

        log::info!("Pool profiler setup completed for {instrument_id}, now processing live events");
    }

    fn fail_pool_hydration(&mut self, instrument_id: &InstrumentId, reason: &str) {
        let anchor = self
            .pool_snapshot_pending
            .get(instrument_id)
            .and_then(|request| request.canonical_anchor);

        if let Some(anchor) = anchor {
            let ts_init = self.clock().borrow().timestamp_ns();
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id: anchor.chain_id,
                epoch: anchor.epoch,
                reason: reason.to_string(),
                ts_init,
            });
        } else {
            self.abandon_pool_snapshot(instrument_id);
        }
    }

    fn abandon_pool_snapshot(&mut self, instrument_id: &InstrumentId) {
        self.pool_snapshot_pending.remove(instrument_id);
        self.pool_event_buffers.remove(instrument_id);
    }

    /// Subscribes a pool updater to all relevant pool data topics using typed handlers.
    fn subscribe_pool_updater_topics(&self, instrument_id: InstrumentId, updater: Rc<PoolUpdater>) {
        let priority = Some(self.msgbus_priority());

        // Subscribe swap handler
        let swap_topic = defi::switchboard::get_defi_pool_swaps_topic(instrument_id);
        let swap_handler = TypedHandler(Rc::new(PoolSwapHandler::new(Rc::clone(&updater))));
        msgbus::subscribe_defi_swaps(swap_topic.into(), swap_handler, priority);

        // Subscribe liquidity handler
        let liq_topic = defi::switchboard::get_defi_liquidity_topic(instrument_id);
        let liq_handler = TypedHandler(Rc::new(PoolLiquidityHandler::new(Rc::clone(&updater))));
        msgbus::subscribe_defi_liquidity(liq_topic.into(), liq_handler, priority);

        // Subscribe collect handler
        let collect_topic = defi::switchboard::get_defi_collect_topic(instrument_id);
        let collect_handler = TypedHandler(Rc::new(PoolCollectHandler::new(Rc::clone(&updater))));
        msgbus::subscribe_defi_collects(collect_topic.into(), collect_handler, priority);

        // Subscribe flash handler
        let flash_topic = defi::switchboard::get_defi_flash_topic(instrument_id);
        let flash_handler = TypedHandler(Rc::new(PoolFlashHandler::new(updater)));
        msgbus::subscribe_defi_flash(flash_topic.into(), flash_handler, priority);
    }

    /// Applies buffered events to a pool profiler, filtering to events after the snapshot.
    ///
    /// Returns the count of successfully applied events.
    ///
    /// # Errors
    ///
    /// Returns an error if a post-snapshot event cannot be applied completely.
    fn apply_buffered_events_to_profiler(
        profiler: &mut PoolProfiler,
        events: Vec<DexPoolData>,
        snapshot_block: &BlockPosition,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<usize> {
        let mut applied_count = 0;

        for event in events {
            let event_block = get_event_block_position(&event);

            // Only apply events that occurred after the snapshot
            let is_after_snapshot = event_block.0 > snapshot_block.number
                || (event_block.0 == snapshot_block.number
                    && event_block.1 > snapshot_block.transaction_index)
                || (event_block.0 == snapshot_block.number
                    && event_block.1 == snapshot_block.transaction_index
                    && event_block.2 > snapshot_block.log_index);

            if is_after_snapshot {
                let previous = profiler.last_processed_event.clone();
                profiler.process(&event).map_err(|e| {
                    e.context(format!(
                        "failed to apply buffered event for {instrument_id}"
                    ))
                })?;

                if profiler.last_processed_event != previous {
                    applied_count += 1;
                }
            }
        }

        Ok(applied_count)
    }

    fn setup_pool_updater(&mut self, instrument_id: &InstrumentId, client_id: Option<&ClientId>) {
        // Early return if updater already exists or we are in the middle of setting it up.
        if self.pool_updaters.contains_key(instrument_id)
            || self.pool_snapshot_pending.contains_key(instrument_id)
        {
            log::debug!("Pool updater for {instrument_id} already exists");
            return;
        }

        if self
            .cache()
            .borrow()
            .pool_state_health(instrument_id)
            .is_some()
        {
            log::warn!("Canonical pool {instrument_id} requires an explicit hydration request");
            return;
        }

        log::info!("Setting up pool updater for {instrument_id}");

        // Pool metadata and an initial price do not contain the positions or tick distribution.
        // Only a restored or historically processed profiler can serve an existing legacy replay.
        let has_state = self
            .cache()
            .borrow()
            .pool_profiler(instrument_id)
            .is_some_and(|profiler| {
                profiler.is_initialized && profiler.last_processed_event.is_some()
            });

        if !has_state {
            self.request_pool_snapshot(instrument_id, client_id);
            return;
        }

        // Profiler exists, create updater and subscribe to topics
        let updater = Rc::new(PoolUpdater::new(instrument_id, Rc::clone(self.cache())));

        self.subscribe_pool_updater_topics(*instrument_id, Rc::clone(&updater));
        self.pool_updaters.insert(*instrument_id, updater);

        log::debug!("Created PoolUpdater for instrument ID {instrument_id}");
    }

    fn request_pool_snapshot(
        &mut self,
        instrument_id: &InstrumentId,
        client_id: Option<&ClientId>,
    ) {
        let request_id = UUID4::new();
        let ts_init = self.clock().borrow().timestamp_ns();

        let request = RequestPoolSnapshot::new(
            *instrument_id,
            client_id.copied(),
            request_id,
            ts_init,
            None,
        );

        self.pool_snapshot_pending
            .insert(*instrument_id, request.clone());
        self.pool_event_buffers.entry(*instrument_id).or_default();

        if let Err(e) = self.execute_defi_request(DefiRequestCommand::PoolSnapshot(request)) {
            self.abandon_pool_snapshot(instrument_id);
            log::warn!("Failed to request pool snapshot for {instrument_id}: {e}");
        } else {
            log::debug!("Requested pool snapshot for {instrument_id}");
        }
    }

    fn release_pool_updater(&mut self, instrument_id: &InstrumentId) {
        if let Some(updater) = self.pool_updaters.remove(instrument_id) {
            Self::unsubscribe_pool_updater_topics(*instrument_id, updater);
            log::debug!("Removed PoolUpdater for instrument ID {instrument_id}");
        }

        self.abandon_pool_snapshot(instrument_id);
    }

    pub(crate) fn clear_pool_updaters(&mut self) {
        let chains = self.cache().borrow().canonical_chain_ids();
        let ts_init = self.clock().borrow().timestamp_ns();

        // Reset removes updater ownership, so even complete canonical state loses trust
        for chain_id in chains {
            let epoch = self.cache().borrow().canonical_epoch(chain_id);

            if let Some(epoch) = epoch {
                self.invalidate_canonical_state(&CanonicalStateInvalidated {
                    chain_id,
                    epoch,
                    reason: "Canonical pool updater state was reset".to_string(),
                    ts_init,
                });
            }
        }

        let updaters: Vec<_> = self.pool_updaters.drain().collect();
        for (instrument_id, updater) in updaters {
            Self::unsubscribe_pool_updater_topics(instrument_id, updater);
        }

        self.abandon_pool_snapshots();
        self.canonical_batches.clear();
    }

    pub(crate) fn abandon_pool_snapshots(&mut self) {
        let pending = self
            .pool_snapshot_pending
            .values()
            .filter_map(|request| request.canonical_anchor)
            .map(|anchor| (anchor.chain_id, anchor.epoch))
            .collect::<BTreeSet<_>>();
        let ts_init = self.clock().borrow().timestamp_ns();

        // A closed session cannot complete hydration, but does not undo a complete checkpoint
        for (chain_id, epoch) in pending {
            self.invalidate_canonical_state(&CanonicalStateInvalidated {
                chain_id,
                epoch,
                reason: "Data session ended during canonical hydration".to_string(),
                ts_init,
            });
        }

        self.pool_snapshot_pending.clear();
        self.pool_event_buffers.clear();
    }

    fn unsubscribe_pool_updater_topics(instrument_id: InstrumentId, updater: Rc<PoolUpdater>) {
        let swap_topic = defi::switchboard::get_defi_pool_swaps_topic(instrument_id);
        let swap_handler = TypedHandler(Rc::new(PoolSwapHandler::new(Rc::clone(&updater))));
        msgbus::unsubscribe_defi_swaps(swap_topic.into(), &swap_handler);

        let liq_topic = defi::switchboard::get_defi_liquidity_topic(instrument_id);
        let liq_handler = TypedHandler(Rc::new(PoolLiquidityHandler::new(Rc::clone(&updater))));
        msgbus::unsubscribe_defi_liquidity(liq_topic.into(), &liq_handler);

        let collect_topic = defi::switchboard::get_defi_collect_topic(instrument_id);
        let collect_handler = TypedHandler(Rc::new(PoolCollectHandler::new(Rc::clone(&updater))));
        msgbus::unsubscribe_defi_collects(collect_topic.into(), &collect_handler);

        let flash_topic = defi::switchboard::get_defi_flash_topic(instrument_id);
        let flash_handler = TypedHandler(Rc::new(PoolFlashHandler::new(updater)));
        msgbus::unsubscribe_defi_flash(flash_topic.into(), &flash_handler);
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, sync::Arc};

    use alloy_primitives::{Address, I256, U160, U256};
    use nautilus_common::{cache::Cache, clock::VirtualClock};
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        defi::{
            Chain, DefiData, PoolFeeCollect, PoolFeeProtocolUpdate, PoolFlash, PoolIdentifier,
            PoolLiquidityUpdate, PoolLiquidityUpdateType, PoolSwap,
            chain::chains,
            data::DexPoolData,
            dex::{AmmType, Dex, DexType},
        },
        identifiers::{InstrumentId, Symbol, Venue},
    };
    use rstest::*;

    use super::*;

    #[fixture]
    fn test_instrument_id() -> InstrumentId {
        InstrumentId::new(Symbol::from("ETH/USDC"), Venue::from("UNISWAPV3"))
    }

    #[fixture]
    fn test_chain() -> Arc<Chain> {
        Arc::new(chains::ETHEREUM.clone())
    }

    #[fixture]
    fn test_dex(test_chain: Arc<Chain>) -> Arc<Dex> {
        Arc::new(Dex::new(
            (*test_chain).clone(),
            DexType::UniswapV3,
            "0x1F98431c8aD98523631AE4a59f267346ea31F984",
            12369621,
            AmmType::CLAMM,
            "PoolCreated(address,address,uint24,int24,address)",
            "Swap(address,address,int256,int256,uint160,uint128,int24)",
            "Mint(address,address,int24,int24,uint128,uint256,uint256)",
            "Burn(address,int24,int24,uint128,uint256,uint256)",
            "Collect(address,address,int24,int24,uint128,uint128)",
        ))
    }

    fn create_test_swap(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        block: u64,
        tx_index: u32,
        log_index: u32,
    ) -> PoolSwap {
        PoolSwap::new(
            test_chain,
            test_dex,
            test_instrument_id,
            PoolIdentifier::from_address(Address::ZERO),
            block,
            format!("0x{block:064x}"),
            tx_index,
            log_index,
            UnixNanos::default(),
            UnixNanos::default(),
            Address::ZERO,
            Address::ZERO,
            I256::ZERO,
            I256::ZERO,
            U160::ZERO,
            0,
            0,
        )
    }

    fn create_test_liquidity_update(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        block: u64,
        tx_index: u32,
        log_index: u32,
    ) -> PoolLiquidityUpdate {
        PoolLiquidityUpdate::new(
            test_chain,
            test_dex,
            test_instrument_id,
            PoolIdentifier::from_address(Address::ZERO),
            PoolLiquidityUpdateType::Mint,
            block,
            format!("0x{block:064x}"),
            tx_index,
            log_index,
            None,
            Address::ZERO,
            0,
            U256::ZERO,
            U256::ZERO,
            0,
            0,
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    fn create_test_fee_collect(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        block: u64,
        tx_index: u32,
        log_index: u32,
    ) -> PoolFeeCollect {
        PoolFeeCollect::new(
            test_chain,
            test_dex,
            test_instrument_id,
            PoolIdentifier::from_address(Address::ZERO),
            block,
            format!("0x{block:064x}"),
            tx_index,
            log_index,
            Address::ZERO,
            0,
            0,
            0,
            0,
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    fn create_test_flash(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        block: u64,
        tx_index: u32,
        log_index: u32,
    ) -> PoolFlash {
        PoolFlash::new(
            test_chain,
            test_dex,
            test_instrument_id,
            PoolIdentifier::from_address(Address::ZERO),
            block,
            format!("0x{block:064x}"),
            tx_index,
            log_index,
            UnixNanos::default(),
            UnixNanos::default(),
            Address::ZERO,
            Address::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
        )
    }

    fn create_test_fee_protocol_update(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        block: u64,
        tx_index: u32,
        log_index: u32,
    ) -> PoolFeeProtocolUpdate {
        PoolFeeProtocolUpdate::new(
            test_chain,
            test_dex,
            test_instrument_id,
            PoolIdentifier::from_address(Address::ZERO),
            block,
            format!("0x{block:064x}"),
            tx_index,
            log_index,
            4,
            4,
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    #[rstest]
    #[case::snapshot_pending(true)]
    #[case::no_snapshot_pending(false)]
    fn test_pool_subscriber_events_buffer_only_for_pending_snapshot(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
        #[case] pending: bool,
    ) {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut engine = DataEngine::new(clock, cache, None);
        if pending {
            engine.pool_snapshot_pending.insert(
                test_instrument_id,
                RequestPoolSnapshot::new(
                    test_instrument_id,
                    None,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                ),
            );
        }

        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                104,
                3,
                2,
            )),
            DefiData::PoolLiquidityUpdate(create_test_liquidity_update(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                102,
                1,
                4,
            )),
            DefiData::PoolFeeCollect(create_test_fee_collect(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                103,
                2,
                1,
            )),
            DefiData::PoolFlash(create_test_flash(
                test_instrument_id,
                test_chain,
                test_dex,
                101,
                4,
                3,
            )),
        ];

        for event in &events {
            engine.process_defi_data(event.clone());
        }

        assert_eq!(
            engine.pool_event_buffers.remove(&test_instrument_id),
            pending.then_some(events),
        );
    }

    #[rstest]
    fn test_get_event_block_position_swap(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let swap = create_test_swap(test_instrument_id, test_chain, test_dex, 100, 5, 3);
        let pos = get_event_block_position(&DexPoolData::Swap(swap));
        assert_eq!(pos, (100, 5, 3));
    }

    #[rstest]
    fn test_get_event_block_position_liquidity_update(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let update =
            create_test_liquidity_update(test_instrument_id, test_chain, test_dex, 200, 10, 7);
        let pos = get_event_block_position(&DexPoolData::LiquidityUpdate(update));
        assert_eq!(pos, (200, 10, 7));
    }

    #[rstest]
    fn test_get_event_block_position_fee_collect(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let collect = create_test_fee_collect(test_instrument_id, test_chain, test_dex, 300, 15, 2);
        let pos = get_event_block_position(&DexPoolData::FeeCollect(collect));
        assert_eq!(pos, (300, 15, 2));
    }

    #[rstest]
    fn test_get_event_block_position_flash(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let flash = create_test_flash(test_instrument_id, test_chain, test_dex, 400, 20, 8);
        let pos = get_event_block_position(&DexPoolData::Flash(flash));
        assert_eq!(pos, (400, 20, 8));
    }

    #[rstest]
    fn test_get_event_block_position_fee_protocol_update(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let update =
            create_test_fee_protocol_update(test_instrument_id, test_chain, test_dex, 500, 25, 4);
        let pos = get_event_block_position(&DexPoolData::FeeProtocolUpdate(update));
        assert_eq!(pos, (500, 25, 4));
    }

    #[rstest]
    fn test_convert_and_sort_empty_events() {
        let events = convert_and_sort_buffered_events(vec![]);
        assert!(events.is_empty());
    }

    #[rstest]
    fn test_convert_and_sort_filters_non_pool_events(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                test_chain,
                test_dex,
                100,
                0,
                0,
            )),
            // Block events would be filtered out
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 1);
    }

    #[rstest]
    fn test_convert_and_sort_single_event(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let swap = create_test_swap(test_instrument_id, test_chain, test_dex, 100, 5, 3);
        let events = vec![DefiData::PoolSwap(swap)];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 1);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 5, 3));
    }

    #[rstest]
    fn test_convert_and_sort_already_sorted(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                0,
                0,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                0,
                1,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                test_chain,
                test_dex,
                100,
                1,
                0,
            )),
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 3);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 0, 0));
        assert_eq!(get_event_block_position(&sorted[1]), (100, 0, 1));
        assert_eq!(get_event_block_position(&sorted[2]), (100, 1, 0));
    }

    #[rstest]
    fn test_convert_and_sort_reverse_order(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                2,
                5,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                1,
                3,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                test_chain,
                test_dex,
                100,
                0,
                1,
            )),
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 3);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 0, 1));
        assert_eq!(get_event_block_position(&sorted[1]), (100, 1, 3));
        assert_eq!(get_event_block_position(&sorted[2]), (100, 2, 5));
    }

    #[rstest]
    fn test_convert_and_sort_mixed_blocks(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                102,
                0,
                0,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                5,
                2,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                test_chain,
                test_dex,
                101,
                3,
                1,
            )),
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 3);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 5, 2));
        assert_eq!(get_event_block_position(&sorted[1]), (101, 3, 1));
        assert_eq!(get_event_block_position(&sorted[2]), (102, 0, 0));
    }

    #[rstest]
    fn test_convert_and_sort_mixed_event_types(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                2,
                0,
            )),
            DefiData::PoolLiquidityUpdate(create_test_liquidity_update(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                0,
                0,
            )),
            DefiData::PoolFeeCollect(create_test_fee_collect(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                1,
                0,
            )),
            DefiData::PoolFlash(create_test_flash(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                3,
                0,
            )),
            DefiData::PoolFeeProtocolUpdate(create_test_fee_protocol_update(
                test_instrument_id,
                test_chain,
                test_dex,
                100,
                4,
                0,
            )),
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 5);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 0, 0));
        assert_eq!(get_event_block_position(&sorted[1]), (100, 1, 0));
        assert_eq!(get_event_block_position(&sorted[2]), (100, 2, 0));
        assert_eq!(get_event_block_position(&sorted[3]), (100, 3, 0));
        assert_eq!(get_event_block_position(&sorted[4]), (100, 4, 0));
        assert!(matches!(sorted[4], DexPoolData::FeeProtocolUpdate(_)));
    }

    #[rstest]
    fn test_convert_and_sort_same_block_and_tx_different_log_index(
        test_instrument_id: InstrumentId,
        test_chain: Arc<Chain>,
        test_dex: Arc<Dex>,
    ) {
        let events = vec![
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                5,
                10,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                Arc::clone(&test_chain),
                Arc::clone(&test_dex),
                100,
                5,
                5,
            )),
            DefiData::PoolSwap(create_test_swap(
                test_instrument_id,
                test_chain,
                test_dex,
                100,
                5,
                1,
            )),
        ];
        let sorted = convert_and_sort_buffered_events(events);
        assert_eq!(sorted.len(), 3);
        assert_eq!(get_event_block_position(&sorted[0]), (100, 5, 1));
        assert_eq!(get_event_block_position(&sorted[1]), (100, 5, 5));
        assert_eq!(get_event_block_position(&sorted[2]), (100, 5, 10));
    }
}
