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

//! Complete RPC batches and the single persistent recovery owner for a fixed pool universe.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy::primitives::{Address, B256};
use anyhow::Context;
use nautilus_common::messages::{
    DataEvent,
    defi::{PoolSnapshotResponse, RequestPoolSnapshot},
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_model::{
    defi::{
        Block, PoolIdentifier, PoolProfiler, SharedPool,
        data::{
            CanonicalAnchor, CanonicalPoolBatch, CanonicalPoolBootstrap, CanonicalPoolHydration,
            CanonicalStateInvalidated, DefiData, DexPoolData, block::BLOCK_SCOPED_SNAPSHOT_INDEX,
        },
        rpc::RpcLog,
    },
    identifiers::{ClientId, InstrumentId},
};

use super::core::{BlockchainDataClientCore, POOL_EVENT_SYNC_VERSION, SnapshotValidation};
use crate::{cache::canonical::CanonicalPoolCoverage, exchanges::get_dex_extended};

/// Source progress and outstanding native hydration requests, never a second pool-state cache.
#[derive(Debug)]
pub(super) struct CanonicalFeedState {
    pub(super) anchor: CanonicalAnchor,
    pools: Vec<SharedPool>,
    pending: BTreeMap<InstrumentId, UUID4>,
}

impl BlockchainDataClientCore {
    /// Invalidates old native state and announces the next correlated recovery checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if no trusted ancestor exists within the configured bound, database
    /// fencing fails, or a required native observation cannot be admitted.
    pub(super) async fn begin_canonical_feed(&mut self, client_id: ClientId) -> anyhow::Result<()> {
        let config = self
            .config
            .canonical_pool_data
            .clone()
            .context("Canonical feed is not configured")?;
        let database = self
            .cache
            .database
            .as_ref()
            .context("Canonical database is absent")?
            .clone();
        let stored = database.canonical_state(self.chain.chain_id).await?;
        let expected = stored.as_ref().map(|state| state.anchor);
        let mut ancestor = if let Some(previous) = expected {
            let upper = previous.number;
            let lower = upper.saturating_sub(config.max_reorg_depth);
            let mut common = None;

            for number in (lower..=upper).rev() {
                let Some(observed) = database
                    .canonical_block_hash(self.chain.chain_id, number)
                    .await?
                else {
                    continue;
                };
                let block = self.http_rpc_client.block_header_by_number(number).await?;

                if block.hash.parse::<B256>()? == observed {
                    common = Some(block);
                    break;
                }
            }

            common.context("No trusted common ancestor within the canonical recovery bound")?
        } else {
            let block = self
                .http_rpc_client
                .block_header_by_number(config.start_block)
                .await?;
            anyhow::ensure!(
                block.hash.parse::<B256>()? == config.start_hash,
                "Initial canonical checkpoint hash differs from its trusted configuration"
            );
            block
        };
        ancestor.set_chain(self.chain.name);
        let anchor = database
            .begin_canonical_recovery(self.chain.chain_id, expected, &ancestor)
            .await?;
        self.cache.invalidate_canonical_suffix(ancestor.number);
        self.cache.database = Some(database.canonical_writer(anchor));
        self.cache
            .add_pool_event_blocks_batch(vec![ancestor])
            .await?;

        let mut pools: Vec<_> = config
            .pools
            .iter()
            .map(|instrument| {
                let identifier = PoolIdentifier::Address(instrument.symbol.inner());
                let pool = Arc::clone(self.get_pool(&identifier)?);
                anyhow::ensure!(
                    pool.instrument_id == *instrument,
                    "Canonical pool metadata identity mismatch"
                );
                anyhow::ensure!(
                    pool.creation_block <= anchor.number,
                    "Canonical pool did not exist at the checkpoint"
                );
                Ok(pool)
            })
            .collect::<anyhow::Result<_>>()?;
        pools.sort_by_key(|pool| pool.instrument_id);
        for pool in &pools {
            self.univ3_pool
                .verify_pool_configuration_at_hash(pool.as_ref(), anchor.block_hash)
                .await?;
        }
        let pending: BTreeMap<_, _> = pools
            .iter()
            .map(|pool| (pool.instrument_id, UUID4::new()))
            .collect();
        let requests = pools
            .iter()
            .map(|pool| CanonicalPoolHydration {
                pool: pool.as_ref().clone(),
                request_id: pending[&pool.instrument_id],
            })
            .collect();
        self.canonical = Some(CanonicalFeedState {
            anchor,
            pools,
            pending,
        });
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        self.send_data_checked(DataEvent::DeFi(DefiData::CanonicalInvalidation(
            CanonicalStateInvalidated {
                chain_id: anchor.chain_id,
                epoch: anchor.epoch,
                reason: "Canonical checkpoint hydration started".to_string(),
                ts_init,
            },
        )))?;
        self.send_data_checked(DataEvent::DeFi(DefiData::CanonicalBootstrap(
            CanonicalPoolBootstrap {
                anchor,
                pools: requests,
                client_id,
                ts_init,
            },
        )))
    }

    /// Returns whether every announced native hydration request has a source response.
    pub(super) fn canonical_hydration_complete(&self) -> bool {
        self.canonical
            .as_ref()
            .is_some_and(|state| state.pending.is_empty())
    }

    /// Resolves one exact native request using a hash-pinned and complete snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if source state is inconsistent, topology is incomplete, or IO fails.
    /// A stale request returns `None` and cannot replace the new epoch's state.
    pub(super) async fn canonical_snapshot_response(
        &mut self,
        request: &RequestPoolSnapshot,
    ) -> anyhow::Result<Option<PoolSnapshotResponse>> {
        let Some(state) = &self.canonical else {
            return Ok(None);
        };

        if request.canonical_anchor != Some(state.anchor)
            || state.pending.get(&request.instrument_id) != Some(&request.request_id)
        {
            return Ok(None);
        }

        let anchor = state.anchor;
        let pool = Arc::clone(
            state
                .pools
                .iter()
                .find(|pool| pool.instrument_id == request.instrument_id)
                .context("Canonical request is outside the fixed universe")?,
        );
        let database = self
            .cache
            .database
            .as_ref()
            .context("Canonical database is absent")?
            .clone();
        let stored = database
            .load_latest_pool_snapshot(
                anchor.chain_id,
                &pool.pool_identifier,
                Some(anchor.number),
                true,
            )
            .await?;
        let mut restored = None;

        if let Some(snapshot) = stored
            && snapshot.block_position.number == anchor.number
            && snapshot.block_position.transaction_index == BLOCK_SCOPED_SNAPSHOT_INDEX
            && snapshot.block_position.log_index == BLOCK_SCOPED_SNAPSHOT_INDEX
            && snapshot
                .block_position
                .block_hash
                .as_deref()
                .and_then(|hash| hash.parse::<B256>().ok())
                == Some(anchor.block_hash)
        {
            let validation = database
                .get_pool_snapshot_validation_state_for_snapshot(
                    anchor.chain_id,
                    &pool.pool_identifier,
                    &snapshot,
                )
                .await?;

            if validation.as_deref() == Some(SnapshotValidation::OnChain.as_str()) {
                let mut profiler = PoolProfiler::new(Arc::clone(&pool));
                profiler.restore_from_snapshot(snapshot)?;
                restored = Some(profiler);
            }
        }

        let profiler = match restored {
            Some(profiler) => profiler,
            None => {
                let (profiler, verified) = self
                    .bootstrap_pool_profiler_from_rpc_snapshot(&pool, anchor.number)
                    .await?;
                anyhow::ensure!(verified, "Canonical hydration has no on-chain validation");
                profiler
            }
        };
        let snapshot = profiler.extract_snapshot()?;
        anyhow::ensure!(
            snapshot.block_position.number == anchor.number,
            "Canonical snapshot height mismatch"
        );
        anyhow::ensure!(
            snapshot.block_position.transaction_index == BLOCK_SCOPED_SNAPSHOT_INDEX
                && snapshot.block_position.log_index == BLOCK_SCOPED_SNAPSHOT_INDEX,
            "Canonical snapshot is not block scoped"
        );
        anyhow::ensure!(
            snapshot
                .block_position
                .block_hash
                .as_deref()
                .context("Canonical snapshot has no hash")?
                .parse::<B256>()?
                == anchor.block_hash,
            "Canonical snapshot hash mismatch"
        );
        let tick_spacing =
            i32::try_from(pool.tick_spacing.context("Pool tick spacing is absent")?)?;
        let on_chain_ticks = self
            .univ3_pool
            .initialized_ticks_at_hash(&pool.address, tick_spacing, anchor.block_hash)
            .await?;
        let mut restored_ticks: Vec<_> = snapshot
            .ticks
            .iter()
            .filter(|tick| tick.initialized)
            .map(|tick| tick.value)
            .collect();
        restored_ticks.sort_unstable();
        anyhow::ensure!(
            restored_ticks == on_chain_ticks,
            "Canonical snapshot omits or adds initialized price ranges"
        );
        let observed = self
            .http_rpc_client
            .block_header_by_number(anchor.number)
            .await?;
        anyhow::ensure!(
            observed.hash.parse::<B256>()? == anchor.block_hash,
            "Canonical checkpoint changed during hydration"
        );
        self.cache
            .add_pool_snapshot(&pool.dex.name, &pool.pool_identifier, &snapshot)
            .await?;
        database
            .set_pool_snapshot_validation_state_for_snapshot(
                anchor.chain_id,
                &pool.pool_identifier,
                &snapshot,
                SnapshotValidation::OnChain.as_str(),
            )
            .await?;
        let coverage = pool_coverage(&pool)?;
        database
            .complete_canonical_pool_batch(anchor, anchor, &coverage, POOL_EVENT_SYNC_VERSION)
            .await?;
        self.canonical
            .as_mut()
            .context("Canonical recovery state disappeared")?
            .pending
            .remove(&pool.instrument_id);

        if self.canonical_hydration_complete() {
            let coverage = self
                .canonical
                .as_ref()
                .context("Canonical recovery state disappeared")?
                .pools
                .iter()
                .map(pool_coverage)
                .collect::<anyhow::Result<Vec<_>>>()?;
            database
                .finish_canonical_recovery(anchor, &coverage)
                .await?;
        }

        Ok(Some(
            PoolSnapshotResponse::new(request.request_id, snapshot).with_canonical_anchor(anchor),
        ))
    }

    /// Advances exactly one block after complete source acquisition and persistent validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch is stale, a log response is incomplete or malformed, a durable
    /// write fails, or the native input channel is closed. No partial batch is published.
    pub(super) async fn advance_canonical_feed(
        &mut self,
        head: &Block,
        client_id: ClientId,
    ) -> anyhow::Result<bool> {
        let state = self
            .canonical
            .as_ref()
            .context("Canonical source is not initialized")?;

        if !state.pending.is_empty() {
            return Ok(false);
        }

        let previous = state.anchor;
        let pools = state.pools.clone();
        let observed = self
            .http_rpc_client
            .block_header_by_number(previous.number)
            .await?;

        if observed.hash.parse::<B256>()? != previous.block_hash {
            self.begin_canonical_feed(client_id).await?;
            return Ok(false);
        }

        if previous.number >= head.number {
            return Ok(false);
        }

        let number = previous
            .number
            .checked_add(1)
            .context("Canonical block height overflow")?;
        let mut block = self.http_rpc_client.block_header_by_number(number).await?;
        block.set_chain(self.chain.name);

        if block.parent_hash.parse::<B256>()? != previous.block_hash {
            self.begin_canonical_feed(client_id).await?;
            return Ok(false);
        }

        let anchor = CanonicalAnchor {
            number,
            block_hash: block.hash.parse()?,
            ..previous
        };
        let addresses: Vec<_> = pools.iter().map(|pool| pool.address).collect();
        let mut signatures = BTreeSet::new();

        for pool in &pools {
            let dex = get_dex_extended(self.chain.name, &pool.dex.name)
                .context("Unknown canonical pool DEX")?;

            for family in Self::pool_event_families(dex) {
                signatures.insert(family.signature.parse::<B256>()?);
            }
        }

        let logs = self
            .http_rpc_client
            .get_logs_at_hash(
                anchor.block_hash,
                &addresses,
                &signatures.into_iter().collect::<Vec<_>>(),
            )
            .await?;
        self.cache.cache_block_metadata(&block)?;
        let mut events = Vec::with_capacity(logs.len());

        for log in logs {
            events.push(self.parse_canonical_log(&pools, anchor, &log)?);
        }

        events.sort_by_key(|event| (event.transaction_index(), event.log_index()));
        let batch = CanonicalPoolBatch {
            anchor,
            block,
            pools: pools.iter().map(|pool| pool.instrument_id).collect(),
            events,
            ts_init: get_atomic_clock_realtime().get_time_ns(),
        };
        batch.validate()?;
        let current = self.http_rpc_client.block_header_by_number(number).await?;
        anyhow::ensure!(
            current.hash.parse::<B256>()? == anchor.block_hash,
            "Canonical block changed while its logs were queried"
        );
        self.cache
            .database
            .as_ref()
            .context("Canonical database is absent")?
            .authenticate_canonical_block(anchor, &batch.block)
            .await?;
        self.cache
            .add_pool_event_blocks_batch(vec![batch.block.clone()])
            .await?;
        self.persist_canonical_events(&batch.events).await?;
        let coverage = pools
            .iter()
            .map(pool_coverage)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let database = self
            .cache
            .database
            .as_ref()
            .context("Canonical database is absent")?;

        for pool in &coverage {
            database
                .complete_canonical_pool_batch(previous, anchor, pool, POOL_EVENT_SYNC_VERSION)
                .await?;
        }

        database
            .finish_canonical_recovery(anchor, &coverage)
            .await?;
        self.canonical
            .as_mut()
            .context("Canonical source state disappeared")?
            .anchor = anchor;
        self.send_data_checked(DataEvent::DeFi(DefiData::CanonicalBatch(batch)))?;
        Ok(true)
    }

    fn parse_canonical_log(
        &self,
        pools: &[SharedPool],
        anchor: CanonicalAnchor,
        log: &RpcLog,
    ) -> anyhow::Result<DexPoolData> {
        anyhow::ensure!(!log.removed, "Removed log in canonical complete response");
        anyhow::ensure!(
            log.block_hash
                .as_deref()
                .context("Canonical log has no hash")?
                .parse::<B256>()?
                == anchor.block_hash,
            "Canonical log belongs to another block hash"
        );
        let address = log.address.parse::<Address>()?;
        let pool = pools
            .iter()
            .find(|pool| pool.address == address)
            .context("Log is outside the canonical pool universe")?;
        let dex =
            get_dex_extended(self.chain.name, &pool.dex.name).context("Unknown canonical DEX")?;
        let signature = log
            .topics
            .first()
            .context("Canonical log has no event signature")?
            .parse::<B256>()?;
        let matches = |expected: &str| {
            expected
                .parse::<B256>()
                .map(|expected| expected == signature)
        };

        if matches(&dex.swap_created_event)? {
            return self
                .process_pool_swap_event(&dex.parse_swap_event_rpc(log)?, pool)
                .map(DexPoolData::Swap);
        }

        if matches(&dex.mint_created_event)? {
            return self
                .process_pool_mint_event(&dex.parse_mint_event_rpc(log)?, pool, dex)
                .map(DexPoolData::LiquidityUpdate);
        }

        if matches(&dex.burn_created_event)? {
            return self
                .process_pool_burn_event(&dex.parse_burn_event_rpc(log)?, pool, dex)
                .map(DexPoolData::LiquidityUpdate);
        }

        if matches(&dex.collect_created_event)? {
            return self
                .process_pool_collect_event(&dex.parse_collect_event_rpc(log)?, pool, dex)
                .map(DexPoolData::FeeCollect);
        }

        if dex
            .flash_created_event
            .as_deref()
            .map(matches)
            .transpose()?
            .unwrap_or(false)
        {
            return self
                .process_pool_flash_event(&dex.parse_flash_event_rpc(log)?, pool)
                .map(DexPoolData::Flash);
        }

        if dex
            .fee_protocol_update_event
            .as_deref()
            .map(matches)
            .transpose()?
            .unwrap_or(false)
        {
            return self
                .process_pool_fee_protocol_update_event(
                    &dex.parse_fee_protocol_update_event_rpc(log)?,
                    pool,
                )
                .map(DexPoolData::FeeProtocolUpdate);
        }

        if dex
            .fee_protocol_collect_event
            .as_deref()
            .map(matches)
            .transpose()?
            .unwrap_or(false)
        {
            return self
                .process_pool_fee_protocol_collect_event(
                    &dex.parse_fee_protocol_collect_event_rpc(log)?,
                    pool,
                )
                .map(DexPoolData::FeeProtocolCollect);
        }
        anyhow::bail!("Unexpected event family in an already initialized canonical pool")
    }

    async fn persist_canonical_events(&self, events: &[DexPoolData]) -> anyhow::Result<()> {
        let mut swaps = Vec::new();
        let mut liquidity = Vec::new();
        let mut collects = Vec::new();
        let mut flashes = Vec::new();
        let mut protocol_updates = Vec::new();
        let mut protocol_collects = Vec::new();

        for event in events {
            match event {
                DexPoolData::Swap(event) => swaps.push(event.clone()),
                DexPoolData::LiquidityUpdate(event) => liquidity.push(event.clone()),
                DexPoolData::FeeCollect(event) => collects.push(event.clone()),
                DexPoolData::Flash(event) => flashes.push(event.clone()),
                DexPoolData::FeeProtocolUpdate(event) => protocol_updates.push(event.clone()),
                DexPoolData::FeeProtocolCollect(event) => protocol_collects.push(event.clone()),
            }
        }

        self.cache.add_pool_swaps_batch(&swaps, false).await?;
        self.cache
            .add_pool_liquidity_updates_batch(&liquidity, false)
            .await?;
        self.cache
            .add_pool_fee_collects_batch(&collects, false)
            .await?;
        self.cache.add_pool_flash_batch(&flashes).await?;
        self.cache
            .add_pool_fee_protocol_updates_batch(&protocol_updates)
            .await?;
        self.cache
            .add_pool_fee_protocol_collect_batch(&protocol_collects)
            .await
    }
}

fn pool_coverage(pool: &SharedPool) -> anyhow::Result<CanonicalPoolCoverage> {
    let dex =
        get_dex_extended(pool.chain.name, &pool.dex.name).context("Unknown canonical pool DEX")?;
    Ok(CanonicalPoolCoverage {
        dex: pool.dex.name,
        pool: pool.pool_identifier,
        families: BlockchainDataClientCore::pool_event_families(dex)
            .into_iter()
            .map(|family| family.name.to_string())
            .collect(),
    })
}
