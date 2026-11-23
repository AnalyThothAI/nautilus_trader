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

//! Applies pool events to the native cache and reports whether state changed.
//! Ordinary data subscriptions use typed message handlers. Canonical batches call the same
//! updater directly and only publish after every pool and completeness watermark is consistent.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{cache::Cache, msgbus::Handler};
use nautilus_model::{
    defi::{
        PoolFeeCollect, PoolFlash, PoolLiquidityUpdate, PoolSwap,
        data::{DexPoolData, canonical::CanonicalAnchor},
    },
    identifiers::InstrumentId,
};
use ustr::Ustr;

/// The outcome of applying one complete canonical pool batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalBatchReceipt {
    /// The state identity covered by this receipt.
    pub anchor: CanonicalAnchor,
    /// Newly applied native events.
    pub applied: usize,
    /// Events already covered by an identical previously completed batch.
    pub duplicates: usize,
    /// The complete managed pool universe.
    pub pools: Vec<InstrumentId>,
}

/// The state transition produced by an individual native pool event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolApplyOutcome {
    /// The profiler accepted a new event.
    Applied,
    /// The last applied position already covers this identical event identity.
    Duplicate,
    /// No profiler is available for this event.
    MissingProfiler,
    /// The event could not safely update the profiler.
    Rejected(String),
}

/// Handles [`PoolSwap`]s and [`PoolLiquidityUpdate`]s for a single AMM pool.
#[derive(Debug)]
pub struct PoolUpdater {
    id: Ustr,
    instrument_id: InstrumentId,
    cache: Rc<RefCell<Cache>>,
}

impl PoolUpdater {
    /// Creates a new [`PoolUpdater`] bound to the given `instrument_id` and `cache`.
    #[must_use]
    pub fn new(instrument_id: &InstrumentId, cache: Rc<RefCell<Cache>>) -> Self {
        Self {
            id: Ustr::from(&format!("{}-{}", stringify!(PoolUpdater), instrument_id)),
            instrument_id: *instrument_id,
            cache,
        }
    }

    /// Returns the handler ID.
    #[must_use]
    pub fn id(&self) -> Ustr {
        self.id
    }

    /// Applies one pool event and distinguishes failure from native duplicate suppression.
    #[must_use]
    pub fn apply(&self, event: &DexPoolData) -> PoolApplyOutcome {
        if event.instrument_id() != self.instrument_id {
            return PoolApplyOutcome::Rejected("Pool event instrument mismatch".to_string());
        }

        let mut cache = self.cache.borrow_mut();
        let Some(profiler) = cache.pool_profiler_mut(&self.instrument_id) else {
            return PoolApplyOutcome::MissingProfiler;
        };

        let (pool_identifier, dex) = match event {
            DexPoolData::Swap(event) => (event.pool_identifier, &event.dex),
            DexPoolData::LiquidityUpdate(event) => (event.pool_identifier, &event.dex),
            DexPoolData::FeeCollect(event) => (event.pool_identifier, &event.dex),
            DexPoolData::FeeProtocolUpdate(event) => (event.pool_identifier, &event.dex),
            DexPoolData::FeeProtocolCollect(event) => (event.pool_identifier, &event.dex),
            DexPoolData::Flash(event) => (event.pool_identifier, &event.dex),
        };

        if pool_identifier != profiler.pool.pool_identifier
            || event.chain_id() != profiler.pool.chain.chain_id
            || dex != &profiler.pool.dex
        {
            return PoolApplyOutcome::Rejected("Pool event metadata mismatch".to_string());
        }

        if let Some(previous) = &profiler.last_processed_event {
            let before = (
                previous.number,
                previous.transaction_index,
                previous.log_index,
            );
            let position = (
                event.block_number(),
                event.transaction_index(),
                event.log_index(),
            );

            if position < before {
                return PoolApplyOutcome::Rejected("Out-of-order pool event".to_string());
            }

            if position == before {
                if previous.transaction_hash != event.transaction_hash()
                    || previous.block_hash.as_deref() != event.block_hash()
                {
                    return PoolApplyOutcome::Rejected(
                        "Conflicting pool event identity".to_string(),
                    );
                }

                return PoolApplyOutcome::Duplicate;
            }
        }

        match profiler.process(event) {
            Ok(()) => PoolApplyOutcome::Applied,
            Err(e) => PoolApplyOutcome::Rejected(e.to_string()),
        }
    }

    /// Handles a pool swap event on the legacy typed subscription path.
    pub fn handle_pool_swap(&self, swap: &PoolSwap) {
        self.report(self.apply(&DexPoolData::Swap(swap.clone())));
    }

    /// Handles a pool liquidity update on the legacy typed subscription path.
    pub fn handle_pool_liquidity_update(&self, update: &PoolLiquidityUpdate) {
        self.report(self.apply(&DexPoolData::LiquidityUpdate(update.clone())));
    }

    /// Handles a fee collection on the legacy typed subscription path.
    pub fn handle_pool_fee_collect(&self, event: &PoolFeeCollect) {
        self.report(self.apply(&DexPoolData::FeeCollect(event.clone())));
    }

    /// Handles a flash event on the legacy typed subscription path.
    pub fn handle_pool_flash(&self, event: &PoolFlash) {
        self.report(self.apply(&DexPoolData::Flash(event.clone())));
    }

    fn report(&self, outcome: PoolApplyOutcome) {
        match outcome {
            PoolApplyOutcome::Applied | PoolApplyOutcome::Duplicate => {}
            PoolApplyOutcome::MissingProfiler => {
                log::error!("No profiler available for {}", self.instrument_id);
            }
            PoolApplyOutcome::Rejected(e) => {
                log::error!(
                    "Failed to process pool event for {}: {e}",
                    self.instrument_id
                );
            }
        }
    }
}

/// Handler for pool swap events that delegates to a [`PoolUpdater`].
#[derive(Debug)]
pub struct PoolSwapHandler {
    id: Ustr,
    updater: Rc<PoolUpdater>,
}

impl PoolSwapHandler {
    /// Creates a new swap handler delegating to the given updater.
    #[must_use]
    pub fn new(updater: Rc<PoolUpdater>) -> Self {
        Self {
            id: Ustr::from(&format!("PoolSwapHandler-{}", updater.id())),
            updater,
        }
    }
}

impl Handler<PoolSwap> for PoolSwapHandler {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, msg: &PoolSwap) {
        self.updater.handle_pool_swap(msg);
    }
}

/// Handler for pool liquidity update events that delegates to a [`PoolUpdater`].
#[derive(Debug)]
pub struct PoolLiquidityHandler {
    id: Ustr,
    updater: Rc<PoolUpdater>,
}

impl PoolLiquidityHandler {
    /// Creates a new liquidity handler delegating to the given updater.
    #[must_use]
    pub fn new(updater: Rc<PoolUpdater>) -> Self {
        Self {
            id: Ustr::from(&format!("PoolLiquidityHandler-{}", updater.id())),
            updater,
        }
    }
}

impl Handler<PoolLiquidityUpdate> for PoolLiquidityHandler {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, msg: &PoolLiquidityUpdate) {
        self.updater.handle_pool_liquidity_update(msg);
    }
}

/// Handler for pool fee collect events that delegates to a [`PoolUpdater`].
#[derive(Debug)]
pub struct PoolCollectHandler {
    id: Ustr,
    updater: Rc<PoolUpdater>,
}

impl PoolCollectHandler {
    /// Creates a new collect handler delegating to the given updater.
    #[must_use]
    pub fn new(updater: Rc<PoolUpdater>) -> Self {
        Self {
            id: Ustr::from(&format!("PoolCollectHandler-{}", updater.id())),
            updater,
        }
    }
}

impl Handler<PoolFeeCollect> for PoolCollectHandler {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, msg: &PoolFeeCollect) {
        self.updater.handle_pool_fee_collect(msg);
    }
}

/// Handler for pool flash events that delegates to a [`PoolUpdater`].
#[derive(Debug)]
pub struct PoolFlashHandler {
    id: Ustr,
    updater: Rc<PoolUpdater>,
}

impl PoolFlashHandler {
    /// Creates a new flash handler delegating to the given updater.
    #[must_use]
    pub fn new(updater: Rc<PoolUpdater>) -> Self {
        Self {
            id: Ustr::from(&format!("PoolFlashHandler-{}", updater.id())),
            updater,
        }
    }
}

impl Handler<PoolFlash> for PoolFlashHandler {
    fn id(&self) -> Ustr {
        self.id
    }

    fn handle(&self, msg: &PoolFlash) {
        self.updater.handle_pool_flash(msg);
    }
}
