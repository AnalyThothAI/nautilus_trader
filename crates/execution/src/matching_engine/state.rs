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

//! Matching state retained between clean simulation runs with fresh market data.

use indexmap::IndexMap;
use nautilus_model::{
    data::InstrumentClose,
    enums::MarketStatus,
    identifiers::InstrumentId,
    instruments::Instrument,
    types::{Price, Quantity, price::PriceRaw, quantity::QuantityRaw},
};
use serde::{Deserialize, Serialize};

use super::{OrderMatchingEngine, ids_generator::IdsState};

/// Market status, identifiers and consumed displayed liquidity for one instrument.
///
/// A fresh quote with the same displayed size does not replenish consumption. Restoring only
/// orders and positions would therefore create liquidity that the original engine had spent.
/// This state does not include books, bar execution history, open orders, command queues, fill
/// models, or account state. Callers restoring a clean run must restore those separately and
/// wait for fresh market data before placing new orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchingState {
    instrument_id: InstrumentId,
    raw_id: u32,
    ids: IdsState,
    market_status: MarketStatus,
    instrument_close: Option<InstrumentClose>,
    pending_resolution: Option<MarketStatus>,
    expiration_processed: bool,
    option_settlement_failed: bool,
    bids: Vec<ConsumedLevel>,
    asks: Vec<ConsumedLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumedLevel {
    price: Price,
    original_size: Quantity,
    consumed: Quantity,
}

impl MatchingState {
    /// Returns the matching engine's identifier namespace within its simulated venue.
    #[must_use]
    pub const fn raw_id(&self) -> u32 {
        self.raw_id
    }

    /// Returns the instrument whose matching state this describes.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl OrderMatchingEngine {
    /// Returns market status, identifier counters and consumed displayed liquidity.
    #[must_use]
    pub fn matching_state(&self) -> MatchingState {
        let levels = |consumption: &IndexMap<PriceRaw, (QuantityRaw, QuantityRaw)>| {
            consumption
                .iter()
                .map(|(&price, &(original_size, consumed))| ConsumedLevel {
                    price: Price::from_raw(price, self.instrument.price_precision()),
                    original_size: Quantity::from_raw(
                        original_size,
                        self.instrument.size_precision(),
                    ),
                    consumed: Quantity::from_raw(consumed, self.instrument.size_precision()),
                })
                .collect()
        };
        MatchingState {
            instrument_id: self.instrument.id(),
            raw_id: self.raw_id,
            ids: self.ids_generator.state(),
            market_status: self.market_status,
            instrument_close: self.instrument_close,
            pending_resolution: self.pending_resolution,
            expiration_processed: self.expiration_processed,
            option_settlement_failed: self.option_settlement_failed,
            bids: levels(&self.bid_consumption),
            asks: levels(&self.ask_consumption),
        }
    }

    /// Restores matching state before a new engine processes orders.
    ///
    /// This does not restore a book. Subsequent fresh market data and matching use the ordinary
    /// consumption rule, including replenishment when a level's displayed size changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine has active orders, pending fills, or prior consumption,
    /// or if the state's instrument, precision, level uniqueness, or quantities are invalid.
    pub fn restore_matching_state(&mut self, state: MatchingState) -> anyhow::Result<()> {
        anyhow::ensure!(
            state.instrument_id == self.instrument.id(),
            "matching state instrument mismatch"
        );
        anyhow::ensure!(
            self.get_open_orders().is_empty()
                && !self.has_pending_fills()
                && self.bid_consumption.is_empty()
                && self.ask_consumption.is_empty(),
            "matching state requires a new matching engine"
        );
        let levels = |levels: Vec<ConsumedLevel>| -> anyhow::Result<_> {
            let mut result = IndexMap::new();

            for level in levels {
                self.check_price_precision(level.price.precision, "liquidity price")?;
                self.check_size_precision(
                    level.original_size.precision,
                    "liquidity original_size",
                )?;
                self.check_size_precision(level.consumed.precision, "liquidity consumed")?;
                anyhow::ensure!(
                    level.consumed <= level.original_size,
                    "consumed liquidity exceeds original size"
                );
                anyhow::ensure!(
                    result
                        .insert(
                            level.price.raw(),
                            (level.original_size.raw(), level.consumed.raw())
                        )
                        .is_none(),
                    "duplicate liquidity price"
                );
            }
            Ok(result)
        };
        let bids = levels(state.bids)?;
        let asks = levels(state.asks)?;
        self.bid_consumption = bids;
        self.ask_consumption = asks;
        self.raw_id = state.raw_id;
        self.ids_generator.restore_state(state.raw_id, state.ids);
        self.market_status = state.market_status;
        self.instrument_close = state.instrument_close;
        self.pending_resolution = state.pending_resolution;
        self.expiration_processed = state.expiration_processed;
        self.option_settlement_failed = state.option_settlement_failed;
        Ok(())
    }
}
