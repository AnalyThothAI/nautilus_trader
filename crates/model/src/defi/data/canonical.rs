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

//! Hash-bound identities and complete batches for canonical pool state.

use alloy_primitives::B256;
use anyhow::Context;
use nautilus_core::{UUID4, UnixNanos};
use serde::{Deserialize, Serialize};

use super::{Block, DexPoolData};
use crate::{
    defi::{Chain, Pool},
    identifiers::{ClientId, InstrumentId},
};

/// Identifies a complete chain state within one fenced recovery epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalAnchor {
    /// The chain containing the state.
    pub chain_id: u32,
    /// The persistent epoch captured before an asynchronous operation starts.
    pub epoch: u64,
    /// The height through which all required logs have been applied.
    pub number: u64,
    /// The immutable hash observed for that height.
    pub block_hash: B256,
}

/// Binds one pool definition to a snapshot request identity preserved in observation replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalPoolHydration {
    /// The immutable pool metadata used by the native profiler.
    pub pool: Pool,
    /// The identity the data engine uses for its actual snapshot request.
    pub request_id: UUID4,
}

/// Announces a recovery checkpoint before the data engine requests its complete snapshots.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct CanonicalPoolBootstrap {
    /// The hash-bound checkpoint and new recovery epoch.
    pub anchor: CanonicalAnchor,
    /// The complete declared universe and its request identities.
    pub pools: Vec<CanonicalPoolHydration>,
    /// The native client that owns this recovery operation.
    pub client_id: ClientId,
    /// The receive timestamp; observation sequence is assigned by the runner.
    pub ts_init: UnixNanos,
}

/// Contains every required pool log for one block and the complete declared universe.
///
/// An empty event list proves a zero-event block only when the source completed its
/// hash-bound log query. It is not inferred from a new-head notification or a timeout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct CanonicalPoolBatch {
    /// The complete state this batch will produce.
    pub anchor: CanonicalAnchor,
    /// The independently checked header for this exact block hash.
    pub block: Block,
    /// All managed instruments, sorted strictly by instrument ID.
    pub pools: Vec<InstrumentId>,
    /// Required events in strict transaction and log order, without duplicates.
    pub events: Vec<DexPoolData>,
    /// The receive timestamp; observation sequence is assigned by the runner.
    pub ts_init: UnixNanos,
}

impl CanonicalPoolBatch {
    /// Checks the batch identity, universe, ordering, and every observed event hash.
    ///
    /// This validates a source's complete-batch claim. The source remains responsible
    /// for obtaining all required event families, including an empty query response.
    ///
    /// # Errors
    ///
    /// Returns an error if headers are inconsistent, hashes are missing, the universe is invalid,
    /// logs are out of order or duplicated, or an event is outside the declared block or pools.
    pub fn validate(&self) -> anyhow::Result<()> {
        let chain =
            Chain::from_chain_id(self.anchor.chain_id).context("Unknown canonical batch chain")?;
        anyhow::ensure!(
            self.block.chain == Some(chain.name),
            "Canonical batch chain mismatch"
        );
        anyhow::ensure!(
            self.block.number == self.anchor.number,
            "Canonical batch height mismatch"
        );
        anyhow::ensure!(
            self.block
                .hash
                .parse::<B256>()
                .context("Invalid canonical block hash")?
                == self.anchor.block_hash,
            "Canonical batch hash mismatch"
        );
        self.block
            .parent_hash
            .parse::<B256>()
            .context("Invalid canonical parent hash")?;
        anyhow::ensure!(
            !self.pools.is_empty(),
            "Canonical batch has no managed pools"
        );
        anyhow::ensure!(
            self.pools.windows(2).all(|pair| pair[0] < pair[1]),
            "Canonical batch pools must be unique and sorted"
        );

        let mut previous: Option<(u32, u32)> = None;

        for event in &self.events {
            anyhow::ensure!(
                event.chain_id() == self.anchor.chain_id,
                "Canonical event chain mismatch"
            );
            anyhow::ensure!(
                event.block_number() == self.anchor.number,
                "Canonical event height mismatch"
            );
            anyhow::ensure!(
                event
                    .block_hash()
                    .context("Canonical event has no observed block hash")?
                    .parse::<B256>()
                    .context("Invalid canonical event block hash")?
                    == self.anchor.block_hash,
                "Canonical event hash mismatch"
            );
            anyhow::ensure!(
                self.pools.binary_search(&event.instrument_id()).is_ok(),
                "Canonical event is outside the managed universe"
            );
            let position = (event.transaction_index(), event.log_index());

            if let Some(last) = previous {
                anyhow::ensure!(
                    last < position,
                    "Canonical events are duplicated or out of order"
                );
                anyhow::ensure!(
                    last.1 < position.1,
                    "Canonical log indexes must strictly increase"
                );
            }

            previous = Some(position);
        }

        Ok(())
    }
}

/// Invalidates a chain's previous state before recovery work is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct CanonicalStateInvalidated {
    /// The affected chain.
    pub chain_id: u32,
    /// The recovery epoch; older responses cannot restore this state.
    pub epoch: u64,
    /// The sanitized reason the previous state cannot be used.
    pub reason: String,
    /// The receive timestamp; observation sequence is assigned by the runner.
    pub ts_init: UnixNanos,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::defi::Blockchain;

    fn empty_batch() -> CanonicalPoolBatch {
        let block_hash = B256::repeat_byte(1);
        CanonicalPoolBatch {
            anchor: CanonicalAnchor {
                chain_id: 1,
                epoch: 3,
                number: 42,
                block_hash,
            },
            block: Block::new(
                block_hash.to_string(),
                B256::repeat_byte(2).to_string(),
                42,
                "0x0000000000000000000000000000000000000000".into(),
                30_000_000,
                0,
                UnixNanos::from(1_000),
                Some(Blockchain::Ethereum),
            ),
            pools: vec![InstrumentId::from("POOL.ETHEREUM-UNISWAPV3")],
            events: Vec::new(),
            ts_init: UnixNanos::from(2_000),
        }
    }

    #[rstest]
    fn complete_zero_event_batch_is_valid() {
        assert!(empty_batch().validate().is_ok());
    }

    #[rstest]
    fn same_height_different_hash_is_rejected() {
        let mut batch = empty_batch();
        batch.block.hash = B256::repeat_byte(9).to_string();
        assert!(batch.validate().is_err());
    }

    #[rstest]
    fn duplicated_universe_does_not_prove_coverage() {
        let mut batch = empty_batch();
        batch.pools.push(batch.pools[0]);
        assert!(batch.validate().is_err());
    }

    #[rstest]
    fn missing_chain_does_not_authenticate_a_header() {
        let mut batch = empty_batch();
        batch.block.chain = None;
        assert!(batch.validate().is_err());
    }
}
