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

//! Shared structural validation for canonical Uniswap V3 hydration and frozen native views.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::B256;

use super::{PoolProfiler, PoolSnapshot, position::PoolPosition, snapshot::PoolState};
use crate::defi::{
    Pool,
    data::{block::BLOCK_SCOPED_SNAPSHOT_INDEX, canonical::CanonicalAnchor},
    tick_map::{
        liquidity_math::{tick_spacing_to_max_liquidity_per_tick, try_liquidity_math_add},
        tick::PoolTick,
        tick_math::{
            MAX_SQRT_RATIO, MIN_SQRT_RATIO, get_sqrt_ratio_at_tick, get_tick_at_sqrt_ratio,
        },
    },
};

/// Validates the current native profiler using live liquidity from its tick map.
///
/// The last pool event position remains distinct from a canonical complete-through watermark;
/// callers must validate that watermark separately when freezing a multi-pool view.
///
/// # Errors
///
/// Returns an error if the profiler is uninitialized or its current pool structure is inconsistent.
pub fn validate_pool_profiler(profiler: &PoolProfiler) -> anyhow::Result<()> {
    anyhow::ensure!(
        profiler.is_initialized,
        "Canonical profiler is not initialized"
    );
    // PoolProfiler maintains live liquidity in TickMap; extract_snapshot uses this same projection
    let mut state = profiler.state.clone();
    state.liquidity = profiler.tick_map.liquidity;
    validate_pool_state(
        &profiler.pool,
        &state,
        profiler.get_all_positions(),
        profiler.tick_map.get_all_ticks().values(),
    )
}

/// Validates the identity and structure of a block-scoped Uniswap V3 hydration snapshot.
///
/// A trusted source must independently establish complete, hash-pinned acquisition. This check
/// validates the supplied state; it does not turn an incomplete provider response into proof.
///
/// # Errors
///
/// Returns an error if the chain, instrument, hash, block position, or pool structure is inconsistent.
pub fn validate_canonical_snapshot(
    pool: &Pool,
    snapshot: &PoolSnapshot,
    anchor: &CanonicalAnchor,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        pool.instrument_id == snapshot.instrument_id,
        "Snapshot instrument mismatch"
    );
    anyhow::ensure!(
        pool.chain.chain_id == anchor.chain_id,
        "Snapshot chain mismatch"
    );
    anyhow::ensure!(
        snapshot.block_position.number == anchor.number,
        "Snapshot height mismatch"
    );
    let hash = snapshot
        .block_position
        .block_hash
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Snapshot has no observed block hash"))?
        .parse::<B256>()?;
    anyhow::ensure!(hash == anchor.block_hash, "Snapshot block hash mismatch");
    anyhow::ensure!(
        snapshot.block_position.transaction_index == BLOCK_SCOPED_SNAPSHOT_INDEX
            && snapshot.block_position.log_index == BLOCK_SCOPED_SNAPSHOT_INDEX,
        "Canonical snapshot is not complete through its block"
    );
    validate_pool_state(
        pool,
        &snapshot.state,
        snapshot.positions.iter(),
        snapshot.ticks.iter(),
    )
}

/// Validates Uniswap V3 price, position, tick and active-liquidity invariants without cloning state.
///
/// # Errors
///
/// Returns an error if bounds are invalid, records are duplicated or missing, arithmetic overflows,
/// active or boundary liquidity is inconsistent, or protocol fee configuration is unsupported.
pub fn validate_pool_state<'a>(
    pool: &Pool,
    state: &PoolState,
    positions: impl IntoIterator<Item = &'a PoolPosition>,
    ticks: impl IntoIterator<Item = &'a PoolTick>,
) -> anyhow::Result<()> {
    let spacing = pool
        .tick_spacing
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0 && *value <= PoolTick::MAX_TICK)
        .ok_or_else(|| anyhow::anyhow!("Invalid pool tick spacing"))?;
    anyhow::ensure!(
        (MIN_SQRT_RATIO..MAX_SQRT_RATIO).contains(&state.price_sqrt_ratio_x96),
        "Snapshot sqrt price is outside the supported range"
    );
    anyhow::ensure!(
        (PoolTick::MIN_TICK..PoolTick::MAX_TICK).contains(&state.current_tick),
        "Snapshot current tick is outside the supported range"
    );
    let price_tick = get_tick_at_sqrt_ratio(state.price_sqrt_ratio_x96);
    let left_boundary = state.current_tick.checked_add(1) == Some(price_tick)
        && get_sqrt_ratio_at_tick(price_tick) == state.price_sqrt_ratio_x96;
    anyhow::ensure!(
        state.current_tick == price_tick || left_boundary,
        "Snapshot price and current tick disagree"
    );

    let mut position_keys = BTreeSet::new();
    let mut expected_ticks = BTreeMap::<i32, (u128, i128)>::new();
    let mut active_liquidity = 0_u128;

    for position in positions {
        anyhow::ensure!(
            position_keys.insert((position.owner, position.tick_lower, position.tick_upper)),
            "Snapshot has duplicate positions"
        );
        anyhow::ensure!(
            position.tick_lower < position.tick_upper
                && position.tick_lower >= PoolTick::MIN_TICK
                && position.tick_upper <= PoolTick::MAX_TICK
                && position.tick_lower % spacing == 0
                && position.tick_upper % spacing == 0,
            "Snapshot position has invalid tick bounds"
        );

        if position.liquidity == 0 {
            continue;
        }

        let delta = i128::try_from(position.liquidity).map_err(|_| {
            anyhow::anyhow!("Snapshot position liquidity exceeds the supported range")
        })?;

        for (tick, signed) in [(position.tick_lower, delta), (position.tick_upper, -delta)] {
            let totals = expected_ticks.entry(tick).or_default();
            totals.0 = totals
                .0
                .checked_add(position.liquidity)
                .ok_or_else(|| anyhow::anyhow!("Snapshot gross liquidity overflow"))?;
            totals.1 = totals
                .1
                .checked_add(signed)
                .ok_or_else(|| anyhow::anyhow!("Snapshot net liquidity overflow"))?;
        }

        if position.tick_lower <= state.current_tick && state.current_tick < position.tick_upper {
            active_liquidity = active_liquidity
                .checked_add(position.liquidity)
                .ok_or_else(|| anyhow::anyhow!("Snapshot active liquidity overflow"))?;
        }
    }

    anyhow::ensure!(
        active_liquidity == state.liquidity,
        "Snapshot active liquidity is incomplete"
    );
    // Reuse the protocol's native cap, then validate every price interval, including those
    // outside the current active range. A distant impossible curve must not become Ready.
    let maximum = tick_spacing_to_max_liquidity_per_tick(spacing);
    let mut interval_liquidity = 0_u128;

    for (gross, net) in expected_ticks.values() {
        anyhow::ensure!(
            *gross <= maximum,
            "Snapshot exceeds native per-tick gross liquidity limit"
        );
        interval_liquidity = try_liquidity_math_add(interval_liquidity, *net).map_err(|e| {
            anyhow::anyhow!("Snapshot liquidity curve leaves the uint128 range: {e}")
        })?;
    }
    anyhow::ensure!(
        interval_liquidity == 0,
        "Snapshot liquidity curve does not close"
    );
    let mut tick_values = BTreeSet::new();

    for tick in ticks {
        anyhow::ensure!(
            tick_values.insert(tick.value),
            "Snapshot has duplicate tick records"
        );
        anyhow::ensure!(
            tick.value >= PoolTick::MIN_TICK
                && tick.value <= PoolTick::MAX_TICK
                && tick.value % spacing == 0,
            "Snapshot tick is outside the pool grid"
        );
        let expected = expected_ticks.remove(&tick.value).unwrap_or_default();
        anyhow::ensure!(
            (tick.liquidity_gross, tick.liquidity_net) == expected,
            "Snapshot ticks disagree with position liquidity"
        );
        anyhow::ensure!(
            tick.initialized == (tick.liquidity_gross > 0),
            "Snapshot tick initialization disagrees with liquidity"
        );
    }

    anyhow::ensure!(
        expected_ticks.is_empty(),
        "Snapshot is missing position boundary ticks"
    );
    let protocol_fees = [state.fee_protocol % 16, state.fee_protocol >> 4];
    anyhow::ensure!(
        protocol_fees
            .iter()
            .all(|fee| *fee == 0 || (4..=10).contains(fee)),
        "Snapshot has invalid Uniswap V3 protocol fee denominators"
    );
    anyhow::ensure!(
        state.fee_protocol0_basis_points.is_none() && state.fee_protocol1_basis_points.is_none(),
        "Canonical Uniswap V3 hydration does not accept basis-point protocol fees"
    );
    Ok(())
}
