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

use alloy_primitives::{Address, B256, U160, U256};
use nautilus_common::{
    actor::registry::{deregister_actor, get_actor_unchecked, register_actor},
    defi::cache::PoolStateHealth,
};
use nautilus_core::UnixNanos;
use nautilus_model::defi::{
    Block, Blockchain, PoolFeeProtocolUpdate, PoolIdentifier, PoolLiquidityUpdate,
    PoolLiquidityUpdateType, SharedPool,
    data::{
        block::{BLOCK_SCOPED_SNAPSHOT_INDEX, BlockPosition},
        canonical::{
            CanonicalAnchor, CanonicalPoolBatch, CanonicalPoolBootstrap, CanonicalPoolHydration,
            CanonicalStateInvalidated,
        },
    },
    pool_analysis::snapshot::{PoolAnalytics, PoolState},
};
use ustr::Ustr;

use super::*;

fn anchor(epoch: u64, number: u64) -> CanonicalAnchor {
    CanonicalAnchor {
        chain_id: 1,
        epoch,
        number,
        block_hash: B256::from(U256::from(number).to_be_bytes::<32>()),
    }
}

fn snapshot(fixture: &DefiFixture, anchor: CanonicalAnchor) -> PoolSnapshot {
    let state = PoolState {
        price_sqrt_ratio_x96: fixture.pool.initial_sqrt_price_x96.unwrap(),
        current_tick: 0,
        ..Default::default()
    };
    PoolSnapshot::new(
        fixture.instrument_id(),
        state,
        vec![],
        vec![],
        PoolAnalytics::default(),
        BlockPosition::new(
            anchor.number,
            anchor.block_hash.to_string(),
            BLOCK_SCOPED_SNAPSHOT_INDEX,
            BLOCK_SCOPED_SNAPSHOT_INDEX,
        )
        .with_block_hash(Some(anchor.block_hash.to_string())),
        UnixNanos::from(1000),
        UnixNanos::from(1000),
    )
}

fn begin(fixture: &DefiFixture, anchor: CanonicalAnchor) -> UUID4 {
    fixture
        .engine
        .borrow_mut()
        .request_pool_snapshot_at(
            &fixture.instrument_id(),
            Some(&fixture.client_ids[0]),
            anchor,
        )
        .unwrap()
}

fn respond(fixture: &DefiFixture, request: UUID4, anchor: CanonicalAnchor, snapshot: PoolSnapshot) {
    let response = PoolSnapshotResponse::new(request, snapshot).with_canonical_anchor(anchor);
    fixture.engine.borrow_mut().process(&response);
}

fn hydrate(fixture: &DefiFixture, anchor: CanonicalAnchor) {
    let request = begin(fixture, anchor);
    respond(fixture, request, anchor, snapshot(fixture, anchor));
    assert_eq!(
        health(fixture),
        Some(PoolStateHealth::Ready {
            complete_through: anchor
        })
    );
}

#[expect(clippy::await_holding_refcell_ref)] // Single-threaded native engine
async fn disconnect(fixture: &DefiFixture) {
    fixture.engine.borrow_mut().disconnect().await.unwrap();
}

fn health(fixture: &DefiFixture) -> Option<PoolStateHealth> {
    fixture
        .engine
        .borrow()
        .cache()
        .borrow()
        .pool_state_health(&fixture.instrument_id())
        .cloned()
}

fn batch(fixture: &DefiFixture, anchor: CanonicalAnchor) -> CanonicalPoolBatch {
    CanonicalPoolBatch {
        anchor,
        block: Block::new(
            anchor.block_hash.to_string(),
            self::anchor(anchor.epoch, anchor.number - 1)
                .block_hash
                .to_string(),
            anchor.number,
            "0x0000000000000000000000000000000000000000".into(),
            30_000_000,
            0,
            UnixNanos::from(1000),
            Some(Blockchain::Ethereum),
        ),
        pools: vec![fixture.instrument_id()],
        events: vec![],
        ts_init: UnixNanos::from(2000),
    }
}

fn fee_update(
    fixture: &DefiFixture,
    anchor: CanonicalAnchor,
    log_index: u32,
) -> PoolFeeProtocolUpdate {
    let mut update = PoolFeeProtocolUpdate::new(
        Arc::clone(&fixture.pool.chain),
        Arc::clone(&fixture.pool.dex),
        fixture.instrument_id(),
        fixture.pool.pool_identifier,
        anchor.number,
        B256::repeat_byte(7).to_string(),
        0,
        log_index,
        4,
        4,
        UnixNanos::from(1000),
        UnixNanos::from(2000),
    );
    update.block_hash = Some(anchor.block_hash.to_string());
    update
}

#[rstest]
fn source_bootstrap_uses_recorded_request_identity_through_native_ingress() {
    let fixture = DefiFixture::new(false);
    let anchor = anchor(1, 1000);
    let request_id = UUID4::new();
    let bootstrap = CanonicalPoolBootstrap {
        anchor,
        pools: vec![CanonicalPoolHydration {
            pool: fixture.pool.clone(),
            request_id,
        }],
        client_id: fixture.client_ids[0],
        ts_init: UnixNanos::from(1000),
    };
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(bootstrap));
    assert_eq!(fixture.snapshot_requests(0), vec![request_id]);
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Hydrating { anchor, request_id })
    );
    respond(&fixture, request_id, anchor, snapshot(&fixture, anchor));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: anchor
        })
    );
}

#[rstest]
#[case::hydrated_checkpoint(false)]
#[case::complete_zero_event_block(true)]
#[tokio::test]
async fn disconnect_preserves_complete_canonical_universe(#[case] advance: bool) {
    let fixture = DefiFixture::new(false);
    let observer = ObserverGuard::start(&fixture);
    let initial = anchor(1, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: initial,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(1000),
        }));

    for entry in &pools {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    let complete_through = if advance {
        let mut next = batch(&fixture, anchor(1, 1001));
        next.pools = pools.iter().map(|entry| entry.pool.instrument_id).collect();
        fixture
            .engine
            .borrow_mut()
            .process_defi_data(DefiData::CanonicalBatch(next.clone()));
        next.anchor
    } else {
        initial
    };
    let invalidations = observer.observations.borrow().invalidations;
    disconnect(&fixture).await;

    // Repeated responses from the ended session cannot publish another readiness notification
    for entry in &pools {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    let engine = fixture.engine.borrow();
    let cache = engine.cache().borrow();
    let frozen = cache.canonical_pool_snapshots(complete_through).unwrap();
    assert_eq!(frozen.len(), 3);
    assert_eq!(observer.observations.borrow().ready, vec![initial]);
    assert_eq!(observer.observations.borrow().invalidations, invalidations);
    assert_eq!(
        observer.observations.borrow().batches,
        if advance {
            vec![complete_through]
        } else {
            vec![]
        }
    );

    for entry in &pools {
        let id = entry.pool.instrument_id;
        assert_eq!(
            cache.pool_state_health(&id),
            Some(&PoolStateHealth::Ready { complete_through })
        );
        assert_eq!(
            cache
                .pool_profiler(&id)
                .unwrap()
                .last_processed_event
                .as_ref()
                .unwrap()
                .number,
            initial.number
        );
    }
}

#[rstest]
#[case::all_pending(0)]
#[case::partially_hydrated(2)]
#[tokio::test]
async fn disconnect_invalidates_incomplete_canonical_universe(#[case] completed: usize) {
    let fixture = DefiFixture::new(false);
    let observer = ObserverGuard::start(&fixture);
    let initial = anchor(1, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: initial,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(1000),
        }));

    for entry in pools.iter().take(completed) {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    let invalidations = observer.observations.borrow().invalidations;
    disconnect(&fixture).await;

    for entry in &pools {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    let engine = fixture.engine.borrow();
    let cache = engine.cache().borrow();
    assert!(cache.canonical_pool_snapshots(initial).is_err());
    assert!(observer.observations.borrow().ready.is_empty());
    assert_eq!(
        observer.observations.borrow().invalidations,
        invalidations + 1
    );

    for entry in pools {
        assert!(cache.pool_profiler(&entry.pool.instrument_id).is_none());
        assert!(matches!(
            cache.pool_state_health(&entry.pool.instrument_id),
            Some(PoolStateHealth::Dirty { anchor, .. }) if *anchor == initial
        ));
    }
}

#[rstest]
#[tokio::test]
async fn new_bootstrap_after_disconnect_fences_old_epoch() {
    let fixture = DefiFixture::new(false);
    let initial = anchor(1, 1000);
    let old_pools = pool_universe(&fixture);
    let old_bootstrap = CanonicalPoolBootstrap {
        anchor: initial,
        pools: old_pools.clone(),
        client_id: fixture.client_ids[0],
        ts_init: UnixNanos::from(1000),
    };
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(old_bootstrap.clone()));

    for entry in &old_pools {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    disconnect(&fixture).await;
    let recovered = anchor(2, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: recovered,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(2000),
        }));

    for entry in &old_pools {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);
    }

    let mut obsolete = batch(&fixture, anchor(1, 1001));
    obsolete.pools = pools.iter().map(|entry| entry.pool.instrument_id).collect();
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(obsolete));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(old_bootstrap));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalInvalidation(CanonicalStateInvalidated {
            chain_id: initial.chain_id,
            epoch: initial.epoch,
            reason: "Late failure from the ended session".to_string(),
            ts_init: UnixNanos::from(3000),
        }));

    for entry in &pools {
        let engine = fixture.engine.borrow();
        let cache = engine.cache().borrow();
        assert!(cache.pool_profiler(&entry.pool.instrument_id).is_none());
        assert_eq!(
            cache.pool_state_health(&entry.pool.instrument_id),
            Some(&PoolStateHealth::Hydrating {
                anchor: recovered,
                request_id: entry.request_id,
            })
        );
    }

    for (index, entry) in pools.iter().enumerate() {
        let mut state = snapshot(&fixture, recovered);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, recovered, state);
        assert_eq!(
            fixture
                .engine
                .borrow()
                .cache()
                .borrow()
                .canonical_pool_snapshots(recovered)
                .is_ok(),
            index + 1 == pools.len()
        );
    }

    let mut next = batch(&fixture, anchor(2, 1001));
    next.pools = pools.iter().map(|entry| entry.pool.instrument_id).collect();
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(
        fixture
            .engine
            .borrow()
            .cache()
            .borrow()
            .canonical_pool_snapshots(next.anchor)
            .unwrap()
            .len(),
        3
    );
}

#[rstest]
fn reset_invalidates_canonical_trust_before_a_later_subscription() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    fixture.engine.borrow_mut().reset();
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

#[rstest]
fn warm_pool_metadata_waits_for_a_complete_snapshot() {
    let mut fixture = DefiFixture::new(true);
    fixture.subscribe(0, PoolShape::Swaps, fixture.client_ids[0]);
    assert_eq!(fixture.snapshot_requests(0).len(), 1);
    assert!(!fixture.has_profiler());
    fixture.answer_latest_request();
    assert!(fixture.has_profiler());
}

#[rstest]
fn initialized_price_only_profiler_does_not_bypass_snapshot_request() {
    let mut fixture = DefiFixture::new(true);
    let mut profiler = PoolProfiler::new(Arc::new(fixture.pool.clone()));
    profiler
        .initialize(fixture.pool.initial_sqrt_price_x96.unwrap())
        .unwrap();
    fixture
        .engine
        .borrow()
        .cache()
        .borrow_mut()
        .add_pool_profiler(profiler)
        .unwrap();
    fixture.subscribe(0, PoolShape::Swaps, fixture.client_ids[0]);
    assert_eq!(fixture.snapshot_requests(0).len(), 1);
}

#[rstest]
fn bad_buffered_event_does_not_install_partial_hydration() {
    let mut fixture = DefiFixture::new(true);
    fixture.subscribe(0, PoolShape::Swaps, fixture.client_ids[0]);
    let snapshot = fixture.snapshot();
    let mut invalid = fixture.swap.clone();
    invalid.block = snapshot.block_position.number + 1;
    invalid.sqrt_price_x96 = U160::ZERO;
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::PoolSwap(invalid));
    fixture.respond(fixture.snapshot_requests(0)[0], snapshot);
    assert!(!fixture.has_profiler());
}

#[rstest]
fn canonical_hydration_replaces_warm_state_and_rejects_old_epoch_response() {
    let fixture = DefiFixture::new(true);
    let before = anchor(2, 1000);
    let old_request = begin(&fixture, before);
    let after = anchor(3, 1000);
    let new_request = begin(&fixture, after);
    respond(&fixture, old_request, before, snapshot(&fixture, before));
    assert!(!fixture.has_profiler());
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Hydrating {
            anchor: after,
            request_id: new_request
        })
    );
    respond(&fixture, new_request, after, snapshot(&fixture, after));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: after
        })
    );
}

#[rstest]
fn force_hydration_removes_existing_profiler_until_the_new_response() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let request = begin(&fixture, anchor(1, 1000));
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Hydrating { .. })
    ));
    respond(
        &fixture,
        request,
        anchor(1, 1000),
        snapshot(&fixture, anchor(1, 1000)),
    );
    assert!(fixture.has_profiler());
}

#[rstest]
#[case::missing_hash(0)]
#[case::wrong_hash(1)]
#[case::event_position_only(2)]
#[case::invalid_price(3)]
#[case::missing_liquidity(4)]
#[case::wrong_tick(5)]
fn incomplete_canonical_snapshot_cannot_be_ready(#[case] defect: u8) {
    let fixture = DefiFixture::new(true);
    let anchor = anchor(1, 1000);
    let request = begin(&fixture, anchor);
    let mut snapshot = snapshot(&fixture, anchor);
    match defect {
        0 => snapshot.block_position.block_hash = None,
        1 => snapshot.block_position.block_hash = Some(B256::repeat_byte(99).to_string()),
        2 => snapshot.block_position.log_index = 0,
        3 => snapshot.state.price_sqrt_ratio_x96 = U160::ZERO,
        4 => snapshot.state.liquidity = 1,
        5 => snapshot.state.current_tick = 30,
        _ => unreachable!(),
    }
    respond(&fixture, request, anchor, snapshot);
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

#[rstest]
fn unbound_snapshot_cannot_satisfy_canonical_request() {
    let fixture = DefiFixture::new(true);
    let anchor = anchor(1, 1000);
    begin(&fixture, anchor);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::PoolSnapshot(snapshot(&fixture, anchor)));
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Hydrating { .. })
    ));
}

#[rstest]
fn empty_complete_block_advances_completeness_without_a_pool_event() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let batch = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch.clone()));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: batch.anchor
        })
    );
    assert_eq!(fixture.profiler_last_block(), Some(1000));
}

#[rstest]
fn canonical_mint_checks_live_tick_map_liquidity_after_native_application() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let mut batch = batch(&fixture, anchor(1, 1001));
    let mut mint = PoolLiquidityUpdate::new(
        Arc::clone(&fixture.pool.chain),
        Arc::clone(&fixture.pool.dex),
        fixture.instrument_id(),
        fixture.pool.pool_identifier,
        PoolLiquidityUpdateType::Mint,
        batch.anchor.number,
        B256::repeat_byte(7).to_string(),
        0,
        0,
        Some(Address::ZERO),
        Address::ZERO,
        1000,
        U256::from(1),
        U256::from(1),
        -10,
        10,
        UnixNanos::from(1000),
        UnixNanos::from(2000),
    );
    mint.block_hash = Some(batch.anchor.block_hash.to_string());
    batch.events.push(DexPoolData::LiquidityUpdate(mint));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch.clone()));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: batch.anchor
        })
    );
    assert_eq!(
        fixture
            .engine
            .borrow()
            .cache()
            .borrow()
            .pool_profiler(&fixture.instrument_id())
            .unwrap()
            .get_active_liquidity(),
        1000
    );
}

#[rstest]
fn raw_pool_events_do_not_double_apply_canonical_state() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let mut batch = batch(&fixture, anchor(1, 1001));
    let update = fee_update(&fixture, batch.anchor, 0);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::PoolFeeProtocolUpdate(update.clone()));
    assert_eq!(fixture.profiler_last_block(), Some(1000));
    batch
        .events
        .push(DexPoolData::FeeProtocolUpdate(update.clone()));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::PoolFeeProtocolUpdate(update));
    assert_eq!(fixture.profiler_last_block(), Some(1001));
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Ready { .. })
    ));
}

#[rstest]
#[case::missing_pool(0)]
#[case::wrong_parent(1)]
#[case::gap(2)]
#[case::duplicate_log(3)]
#[case::bad_later_event(4)]
fn invalid_complete_batch_does_not_expose_partial_state(#[case] defect: u8) {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let mut batch = batch(&fixture, anchor(1, 1001));
    match defect {
        0 => batch.pools.clear(),
        1 => batch.block.parent_hash = B256::repeat_byte(99).to_string(),
        2 => batch = self::batch(&fixture, anchor(1, 1002)),
        3 => {
            let update = DexPoolData::FeeProtocolUpdate(fee_update(&fixture, batch.anchor, 0));
            batch.events = vec![update.clone(), update];
        }
        4 => {
            batch.events.push(DexPoolData::FeeProtocolUpdate(fee_update(
                &fixture,
                batch.anchor,
                0,
            )));
            let mut invalid = fee_update(&fixture, batch.anchor, 1);
            invalid.fee_protocol0_new = 99;
            batch.events.push(DexPoolData::FeeProtocolUpdate(invalid));
        }
        _ => unreachable!(),
    }
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch));
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

#[rstest]
fn obsolete_batch_and_invalidation_cannot_damage_newer_epoch() {
    let fixture = DefiFixture::new(true);
    let current = anchor(4, 1000);
    hydrate(&fixture, current);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch(&fixture, anchor(3, 1001))));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalInvalidation(CanonicalStateInvalidated {
            chain_id: 1,
            epoch: 3,
            reason: "Delayed old recovery".into(),
            ts_init: UnixNanos::from(3000),
        }));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: current
        })
    );
    assert!(fixture.has_profiler());
}

#[rstest]
fn reorg_same_height_hash_invalidates_state_and_inflight_snapshot() {
    let fixture = DefiFixture::new(true);
    let before = anchor(1, 1000);
    let old = begin(&fixture, before);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalInvalidation(CanonicalStateInvalidated {
            chain_id: 1,
            epoch: 2,
            reason: "Same-height reorg".into(),
            ts_init: UnixNanos::from(3000),
        }));
    respond(&fixture, old, before, snapshot(&fixture, before));
    assert!(!fixture.has_profiler());
    assert!(
        matches!(health(&fixture), Some(PoolStateHealth::Dirty { anchor, .. }) if anchor.epoch == 2)
    );
}

#[derive(Debug, Default)]
struct Observations {
    ready: Vec<CanonicalAnchor>,
    ready_frozen: Vec<Vec<(SharedPool, PoolSnapshot)>>,
    batches: Vec<CanonicalAnchor>,
    complete: Vec<Vec<PoolStateHealth>>,
    frozen: Vec<Vec<(SharedPool, PoolSnapshot)>>,
    invalidations: usize,
}

#[derive(Debug)]
struct CanonicalObserver {
    core: DataActorCore,
    observations: Rc<RefCell<Observations>>,
}

nautilus_actor!(CanonicalObserver);

impl DataActor for CanonicalObserver {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_canonical_pool_state(1);
        Ok(())
    }

    fn on_canonical_pool_ready(&mut self, anchor: &CanonicalAnchor) -> anyhow::Result<()> {
        let frozen = self.cache().canonical_pool_snapshots(*anchor)?;
        let mut observations = self.observations.borrow_mut();
        observations.ready.push(*anchor);
        observations.ready_frozen.push(frozen);
        Ok(())
    }

    fn on_canonical_pool_batch(&mut self, batch: &CanonicalPoolBatch) -> anyhow::Result<()> {
        let states = batch
            .pools
            .iter()
            .map(|id| self.cache().pool_state_health(id).unwrap())
            .collect();
        let frozen = self.cache().canonical_pool_snapshots(batch.anchor)?;
        let mut observations = self.observations.borrow_mut();
        observations.batches.push(batch.anchor);
        observations.complete.push(states);
        observations.frozen.push(frozen);
        Ok(())
    }

    fn on_canonical_state_invalidated(
        &mut self,
        _event: &CanonicalStateInvalidated,
    ) -> anyhow::Result<()> {
        self.observations.borrow_mut().invalidations += 1;
        Ok(())
    }
}

struct ObserverGuard {
    id: Ustr,
    observations: Rc<RefCell<Observations>>,
}

impl ObserverGuard {
    fn start(fixture: &DefiFixture) -> Self {
        let observations = Rc::new(RefCell::new(Observations::default()));
        let mut actor = CanonicalObserver {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("CANONICAL-OBSERVER")),
                ..Default::default()
            }),
            observations: Rc::clone(&observations),
        };
        let engine = fixture.engine.borrow();
        actor
            .register(
                TraderId::from("TRADER-001"),
                Rc::clone(engine.clock()),
                Rc::clone(engine.cache()),
            )
            .unwrap();
        let id = actor.actor_id().inner();
        register_actor(actor);
        get_actor_unchecked::<CanonicalObserver>(&id)
            .start()
            .unwrap();
        Self { id, observations }
    }
}

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        {
            let mut actor = get_actor_unchecked::<CanonicalObserver>(&self.id);
            if actor.is_running() {
                actor.stop().unwrap();
            }
            actor.dispose().unwrap();
        }
        deregister_actor(&self.id);
    }
}

#[rstest]
fn native_actor_sees_only_complete_batches_once_and_honors_stop() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    let batch = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch.clone()));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(batch.clone()));
    assert_eq!(observer.observations.borrow().batches, vec![batch.anchor]);
    assert_eq!(
        observer.observations.borrow().complete,
        vec![vec![PoolStateHealth::Ready {
            complete_through: batch.anchor
        }]]
    );
    get_actor_unchecked::<CanonicalObserver>(&observer.id)
        .stop()
        .unwrap();
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(self::batch(
            &fixture,
            anchor(1, 1002),
        )));
    assert_eq!(observer.observations.borrow().batches.len(), 1);
}

#[rstest]
fn native_actor_receives_failure_invalidation_instead_of_false_ready() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    let mut invalid = batch(&fixture, anchor(1, 1001));
    invalid.block.parent_hash = B256::repeat_byte(99).to_string();
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(invalid));
    assert!(observer.observations.borrow().batches.is_empty());
    assert_eq!(observer.observations.borrow().invalidations, 1);
}

fn pool_universe(fixture: &DefiFixture) -> Vec<CanonicalPoolHydration> {
    let mut pools = (1..=3u8)
        .map(|index| {
            let address = Address::repeat_byte(index);
            let mut pool = Pool::new(
                Arc::clone(&fixture.pool.chain),
                Arc::clone(&fixture.pool.dex),
                address,
                PoolIdentifier::new(address.to_string()),
                0,
                fixture.pool.token0.clone(),
                fixture.pool.token1.clone(),
                fixture.pool.fee,
                fixture.pool.tick_spacing,
                UnixNanos::from(1),
            );
            pool.initialize(fixture.pool.initial_sqrt_price_x96.unwrap(), 0);
            CanonicalPoolHydration {
                pool,
                request_id: UUID4::new(),
            }
        })
        .collect::<Vec<_>>();
    pools.sort_by_key(|entry| entry.pool.instrument_id);
    pools
}

#[rstest]
#[case::complete(false)]
#[case::second_pool_rejected(true)]
fn three_pool_batch_is_observed_only_after_all_state_is_complete(#[case] reject_second: bool) {
    let fixture = DefiFixture::new(false);
    let initial = anchor(1, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: initial,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(1000),
        }));
    assert_eq!(fixture.snapshot_requests(0).len(), 3);
    for (index, entry) in pools.iter().enumerate() {
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, initial, state);

        if index < pools.len() - 1 {
            assert!(
                fixture
                    .engine
                    .borrow()
                    .cache()
                    .borrow()
                    .canonical_pool_snapshots(initial)
                    .is_err()
            );
        }
    }
    let observer = ObserverGuard::start(&fixture);
    let mut next = batch(&fixture, anchor(1, 1001));
    next.pools = pools.iter().map(|entry| entry.pool.instrument_id).collect();
    for (index, entry) in pools.iter().take(2).enumerate() {
        let mut update = fee_update(&fixture, next.anchor, u32::try_from(index).unwrap());
        update.instrument_id = entry.pool.instrument_id;
        update.pool_identifier = entry.pool.pool_identifier;
        if reject_second && index == 1 {
            update.fee_protocol0_new = 99;
        }
        next.events.push(DexPoolData::FeeProtocolUpdate(update));
    }
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    let engine = fixture.engine.borrow();
    let cache = engine.cache().borrow();
    if reject_second {
        assert!(cache.canonical_pool_snapshots(next.anchor).is_err());
        assert!(observer.observations.borrow().batches.is_empty());
        assert_eq!(observer.observations.borrow().invalidations, 1);

        for entry in &pools {
            assert!(cache.pool_profiler(&entry.pool.instrument_id).is_none());
            assert!(matches!(
                cache.pool_state_health(&entry.pool.instrument_id),
                Some(PoolStateHealth::Dirty { .. })
            ));
        }
    } else {
        assert_eq!(observer.observations.borrow().batches, vec![next.anchor]);
        assert_eq!(
            observer.observations.borrow().complete,
            vec![vec![
                PoolStateHealth::Ready {
                    complete_through: next.anchor
                };
                3
            ]],
        );
        let frozen = cache.canonical_pool_snapshots(next.anchor).unwrap();
        assert_eq!(frozen.len(), 3);
        for (pool, snapshot) in &frozen {
            nautilus_model::defi::pool_analysis::validation::validate_canonical_snapshot(
                pool,
                snapshot,
                &next.anchor,
            )
            .unwrap();
            assert_eq!(snapshot.block_position.number, next.anchor.number);
            assert_eq!(
                snapshot.block_position.log_index,
                BLOCK_SCOPED_SNAPSHOT_INDEX
            );
        }
        let mut wrong_anchor = next.anchor;
        wrong_anchor.block_hash = B256::repeat_byte(99);
        assert!(cache.canonical_pool_snapshots(wrong_anchor).is_err());
        // The quiet third pool advances completeness without inventing an event position
        assert_eq!(
            cache
                .pool_profiler(&pools[2].pool.instrument_id)
                .unwrap()
                .last_processed_event
                .as_ref()
                .unwrap()
                .number,
            initial.number,
        );
    }
}

#[rstest]
fn matching_request_with_wrong_response_anchor_invalidates_hydration() {
    let fixture = DefiFixture::new(true);
    let expected = anchor(1, 1000);
    let request = begin(&fixture, expected);
    respond(
        &fixture,
        request,
        anchor(1, 999),
        snapshot(&fixture, expected),
    );
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

struct RejectingSnapshotClient {
    client_id: ClientId,
    fail_at: usize,
    requests: std::cell::Cell<usize>,
}

impl DataClient for RejectingSnapshotClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }
    fn venue(&self) -> Option<Venue> {
        None
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn is_connected(&self) -> bool {
        true
    }
    fn is_disconnected(&self) -> bool {
        false
    }
    fn request_pool_snapshot(&self, _request: RequestPoolSnapshot) -> anyhow::Result<()> {
        let requests = self.requests.get() + 1;
        self.requests.set(requests);
        anyhow::ensure!(
            requests != self.fail_at,
            "Injected snapshot dispatch failure"
        );
        Ok(())
    }
}

#[rstest]
#[case::first_pool(1)]
#[case::middle_pool(2)]
fn failed_bootstrap_keeps_full_universe_and_recovers_next_epoch(#[case] fail_at: usize) {
    let fixture = DefiFixture::new(false);
    let pools = pool_universe(&fixture);
    let client_id = ClientId::from("DEFI-REJECTING-SNAPSHOT");
    fixture.engine.borrow_mut().register_client(
        DataClientAdapter::new(
            client_id,
            None,
            false,
            false,
            Box::new(RejectingSnapshotClient {
                client_id,
                fail_at,
                requests: std::cell::Cell::new(0),
            }),
        ),
        None,
    );
    let bootstrap = CanonicalPoolBootstrap {
        anchor: anchor(1, 1000),
        pools: pools.clone(),
        client_id,
        ts_init: UnixNanos::from(1000),
    };
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(bootstrap));
    {
        let engine = fixture.engine.borrow();
        let cache = engine.cache().borrow();
        assert_eq!(cache.canonical_pool_ids(1).len(), 3);

        for entry in &pools {
            assert!(matches!(
                cache.pool_state_health(&entry.pool.instrument_id),
                Some(PoolStateHealth::Dirty { .. })
            ));
            assert!(cache.pool_profiler(&entry.pool.instrument_id).is_none());
        }
    }
    let recovered = anchor(2, 1000);
    let mut pools = pools;
    for entry in &mut pools {
        entry.request_id = UUID4::new();
    }
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: recovered,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(2000),
        }));
    assert_eq!(fixture.snapshot_requests(0).len(), 3);
    for entry in &pools {
        let mut state = snapshot(&fixture, recovered);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, recovered, state);
    }
    let observer = ObserverGuard::start(&fixture);
    let mut next = batch(&fixture, anchor(2, 1001));
    next.pools = pools.iter().map(|entry| entry.pool.instrument_id).collect();
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(observer.observations.borrow().batches, vec![next.anchor]);
    assert_eq!(
        observer.observations.borrow().complete,
        vec![vec![
            PoolStateHealth::Ready {
                complete_through: next.anchor
            };
            3
        ]]
    );
}

#[rstest]
fn wrong_chain_hydration_preserves_existing_ready_pool_and_updater() {
    let fixture = DefiFixture::new(true);
    let initial = anchor(1, 1000);
    hydrate(&fixture, initial);
    let mut wrong = initial;
    wrong.chain_id = 42161;
    assert!(
        fixture
            .engine
            .borrow_mut()
            .request_pool_snapshot_at(
                &fixture.instrument_id(),
                Some(&fixture.client_ids[0]),
                wrong,
            )
            .is_err()
    );
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: initial
        })
    );
    let next = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: next.anchor
        })
    );
}

#[rstest]
#[case::same_chain_payload(false)]
#[case::conflicting_chain_payload(true)]
fn duplicate_batch_ignores_receive_time_but_rejects_payload_change(#[case] change_payload: bool) {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    let mut next = batch(&fixture, anchor(1, 1001));
    next.events.push(DexPoolData::FeeProtocolUpdate(fee_update(
        &fixture,
        next.anchor,
        0,
    )));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    next.ts_init = UnixNanos::from(9000);
    let DexPoolData::FeeProtocolUpdate(update) = &mut next.events[0] else {
        unreachable!()
    };
    update.ts_init = UnixNanos::from(8000);
    if change_payload {
        update.fee_protocol0_new = 5;
    }
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(observer.observations.borrow().batches, vec![next.anchor]);
    if change_payload {
        assert_eq!(observer.observations.borrow().invalidations, 1);
        assert!(matches!(
            health(&fixture),
            Some(PoolStateHealth::Dirty { .. })
        ));
    } else {
        assert_eq!(observer.observations.borrow().invalidations, 0);
        assert_eq!(
            health(&fixture),
            Some(PoolStateHealth::Ready {
                complete_through: next.anchor
            })
        );
    }
}

#[rstest]
fn canonical_event_for_another_pool_identifier_cannot_mutate_ready_state() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    let mut next = batch(&fixture, anchor(1, 1001));
    let mut update = fee_update(&fixture, next.anchor, 0);
    update.pool_identifier = PoolIdentifier::from_address(Address::repeat_byte(99));
    next.events.push(DexPoolData::FeeProtocolUpdate(update));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next));
    assert!(observer.observations.borrow().batches.is_empty());
    assert_eq!(observer.observations.borrow().invalidations, 1);
    assert!(!fixture.has_profiler());
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

#[rstest]
#[case::hydrating(true)]
#[case::ready(false)]
fn canonical_state_survives_unrelated_raw_subscription_retirement(#[case] pending: bool) {
    let mut fixture = DefiFixture::new(true);
    let initial = anchor(1, 1000);
    let request = begin(&fixture, initial);
    if !pending {
        respond(&fixture, request, initial, snapshot(&fixture, initial));
    }
    let client_id = fixture.client_ids[0];
    fixture.subscribe(0, PoolShape::Swaps, client_id);
    fixture.unsubscribe(0, PoolShape::Swaps, client_id);
    if pending {
        respond(&fixture, request, initial, snapshot(&fixture, initial));
    }
    let next = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: next.anchor
        })
    );
}

#[rstest]
fn native_callback_freezes_owned_pool_state_before_the_next_batch() {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    for number in [1001, 1002] {
        let mut next = batch(&fixture, anchor(1, number));
        let mut update = fee_update(&fixture, next.anchor, 0);
        update.fee_protocol0_new = if number == 1001 { 4 } else { 5 };
        next.events.push(DexPoolData::FeeProtocolUpdate(update));
        fixture
            .engine
            .borrow_mut()
            .process_defi_data(DefiData::CanonicalBatch(next));
    }
    let observations = observer.observations.borrow();
    assert_eq!(observations.frozen.len(), 2);
    assert_eq!(
        observations.frozen[0][0]
            .1
            .state
            .uniswap_v3_fee_protocol(true),
        4
    );
    assert_eq!(
        observations.frozen[1][0]
            .1
            .state
            .uniswap_v3_fee_protocol(true),
        5
    );
    assert_eq!(observations.frozen[0][0].1.block_position.number, 1001,);
}

#[rstest]
#[case::height(0)]
#[case::parent_hash(1)]
#[case::base_fee(2)]
fn duplicate_hash_cannot_hide_conflicting_header_fields(#[case] defect: u8) {
    let fixture = DefiFixture::new(true);
    hydrate(&fixture, anchor(1, 1000));
    let observer = ObserverGuard::start(&fixture);
    let mut next = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    match defect {
        0 => next.block.number = 999,
        1 => next.block.parent_hash = B256::repeat_byte(99).to_string(),
        2 => next.block.base_fee_per_gas = Some(U256::from(123)),
        _ => unreachable!(),
    }
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next));
    assert_eq!(observer.observations.borrow().batches.len(), 1);
    assert_eq!(observer.observations.borrow().invalidations, 1);
    assert!(matches!(
        health(&fixture),
        Some(PoolStateHealth::Dirty { .. })
    ));
}

#[rstest]
fn forced_state_replacement_discards_the_previous_batch_receipt() {
    let fixture = DefiFixture::new(true);
    let initial = anchor(1, 1000);
    hydrate(&fixture, initial);
    let next = batch(&fixture, anchor(1, 1001));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    let request = begin(&fixture, initial);
    respond(&fixture, request, initial, snapshot(&fixture, initial));
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBatch(next.clone()));
    assert_eq!(
        health(&fixture),
        Some(PoolStateHealth::Ready {
            complete_through: next.anchor
        })
    );
}

#[rstest]
#[case::complete_universe(false)]
#[case::final_snapshot_invalid(true)]
fn native_ready_fires_only_after_final_qualified_snapshot_and_honors_stop(
    #[case] reject_final: bool,
) {
    let fixture = DefiFixture::new(false);
    let observer = ObserverGuard::start(&fixture);
    let initial = anchor(1, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: initial,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(1000),
        }));

    for (index, entry) in pools.iter().enumerate() {
        assert!(observer.observations.borrow().ready.is_empty());
        let mut state = snapshot(&fixture, initial);
        state.instrument_id = entry.pool.instrument_id;
        if reject_final && index == pools.len() - 1 {
            state.block_position.block_hash = Some(B256::repeat_byte(99).to_string());
        }
        respond(&fixture, entry.request_id, initial, state.clone());
        // A duplicate/stale correlation cannot publish a second derived Ready notification.
        respond(&fixture, entry.request_id, initial, state);
    }

    if reject_final {
        assert!(observer.observations.borrow().ready.is_empty());
        assert!(observer.observations.borrow().ready_frozen.is_empty());
        assert!(
            fixture
                .engine
                .borrow()
                .cache()
                .borrow()
                .canonical_pool_snapshots(initial)
                .is_err()
        );
    } else {
        assert_eq!(observer.observations.borrow().ready, vec![initial]);
        let observations = observer.observations.borrow();
        assert_eq!(observations.ready_frozen.len(), 1);
        assert_eq!(observations.ready_frozen[0].len(), 3);
        assert!(observations.ready_frozen[0].iter().all(|(_, snapshot)| {
            snapshot.block_position.number == initial.number
                && snapshot.block_position.block_hash.as_deref()
                    == Some(initial.block_hash.to_string().as_str())
        }));
    }
    assert!(
        observer.observations.borrow().batches.is_empty(),
        "Initial readiness cannot invent a market block batch"
    );
    let count = observer.observations.borrow().ready.len();
    get_actor_unchecked::<CanonicalObserver>(&observer.id)
        .stop()
        .unwrap();
    let recovered = anchor(2, 1000);
    let pools = pool_universe(&fixture);
    fixture
        .engine
        .borrow_mut()
        .process_defi_data(DefiData::CanonicalBootstrap(CanonicalPoolBootstrap {
            anchor: recovered,
            pools: pools.clone(),
            client_id: fixture.client_ids[0],
            ts_init: UnixNanos::from(2000),
        }));

    for entry in pools {
        let mut state = snapshot(&fixture, recovered);
        state.instrument_id = entry.pool.instrument_id;
        respond(&fixture, entry.request_id, recovered, state);
    }
    assert!(
        fixture
            .engine
            .borrow()
            .cache()
            .borrow()
            .canonical_pool_snapshots(recovered)
            .is_ok()
    );
    assert_eq!(observer.observations.borrow().ready.len(), count);
}

#[rstest]
#[case::native_tick_cap_is_valid(false)]
#[case::distant_tick_cap_exceeded(true)]
fn canonical_hydration_checks_distant_liquidity_limits(#[case] exceeds_cap: bool) {
    use nautilus_model::defi::{
        pool_analysis::position::PoolPosition,
        tick_map::{liquidity_math::tick_spacing_to_max_liquidity_per_tick, tick::PoolTick},
    };

    let fixture = DefiFixture::new(true);
    let observer = ObserverGuard::start(&fixture);
    let initial = anchor(1, 1000);
    let spacing = i32::try_from(fixture.pool.tick_spacing.unwrap()).unwrap();
    let liquidity = tick_spacing_to_max_liquidity_per_tick(spacing) + u128::from(exceeds_cap);
    let signed = i128::try_from(liquidity).unwrap();
    let lower = spacing * 100;
    let upper = spacing * 200;
    let mut state = snapshot(&fixture, initial);
    state.positions = vec![PoolPosition::new(
        Address::repeat_byte(17),
        lower,
        upper,
        signed,
    )];
    state.ticks = vec![
        PoolTick::new(
            lower,
            liquidity,
            signed,
            U256::ZERO,
            U256::ZERO,
            true,
            initial.number,
        ),
        PoolTick::new(
            upper,
            liquidity,
            -signed,
            U256::ZERO,
            U256::ZERO,
            true,
            initial.number,
        ),
    ];
    assert_eq!(
        state.state.liquidity, 0,
        "The bad range is away from the current price"
    );
    let request = begin(&fixture, initial);
    respond(&fixture, request, initial, state);

    if exceeds_cap {
        assert!(matches!(
            health(&fixture),
            Some(PoolStateHealth::Dirty { .. })
        ));
        assert!(!fixture.has_profiler());
        assert!(observer.observations.borrow().ready.is_empty());
    } else {
        assert_eq!(
            health(&fixture),
            Some(PoolStateHealth::Ready {
                complete_through: initial
            })
        );
        assert_eq!(observer.observations.borrow().ready, vec![initial]);
    }
}
