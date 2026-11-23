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

use nautilus_common::{
    defi::switchboard::get_defi_blocks_topic,
    observation::{DataObservationRecorder, ObservationKey, current_data_observation},
};
use nautilus_model::defi::{Blockchain, DefiData, data::Block};

use super::*;

#[derive(Debug, Default)]
struct HeadProbeState {
    started: bool,
    head_at_start: Option<(Block, ObservationKey)>,
    publications: Vec<(bool, Block, ObservationKey, Block, ObservationKey)>,
}

#[derive(Debug)]
struct StartupHeadProbe {
    core: DataActorCore,
    state: Rc<RefCell<HeadProbeState>>,
}

impl DataActor for StartupHeadProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let head = self.cache().latest_observed_block(1);
        let mut state = self.state.borrow_mut();
        state.head_at_start = head;
        state.started = true;
        Ok(())
    }
}

nautilus_actor!(StartupHeadProbe);

fn head(number: u64) -> Block {
    Block::new(
        format!("0x{number:064x}"),
        format!("0x{:064x}", number - 1),
        number,
        Ustr::from("0x0000000000000000000000000000000000000001"),
        30_000_000,
        17_000_000,
        (number * 12_000_000_000).into(),
        Some(Blockchain::Ethereum),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn startup_consumes_observed_head_before_trader_and_cache_retains_first_key() {
    let config = LiveNodeConfig {
        trader_id: TraderId::from("OBSERVED-HEAD-001"),
        exec_engine: crate::config::LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        timeout_connection: Duration::ZERO,
        timeout_reconciliation: Duration::ZERO,
        timeout_portfolio: Duration::ZERO,
        timeout_disconnection: Duration::ZERO,
        delay_post_stop: Duration::ZERO,
        timeout_shutdown: Duration::ZERO,
        ..Default::default()
    };
    let mut node = LiveNode::build("ObservedHeadStartup".to_owned(), Some(config)).unwrap();
    let state = Rc::new(RefCell::new(HeadProbeState::default()));
    node.add_actor(StartupHeadProbe {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::from("OBSERVED-HEAD-PROBE")),
            ..Default::default()
        }),
        state: state.clone(),
    })
    .unwrap();

    let cache = node.kernel.cache.clone();
    let callback_state = state.clone();
    let handler = TypedHandler::from(move |block: &Block| {
        let at = current_data_observation().unwrap();
        // The mutable write must have ended before the native publication reaches consumers.
        let (cached, first) = cache.borrow().latest_observed_block(1).unwrap();
        let mut state = callback_state.borrow_mut();
        let started = state.started;
        state
            .publications
            .push((started, block.clone(), at, cached, first));
    });
    let topic = get_defi_blocks_topic(Blockchain::Ethereum);
    msgbus::subscribe_defi_blocks(topic.into(), handler.clone(), None);
    let inputs = Rc::new(RefCell::new(Vec::new()));
    let recorded = inputs.clone();
    let recorder = DataObservationRecorder::install(node.kernel.clock.clone(), move |at, event| {
        let DataEvent::DeFi(DefiData::Block(block)) = event else {
            anyhow::bail!("Unexpected native input in head startup fixture");
        };
        recorded.borrow_mut().push((block.clone(), at));
        Ok(())
    })
    .unwrap();

    // Queue the raw header before startup. The real startup flush consumes it before trader.start,
    // so a strategy which only mirrors on_block would lose this first decision opportunity.
    let first = head(100);
    get_data_event_sender()
        .send(DataEvent::DeFi(DefiData::Block(first.clone())))
        .unwrap();
    assert!(
        node.kernel
            .cache
            .borrow()
            .latest_observed_block(1)
            .is_none()
    );
    node.start().await.unwrap();

    let first_key = inputs.borrow()[0].1;
    {
        let actual = state.borrow();
        assert!(actual.started);
        assert_eq!(actual.publications.len(), 1);
        assert!(!actual.publications[0].0);
        assert_eq!(actual.publications[0].2, first_key);
        assert_eq!(actual.publications[0].4, first_key);
        let (original, at_start) = actual.head_at_start.as_ref().unwrap();
        assert_eq!(*at_start, first_key);
        // Block::PartialEq is hash-only. Check original header fields independently.
        assert_eq!(original.number, first.number);
        assert_eq!(original.parent_hash, first.parent_hash);
        assert_eq!(original.gas_used, first.gas_used);
        assert_eq!(original.timestamp, first.timestamp);
    }

    get_data_event_sender()
        .send(DataEvent::DeFi(DefiData::Block(first.clone())))
        .unwrap();
    node.runner.as_mut().unwrap().flush_pending_data();
    assert_eq!(inputs.borrow()[1].1.sequence, first_key.sequence + 1);
    assert_eq!(
        node.kernel
            .cache
            .borrow()
            .latest_observed_block(1)
            .unwrap()
            .1,
        first_key
    );
    {
        let actual = state.borrow();
        assert!(actual.publications[1].0);
        assert_eq!(actual.publications[1].2.sequence, first_key.sequence + 1);
        assert_eq!(actual.publications[1].4, first_key);
    }

    let successor = head(101);
    get_data_event_sender()
        .send(DataEvent::DeFi(DefiData::Block(successor.clone())))
        .unwrap();
    node.runner.as_mut().unwrap().flush_pending_data();
    let successor_key = inputs.borrow()[2].1;
    let (cached, at) = node.kernel.cache.borrow().latest_observed_block(1).unwrap();
    assert_eq!(cached.hash, successor.hash);
    assert_eq!(at, successor_key);
    assert_eq!(state.borrow().publications[2].4, successor_key);
    recorder.check_error().unwrap();
    assert_eq!(recorder.last_observation(), Some(successor_key));
    drop(recorder);
    msgbus::unsubscribe_defi_blocks(topic.into(), &handler);
    node.stop().await.unwrap();
    node.kernel.cache.borrow_mut().reset();
    assert!(
        node.kernel
            .cache
            .borrow()
            .latest_observed_block(1)
            .is_none()
    );
    node.dispose();
}
