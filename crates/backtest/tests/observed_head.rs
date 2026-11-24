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

#![cfg(feature = "defi")]

use std::{cell::RefCell, rc::Rc};

use nautilus_backtest::{config::BacktestEngineConfig, engine::BacktestEngine};
use nautilus_common::{
    cache::Cache,
    defi::switchboard::get_defi_blocks_topic,
    messages::DataEvent,
    msgbus::{self, TypedHandler},
    observation::{ObservationKey, ObservedDataEvent, current_data_observation},
};
use nautilus_model::defi::{Blockchain, DefiData, data::Block};
use rstest::rstest;
use ustr::Ustr;

fn head(number: u64, chain: Blockchain) -> Block {
    Block::new(
        format!("0x{number:064x}"),
        format!("0x{:064x}", number - 1),
        number,
        Ustr::from("0x0000000000000000000000000000000000000001"),
        30_000_000,
        17_000_000,
        (number * 12_000_000_000).into(),
        Some(chain),
    )
}

#[rstest]
fn fresh_observed_replay_preserves_original_head_and_first_key_across_chunks() {
    let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
    let published = Rc::new(RefCell::new(Vec::new()));
    let actual = published.clone();
    let cache = engine.kernel().cache.clone();
    let topic = get_defi_blocks_topic(Blockchain::Ethereum);
    let handler = TypedHandler::from(move |block: &Block| {
        let at = current_data_observation().unwrap();
        let (original, first) = cache.borrow().latest_observed_block(1).unwrap();
        actual
            .borrow_mut()
            .push((block.hash.clone(), at, original, first));
    });
    msgbus::subscribe_defi_blocks(topic.into(), handler.clone(), None);

    let first = head(100, Blockchain::Ethereum);
    let second = head(101, Blockchain::Ethereum);
    let blocks = [first.clone(), first.clone(), second.clone()];
    for (index, block) in blocks.into_iter().enumerate() {
        let key = ObservationKey {
            sequence: index as u64 + 1,
            ts_observed: 2_000_000_000_000.into(),
        };
        engine
            .add_data_observations(
                vec![ObservedDataEvent {
                    key,
                    event: DataEvent::DeFi(DefiData::Block(block)),
                }],
                None,
            )
            .unwrap();
        engine
            .run(Some(1_999_999_999_999.into()), None, None, index < 2)
            .unwrap();
        assert_eq!(engine.last_data_observation(), Some(key));
        let (original, first_at) = engine
            .kernel()
            .cache
            .borrow()
            .latest_observed_block(1)
            .unwrap();
        assert_eq!(first_at.sequence, if index < 2 { 1 } else { 3 });
        let expected = if index < 2 { &first } else { &second };
        assert_eq!(original.hash, expected.hash);
        assert_eq!(original.parent_hash, expected.parent_hash);
        assert_eq!(original.number, expected.number);
        assert_eq!(original.gas_used, expected.gas_used);
        assert_eq!(original.timestamp, expected.timestamp);
        // Streaming removal discards consumed payloads, not the actual native head observation.
        engine.clear_data();
    }
    {
        let actual = published.borrow();
        assert_eq!(actual.len(), 3);
        assert_eq!(
            actual
                .iter()
                .map(|value| value.1.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            actual
                .iter()
                .map(|value| value.3.sequence)
                .collect::<Vec<_>>(),
            vec![1, 1, 3]
        );
        assert_eq!(actual[0].3.ts_observed, actual[2].3.ts_observed);
    }
    msgbus::unsubscribe_defi_blocks(topic.into(), &handler);
    assert!(engine.reset().is_err());
    engine.dispose();

    // Observed sessions run once. A new native engine owns a clean observation cache.
    let mut fresh = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
    assert!(
        fresh
            .kernel()
            .cache
            .borrow()
            .latest_observed_block(1)
            .is_none()
    );
    fresh.dispose();
}

#[rstest]
fn observed_head_cache_is_chain_scoped_and_reorgs_replace_the_current_episode() {
    let mut cache = Cache::default();
    let first = head(100, Blockchain::Ethereum);
    let key = |sequence| ObservationKey {
        sequence,
        ts_observed: 2_000_000_000_000.into(),
    };
    cache.add_observed_block(first.clone(), key(1)).unwrap();
    cache
        .add_observed_block(head(101, Blockchain::Ethereum), key(2))
        .unwrap();
    cache.add_observed_block(first, key(3)).unwrap();
    cache
        .add_observed_block(head(100, Blockchain::Base), key(4))
        .unwrap();
    assert_eq!(cache.latest_observed_block(1).unwrap().1, key(3));
    assert_eq!(cache.latest_observed_block(8453).unwrap().1, key(4));
    assert!(cache.latest_observed_block(999_999).is_none());
    let mut unscoped = head(102, Blockchain::Ethereum);
    unscoped.chain = None;
    assert!(cache.add_observed_block(unscoped, key(5)).is_err());
    assert!(
        cache
            .add_observed_block(head(102, Blockchain::Ethereum), key(0))
            .is_err()
    );
    assert_eq!(cache.latest_observed_block(1).unwrap().1, key(3));
    cache.reset();
    assert!(cache.latest_observed_block(1).is_none());
    assert!(cache.latest_observed_block(8453).is_none());
}

#[rstest]
fn historical_input_without_observation_does_not_invent_a_head_receipt() {
    let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
    let topic = get_defi_blocks_topic(Blockchain::Ethereum);
    let published = Rc::new(RefCell::new(Vec::new()));
    let actual = published.clone();
    let handler = TypedHandler::from(move |block: &Block| {
        assert!(current_data_observation().is_none());
        actual.borrow_mut().push(block.hash.clone());
    });
    msgbus::subscribe_defi_blocks(topic.into(), handler.clone(), None);
    let block = head(100, Blockchain::Ethereum);
    engine
        .kernel()
        .data_engine
        .borrow_mut()
        .process_defi_data(DefiData::Block(block.clone()));
    assert_eq!(*published.borrow(), vec![block.hash]);
    assert!(
        engine
            .kernel()
            .cache
            .borrow()
            .latest_observed_block(1)
            .is_none()
    );
    msgbus::unsubscribe_defi_blocks(topic.into(), &handler);
    engine.dispose();
}
