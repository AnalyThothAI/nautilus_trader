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

use futures_util::stream;
use nautilus_core::UnixNanos;
use rstest::rstest;
use serde_json::json;
use ustr::Ustr;

use super::*;
use crate::{
    cache::tests::{PoolEventSyncFixture, pool_event_sync_fixture},
    exchanges::parsing::uniswap_v3::pool_created::parse_pool_created_event_hypersync,
    hypersync::HypersyncLog,
    rpc::http::BlockchainHttpRpcClient,
};

fn dependencies(
    fixture: &PoolEventSyncFixture,
) -> anyhow::Result<(
    DexExtended,
    BlockchainDataClientConfig,
    Erc20Contract,
    HyperSyncClient,
)> {
    let mut dex = DexExtended::new((*fixture.dex).clone());
    dex.set_pool_created_event_hypersync_parsing(parse_pool_created_event_hypersync);
    let config = BlockchainDataClientConfig::builder()
        .chain(Arc::clone(&fixture.chain))
        .http_rpc_url("http://127.0.0.1:9".into())
        .multicall_calls_per_rpc_request(3)
        .build();
    let rpc = Arc::new(BlockchainHttpRpcClient::new(
        "http://127.0.0.1:9".into(),
        Some(100),
        None,
    )?);
    let erc20 = Erc20Contract::new(rpc, true);
    let hypersync = HyperSyncClient::new_with_api_token(
        Arc::clone(&fixture.chain),
        None,
        CancellationToken::new(),
        "00000000-0000-4000-8000-000000000450".into(),
    )?;
    Ok((dex, config, erc20, hypersync))
}

fn creation_log(
    fixture: &PoolEventSyncFixture,
    address: Address,
    token0: Address,
    token1: Address,
) -> anyhow::Result<HypersyncLog> {
    Ok(serde_json::from_value(json!({
        "removed": null, "log_index": "0x0", "transaction_index": "0x0",
        "transaction_hash": format!("0x{:064x}", 450),
        "block_hash": format!("0x{:064x}", 12), "block_number": "0xc",
        "address": fixture.dex.factory.to_string(),
        "data": format!("0x{:064x}{:0>64}", 10, address.to_string().trim_start_matches("0x")),
        "topics": [
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118",
            format!("0x{:0>64}", token0.to_string().trim_start_matches("0x")),
            format!("0x{:0>64}", token1.to_string().trim_start_matches("0x")),
            format!("0x{:064x}", 500),
        ],
    }))?)
}

#[rstest]
#[tokio::test]
async fn selected_discovery_skips_unrelated_token_rpc_and_never_advances_factory_watermark()
-> anyhow::Result<()> {
    let Some(mut fixture) = pool_event_sync_fixture().await? else {
        return Ok(());
    };
    let result = Box::pin(async {
        let (dex, config, erc20, hypersync) = dependencies(&fixture)?;
        let selected_address = Address::repeat_byte(55);
        let selected_id = PoolIdentifier::from_address(selected_address);
        let unrelated = creation_log(
            &fixture,
            Address::repeat_byte(66),
            Address::repeat_byte(77),
            Address::repeat_byte(88),
        )?;
        let selected = creation_log(
            &fixture,
            selected_address,
            fixture.pool.token0.address,
            fixture.pool.token1.address,
        )?;
        let block = Block::new(
            format!("0x{:064x}", 12),
            format!("0x{:064x}", 11),
            12,
            Ustr::from("0x0000000000000000000000000000000000000000"),
            30_000_000,
            0,
            UnixNanos::from(1_700_000_012_000_000_000),
            Some(fixture.chain.name),
        );
        let required = HashSet::from([selected_id]);
        let items = stream::iter(vec![
            Ok(PoolEventStreamItem::Block(block)),
            Ok(PoolEventStreamItem::Log(unrelated)),
            Ok(PoolEventStreamItem::Log(selected)),
        ]);
        let mut service = PoolDiscoveryService::new(
            Arc::clone(&fixture.chain),
            &mut fixture.cache,
            &erc20,
            &hypersync,
            CancellationToken::new(),
            config,
        );
        Box::pin(service.sync_pool_stream(&dex, 10, 12, Some(&required), items)).await?;
        let pool = service.cache.get_pool(&selected_id).unwrap();
        assert_eq!(pool.token0.address, fixture.pool.token0.address);
        assert_eq!(pool.token1.address, fixture.pool.token1.address);
        assert!(
            service
                .cache
                .get_pool(&PoolIdentifier::from_address(Address::repeat_byte(66)))
                .is_none()
        );
        assert_eq!(
            service.cache.get_dex_last_synced_block(&dex.name).await?,
            None
        );
        assert!(
            service
                .cache
                .database
                .as_ref()
                .unwrap()
                .load_pool(
                    Arc::clone(&fixture.chain),
                    &dex.name.to_string(),
                    &selected_id
                )
                .await?
                .is_some()
        );
        Ok(())
    })
    .await;
    fixture.schema.cleanup().await?;
    result
}

#[rstest]
#[tokio::test]
async fn discovery_stream_failure_missing_pool_and_cancel_are_errors() -> anyhow::Result<()> {
    let Some(mut fixture) = pool_event_sync_fixture().await? else {
        return Ok(());
    };
    let result = Box::pin(async {
        let (dex, config, erc20, hypersync) = dependencies(&fixture)?;
        let required = HashSet::from([PoolIdentifier::from_address(Address::repeat_byte(55))]);
        let cancellation = CancellationToken::new();
        let mut service = PoolDiscoveryService::new(
            Arc::clone(&fixture.chain),
            &mut fixture.cache,
            &erc20,
            &hypersync,
            cancellation.clone(),
            config,
        );

        for selection in [None, Some(&required)] {
            let failed = stream::iter(vec![Err(anyhow::anyhow!(
                "injected provider stream failure"
            ))]);
            assert!(
                Box::pin(service.sync_pool_stream(&dex, 10, 12, selection, failed))
                    .await
                    .is_err()
            );
            assert_eq!(
                service.cache.get_dex_last_synced_block(&dex.name).await?,
                None
            );
        }
        assert!(
            Box::pin(service.sync_pool_stream(&dex, 10, 12, Some(&required), stream::empty()))
                .await
                .is_err()
        );
        cancellation.cancel();
        assert!(
            Box::pin(service.sync_pool_stream(&dex, 10, 12, None, stream::pending()))
                .await
                .is_err()
        );
        assert_eq!(
            service.cache.get_dex_last_synced_block(&dex.name).await?,
            None
        );
        Ok(())
    })
    .await;
    fixture.schema.cleanup().await?;
    result
}

#[rstest]
#[tokio::test]
async fn selected_discovery_reuses_only_valid_cached_universe() -> anyhow::Result<()> {
    let Some(mut fixture) = pool_event_sync_fixture().await? else {
        return Ok(());
    };
    let result = Box::pin(async {
        let (dex, config, erc20, hypersync) = dependencies(&fixture)?;
        let identifier = fixture.pool.pool_identifier;
        let mut service = PoolDiscoveryService::new(
            Arc::clone(&fixture.chain),
            &mut fixture.cache,
            &erc20,
            &hypersync,
            CancellationToken::new(),
            config,
        );
        Box::pin(service.sync_selected_pools(&dex, 10, 12, &[identifier])).await?;
        assert!(
            Box::pin(service.sync_selected_pools(&dex, 9, 9, &[identifier]))
                .await
                .is_err()
        );
        assert!(
            Box::pin(service.sync_selected_pools(&dex, 10, 12, &[identifier, identifier]))
                .await
                .is_err()
        );
        assert!(
            Box::pin(service.sync_selected_pools(&dex, 10, 12, &[]))
                .await
                .is_err()
        );
        assert_eq!(
            service.cache.get_dex_last_synced_block(&dex.name).await?,
            None
        );
        Ok(())
    })
    .await;
    fixture.schema.cleanup().await?;
    result
}
