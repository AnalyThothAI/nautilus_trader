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

use alloy::primitives::{B256, U256};
use nautilus_common::live::sender::EventSender;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_infrastructure::sql::pg::PostgresConnectOptions;
use nautilus_model::{
    defi::{Chain, SharedChain},
    identifiers::InstrumentId,
};
use parking_lot::Mutex;
use rstest::rstest;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::config::CanonicalPoolDataConfig;

fn canonical_config(
    chain: SharedChain,
    pools: Vec<InstrumentId>,
    url: String,
) -> BlockchainDataClientConfig {
    BlockchainDataClientConfig::builder()
        .chain(chain)
        .http_rpc_url(url.into())
        .hypersync_api_token("00000000-0000-4000-8000-000000000450".into())
        .postgres_cache_database_config(PostgresConnectOptions::new(
            "127.0.0.1".into(),
            1,
            "unused".into(),
            "unused".into(),
            "unused".into(),
        ))
        .canonical_pool_data(CanonicalPoolDataConfig {
            pools,
            start_block: 12,
            start_hash: B256::from(U256::from(12).to_be_bytes::<32>()),
            max_reorg_depth: 4,
            poll_interval_ms: 10,
            rpc_timeout_seconds: 10,
        })
        .build()
}

fn test_anchor() -> CanonicalAnchor {
    CanonicalAnchor {
        chain_id: 1,
        epoch: 7,
        number: 12,
        block_hash: B256::repeat_byte(12),
    }
}

#[rstest]
fn canonical_failure_retains_first_reason_cancels_and_fences_current_epoch() {
    let failure = Mutex::new(None);
    let anchor = Mutex::new(Some(test_anchor()));
    let cancellation = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let tx: EventSender<DataEvent> = tx.into();
    BlockchainDataClient::fail_canonical_source(
        &failure,
        &anchor,
        &tx,
        &cancellation,
        1,
        &anyhow::anyhow!("first failure"),
    );
    BlockchainDataClient::fail_canonical_source(
        &failure,
        &anchor,
        &tx,
        &cancellation,
        1,
        &anyhow::anyhow!("later failure"),
    );
    assert!(cancellation.is_cancelled());
    assert!(failure.lock().as_deref().unwrap().contains("first failure"));
    for _ in 0..2 {
        let DataEvent::DeFi(DefiData::CanonicalInvalidation(event)) = rx.try_recv().unwrap() else {
            panic!("A failure must publish native invalidation");
        };
        assert_eq!(event.epoch, test_anchor().epoch);
        assert!(event.reason.contains("first failure"));
        assert!(!event.reason.contains("later failure"));
    }
}

#[rstest]
fn canonical_failure_remains_latched_when_native_receiver_is_closed() {
    let failure = Mutex::new(None);
    let anchor = Mutex::new(Some(test_anchor()));
    let cancellation = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    drop(rx);
    BlockchainDataClient::fail_canonical_source(
        &failure,
        &anchor,
        &tx.into(),
        &cancellation,
        1,
        &anyhow::anyhow!("native receiver closed"),
    );
    assert!(cancellation.is_cancelled());
    assert!(failure.lock().is_some());
}

#[cfg(feature = "hypersync")]
#[rstest]
#[tokio::test]
async fn canonical_commands_reject_unconfigured_pool_universe() -> anyhow::Result<()> {
    let chain = Arc::new(Chain::from_chain_id(1).unwrap().clone());
    let allowed =
        InstrumentId::from("0x1111111111111111111111111111111111111111.Ethereum:UniswapV3");
    let outside =
        InstrumentId::from("0x2222222222222222222222222222222222222222.Ethereum:UniswapV3");
    let config = canonical_config(chain, vec![allowed], "http://127.0.0.1:9".into());
    let mut core = BlockchainDataClientCore::new(config, None, None, CancellationToken::new())?;
    for instrument_id in [allowed, outside] {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::Pool(SubscribePool::new(
            instrument_id,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )));
        assert_eq!(
            Box::pin(BlockchainDataClient::handle_canonical_command(
                command, &mut core
            ))
            .await
            .is_ok(),
            instrument_id == allowed,
        );
    }
    assert!(core.canonical.is_none());
    Ok(())
}

#[rstest]
#[case::owner_sees_sender_drop(true)]
#[case::observer_sees_receiver_drop(false)]
#[tokio::test]
async fn canonical_shutdown_does_not_latch_sibling_watch_close(
    #[case] drop_sender: bool,
) -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    let (head_tx, mut head_rx) = tokio::sync::watch::channel(0_u64);
    let result = BlockchainDataClient::canonical_until_cancelled(&cancellation, async {
        // Force the window after the biased cancellation branch was polled but before
        // the work branch returns its sibling-close error; no wall-clock race is needed.
        cancellation.cancel();

        if drop_sender {
            drop(head_tx);
            head_rx
                .changed()
                .await
                .context("Canonical head observer stopped")?;
        } else {
            drop(head_rx);
            head_tx
                .send(1)
                .context("Canonical source head receiver is closed")?;
        }
        Ok(())
    })
    .await;
    let failure = Mutex::new(None);
    let anchor = Mutex::new(Some(test_anchor()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    if let Err(e) = result {
        BlockchainDataClient::fail_canonical_source(
            &failure,
            &anchor,
            &sender.into(),
            &cancellation,
            1,
            &e,
        );
    }
    assert!(cancellation.is_cancelled());
    assert!(
        failure.lock().is_none(),
        "Ordinary shutdown cannot make canonical state Dirty"
    );
    assert!(
        receiver.try_recv().is_err(),
        "No failure observation is invented during shutdown"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn canonical_preexisting_cancellation_does_not_poll_the_work_future() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let polled = std::cell::Cell::new(false);
    let result = BlockchainDataClient::canonical_until_cancelled(&cancellation, async {
        polled.set(true);
        anyhow::bail!("Unobserved sibling channel closure")
    })
    .await;
    assert!(result.is_ok());
    assert!(
        !polled.get(),
        "Cancellation must win when both branches would be ready"
    );
}

#[rstest]
#[tokio::test]
async fn canonical_known_failure_is_not_erased_by_subsequent_cancellation() -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    let known = BlockchainDataClient::canonical_until_cancelled(&cancellation, async {
        anyhow::bail!("Known archive RPC failure")
    })
    .await
    .expect_err("A failure observed before cancellation must remain an error");
    cancellation.cancel();
    let failure = Mutex::new(None);
    let anchor = Mutex::new(Some(test_anchor()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sender = sender.into();
    BlockchainDataClient::fail_canonical_source(
        &failure,
        &anchor,
        &sender,
        &cancellation,
        1,
        &known,
    );
    let shutdown = BlockchainDataClient::canonical_until_cancelled(&cancellation, async {
        anyhow::bail!("Later shutdown channel error")
    })
    .await;
    assert!(shutdown.is_ok());
    assert!(
        failure
            .lock()
            .as_deref()
            .is_some_and(|reason| reason.contains("Known archive RPC failure"))
    );
    let DataEvent::DeFi(DefiData::CanonicalInvalidation(event)) = receiver.try_recv()? else {
        anyhow::bail!("The known failure must retain its native invalidation");
    };
    assert_eq!(event.epoch, test_anchor().epoch);
    assert!(event.reason.contains("Known archive RPC failure"));
    Ok(())
}

#[cfg(feature = "hypersync")]
mod source {
    use std::{net::SocketAddr, path::Path, sync::Arc};

    use alloy::{
        primitives::{Bytes, U160},
        sol_types::SolCall,
    };
    use axum::{Json, Router, extract::State, routing::post};
    use nautilus_core::hex;
    use nautilus_infrastructure::sql::pg::{get_postgres_connect_options, init_postgres};
    use nautilus_model::defi::{
        Block, Blockchain, Pool,
        data::{
            CanonicalPoolBootstrap,
            block::{BLOCK_SCOPED_SNAPSHOT_INDEX, BlockPosition},
        },
        pool_analysis::snapshot::{PoolAnalytics, PoolSnapshot, PoolState},
    };
    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};

    use super::*;
    use crate::{
        cache::{database::BlockchainCacheDatabase, tests::pool_event_sync_fixture},
        contracts::base::Multicall3,
    };

    #[derive(Default)]
    struct MockState {
        requests: Vec<Value>,
        bitmap_words: usize,
        fail_logs: bool,
        wrong_metadata: bool,
        pool: Option<Pool>,
    }

    struct MockRpc {
        address: SocketAddr,
        state: Arc<Mutex<MockState>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for MockRpc {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl MockRpc {
        async fn start(fail_logs: bool, pool: Pool) -> anyhow::Result<Self> {
            let state = Arc::new(Mutex::new(MockState {
                fail_logs,
                pool: Some(pool),
                ..Default::default()
            }));
            let app = Router::new()
                .route("/", post(handle))
                .with_state(Arc::clone(&state));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = async move { axum::serve(listener, app).await.unwrap() };

            let task = tokio::spawn(server); // tokio-import-ok: test-only runtime
            Ok(Self {
                address,
                state,
                task,
            })
        }

        fn url(&self) -> String {
            format!("http://{}", self.address)
        }
    }

    fn block_hash(number: u64) -> String {
        format!("0x{number:064x}")
    }

    fn block(number: u64, chain: Blockchain) -> Block {
        Block::new(
            block_hash(number),
            block_hash(number - 1),
            number,
            Ustr::from("0x0000000000000000000000000000000000000000"),
            30_000_000,
            0,
            UnixNanos::from((1_700_000_000 + number) * 1_000_000_000),
            Some(chain),
        )
        .with_base_fee(U256::from(10))
    }

    fn rpc_result(state: &mut MockState, request: &Value) -> anyhow::Result<Value> {
        state.requests.push(request.clone());
        match request["method"].as_str().context("Missing RPC method")? {
            "eth_getBlockByNumber" => {
                let number = request["params"][0]
                    .as_str()
                    .context("Missing block selector")?;
                let number = u64::from_str_radix(number.trim_start_matches("0x"), 16)?;
                Ok(json!({
                    "number": format!("0x{number:x}"),
                    "hash": block_hash(number),
                    "parentHash": block_hash(number - 1),
                    "miner": "0x0000000000000000000000000000000000000000",
                    "gasLimit": "0x1c9c380", "gasUsed": "0x0", "baseFeePerGas": "0xa",
                    "timestamp": format!("0x{:x}", 1_700_000_000 + number),
                }))
            }
            "eth_call" => {
                anyhow::ensure!(
                    request["params"][1]
                        == json!({
                            "blockHash": block_hash(12), "requireCanonical": true,
                        }),
                    "Every snapshot bitmap chunk must use the immutable canonical hash"
                );
                let input = request["params"][0]["data"]
                    .as_str()
                    .context("Missing call data")?;
                let bytes = hex::decode(input.trim_start_matches("0x"))?;
                let call = Multicall3::tryAggregateCall::abi_decode(&bytes)?;
                let pool = state
                    .pool
                    .as_ref()
                    .context("Missing mock pool metadata")?
                    .clone();
                let results = call
                    .calls
                    .iter()
                    .map(|call| {
                        let selector = call.callData.get(..4).context("Missing mock selector")?;
                        let is = |signature: &str| {
                            selector == &alloy::primitives::keccak256(signature)[..4]
                        };
                        let word = if is("tickBitmap(int16)") {
                            state.bitmap_words += 1;
                            U256::ZERO
                        } else if is("token0()") {
                            U256::from_be_slice(pool.token0.address.as_slice())
                        } else if is("token1()") {
                            U256::from_be_slice(pool.token1.address.as_slice())
                        } else if is("fee()") {
                            U256::from(
                                pool.fee.context("Missing fee")? + u32::from(state.wrong_metadata),
                            )
                        } else if is("tickSpacing()") {
                            U256::from(pool.tick_spacing.context("Missing tick spacing")?)
                        } else if is("factory()") {
                            U256::from_be_slice(pool.dex.factory.as_slice())
                        } else {
                            anyhow::bail!("Unexpected mock contract selector");
                        };
                        Ok(Multicall3::Result {
                            success: true,
                            returnData: Bytes::from(word.to_be_bytes::<32>().to_vec()),
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(json!(hex::encode_prefixed(
                    Multicall3::tryAggregateCall::abi_encode_returns(&results)
                )))
            }
            "eth_getLogs" => {
                anyhow::ensure!(!state.fail_logs, "Injected complete-log query failure");
                let filter = &request["params"][0];
                anyhow::ensure!(
                    filter["blockHash"] == block_hash(13),
                    "Logs must be pinned to block 13"
                );
                anyhow::ensure!(
                    filter.get("fromBlock").is_none() && filter.get("toBlock").is_none(),
                    "Hash-pinned logs cannot also use a mutable height range"
                );
                anyhow::ensure!(
                    !filter["address"]
                        .as_array()
                        .context("Missing pool universe")?
                        .is_empty(),
                    "Missing pool universe"
                );
                anyhow::ensure!(
                    filter["topics"][0]
                        .as_array()
                        .context("Missing event families")?
                        .len()
                        >= 7,
                    "Complete query omitted event families"
                );
                Ok(json!([]))
            }
            method => anyhow::bail!("Unexpected mock RPC method {method}"),
        }
    }

    async fn handle(
        State(state): State<Arc<Mutex<MockState>>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        let result = rpc_result(&mut state.lock(), &request);
        Json(match result {
            Ok(result) => json!({"jsonrpc": "2.0", "id": request["id"], "result": result}),
            Err(e) => json!({"jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32000, "message": e.to_string()}}),
        })
    }

    #[rstest]
    #[case::empty_complete_block(false, false, false)]
    #[case::query_failure_is_not_an_empty_batch(true, false, false)]
    #[case::wrong_pool_metadata_cannot_bootstrap(false, true, false)]
    #[case::wrong_initial_checkpoint_cannot_bootstrap(false, false, true)]
    #[tokio::test]
    async fn canonical_source_closes_native_requests_and_pg_coverage(
        #[case] fail_logs: bool,
        #[case] wrong_metadata: bool,
        #[case] wrong_checkpoint: bool,
    ) -> anyhow::Result<()> {
        let Some(mut fixture) = pool_event_sync_fixture().await? else {
            return Ok(());
        };
        let result: anyhow::Result<()> = Box::pin(async {
            let server = MockRpc::start(fail_logs, fixture.pool.clone()).await?;
            server.state.lock().wrong_metadata = wrong_metadata;
            let database = fixture.database().clone();
            database.ensure_canonical_schema().await?;
            let mut pool = fixture.pool.clone();
            pool.initialize(U160::from(1u128 << 96), 0);
            fixture.cache.add_pool(pool.clone()).await?;
            fixture
                .cache
                .add_pool_event_blocks_batch(vec![block(12, fixture.chain.name)])
                .await?;
            let snapshot = empty_checkpoint_snapshot(&pool);
            fixture
                .cache
                .add_pool_snapshot(&pool.dex.name, &pool.pool_identifier, &snapshot)
                .await?;
            database
                .set_pool_snapshot_validation_state_for_snapshot(
                    fixture.chain.chain_id,
                    &pool.pool_identifier,
                    &snapshot,
                    "on_chain",
                )
                .await?;
            let mut config = canonical_config(
                Arc::clone(&fixture.chain),
                vec![pool.instrument_id],
                server.url(),
            );

            if wrong_checkpoint {
                config.canonical_pool_data.as_mut().unwrap().start_hash = B256::repeat_byte(99);
            }
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            let mut core = BlockchainDataClientCore::new(
                config,
                None,
                Some(sender.into()),
                CancellationToken::new(),
            )?;
            core.cache = std::mem::replace(
                &mut fixture.cache,
                BlockchainCache::new(Arc::clone(&fixture.chain)),
            );
            let client_id = ClientId::from("CANONICAL-SOURCE-TEST");
            let began = core.begin_canonical_feed(client_id).await;
            if wrong_metadata || wrong_checkpoint {
                assert!(began.is_err());
                assert!(core.canonical.is_none());
                assert!(!core.canonical_hydration_complete());
                assert!(
                    receiver.try_recv().is_err(),
                    "Rejected bootstrap cannot publish native ready inputs"
                );
                let durable = database.canonical_state(fixture.chain.chain_id).await?;

                if wrong_checkpoint {
                    assert!(
                        durable.is_none(),
                        "Untrusted initial hash must not create a durable anchor"
                    );
                } else {
                    assert!(
                        durable.is_some_and(|state| state.recovering),
                        "Metadata rejection must retain the recovery fence"
                    );
                }
                core.disconnect().await;
                return Ok(());
            }
            began?;
            let DataEvent::DeFi(DefiData::CanonicalInvalidation(invalidation)) =
                receiver.try_recv()?
            else {
                anyhow::bail!("Source must invalidate before bootstrap");
            };
            let DataEvent::DeFi(DefiData::CanonicalBootstrap(bootstrap)) = receiver.try_recv()?
            else {
                anyhow::bail!("Source must publish the recorded native bootstrap");
            };
            assert_eq!(bootstrap.anchor.epoch, invalidation.epoch);
            assert_eq!(bootstrap.pools.len(), 1);
            assert!(!core.canonical_hydration_complete());

            let stale = RequestPoolSnapshot::new(
                pool.instrument_id,
                Some(client_id),
                UUID4::new(),
                UnixNanos::default(),
                None,
            )
            .with_canonical_anchor(bootstrap.anchor);
            Box::pin(BlockchainDataClient::handle_canonical_command(
                DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(stale)),
                &mut core,
            ))
            .await?;
            assert!(receiver.try_recv().is_err());
            assert!(!core.canonical_hydration_complete());

            let request = RequestPoolSnapshot::new(
                pool.instrument_id,
                Some(client_id),
                bootstrap.pools[0].request_id,
                UnixNanos::default(),
                None,
            )
            .with_canonical_anchor(bootstrap.anchor);
            Box::pin(BlockchainDataClient::handle_canonical_command(
                DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(request.clone())),
                &mut core,
            ))
            .await?;
            let DataEvent::PoolSnapshotResponse(response) = receiver.try_recv()? else {
                anyhow::bail!("Source must respond through the native correlated envelope");
            };
            assert_eq!(response.correlation_id, request.request_id);
            assert_eq!(response.canonical_anchor, Some(bootstrap.anchor));
            assert_eq!(
                response.data.block_position.block_hash.as_deref(),
                Some(block_hash(12).as_str())
            );
            assert!(core.canonical_hydration_complete());
            assert!(
                server.state.lock().bitmap_words > 600,
                "Every bitmap word must be queried"
            );
            let ready = database
                .canonical_state(fixture.chain.chain_id)
                .await?
                .unwrap();
            assert!(!ready.recovering);
            assert_eq!(ready.anchor, bootstrap.anchor);

            let advance = core
                .advance_canonical_feed(&block(13, fixture.chain.name), client_id)
                .await;

            if fail_logs {
                assert!(advance.is_err());
                assert!(receiver.try_recv().is_err());
                assert_eq!(
                    database
                        .canonical_state(fixture.chain.chain_id)
                        .await?
                        .unwrap()
                        .anchor,
                    bootstrap.anchor
                );
                assert!(
                    database
                        .canonical_block_hash(fixture.chain.chain_id, 13)
                        .await?
                        .is_none()
                );
            } else {
                assert!(advance?);
                let DataEvent::DeFi(DefiData::CanonicalBatch(batch)) = receiver.try_recv()? else {
                    anyhow::bail!("A complete query must publish one canonical batch");
                };
                assert!(batch.events.is_empty());
                assert_eq!(batch.pools, vec![pool.instrument_id]);
                assert_eq!(batch.anchor.number, 13);
                assert_eq!(
                    database
                        .canonical_state(fixture.chain.chain_id)
                        .await?
                        .unwrap()
                        .anchor,
                    batch.anchor
                );
            }
            core.disconnect().await;
            Ok(())
        })
        .await;
        fixture.schema.cleanup().await?;
        result
    }

    fn empty_checkpoint_snapshot(pool: &Pool) -> PoolSnapshot {
        PoolSnapshot::new(
            pool.instrument_id,
            PoolState {
                price_sqrt_ratio_x96: U160::from(1u128 << 96),
                current_tick: 0,
                ..Default::default()
            },
            vec![],
            vec![],
            PoolAnalytics::default(),
            BlockPosition::new(
                12,
                block_hash(12),
                BLOCK_SCOPED_SNAPSHOT_INDEX,
                BLOCK_SCOPED_SNAPSHOT_INDEX,
            )
            .with_block_hash(Some(block_hash(12))),
            UnixNanos::from(1_700_000_012_000_000_000),
            UnixNanos::from(1_700_000_012_000_000_000),
        )
    }

    async fn drop_restart_database(admin: &PgPool, name: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            name.starts_with("xarb_mev_test_restart_")
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "Refusing to clean a database outside this isolated test"
        );
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE \"{name}\" WITH (FORCE)"
        )))
        .execute(admin)
        .await?;
        sqlx::query(AssertSqlSafe(format!("DROP ROLE IF EXISTS \"{name}\"")))
            .execute(admin)
            .await?;
        Ok(())
    }

    async fn create_restart_database(admin: &PgPool) -> anyhow::Result<PostgresConnectOptions> {
        anyhow::ensure!(
            std::env::var_os("POSTGRES_PORT").is_some(),
            "The restart integration test requires an explicitly isolated PostgreSQL service"
        );
        let mut config = get_postgres_connect_options(None, None, None, None, None);
        config.database = format!(
            "xarb_mev_test_restart_{}",
            UUID4::new().to_string().replace('-', "")
        );
        sqlx::query(AssertSqlSafe(format!(
            "CREATE DATABASE \"{}\"",
            config.database
        )))
        .execute(admin)
        .await?;
        let result = async {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect_with(config.clone().into())
                .await?;
            let schema = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../schema/sql");
            let initialized = init_postgres(
                &pool,
                config.database.clone(),
                "isolated-canonical-test".into(),
                Some(schema.to_string_lossy().into_owned()),
            )
            .await;
            pool.close().await;
            initialized
        }
        .await;

        if let Err(e) = result {
            drop_restart_database(admin, &config.database).await?;
            return Err(e);
        }
        Ok(config)
    }

    async fn seed_restart_database(
        config: &PostgresConnectOptions,
        pool: &Pool,
    ) -> anyhow::Result<BlockchainCacheDatabase> {
        let database = BlockchainCacheDatabase::connect(config.clone().into()).await?;
        database.seed_chain(&pool.chain).await?;
        database.ensure_canonical_schema().await?;
        database.create_block_partition(&pool.chain).await?;
        database.create_token_partition(&pool.chain).await?;
        let mut cache = BlockchainCache::new(Arc::clone(&pool.chain));
        cache.database = Some(database.clone());
        cache.add_dex(Arc::clone(&pool.dex)).await?;
        cache.add_token(pool.token0.clone()).await?;
        cache.add_token(pool.token1.clone()).await?;
        cache.add_pool(pool.clone()).await?;
        cache
            .add_pool_event_blocks_batch(vec![
                block(pool.creation_block, pool.chain.name),
                block(12, pool.chain.name),
            ])
            .await?;
        let snapshot = empty_checkpoint_snapshot(pool);
        cache
            .add_pool_snapshot(&pool.dex.name, &pool.pool_identifier, &snapshot)
            .await?;
        database
            .set_pool_snapshot_validation_state_for_snapshot(
                pool.chain.chain_id,
                &pool.pool_identifier,
                &snapshot,
                "on_chain",
            )
            .await?;
        Ok(database)
    }

    async fn connect_and_begin(
        config: BlockchainDataClientConfig,
        client_id: ClientId,
    ) -> anyhow::Result<(
        BlockchainDataClientCore,
        tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
        CanonicalPoolBootstrap,
    )> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut core = BlockchainDataClientCore::new(
            config,
            None,
            Some(sender.into()),
            CancellationToken::new(),
        )?;
        Box::pin(core.connect()).await?;
        assert!(
            core.canonical.is_none(),
            "Connection cannot reuse durable Ready"
        );
        assert!(!core.canonical_hydration_complete());
        core.begin_canonical_feed(client_id).await?;
        let DataEvent::DeFi(DefiData::CanonicalInvalidation(invalidation)) = receiver.try_recv()?
        else {
            anyhow::bail!("Connected source must invalidate before bootstrap");
        };
        let DataEvent::DeFi(DefiData::CanonicalBootstrap(bootstrap)) = receiver.try_recv()? else {
            anyhow::bail!("Connected source must announce its new native hydration requests");
        };
        assert_eq!(invalidation.epoch, bootstrap.anchor.epoch);
        assert!(!core.canonical_hydration_complete());
        Ok((core, receiver, bootstrap))
    }

    fn hydration_request(
        bootstrap: &CanonicalPoolBootstrap,
        client_id: ClientId,
    ) -> RequestPoolSnapshot {
        RequestPoolSnapshot::new(
            bootstrap.pools[0].pool.instrument_id,
            Some(client_id),
            bootstrap.pools[0].request_id,
            UnixNanos::default(),
            None,
        )
        .with_canonical_anchor(bootstrap.anchor)
    }

    async fn hydrate_connected_core(
        core: &mut BlockchainDataClientCore,
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
        request: RequestPoolSnapshot,
    ) -> anyhow::Result<()> {
        let expected_anchor = request.canonical_anchor;
        let request_id = request.request_id;
        Box::pin(BlockchainDataClient::handle_canonical_command(
            DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(request)),
            core,
        ))
        .await?;
        let DataEvent::PoolSnapshotResponse(response) = receiver.try_recv()? else {
            anyhow::bail!("Hydration must return the native correlated response");
        };
        assert_eq!(response.canonical_anchor, expected_anchor);
        assert_eq!(response.correlation_id, request_id);
        assert!(core.canonical_hydration_complete());
        Ok(())
    }

    #[rstest]
    #[case::completed_previous_epoch(true)]
    #[case::interrupted_previous_epoch(false)]
    #[ignore = "requires isolated PostgreSQL CREATE DATABASE and CREATE ROLE privileges"]
    #[tokio::test]
    async fn canonical_reconnect_renews_epoch_after_complete_connect(
        #[case] complete_first: bool,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            std::env::var_os("POSTGRES_PORT").is_some(),
            "Set POSTGRES_PORT to an isolated PostgreSQL test service"
        );
        let fixture = pool_event_sync_fixture()
            .await?
            .context("Explicit PostgreSQL test service is unavailable")?;
        let pg_config = create_restart_database(&fixture.schema.admin_pool).await?;
        let result: anyhow::Result<()> = Box::pin(async {
            let mut pool = fixture.pool.clone();
            pool.initialize(U160::from(1u128 << 96), 0);
            let server = MockRpc::start(false, pool.clone()).await?;
            let database = seed_restart_database(&pg_config, &pool).await?;
            let mut config = canonical_config(
                Arc::clone(&fixture.chain),
                vec![pool.instrument_id],
                server.url(),
            );
            config.postgres_cache_database_config = Some(pg_config.clone());
            let client_id = ClientId::from("CANONICAL-RESTART-TEST");
            let (mut first, mut first_rx, first_bootstrap) =
                connect_and_begin(config.clone(), client_id).await?;
            let old_writer = first
                .cache
                .database
                .as_ref()
                .context("First connection has no database")?
                .clone();
            let old_request = hydration_request(&first_bootstrap, client_id);
            if complete_first {
                hydrate_connected_core(&mut first, &mut first_rx, old_request.clone()).await?;
            }
            let previous = database
                .canonical_state(fixture.chain.chain_id)
                .await?
                .context("First recovery state is missing")?;
            assert_eq!(previous.recovering, !complete_first);
            first.disconnect().await;
            drop(first);
            drop(first_rx);

            // This second call traverses the real public connect path, including
            // its database initialization and DEX metadata registration.
            let (mut second, mut second_rx, second_bootstrap) =
                connect_and_begin(config, client_id).await?;
            assert_eq!(
                second_bootstrap.anchor.epoch,
                first_bootstrap.anchor.epoch + 1
            );
            assert_eq!(
                second_bootstrap.anchor.block_hash,
                first_bootstrap.anchor.block_hash
            );
            assert_ne!(
                second_bootstrap.pools[0].request_id,
                first_bootstrap.pools[0].request_id
            );
            assert!(
                database
                    .canonical_state(fixture.chain.chain_id)
                    .await?
                    .context("Reconnected recovery state is missing")?
                    .recovering
            );
            Box::pin(BlockchainDataClient::handle_canonical_command(
                DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(old_request)),
                &mut second,
            ))
            .await?;
            assert!(
                second_rx.try_recv().is_err(),
                "Old request cannot satisfy new hydration"
            );
            assert!(!second.canonical_hydration_complete());
            assert!(old_writer.add_dex(Arc::clone(&pool.dex)).await.is_err());
            hydrate_connected_core(
                &mut second,
                &mut second_rx,
                hydration_request(&second_bootstrap, client_id),
            )
            .await?;
            let ready = database
                .canonical_state(fixture.chain.chain_id)
                .await?
                .context("Final recovery state is missing")?;
            assert!(!ready.recovering);
            assert_eq!(ready.anchor, second_bootstrap.anchor);
            second.disconnect().await;
            Ok(())
        })
        .await;
        let cleanup = drop_restart_database(&fixture.schema.admin_pool, &pg_config.database).await;
        fixture.schema.cleanup().await?;
        cleanup?;
        result
    }
}
