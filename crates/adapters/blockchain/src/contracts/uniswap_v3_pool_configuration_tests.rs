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

use alloy::primitives::{Bytes, aliases::U24};
use axum::{Json, Router, routing::post};
use nautilus_model::defi::{AmmType, Chain, Dex, DexType, PoolIdentifier, Token};
use parking_lot::Mutex;
use rstest::rstest;
use serde_json::{Value, json};

use super::*;

fn pool() -> Pool {
    let chain = Arc::new(Chain::from_chain_id(1).unwrap().clone());
    let dex = Arc::new(Dex::new(
        chain.as_ref().clone(),
        DexType::UniswapV3,
        &Address::repeat_byte(10).to_string(),
        12_369_621,
        AmmType::CLAMM,
        "PoolCreated(address,address,uint24,int24,address)",
        "Swap(address,address,int256,int256,uint160,uint128,int24)",
        "Mint(address,address,int24,int24,uint128,uint256,uint256)",
        "Burn(address,int24,int24,uint128,uint256,uint256)",
        "Collect(address,address,int24,int24,uint128,uint128)",
    ));
    let token0 = Token::new(
        Arc::clone(&chain),
        Address::repeat_byte(20),
        "First".into(),
        "ONE".into(),
        18,
    );
    let token1 = Token::new(
        Arc::clone(&chain),
        Address::repeat_byte(30),
        "Second".into(),
        "TWO".into(),
        6,
    );
    let address = Address::repeat_byte(40);
    Pool::new(
        chain,
        Arc::clone(&dex),
        address,
        PoolIdentifier::from_address(address),
        12_369_622,
        token0,
        token1,
        Some(500),
        Some(10),
        UnixNanos::default(),
    )
}

fn responses(pool: &Pool) -> Vec<Multicall3::Result> {
    [
        UniswapV3Pool::token0Call::abi_encode_returns(&pool.token0.address),
        UniswapV3Pool::token1Call::abi_encode_returns(&pool.token1.address),
        UniswapV3Pool::feeCall::abi_encode_returns(&U24::from(pool.fee.unwrap())),
        UniswapV3Pool::tickSpacingCall::abi_encode_returns(
            &I24::try_from(pool.tick_spacing.unwrap() as i32).unwrap(),
        ),
        UniswapV3Pool::factoryCall::abi_encode_returns(&pool.dex.factory),
    ]
    .into_iter()
    .map(|bytes| Multicall3::Result {
        success: true,
        returnData: Bytes::from(bytes),
    })
    .collect()
}

#[rstest]
#[case("token0")]
#[case("token1")]
#[case("fee")]
#[case("tickSpacing")]
#[case("factory")]
fn immutable_configuration_rejects_each_discovered_field_mismatch(#[case] field: &str) {
    let expected = pool();
    let mut discovered = expected.clone();
    match field {
        "token0" => discovered.token0.address = Address::repeat_byte(41),
        "token1" => discovered.token1.address = Address::repeat_byte(41),
        "fee" => discovered.fee = Some(3_000),
        "tickSpacing" => discovered.tick_spacing = Some(60),
        "factory" => Arc::make_mut(&mut discovered.dex).factory = Address::repeat_byte(41),
        _ => unreachable!(),
    }
    let result = verify_pool_configuration_results(&discovered, &responses(&expected));
    assert!(
        matches!(result, Err(UniswapV3PoolError::CallFailed { field: failed, .. }) if failed == field)
    );
}

#[rstest]
fn immutable_configuration_rejects_partial_failed_or_malformed_responses() {
    let pool = pool();
    let complete = responses(&pool);
    assert!(verify_pool_configuration_results(&pool, &complete).is_ok());
    for length in 0..5 {
        assert!(verify_pool_configuration_results(&pool, &complete[..length]).is_err());
    }

    for index in 0..5 {
        let mut failed = complete.clone();
        failed[index].success = false;
        assert!(verify_pool_configuration_results(&pool, &failed).is_err());
        let mut malformed = complete.clone();
        malformed[index].returnData = Bytes::new();
        assert!(verify_pool_configuration_results(&pool, &malformed).is_err());
    }
}

fn rpc_result(request: &Value, pool: &Pool, hash: B256) -> anyhow::Result<Value> {
    anyhow::ensure!(request["method"] == "eth_call", "Unexpected RPC method");
    anyhow::ensure!(
        request["params"][1] == json!({"blockHash": hash, "requireCanonical": true}),
        "Metadata RPC was not pinned to the checkpoint hash"
    );
    let encoded = request["params"][0]["data"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing call data"))?;
    let call =
        Multicall3::tryAggregateCall::abi_decode(&hex::decode(encoded.trim_start_matches("0x"))?)?;
    let selectors = [
        UniswapV3Pool::token0Call::SELECTOR,
        UniswapV3Pool::token1Call::SELECTOR,
        UniswapV3Pool::feeCall::SELECTOR,
        UniswapV3Pool::tickSpacingCall::SELECTOR,
        UniswapV3Pool::factoryCall::SELECTOR,
    ];
    let encoded_results = responses(pool);
    let results = call
        .calls
        .into_iter()
        .map(|call| {
            anyhow::ensure!(call.target == pool.address, "Wrong metadata pool target");
            let index = selectors
                .iter()
                .position(|selector| call.callData.as_ref() == selector)
                .ok_or_else(|| anyhow::anyhow!("Unknown pool configuration getter"))?;
            Ok(encoded_results[index].clone())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!(hex::encode_prefixed(
        Multicall3::tryAggregateCall::abi_encode_returns(&results)
    )))
}

#[rstest]
#[case(2, 3)]
#[case(10, 1)]
#[tokio::test]
async fn immutable_configuration_uses_hash_for_every_multicall_chunk(
    #[case] chunk_size: u32,
    #[case] expected_requests: usize,
) -> anyhow::Result<()> {
    let pool = pool();
    let hash = B256::repeat_byte(99);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&requests);
    let expected_pool = pool.clone();
    let app = Router::new().route("/", post(move |Json(request): Json<Value>| {
        let observed = Arc::clone(&observed);
        let expected_pool = expected_pool.clone();
        async move {
            observed.lock().push(request.clone());
            Json(match rpc_result(&request, &expected_pool, hash) {
                Ok(result) => json!({"jsonrpc": "2.0", "id": request["id"], "result": result}),
                Err(e) => json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32000, "message": e.to_string()}}),
            })
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = async move { axum::serve(listener, app).await };

    let task = tokio::spawn(server); // tokio-import-ok: test-only runtime
    let rpc = Arc::new(BlockchainHttpRpcClient::new(
        format!("http://{address}"),
        Some(100),
        None,
    )?);
    let contract = UniswapV3PoolContract::new(rpc, chunk_size);
    let result = contract
        .verify_pool_configuration_at_hash(&pool, hash)
        .await;
    task.abort();
    result?;
    assert_eq!(requests.lock().len(), expected_requests);
    Ok(())
}
