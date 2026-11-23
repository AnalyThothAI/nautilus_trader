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

use std::{collections::VecDeque, sync::Arc, time::Duration};

use alloy::primitives::B256;
use anyhow::Context;
use nautilus_common::{
    clients::DataClient,
    defi::RequestPoolSnapshot,
    live::sender::EventSender,
    messages::{
        DataEvent,
        defi::{
            DefiDataCommand, DefiRequestCommand, DefiSubscribeCommand, DefiUnsubscribeCommand,
            PoolSnapshotResponse, SubscribeBlocks, SubscribePool, SubscribePoolFeeCollects,
            SubscribePoolFlashEvents, SubscribePoolLiquidityUpdates, SubscribePoolSwaps,
            UnsubscribeBlocks, UnsubscribePool, UnsubscribePoolFeeCollects,
            UnsubscribePoolFlashEvents, UnsubscribePoolLiquidityUpdates, UnsubscribePoolSwaps,
        },
    },
};
use nautilus_live::{
    SocketControlFactory,
    task::{TaskGroup, TaskGroupGuard},
};
use nautilus_model::{
    defi::{
        Block, DefiData, DexType, PoolIdentifier, SharedChain,
        data::{CanonicalAnchor, CanonicalStateInvalidated},
        validation::validate_address,
    },
    identifiers::{ClientId, Venue},
};
use ustr::Ustr;

use crate::{
    cache::BlockchainCache,
    config::BlockchainDataClientConfig,
    data::{
        block_hash_matches,
        core::BlockchainDataClientCore,
        subscription::{BlockFeedBackend, BlockFeedOwner},
    },
    exchanges::get_dex_extended,
    rpc::{
        BlockchainRpcClient,
        types::{BlockchainMessage, RpcEventType},
    },
};

const MAX_PENDING_POOL_MESSAGES: usize = 10_000;

/// A client for interacting with blockchain data from multiple sources.
///
/// The `BlockchainDataClient` serves as a facade that coordinates between different blockchain
/// data providers, caching mechanisms, and contract interactions. It provides a unified interface
/// for retrieving and processing blockchain data, particularly focused on DeFi protocols.
///
/// This client supports two primary data sources:
/// 1. Direct RPC connections to blockchain nodes (via WebSocket).
/// 2. HyperSync API for efficient historical data queries.
#[derive(Debug)]
pub struct BlockchainDataClient {
    /// The client ID used to identify this client with the data engine.
    pub client_id: ClientId,
    /// The blockchain being targeted by this client instance.
    pub chain: SharedChain,
    /// Configuration parameters for the blockchain data client.
    pub config: BlockchainDataClientConfig,
    /// The core client instance that handles blockchain operations.
    /// Wrapped in Option to allow moving it into the background processing task.
    pub core_client: Option<BlockchainDataClientCore>,
    socket_factory: SocketControlFactory,
    hypersync_rx: Option<tokio::sync::mpsc::UnboundedReceiver<BlockchainMessage>>,
    hypersync_tx: Option<tokio::sync::mpsc::UnboundedSender<BlockchainMessage>>,
    command_tx: tokio::sync::mpsc::UnboundedSender<DefiDataCommand>,
    command_rx: Option<tokio::sync::mpsc::UnboundedReceiver<DefiDataCommand>>,
    session_tasks: TaskGroup,
    canonical_failure: Arc<parking_lot::Mutex<Option<String>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl BlockchainDataClient {
    /// Creates a new [`BlockchainDataClient`] instance for the specified configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if DEX selections contain duplicates.
    pub fn new(client_id: ClientId, config: BlockchainDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;

        let chain = Arc::clone(&config.chain);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (hypersync_tx, hypersync_rx) = tokio::sync::mpsc::unbounded_channel();
        let socket_factory = SocketControlFactory::new(client_id, None);
        let session_tasks = TaskGroup::new();
        Ok(Self {
            client_id,
            chain,
            core_client: None,
            socket_factory,
            config,
            hypersync_rx: Some(hypersync_rx),
            hypersync_tx: Some(hypersync_tx),
            command_tx,
            command_rx: Some(command_rx),
            cancellation_token: session_tasks.cancellation_token(),
            session_tasks,
            canonical_failure: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    /// Spawns the main processing task that handles commands and blockchain data.
    ///
    /// This method creates a background task that:
    /// 1. Processes subscription/unsubscription commands from the command channel
    /// 2. Handles incoming blockchain data from HyperSync
    /// 3. Processes RPC messages if RPC client is configured
    /// 4. Routes processed data to subscribers
    fn spawn_process_task(
        &mut self,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<()>>> {
        anyhow::ensure!(self.command_rx.is_some(), "Command receiver already taken");
        if self.config.canonical_pool_data.is_some() {
            anyhow::ensure!(
                self.canonical_failure().is_none(),
                "Failed canonical source requires a new run"
            );
            return self.spawn_canonical_process_task();
        }

        let cancellation_token = self.cancellation_token.clone();

        let data_tx = nautilus_common::live::runner::get_data_event_sender();

        let hypersync_tx = self.hypersync_tx.clone();

        let mut core_client = BlockchainDataClientCore::new(
            self.config.clone(),
            hypersync_tx,
            Some(data_tx),
            cancellation_token.clone(),
        )?;
        let command_rx = self
            .command_rx
            .take()
            .expect("command receiver was checked");
        let mut hypersync_rx = self
            .hypersync_rx
            .take()
            .expect("HyperSync receiver is available before startup");
        self.hypersync_tx = None;
        core_client.set_socket_control(self.socket_factory.control("blockchain-rpc"));

        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let future = async move {
            log::debug!("Started task 'process'");

            if let Err(e) = core_client.connect().await {
                let _ = startup_tx.send(Err(e));
                return;
            }
            let _ = startup_tx.send(Ok(()));

            let mut command_rx = command_rx;
            let mut pending_pool_messages = VecDeque::new();

            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => {
                        log::debug!("Received cancellation signal in Blockchain data client process task");
                        core_client.disconnect().await;
                        break;
                    }
                    command = command_rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                DefiDataCommand::Subscribe(cmd) => {
                                    let chain = cmd.blockchain();
                                    if chain != core_client.chain.name {
                                        log::error!("Incorrect blockchain for subscribe command: {chain}");
                                        continue;
                                    }

                                      if let Err(e) = Self::handle_subscribe_command(cmd, &mut core_client).await{
                                        log::error!("Error processing subscribe command: {e}");
                                    }
                                }
                                DefiDataCommand::Unsubscribe(cmd) => {
                                    let chain = cmd.blockchain();
                                    if chain != core_client.chain.name {
                                        log::error!("Incorrect blockchain for subscribe command: {chain}");
                                        continue;
                                    }

                                    if let Err(e) = Self::handle_unsubscribe_command(cmd, &mut core_client).await{
                                        log::error!("Error processing subscribe command: {e}");
                                    }
                                }
                                DefiDataCommand::Request(cmd) => {
                                    if let Err(e) = Self::handle_request_command(cmd, &mut core_client).await {
                                        log::error!("Error processing request command: {e}");
                                    }
                                }
                            }
                        } else {
                            log::debug!("Command channel closed");
                            break;
                        }
                    }
                    data = hypersync_rx.recv() => {
                        if let Some(msg) = data {
                            Self::process_live_blockchain_message(
                                msg,
                                &mut core_client,
                                &mut pending_pool_messages,
                            )
                            .await;
                        } else {
                            log::debug!("HyperSync data channel closed");
                            break;
                        }
                    }
                    msg = async {
                        match core_client.rpc_client {
                            Some(ref mut rpc_client) => rpc_client.next_rpc_message().await,
                            None => std::future::pending().await,  // Never resolves
                        }
                    } => {
                        // This branch only fires when we actually receive a message
                        match msg {
                            Ok(msg) => {
                                Self::process_live_blockchain_message(
                                    msg,
                                    &mut core_client,
                                    &mut pending_pool_messages,
                                )
                                .await;
                            }
                            Err(e) => {
                                log::error!("Error processing RPC message: {e}");
                            }
                        }
                    }
                }
            }

            log::debug!("Stopped task 'process'");
        };

        self.session_tasks
            .spawn(future)
            .map_err(|e| anyhow::anyhow!("Failed to register blockchain process task: {e}"))?;
        Ok(startup_rx)
    }

    /// Starts the fixed-universe source and an independent, bounded head observer.
    /// Ends an owner or observer without treating sibling channel teardown as source failure.
    ///
    /// Prefer an already observed cancellation. The second check covers cancellation occurring
    /// while the work future is polled, before its watch-channel error is classified. Once an
    /// error is returned, callers latch it unconditionally, even if shutdown starts afterwards.
    async fn canonical_until_cancelled<F>(
        cancellation: &tokio_util::sync::CancellationToken,
        work: F,
    ) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = anyhow::Result<()>>,
    {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            result = work => {
                if cancellation.is_cancelled() { Ok(()) } else { result }
            }
        }
    }

    fn spawn_canonical_process_task(
        &mut self,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<()>>> {
        let data_tx = nautilus_common::live::runner::get_data_event_sender();
        let cancellation = self.cancellation_token.clone();
        let mut core = BlockchainDataClientCore::new(
            self.config.clone(),
            None,
            Some(data_tx.clone()),
            cancellation.clone(),
        )?;
        let mut commands = self
            .command_rx
            .take()
            .context("Canonical command receiver already taken")?;
        self.hypersync_rx.take();
        self.hypersync_tx.take();
        let http = Arc::clone(&core.http_rpc_client);
        let config = self
            .config
            .canonical_pool_data
            .clone()
            .context("Canonical source is not configured")?;
        let chain = self.chain.name;
        let chain_id = self.chain.chain_id;
        let client_id = self.client_id;
        let failure = Arc::clone(&self.canonical_failure);
        let observed_anchor = Arc::new(parking_lot::Mutex::new(None::<CanonicalAnchor>));
        let head_anchor = Arc::clone(&observed_anchor);
        let head_failure = Arc::clone(&failure);
        let head_cancel = cancellation.clone();
        let head_tx_data = data_tx.clone();
        let (head_tx, mut head_rx) = tokio::sync::watch::channel(None::<Block>);
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();

        self.session_tasks
            .spawn(async move {
                let result = async {
                    let mut interval =
                        tokio::time::interval(Duration::from_millis(config.poll_interval_ms));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut last_hash = None;

                    loop {
                        interval.tick().await;
                        let mut head = http.latest_block_header().await?;
                        head.set_chain(chain);
                        let hash = head.hash.parse::<B256>()?;
                        head.parent_hash.parse::<B256>()?;

                        if last_hash == Some(hash) {
                            continue;
                        }

                        // Deliver freshness before slow pool hydration, log acquisition or SQL work.
                        head_tx_data
                            .send(DataEvent::DeFi(DefiData::Block(head.clone())))
                            .map_err(|_| {
                                anyhow::anyhow!("Native head observation channel is closed")
                            })?;
                        head_tx.send(Some(head)).map_err(|_| {
                            anyhow::anyhow!("Canonical source head receiver is closed")
                        })?;
                        last_hash = Some(hash);
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), anyhow::Error>(())
                };

                if let Err(e) = Self::canonical_until_cancelled(&head_cancel, result).await {
                    Self::fail_canonical_source(
                        &head_failure,
                        &head_anchor,
                        &head_tx_data,
                        &head_cancel,
                        chain_id,
                        &e,
                    );
                }
            })
            .map_err(|e| anyhow::anyhow!("Failed to register canonical head observer: {e}"))?;

        self.session_tasks.spawn(async move {
            // connect() must not await native hydration requests: the runner may start only
            // after client startup completes. Readiness is established by the native cache.
            let connected = tokio::select! {
                () = cancellation.cancelled() => Err(anyhow::anyhow!("Canonical startup was cancelled")),
                connected = core.connect() => connected,
            };

            if let Err(e) = connected {
                let message = format!("Canonical source startup failed: {e:#}");
                Self::fail_canonical_source(&failure, &observed_anchor, &data_tx, &cancellation,
                    chain_id, &anyhow::anyhow!(message.clone()));
                let _ = startup_tx.send(Err(anyhow::anyhow!(message)));
                core.disconnect().await;
                return;
            }

            if startup_tx.send(Ok(())).is_err() {
                cancellation.cancel();
                core.disconnect().await;
                return;
            }

            let source = async {
                core.begin_canonical_feed(client_id).await?;
                *observed_anchor.lock() = core.canonical.as_ref().map(|state| state.anchor);
                let mut needs_check = true;

                loop {
                    let target = head_rx.borrow().clone();
                    let can_advance = needs_check && core.canonical_hydration_complete() && target.is_some();

                    tokio::select! {
                        biased;
                        command = commands.recv() => {
                            let command = command.context("Canonical native command channel is closed")?;
                            Box::pin(Self::handle_canonical_command(command, &mut core)).await?;
                            if core.canonical_hydration_complete() { needs_check = true; }
                        }
                        changed = head_rx.changed() => {
                            changed.context("Canonical head observer stopped")?;
                            needs_check = true;
                        }
                        () = tokio::task::yield_now(), if can_advance => {
                            let head = target.as_ref().context("Canonical target disappeared")?;
                            let advanced = core.advance_canonical_feed(head, client_id).await?;
                            let anchor = core.canonical.as_ref().map(|state| state.anchor);
                            *observed_anchor.lock() = anchor;
                            needs_check = advanced && anchor.is_some_and(|anchor| anchor.number < head.number);
                        }
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            };
            let result =
                Box::pin(Self::canonical_until_cancelled(&cancellation, source)).await;

            if let Err(e) = result {
                Self::fail_canonical_source(&failure, &observed_anchor, &data_tx, &cancellation,
                    chain_id, &e);
            } else if let Some(reason) = failure.lock().clone() {
                // A head failure can interrupt a long snapshot read. Invalidate the current
                // epoch again after cancellation, so no bootstrap queued in between survives.
                let epoch = core.canonical.as_ref().map_or(0, |state| state.anchor.epoch);
                let _ = data_tx.send(DataEvent::DeFi(DefiData::CanonicalInvalidation(
                    CanonicalStateInvalidated { chain_id, epoch, reason,
                        ts_init: nautilus_core::time::get_atomic_clock_realtime().get_time_ns() },
                )));
            }
            core.disconnect().await;
        }).map_err(|e| anyhow::anyhow!("Failed to register canonical source owner: {e}"))?;
        Ok(startup_rx)
    }

    fn fail_canonical_source(
        failure: &parking_lot::Mutex<Option<String>>,
        anchor: &parking_lot::Mutex<Option<CanonicalAnchor>>,
        data_tx: &EventSender<DataEvent>,
        cancellation: &tokio_util::sync::CancellationToken,
        chain_id: u32,
        e: &anyhow::Error,
    ) {
        let reason = {
            let mut failure = failure.lock();
            failure
                .get_or_insert_with(|| format!("Canonical source failed: {e:#}"))
                .clone()
        };
        let epoch = anchor.lock().map_or(0, |anchor| anchor.epoch);
        let _ = data_tx.send(DataEvent::DeFi(DefiData::CanonicalInvalidation(
            CanonicalStateInvalidated {
                chain_id,
                epoch,
                reason,
                ts_init: nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
            },
        )));
        cancellation.cancel();
    }

    /// Returns the first canonical source failure, retained through shutdown for run sealing.
    #[must_use]
    pub fn canonical_failure(&self) -> Option<String> {
        self.canonical_failure.lock().clone()
    }

    async fn handle_canonical_command(
        command: DefiDataCommand,
        core: &mut BlockchainDataClientCore,
    ) -> anyhow::Result<()> {
        let instrument = match command {
            DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(request)) => {
                if let Some(response) = core.canonical_snapshot_response(&request).await? {
                    core.send_data_checked(DataEvent::PoolSnapshotResponse(response))?;
                }
                return Ok(());
            }
            DefiDataCommand::Subscribe(DefiSubscribeCommand::Blocks(command)) => {
                anyhow::ensure!(
                    command.chain == core.chain.name,
                    "Canonical subscription chain mismatch"
                );
                return Ok(());
            }
            DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::Blocks(command)) => {
                anyhow::ensure!(
                    command.chain == core.chain.name,
                    "Canonical subscription chain mismatch"
                );
                return Ok(());
            }
            DefiDataCommand::Subscribe(command) => match command {
                DefiSubscribeCommand::Pool(command) => command.instrument_id,
                DefiSubscribeCommand::PoolSwaps(command) => command.instrument_id,
                DefiSubscribeCommand::PoolLiquidityUpdates(command) => command.instrument_id,
                DefiSubscribeCommand::PoolFeeCollects(command) => command.instrument_id,
                DefiSubscribeCommand::PoolFlashEvents(command) => command.instrument_id,
                DefiSubscribeCommand::Blocks(_) => unreachable!(),
            },
            DefiDataCommand::Unsubscribe(command) => match command {
                DefiUnsubscribeCommand::Pool(command) => command.instrument_id,
                DefiUnsubscribeCommand::PoolSwaps(command) => command.instrument_id,
                DefiUnsubscribeCommand::PoolLiquidityUpdates(command) => command.instrument_id,
                DefiUnsubscribeCommand::PoolFeeCollects(command) => command.instrument_id,
                DefiUnsubscribeCommand::PoolFlashEvents(command) => command.instrument_id,
                DefiUnsubscribeCommand::Blocks(_) => unreachable!(),
            },
        };
        anyhow::ensure!(
            core.config
                .canonical_pool_data
                .as_ref()
                .is_some_and(|config| config.pools.contains(&instrument)),
            "Canonical subscription is outside the fixed pool universe"
        );
        // Pool metadata and every mutation travel in bootstrap / complete batch observations.
        // Native subscription disposal removes consumers; it never changes the source universe.
        Ok(())
    }

    async fn process_live_blockchain_message(
        msg: BlockchainMessage,
        core_client: &mut BlockchainDataClientCore,
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
    ) {
        let is_block = matches!(&msg, BlockchainMessage::Block(_));
        let Some(msg) =
            Self::ready_live_blockchain_message(msg, &core_client.cache, pending_pool_messages)
        else {
            return;
        };

        if let Some(data) = Self::data_event_from_blockchain_message(msg, core_client).await {
            core_client.send_data(data);
        }

        if is_block {
            for data in Self::drain_pending_pool_messages(core_client, pending_pool_messages).await
            {
                core_client.send_data(data);
            }
        }
    }

    async fn drain_pending_pool_messages(
        core_client: &mut BlockchainDataClientCore,
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
    ) -> Vec<DataEvent> {
        let ready_messages = Self::drain_pending_pool_messages_with_cached_metadata(
            pending_pool_messages,
            &core_client.cache,
        );
        let mut data_events = Vec::with_capacity(ready_messages.len());

        for msg in ready_messages {
            if let Some(data) = Self::data_event_from_blockchain_message(msg, core_client).await {
                data_events.push(data);
            }
        }

        data_events
    }

    fn drain_pending_pool_messages_with_cached_metadata(
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
        cache: &BlockchainCache,
    ) -> Vec<BlockchainMessage> {
        let pending_count = pending_pool_messages.len();
        let mut ready_messages = Vec::new();

        for _ in 0..pending_count {
            let Some(msg) = pending_pool_messages.pop_front() else {
                break;
            };

            if Self::pool_event_missing_block_metadata(&msg, cache).is_some() {
                pending_pool_messages.push_back(msg);
                continue;
            }

            ready_messages.push(msg);
        }

        ready_messages
    }

    fn queue_pending_pool_message(
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
        msg: BlockchainMessage,
    ) {
        if pending_pool_messages.len() >= MAX_PENDING_POOL_MESSAGES
            && let Some(dropped_msg) = pending_pool_messages.pop_front()
            && let Some(block_number) = Self::pool_event_block_number(&dropped_msg)
        {
            log::warn!(
                "Dropping oldest live pool event waiting for block {block_number} metadata; pending buffer reached {MAX_PENDING_POOL_MESSAGES} messages"
            );
        }

        pending_pool_messages.push_back(msg);
    }

    fn ready_live_blockchain_message(
        msg: BlockchainMessage,
        cache: &BlockchainCache,
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
    ) -> Option<BlockchainMessage> {
        if let BlockchainMessage::PoolLogRemoved {
            pool_identifier,
            transaction_hash,
            log_index,
        } = &msg
        {
            Self::purge_pending_pool_messages(
                pending_pool_messages,
                pool_identifier,
                transaction_hash,
                *log_index,
            );
            return None;
        }

        if let Some(block_number) = Self::pool_event_missing_block_metadata(&msg, cache) {
            log::debug!("Deferring live pool event until block {block_number} metadata is cached");
            Self::queue_pending_pool_message(pending_pool_messages, msg);
            None
        } else {
            Some(msg)
        }
    }

    fn purge_pending_pool_messages(
        pending_pool_messages: &mut VecDeque<BlockchainMessage>,
        pool_identifier: &PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) {
        pending_pool_messages.retain(|msg| {
            let is_match = Self::pool_event_identity(msg).is_some_and(
                |(pending_pool_identifier, pending_transaction_hash, pending_log_index)| {
                    pending_pool_identifier == pool_identifier
                        && pending_transaction_hash.eq_ignore_ascii_case(transaction_hash)
                        && pending_log_index == log_index
                },
            );

            !is_match
        });
    }

    fn pool_event_identity(msg: &BlockchainMessage) -> Option<(&PoolIdentifier, &str, u32)> {
        match msg {
            BlockchainMessage::SwapEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::MintEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::BurnEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::CollectEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::FlashEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::FeeProtocolUpdateEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::FeeProtocolCollectEvent(event) => Some((
                &event.pool_identifier,
                event.transaction_hash.as_str(),
                event.log_index,
            )),
            BlockchainMessage::Block(_) | BlockchainMessage::PoolLogRemoved { .. } => None,
        }
    }

    fn pool_event_missing_block_metadata(
        msg: &BlockchainMessage,
        cache: &BlockchainCache,
    ) -> Option<u64> {
        let block_number = Self::pool_event_block_number(msg)?;
        let observed_hash = Self::pool_event_block_hash(msg);
        let hash_matches = cache
            .get_block_hash(block_number)
            .is_some_and(|cached| block_hash_matches(observed_hash, cached));
        (cache.get_block_timestamp(block_number).is_none() || !hash_matches).then_some(block_number)
    }

    fn pool_event_block_number(msg: &BlockchainMessage) -> Option<u64> {
        match msg {
            BlockchainMessage::SwapEvent(event) => Some(event.block_number),
            BlockchainMessage::MintEvent(event) => Some(event.block_number),
            BlockchainMessage::BurnEvent(event) => Some(event.block_number),
            BlockchainMessage::CollectEvent(event) => Some(event.block_number),
            BlockchainMessage::FlashEvent(event) => Some(event.block_number),
            BlockchainMessage::FeeProtocolUpdateEvent(event) => Some(event.block_number),
            BlockchainMessage::FeeProtocolCollectEvent(event) => Some(event.block_number),
            BlockchainMessage::Block(_) | BlockchainMessage::PoolLogRemoved { .. } => None,
        }
    }

    fn pool_event_block_hash(msg: &BlockchainMessage) -> Option<B256> {
        match msg {
            BlockchainMessage::SwapEvent(event) => event.block_hash,
            BlockchainMessage::MintEvent(event) => event.block_hash,
            BlockchainMessage::BurnEvent(event) => event.block_hash,
            BlockchainMessage::CollectEvent(event) => event.block_hash,
            BlockchainMessage::FlashEvent(event) => event.block_hash,
            BlockchainMessage::FeeProtocolUpdateEvent(event) => event.block_hash,
            BlockchainMessage::FeeProtocolCollectEvent(event) => event.block_hash,
            BlockchainMessage::Block(_) | BlockchainMessage::PoolLogRemoved { .. } => None,
        }
    }

    async fn data_event_from_blockchain_message(
        msg: BlockchainMessage,
        core_client: &mut BlockchainDataClientCore,
    ) -> Option<DataEvent> {
        match msg {
            BlockchainMessage::Block(block) => {
                if let Err(e) = core_client.cache.add_block(block.clone()).await {
                    log::error!("Failed to cache block {}: {e}", block.number);
                }

                Some(DataEvent::DeFi(DefiData::Block(block)))
            }
            BlockchainMessage::PoolLogRemoved { .. } => None,
            BlockchainMessage::SwapEvent(swap_event) => {
                match core_client.get_pool(&swap_event.pool_identifier) {
                    Ok(pool) => match core_client.process_pool_swap_event(&swap_event, pool) {
                        Ok(swap) => Some(DataEvent::DeFi(DefiData::PoolSwap(swap))),
                        Err(e) => {
                            log::error!("Error processing pool swap event: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            swap_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::BurnEvent(burn_event) => {
                match core_client.get_pool(&burn_event.pool_identifier) {
                    Ok(pool) => {
                        let dex_extended = get_dex_extended(core_client.chain.name, &pool.dex.name)
                            .expect("Failed to get dex extended");

                        match core_client.process_pool_burn_event(&burn_event, pool, dex_extended) {
                            Ok(update) => {
                                Some(DataEvent::DeFi(DefiData::PoolLiquidityUpdate(update)))
                            }
                            Err(e) => {
                                log::error!("Error processing pool burn event: {e}");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            burn_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::MintEvent(mint_event) => {
                match core_client.get_pool(&mint_event.pool_identifier) {
                    Ok(pool) => {
                        let dex_extended = get_dex_extended(core_client.chain.name, &pool.dex.name)
                            .expect("Failed to get dex extended");

                        match core_client.process_pool_mint_event(&mint_event, pool, dex_extended) {
                            Ok(update) => {
                                Some(DataEvent::DeFi(DefiData::PoolLiquidityUpdate(update)))
                            }
                            Err(e) => {
                                log::error!("Error processing pool mint event: {e}");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            mint_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::CollectEvent(collect_event) => {
                match core_client.get_pool(&collect_event.pool_identifier) {
                    Ok(pool) => {
                        let dex_extended = get_dex_extended(core_client.chain.name, &pool.dex.name)
                            .expect("Failed to get dex extended");

                        match core_client.process_pool_collect_event(
                            &collect_event,
                            pool,
                            dex_extended,
                        ) {
                            Ok(update) => Some(DataEvent::DeFi(DefiData::PoolFeeCollect(update))),
                            Err(e) => {
                                log::error!("Error processing pool collect event: {e}");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            collect_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::FlashEvent(flash_event) => {
                match core_client.get_pool(&flash_event.pool_identifier) {
                    Ok(pool) => match core_client.process_pool_flash_event(&flash_event, pool) {
                        Ok(flash) => Some(DataEvent::DeFi(DefiData::PoolFlash(flash))),
                        Err(e) => {
                            log::error!("Error processing pool flash event: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            flash_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::FeeProtocolUpdateEvent(update_event) => {
                match core_client.get_pool(&update_event.pool_identifier) {
                    Ok(pool) => match core_client
                        .process_pool_fee_protocol_update_event(&update_event, pool)
                    {
                        Ok(update) => {
                            Some(DataEvent::DeFi(DefiData::PoolFeeProtocolUpdate(update)))
                        }
                        Err(e) => {
                            log::error!("Error processing pool fee-protocol update event: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            update_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
            BlockchainMessage::FeeProtocolCollectEvent(collect_event) => {
                match core_client.get_pool(&collect_event.pool_identifier) {
                    Ok(pool) => match core_client
                        .process_pool_fee_protocol_collect_event(&collect_event, pool)
                    {
                        Ok(collect) => {
                            Some(DataEvent::DeFi(DefiData::PoolFeeProtocolCollect(collect)))
                        }
                        Err(e) => {
                            log::error!("Error processing pool fee-protocol collect event: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        log::error!(
                            "Failed to get pool {} with error {:?}",
                            collect_event.pool_identifier,
                            e
                        );
                        None
                    }
                }
            }
        }
    }

    /// Processes DeFi subscription commands to start receiving specific blockchain data.
    async fn handle_subscribe_command(
        command: DefiSubscribeCommand,
        core_client: &mut BlockchainDataClientCore,
    ) -> anyhow::Result<()> {
        match command {
            DefiSubscribeCommand::Blocks(_cmd) => {
                log::debug!("Processing subscribe blocks command");

                Self::subscribe_block_feed(core_client, BlockFeedOwner::Explicit).await?;

                Ok(())
            }
            DefiSubscribeCommand::Pool(cmd) => {
                log::debug!(
                    "Processing subscribe pool command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Invalid pool address '{}' failed with error: {:?}",
                                cmd.instrument_id,
                                e
                            )
                        })?;

                    core_client
                        .subscription_manager
                        .subscribe_pool(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;

                    log::debug!(
                        "Subscribed to all pool events for {} at address {}",
                        cmd.instrument_id,
                        pool_address
                    );
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiSubscribeCommand::PoolSwaps(cmd) => {
                log::debug!(
                    "Processing subscribe pool swaps command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Invalid pool swap address '{}' failed with error: {:?}",
                                cmd.instrument_id,
                                e
                            )
                        })?;
                    core_client
                        .subscription_manager
                        .subscribe_swaps(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiSubscribeCommand::PoolLiquidityUpdates(cmd) => {
                log::debug!(
                    "Processing subscribe pool liquidity updates command for address: {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!("Invalid pool swap address: {}", cmd.instrument_id)
                        })?;
                    core_client
                        .subscription_manager
                        .subscribe_burns(dex, pool_address);
                    core_client
                        .subscription_manager
                        .subscribe_mints(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiSubscribeCommand::PoolFeeCollects(cmd) => {
                log::debug!(
                    "Processing subscribe pool fee collects command for address: {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "Invalid pool fee collect address: {}",
                                cmd.instrument_id
                            )
                        })?;
                    core_client
                        .subscription_manager
                        .subscribe_collects(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiSubscribeCommand::PoolFlashEvents(cmd) => {
                log::debug!(
                    "Processing subscribe pool flash command for address: {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "Invalid pool flash subscribe address: {}",
                                cmd.instrument_id
                            )
                        })?;
                    core_client
                        .subscription_manager
                        .subscribe_flashes(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
        }
    }

    /// Processes DeFi unsubscription commands to stop receiving specific blockchain data.
    async fn handle_unsubscribe_command(
        command: DefiUnsubscribeCommand,
        core_client: &mut BlockchainDataClientCore,
    ) -> anyhow::Result<()> {
        match command {
            DefiUnsubscribeCommand::Blocks(_cmd) => {
                log::debug!("Processing unsubscribe blocks command");

                Self::unsubscribe_block_feed(core_client, BlockFeedOwner::Explicit).await?;

                Ok(())
            }
            DefiUnsubscribeCommand::Pool(cmd) => {
                log::debug!(
                    "Processing unsubscribe pool command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!("Invalid pool address: {}", cmd.instrument_id)
                        })?;

                    core_client
                        .subscription_manager
                        .unsubscribe_pool(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;

                    log::debug!(
                        "Unsubscribed from all pool events for {} at address {}",
                        cmd.instrument_id,
                        pool_address
                    );
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiUnsubscribeCommand::PoolSwaps(cmd) => {
                log::debug!("Processing unsubscribe pool swaps command");

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!("Invalid pool swap address: {}", cmd.instrument_id)
                        })?;
                    core_client
                        .subscription_manager
                        .unsubscribe_swaps(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiUnsubscribeCommand::PoolLiquidityUpdates(cmd) => {
                log::debug!(
                    "Processing unsubscribe pool liquidity updates command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!("Invalid pool swap address: {}", cmd.instrument_id)
                        })?;
                    core_client
                        .subscription_manager
                        .unsubscribe_burns(dex, pool_address);
                    core_client
                        .subscription_manager
                        .unsubscribe_mints(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiUnsubscribeCommand::PoolFeeCollects(cmd) => {
                log::debug!(
                    "Processing unsubscribe pool fee collects command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "Invalid pool fee collect address: {}",
                                cmd.instrument_id
                            )
                        })?;
                    core_client
                        .subscription_manager
                        .unsubscribe_collects(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
            DefiUnsubscribeCommand::PoolFlashEvents(cmd) => {
                log::debug!(
                    "Processing unsubscribe pool flash command for {}",
                    cmd.instrument_id
                );

                if let Ok((_, dex)) = cmd.instrument_id.venue.parse_dex() {
                    let pool_address = validate_address(cmd.instrument_id.symbol.as_str())
                        .map_err(|_| {
                            anyhow::anyhow!("Invalid pool flash address: {}", cmd.instrument_id)
                        })?;
                    core_client
                        .subscription_manager
                        .unsubscribe_flashes(dex, pool_address);
                    Self::update_rpc_pool_event_subscriptions(core_client, dex).await?;
                    Self::update_hypersync_pool_event_stream(core_client, dex).await?;
                } else {
                    anyhow::bail!(
                        "Invalid venue {}, expected Blockchain DEX format",
                        cmd.instrument_id.venue
                    )
                }

                Ok(())
            }
        }
    }

    async fn update_rpc_pool_event_subscriptions(
        core_client: &mut BlockchainDataClientCore,
        dex: DexType,
    ) -> anyhow::Result<()> {
        let updates = vec![
            (
                RpcEventType::PoolSwap(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_swap_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_swap_event_signature(&dex),
            ),
            (
                RpcEventType::PoolMint(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_mint_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_mint_event_signature(&dex),
            ),
            (
                RpcEventType::PoolBurn(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_burn_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_burn_event_signature(&dex),
            ),
            (
                RpcEventType::PoolCollect(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_collect_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_collect_event_signature(&dex),
            ),
            (
                RpcEventType::PoolFlash(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_flash_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_flash_event_signature(&dex),
            ),
            (
                RpcEventType::PoolFeeProtocolUpdate(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_fee_protocol_update_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_fee_protocol_update_event_signature(&dex),
            ),
            (
                RpcEventType::PoolFeeProtocolCollect(dex),
                core_client
                    .subscription_manager
                    .get_subscribed_pool_fee_protocol_collect_addresses(&dex),
                core_client
                    .subscription_manager
                    .get_dex_pool_fee_protocol_collect_event_signature(&dex),
            ),
        ];

        let has_pool_event_subscriptions = Self::has_active_pool_event_subscriptions(core_client);

        if core_client.rpc_client.is_none() {
            return Ok(());
        }

        if has_pool_event_subscriptions {
            Self::subscribe_block_feed(core_client, BlockFeedOwner::PoolEvents).await?;
        }

        if let Some(ref mut rpc) = core_client.rpc_client {
            for (event_type, addresses, event_signature) in updates {
                if let Some(event_signature) = event_signature {
                    rpc.subscribe_pool_events(event_type, &addresses, event_signature)
                        .await?;
                }
            }
        }

        if !has_pool_event_subscriptions {
            Self::unsubscribe_block_feed(core_client, BlockFeedOwner::PoolEvents).await?;
        }

        Ok(())
    }

    async fn update_hypersync_pool_event_stream(
        core_client: &mut BlockchainDataClientCore,
        dex: DexType,
    ) -> anyhow::Result<()> {
        if core_client.rpc_client.is_some() {
            return Ok(());
        }

        let addresses = core_client
            .subscription_manager
            .get_subscribed_dex_contract_addresses(&dex);
        let event_signatures = core_client
            .subscription_manager
            .get_active_subscribed_dex_event_signatures(&dex);
        let has_pool_event_subscriptions = Self::has_active_pool_event_subscriptions(core_client);

        if has_pool_event_subscriptions {
            Self::subscribe_block_feed(core_client, BlockFeedOwner::PoolEvents).await?;
        }

        core_client
            .hypersync_client
            .update_dex_event_stream(dex, addresses, event_signatures)
            .await;

        if !has_pool_event_subscriptions {
            Self::unsubscribe_block_feed(core_client, BlockFeedOwner::PoolEvents).await?;
        }

        Ok(())
    }

    fn has_active_pool_event_subscriptions(core_client: &BlockchainDataClientCore) -> bool {
        core_client
            .subscription_manager
            .has_pool_event_subscriptions()
    }

    async fn subscribe_block_feed(
        core_client: &mut BlockchainDataClientCore,
        owner: BlockFeedOwner,
    ) -> anyhow::Result<()> {
        let preferred_backend = if core_client.rpc_client.is_some() {
            BlockFeedBackend::Rpc
        } else {
            BlockFeedBackend::HyperSync
        };
        let Some(backend) = core_client
            .subscription_manager
            .add_block_demand(owner, preferred_backend)
        else {
            return Ok(());
        };

        let started_backend = match backend {
            BlockFeedBackend::Rpc => {
                let Some(rpc) = core_client.rpc_client.as_mut() else {
                    anyhow::bail!("RPC block feed selected without an RPC client")
                };

                match rpc.subscribe_blocks().await {
                    Ok(()) => {
                        log::debug!("Successfully subscribed to blocks via RPC");
                        BlockFeedBackend::Rpc
                    }
                    Err(e) if owner == BlockFeedOwner::Explicit => {
                        log::warn!(
                            "RPC blocks subscription failed: {e}, falling back to HyperSync"
                        );
                        core_client.hypersync_client.subscribe_blocks();
                        tokio::task::yield_now().await;
                        BlockFeedBackend::HyperSync
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            BlockFeedBackend::HyperSync => {
                log::debug!("Subscribing to blocks via HyperSync");
                core_client.hypersync_client.subscribe_blocks();
                tokio::task::yield_now().await;
                BlockFeedBackend::HyperSync
            }
        };

        core_client
            .subscription_manager
            .block_feed_started(started_backend);
        Ok(())
    }

    async fn unsubscribe_block_feed(
        core_client: &mut BlockchainDataClientCore,
        owner: BlockFeedOwner,
    ) -> anyhow::Result<()> {
        let Some(backend) = core_client.subscription_manager.remove_block_demand(owner) else {
            log::debug!("Keeping block subscription active while another owner remains");
            return Ok(());
        };

        match backend {
            BlockFeedBackend::Rpc => {
                let Some(rpc) = core_client.rpc_client.as_mut() else {
                    anyhow::bail!("RPC block feed active without an RPC client")
                };
                rpc.unsubscribe_blocks().await?;
                log::debug!("Unsubscribed from blocks via RPC");
            }
            BlockFeedBackend::HyperSync => {
                core_client.hypersync_client.unsubscribe_blocks().await;
                log::debug!("Unsubscribed from blocks via HyperSync");
            }
        }

        core_client.subscription_manager.block_feed_stopped(backend);
        Ok(())
    }

    /// Processes DeFi request commands to fetch specific blockchain data.
    async fn handle_request_command(
        command: DefiRequestCommand,
        core_client: &mut BlockchainDataClientCore,
    ) -> anyhow::Result<()> {
        match command {
            DefiRequestCommand::PoolSnapshot(cmd) => {
                log::debug!("Processing pool snapshot request for {}", cmd.instrument_id);

                let pool_address =
                    validate_address(cmd.instrument_id.symbol.as_str()).map_err(|e| {
                        anyhow::anyhow!(
                            "Invalid pool address '{}' failed with error: {:?}",
                            cmd.instrument_id,
                            e
                        )
                    })?;

                let pool_identifier =
                    PoolIdentifier::Address(Ustr::from(&pool_address.to_string()));

                match core_client.get_pool(&pool_identifier) {
                    Ok(pool) => {
                        let pool = Arc::clone(pool);
                        log::debug!("Found pool for snapshot request: {}", cmd.instrument_id);

                        // Send the pool definition
                        let pool_data = DataEvent::DeFi(DefiData::Pool(pool.as_ref().clone()));
                        core_client.send_data(pool_data);

                        match core_client
                            .bootstrap_latest_pool_profiler(&pool, None)
                            .await
                        {
                            Ok((profiler, already_valid)) => match profiler.extract_snapshot() {
                                Ok(snapshot) => {
                                    log::debug!(
                                        "Saving pool snapshot with {} positions and {} ticks to database...",
                                        snapshot.positions.len(),
                                        snapshot.ticks.len()
                                    );
                                    core_client
                                        .cache
                                        .add_pool_snapshot(
                                            &pool.dex.name,
                                            &pool.pool_identifier,
                                            &snapshot,
                                        )
                                        .await?;

                                    // If the snapshot is usable, send it back to the data engine.
                                    if core_client
                                        .check_snapshot_validity(&profiler, already_valid)
                                        .await?
                                        .is_usable()
                                    {
                                        let response =
                                            PoolSnapshotResponse::new(cmd.request_id, snapshot);
                                        core_client
                                            .send_data(DataEvent::PoolSnapshotResponse(response));
                                    }
                                }
                                Err(e) => log::error!(
                                    "Failed to extract snapshot for {}: {e}",
                                    cmd.instrument_id
                                ),
                            },
                            Err(e) => log::error!(
                                "Failed to bootstrap pool profiler for {} and extract snapshot with error {e}",
                                cmd.instrument_id
                            ),
                        }
                    }
                    Err(e) => {
                        log::warn!("Pool {} not found in cache: {e}", cmd.instrument_id);
                    }
                }

                Ok(())
            }
        }
    }

    /// Waits for the background processing task to complete.
    ///
    /// This method blocks until the spawned process task finishes execution,
    /// which typically happens after a shutdown signal is sent.
    ///
    /// # Errors
    ///
    /// Returns an error if bounded task shutdown fails.
    pub async fn await_process_task_close(&self) -> anyhow::Result<()> {
        self.session_tasks
            .finish_shutdown(Duration::from_secs(2), Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!("Blockchain process task shutdown failed: {e}"))?;
        if let Some(reason) = self.canonical_failure() {
            anyhow::bail!("{reason}");
        }
        Ok(())
    }

    async fn prepare_task_group(&mut self) -> anyhow::Result<()> {
        if !self.session_tasks.is_open() {
            self.session_tasks.begin_shutdown();
            self.await_process_task_close().await?;
            self.reset_channels();
            self.session_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start blockchain task generation: {e}"))?;
            self.cancellation_token = self.session_tasks.cancellation_token();
        }
        Ok(())
    }

    fn reset_channels(&mut self) {
        let (hypersync_tx, hypersync_rx) = tokio::sync::mpsc::unbounded_channel();
        self.hypersync_tx = Some(hypersync_tx);
        self.hypersync_rx = Some(hypersync_rx);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        self.command_tx = command_tx;
        self.command_rx = Some(command_rx);
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for BlockchainDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        // Blockchain data clients don't map to a single venue since they can provide
        // data for multiple DEXs across the blockchain
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting blockchain data client: chain_name={}, dex_ids={:?}, use_hypersync_for_live_data={}, proxy_url={:?}",
            self.chain.name,
            self.config.dex_ids,
            self.config.use_hypersync_for_live_data,
            self.config.proxy_url
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Stopping blockchain data client for '{chain_name}'",
            chain_name = self.chain.name
        );
        self.session_tasks.begin_shutdown();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Resetting blockchain data client for '{chain_name}'",
            chain_name = self.chain.name
        );
        self.session_tasks.begin_shutdown();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Disposing blockchain data client for '{chain_name}'",
            chain_name = self.chain.name
        );
        self.session_tasks.begin_shutdown();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;

        if self.session_tasks.is_open() && !self.session_tasks.is_empty() {
            return Ok(());
        }

        log::info!(
            "Connecting blockchain data client for '{}'",
            self.chain.name
        );

        self.prepare_task_group().await?;
        let setup_guard = TaskGroupGuard::new(&[&self.session_tasks], || {});
        let startup = self.spawn_process_task()?;
        if let Err(e) = startup
            .await
            .context("Blockchain process task stopped before startup")?
        {
            self.session_tasks.begin_shutdown();
            let teardown_result = self.await_process_task_close().await;
            self.reset_channels();

            if let Err(teardown_error) = teardown_result {
                return Err(e.context(format!(
                    "Blockchain data startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e);
        }

        setup_guard.disarm();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Disconnecting blockchain data client for '{}'",
            self.chain.name
        );

        self.session_tasks.begin_shutdown();
        let tasks_result = self.await_process_task_close().await;
        self.reset_channels();

        tasks_result
    }

    fn is_connected(&self) -> bool {
        self.canonical_failure().is_none()
            && self.session_tasks.is_open()
            && !self.session_tasks.is_empty()
            && !self.session_tasks.all_finished()
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    fn subscribe_blocks(&mut self, cmd: SubscribeBlocks) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::Blocks(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn subscribe_pool(&mut self, cmd: SubscribePool) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::Pool(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn subscribe_pool_swaps(&mut self, cmd: SubscribePoolSwaps) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::PoolSwaps(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn subscribe_pool_liquidity_updates(
        &mut self,
        cmd: SubscribePoolLiquidityUpdates,
    ) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::PoolLiquidityUpdates(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn subscribe_pool_fee_collects(&mut self, cmd: SubscribePoolFeeCollects) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::PoolFeeCollects(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn subscribe_pool_flash_events(&mut self, cmd: SubscribePoolFlashEvents) -> anyhow::Result<()> {
        let command = DefiDataCommand::Subscribe(DefiSubscribeCommand::PoolFlashEvents(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_blocks(&mut self, cmd: &UnsubscribeBlocks) -> anyhow::Result<()> {
        let command = DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::Blocks(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_pool(&mut self, cmd: &UnsubscribePool) -> anyhow::Result<()> {
        let command = DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::Pool(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_pool_swaps(&mut self, cmd: &UnsubscribePoolSwaps) -> anyhow::Result<()> {
        let command = DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::PoolSwaps(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_pool_liquidity_updates(
        &mut self,
        cmd: &UnsubscribePoolLiquidityUpdates,
    ) -> anyhow::Result<()> {
        let command =
            DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::PoolLiquidityUpdates(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_pool_fee_collects(
        &mut self,
        cmd: &UnsubscribePoolFeeCollects,
    ) -> anyhow::Result<()> {
        let command =
            DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::PoolFeeCollects(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn unsubscribe_pool_flash_events(
        &mut self,
        cmd: &UnsubscribePoolFlashEvents,
    ) -> anyhow::Result<()> {
        let command =
            DefiDataCommand::Unsubscribe(DefiUnsubscribeCommand::PoolFlashEvents(cmd.clone()));
        self.command_tx.send(command)?;
        Ok(())
    }

    fn request_pool_snapshot(&self, cmd: RequestPoolSnapshot) -> anyhow::Result<()> {
        let command = DefiDataCommand::Request(DefiRequestCommand::PoolSnapshot(cmd));
        self.command_tx.send(command)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use alloy::primitives::{B256, I256, U160, U256, address};
    use nautilus_core::UnixNanos;
    use nautilus_model::defi::{Block, Blockchain, Chain, DexType, Pool, PoolIdentifier, Token};
    use rstest::rstest;

    use super::*;
    use crate::events::{
        burn::BurnEvent, collect::CollectEvent, fee_protocol_collect::FeeProtocolCollectEvent,
        fee_protocol_update::FeeProtocolUpdateEvent, flash::FlashEvent, mint::MintEvent,
        swap::SwapEvent,
    };

    const WETH_USDT_CREATION_BLOCK: u64 = 12_375_326;

    fn fixture_hash(hash: &str) -> B256 {
        B256::from_str(hash).expect("fixture block hash must be valid")
    }

    #[rstest]
    fn pool_event_missing_block_metadata_clears_after_block_cache_update() {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let msg = flash_message(42);

        assert_eq!(
            BlockchainDataClient::pool_event_block_number(&msg),
            Some(42)
        );
        assert_eq!(
            BlockchainDataClient::pool_event_missing_block_metadata(&msg, &cache),
            Some(42)
        );

        let block = test_block(
            42,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block)
            .expect("test block hash must be valid");

        assert_eq!(
            BlockchainDataClient::pool_event_missing_block_metadata(&msg, &cache),
            None
        );
    }

    #[rstest]
    fn queue_pending_pool_message_drops_oldest_entry_at_cap() {
        let mut pending = VecDeque::new();

        for block_number in 0..MAX_PENDING_POOL_MESSAGES {
            BlockchainDataClient::queue_pending_pool_message(
                &mut pending,
                swap_message(block_number as u64),
            );
        }

        BlockchainDataClient::queue_pending_pool_message(
            &mut pending,
            swap_message(MAX_PENDING_POOL_MESSAGES as u64),
        );

        assert_eq!(pending.len(), MAX_PENDING_POOL_MESSAGES);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(pending.front().unwrap()),
            Some(1)
        );
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(pending.back().unwrap()),
            Some(MAX_PENDING_POOL_MESSAGES as u64)
        );
    }

    #[rstest]
    fn drain_pending_pool_messages_releases_events_after_block_metadata_is_cached() {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let block_42 = test_block(
            42,
            "0x4242424242424242424242424242424242424242424242424242424242424242",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block_42)
            .expect("test block hash must be valid");
        let mut pending = VecDeque::from([flash_message(41), flash_message(42)]);

        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert_eq!(ready_messages.len(), 1);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(&ready_messages[0]),
            Some(42)
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(pending.front().unwrap()),
            Some(41)
        );

        let block_41 = test_block(
            41,
            "0x4141414141414141414141414141414141414141414141414141414141414141",
            1_700_000_000_000_000_001,
        );
        cache
            .cache_block_metadata(&block_41)
            .expect("test block hash must be valid");

        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert_eq!(ready_messages.len(), 1);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(&ready_messages[0]),
            Some(41)
        );
        assert!(pending.is_empty());
    }

    #[rstest]
    fn ready_live_blockchain_message_queues_until_block_metadata_is_cached() {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let mut pending = VecDeque::new();

        let message = BlockchainDataClient::ready_live_blockchain_message(
            flash_message(42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert_eq!(pending.len(), 1);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(pending.front().unwrap()),
            Some(42)
        );

        let block = test_block(
            42,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block)
            .expect("test block hash must be valid");

        let message = BlockchainDataClient::ready_live_blockchain_message(
            BlockchainMessage::Block(block),
            &cache,
            &mut pending,
        );
        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert!(matches!(message, Some(BlockchainMessage::Block(_))));
        assert_eq!(ready_messages.len(), 1);
        assert_eq!(
            BlockchainDataClient::pool_event_block_number(&ready_messages[0]),
            Some(42)
        );
        assert!(pending.is_empty());
    }

    #[rstest]
    #[case(swap_message_with_identity)]
    #[case(mint_message_with_identity)]
    #[case(burn_message_with_identity)]
    #[case(collect_message_with_identity)]
    #[case(flash_message_with_identity)]
    #[case(fee_protocol_update_message_with_identity)]
    #[case(fee_protocol_collect_message_with_identity)]
    fn pool_event_with_mismatched_block_hash_defers_until_matching_metadata_arrives(
        #[case] message_with_identity: fn(
            u64,
            Option<&str>,
            PoolIdentifier,
            &str,
            u32,
        ) -> BlockchainMessage,
    ) {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let mut pending = VecDeque::new();
        let pool_identifier = weth_usdt_pool().pool_identifier;

        let superseded = test_block(
            42,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&superseded)
            .expect("test block hash must be valid");

        let message = BlockchainDataClient::ready_live_blockchain_message(
            message_with_identity(
                42,
                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                pool_identifier,
                "0xdeadbeef",
                7,
            ),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert_eq!(pending.len(), 1);

        let canonical = test_block(
            42,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            1_700_000_000_000_000_001,
        );
        cache
            .cache_block_metadata(&canonical)
            .expect("test block hash must be valid");
        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert_eq!(ready_messages.len(), 1);
        let expected_observed =
            fixture_hash("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        assert_eq!(
            BlockchainDataClient::pool_event_block_hash(&ready_messages[0]),
            Some(expected_observed)
        );
        let expected_cached =
            fixture_hash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(cache.get_block_hash(42), Some(expected_cached));
        assert_eq!(
            cache.get_block_timestamp(42),
            Some(&UnixNanos::from(1_700_000_000_000_000_001))
        );
        assert!(pending.is_empty());

        let message = BlockchainDataClient::ready_live_blockchain_message(
            message_with_identity(
                42,
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                pool_identifier,
                "0xf00",
                8,
            ),
            &cache,
            &mut pending,
        );

        assert!(message.is_some());
        assert!(pending.is_empty());
    }

    #[rstest]
    #[case(swap_message_with_identity)]
    #[case(mint_message_with_identity)]
    #[case(burn_message_with_identity)]
    #[case(collect_message_with_identity)]
    #[case(flash_message_with_identity)]
    #[case(fee_protocol_update_message_with_identity)]
    #[case(fee_protocol_collect_message_with_identity)]
    fn removed_pool_log_purges_matching_pending_message_before_metadata_release(
        #[case] message_with_identity: fn(
            u64,
            Option<&str>,
            PoolIdentifier,
            &str,
            u32,
        ) -> BlockchainMessage,
    ) {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let mut pending = VecDeque::new();
        let pool_identifier = weth_usdt_pool().pool_identifier;

        let message = BlockchainDataClient::ready_live_blockchain_message(
            message_with_identity(42, None, pool_identifier, "0xdeadbeef", 42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert_eq!(pending.len(), 1);

        let message = BlockchainDataClient::ready_live_blockchain_message(
            removed_pool_message(pool_identifier, "0xdeadbeef", 42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert!(pending.is_empty());

        let block = test_block(
            42,
            "0x4242424242424242424242424242424242424242424242424242424242424242",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block)
            .expect("test block hash must be valid");
        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert!(ready_messages.is_empty());
        assert!(pending.is_empty());
    }

    #[rstest]
    fn removed_pool_log_purges_only_exact_identity_and_preserves_order() {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let pool_identifier = weth_usdt_pool().pool_identifier;
        let other_pool_identifier =
            PoolIdentifier::from_address(address!("2222222222222222222222222222222222222222"));
        let mut pending = VecDeque::from([
            swap_message_with_identity(42, None, other_pool_identifier, "0xdeadbeef", 42),
            swap_message_with_identity(42, None, pool_identifier, "0xdeadbeef", 42),
            swap_message_with_identity(42, None, pool_identifier, "0xdeadbef0", 42),
            swap_message_with_identity(42, None, pool_identifier, "0xdeadbeef", 43),
        ]);
        let expected = vec![
            (other_pool_identifier, "0xdeadbeef", 42),
            (pool_identifier, "0xdeadbef0", 42),
            (pool_identifier, "0xdeadbeef", 43),
        ];

        let message = BlockchainDataClient::ready_live_blockchain_message(
            removed_pool_message(pool_identifier, "0xDEADBEEF", 42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        let pending_identities: Vec<_> = pending.iter().map(swap_identity).collect();
        assert_eq!(pending_identities, expected);

        let block = test_block(
            42,
            "0x4242424242424242424242424242424242424242424242424242424242424242",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block)
            .expect("test block hash must be valid");
        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        let ready_identities: Vec<_> = ready_messages.iter().map(swap_identity).collect();
        assert_eq!(ready_identities, expected);
        assert!(pending.is_empty());
    }

    #[rstest]
    fn removed_pool_log_without_pending_match_is_noop() {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let mut cache = BlockchainCache::new(chain);
        let mut pending = VecDeque::new();
        let pool_identifier = weth_usdt_pool().pool_identifier;

        let message = BlockchainDataClient::ready_live_blockchain_message(
            removed_pool_message(pool_identifier, "0xdeadbeef", 42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert!(pending.is_empty());

        let message = BlockchainDataClient::ready_live_blockchain_message(
            swap_message_with_identity(42, None, pool_identifier, "0xdeadbeef", 42),
            &cache,
            &mut pending,
        );

        assert!(message.is_none());
        assert_eq!(pending.len(), 1);

        let block = test_block(
            42,
            "0x4242424242424242424242424242424242424242424242424242424242424242",
            1_700_000_000_000_000_000,
        );
        cache
            .cache_block_metadata(&block)
            .expect("test block hash must be valid");
        let ready_messages = BlockchainDataClient::drain_pending_pool_messages_with_cached_metadata(
            &mut pending,
            &cache,
        );

        assert_eq!(ready_messages.len(), 1);
        assert_eq!(
            swap_identity(&ready_messages[0]),
            (pool_identifier, "0xdeadbeef", 42)
        );
        assert!(pending.is_empty());
    }

    fn removed_pool_message(
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        BlockchainMessage::PoolLogRemoved {
            pool_identifier,
            transaction_hash: transaction_hash.to_string(),
            log_index,
        }
    }

    fn swap_message(block_number: u64) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        swap_message_with_identity(block_number, None, pool.pool_identifier, "0x1", 0)
    }

    fn swap_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::SwapEvent(SwapEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            address,
            I256::ZERO,
            I256::ZERO,
            U160::ZERO,
            0,
            0,
        ))
    }

    fn swap_identity(msg: &BlockchainMessage) -> (PoolIdentifier, &str, u32) {
        let BlockchainMessage::SwapEvent(event) = msg else {
            panic!("expected swap message");
        };

        (
            event.pool_identifier,
            &event.transaction_hash,
            event.log_index,
        )
    }

    fn test_block(number: u64, hash: &str, timestamp: u64) -> Block {
        Block::new(
            hash.to_string(),
            "0x0".to_string(),
            number,
            Ustr::from("0x0000000000000000000000000000000000000000"),
            30_000_000,
            21_000,
            UnixNanos::from(timestamp),
            Some(Blockchain::Ethereum),
        )
    }

    fn flash_message(block_number: u64) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        flash_message_with_identity(block_number, None, pool.pool_identifier, "0x1", 0)
    }

    fn flash_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::FlashEvent(FlashEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            address,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
        ))
    }

    fn mint_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::MintEvent(MintEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            address,
            0,
            0,
            0,
            U256::ZERO,
            U256::ZERO,
        ))
    }

    fn burn_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::BurnEvent(BurnEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            0,
            0,
            0,
            U256::ZERO,
            U256::ZERO,
        ))
    }

    fn collect_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::CollectEvent(CollectEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            address,
            0,
            0,
            0,
            0,
        ))
    }

    fn fee_protocol_update_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();

        BlockchainMessage::FeeProtocolUpdateEvent(FeeProtocolUpdateEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            0,
            0,
        ))
    }

    fn fee_protocol_collect_message_with_identity(
        block_number: u64,
        block_hash: Option<&str>,
        pool_identifier: PoolIdentifier,
        transaction_hash: &str,
        log_index: u32,
    ) -> BlockchainMessage {
        let pool = weth_usdt_pool();
        let address = address!("1111111111111111111111111111111111111111");

        BlockchainMessage::FeeProtocolCollectEvent(FeeProtocolCollectEvent::new(
            Arc::clone(&pool.dex),
            pool_identifier,
            block_number,
            block_hash.map(fixture_hash),
            transaction_hash.to_string(),
            0,
            log_index,
            address,
            address,
            0,
            0,
        ))
    }

    fn weth_usdt_pool() -> Arc<Pool> {
        let chain = Arc::new(
            Chain::from_chain_id(1)
                .expect("Ethereum chain should exist")
                .clone(),
        );
        let dex = Arc::clone(
            &get_dex_extended(chain.name, &DexType::UniswapV3)
                .expect("Ethereum UniswapV3 should be registered")
                .dex,
        );
        let pool_address = address!("4e68ccd3e89f51c3074ca5072bbac773960dfa36");
        let token0 = Token::new(
            Arc::clone(&chain),
            address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            "Wrapped Ether".to_string(),
            "WETH".to_string(),
            18,
        );
        let token1 = Token::new(
            Arc::clone(&chain),
            address!("dAC17F958D2ee523a2206206994597C13D831ec7"),
            "Tether USD".to_string(),
            "USDT".to_string(),
            6,
        );

        Arc::new(Pool::new(
            chain,
            dex,
            pool_address,
            PoolIdentifier::from_address(pool_address),
            WETH_USDT_CREATION_BLOCK,
            token0,
            token1,
            Some(3_000),
            Some(60),
            UnixNanos::default(),
        ))
    }
}

#[cfg(test)]
#[path = "canonical_tests.rs"]
mod canonical_tests;
