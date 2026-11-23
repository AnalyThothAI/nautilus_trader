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

//! Bounded transport evidence and opt-in public connection quality policy.
//!
//! Observations are diagnostic, may be dropped, and never replace connection state or order events.
//! Round trips start in the writer and finish in the reader; they include transport buffering,
//! scheduler delay and server Pong handling, not only network propagation.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use nautilus_core::time::get_atomic_clock_realtime;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{dst, transport::TransportError};

const TELEMETRY_CAPACITY: usize = 4096;
static NEXT_TELEMETRY_CLOCK_ID: AtomicU64 = AtomicU64::new(1);
const PROBE_PREFIX: &[u8; 4] = b"NTQ1";
const MAX_SAMPLES: usize = 8;

/// Policy for validating a public replacement before the existing controller installs it.
///
/// This is disabled unless explicitly supplied. It must only be used with public endpoints which
/// permit an unauthenticated, unsubscribed candidate. Proxy connections are observed but never
/// optimized. Candidate attempts consume the configured connection limiter and never rotate around
/// rejection responses. Ordinary failure recovery is independent of every limit below.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketRoutePolicy {
    /// Round trip threshold that every retained heartbeat sample must exceed.
    pub slow_round_trip_ms: u64,
    /// Consecutive heartbeat windows and candidate probes required (between 3 and 8).
    pub samples: usize,
    /// Minimum absolute improvement in the median round trip.
    pub minimum_improvement_ms: u64,
    /// Minimum relative improvement, as an integer percentage between 1 and 99.
    pub minimum_improvement_percent: u32,
    /// Minimum residence time on an installed connection.
    pub minimum_dwell_secs: u64,
    /// Cooldown after each candidate attempt, including failures.
    pub cooldown_secs: u64,
    /// Maximum optimization replacements in any rolling hour.
    pub maximum_replacements_per_hour: usize,
    /// Total deadline for dialing and validating one candidate.
    pub candidate_timeout_ms: u64,
}

impl Default for WebSocketRoutePolicy {
    fn default() -> Self {
        Self {
            slow_round_trip_ms: 50,
            samples: 3,
            minimum_improvement_ms: 10,
            minimum_improvement_percent: 25,
            minimum_dwell_secs: 300,
            cooldown_secs: 300,
            maximum_replacements_per_hour: 2,
            candidate_timeout_ms: 3_000,
        }
    }
}

impl WebSocketRoutePolicy {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if self.slow_round_trip_ms == 0
            || !(3..=MAX_SAMPLES).contains(&self.samples)
            || self.minimum_improvement_ms == 0
            || !(1..100).contains(&self.minimum_improvement_percent)
            || self.minimum_dwell_secs == 0
            || self.cooldown_secs == 0
            || !(1..=12).contains(&self.maximum_replacements_per_hour)
            || !(1..=30_000).contains(&self.candidate_timeout_ms)
        {
            return Err(TransportError::Other(
                "Invalid WebSocket route policy bounds".into(),
            ));
        }

        Ok(())
    }

    pub(crate) fn improves(&self, current_ns: u64, candidate_ns: u64) -> bool {
        current_ns.saturating_sub(candidate_ns)
            >= self.minimum_improvement_ms.saturating_mul(1_000_000)
            && u128::from(candidate_ns) * 100
                <= u128::from(current_ns) * u128::from(100 - self.minimum_improvement_percent)
    }
}

/// Measurements from the actual established socket, without URLs or authentication headers.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WebSocketConnectionDetails {
    /// Actual socket peer; for a proxy this is the proxy peer, not the upstream address.
    pub peer_addr: Option<SocketAddr>,
    /// Whether the socket uses a configured proxy.
    pub proxied: bool,
    /// TCP connect call duration, including hostname resolution when the resolver is internal.
    pub tcp_connect_ns: Option<u64>,
    /// TLS handshake duration when separately observed.
    pub tls_ns: Option<u64>,
    /// Proxy tunnel establishment duration, including DNS, TCP, CONNECT and any TLS.
    pub proxy_tunnel_ns: Option<u64>,
    /// WebSocket upgrade duration.
    pub upgrade_ns: Option<u64>,
    /// Sanitized CDN edge header, when returned by the successful upgrade.
    pub edge_label: Option<String>,
}

/// Outcome of one bounded public route evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketRouteResult {
    /// The same transport that passed the probes was installed.
    Installed,
    /// The candidate did not meet both improvement thresholds.
    NoImprovement,
    /// Candidate dialing, ping validation or the total deadline failed.
    ProbeFailed,
    /// The owning transport changed or stopped while evidence was being collected.
    StaleConnection,
    /// Configured proxy routing does not support candidate optimization.
    ProxyUnsupported,
}

/// Diagnostic event from a WebSocket transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebSocketObservationEvent {
    /// A writer has installed this connection; protocol recovery can still be pending.
    Connected { details: WebSocketConnectionDetails },
    /// A matching, unexpired Pong arrived on the owning connection.
    RoundTrip {
        token: u64,
        writer_started_ns: u64,
        pong_received_ns: u64,
        round_trip_ns: u64,
    },
    /// The send/flush future returned; it can return after the reader has observed the Pong.
    HeartbeatFlushed {
        token: u64,
        flush_returned_ns: u64,
        success: bool,
    },
    /// A bounded route evaluation completed.
    RouteEvaluation {
        result: WebSocketRouteResult,
        current_round_trip_ns: Option<u64>,
        candidate_round_trip_ns: Option<u64>,
    },
    /// Explicit adapter identity link, for example a client order ID to its signed transaction hash.
    Correlation {
        origin_id: String,
        request_id: String,
    },
    /// A native order event reached the execution bus after engine application.
    /// The enclosing epoch is the connection observed now, not the originating response epoch.
    NativeOrderApplied {
        client_order_id: String,
        event_id: String,
        event_type: String,
        terminal: bool,
    },
    /// A control response had no reliable transaction identity. It is never assigned by FIFO.
    UnmatchedResponse { stage: WebSocketSendStage },
    /// A transaction send reached an observed local stage. No payload is retained.
    Send {
        request_id: String,
        stage: WebSocketSendStage,
    },
}

/// Local send stage; none of these proves receipt or matching by the venue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketSendStage {
    /// Signing begins, identified by the client order ID until the hash exists.
    SigningStarted,
    /// Signing returned a signed transaction, identified by its hash.
    SigningFinished,
    /// Signing failed before a transaction hash could be established.
    SigningFailed,
    /// Waiting for the existing nonce sequencer; this does not add a nonce gate.
    NonceWaitStarted,
    /// The existing nonce sequencer permits this transaction to proceed.
    NonceReady,
    /// Waiting for the existing transaction-rate quota.
    QuotaWaitStarted,
    /// The existing transaction-rate quota permits dispatch.
    QuotaReady,
    /// An owned account order frame reached the native reader, before any decoding.
    RawOrderReceived,
    /// An owned account order frame finished typed adapter decoding.
    AdapterOrderDecoded,
    /// The existing native order dispatcher was entered; it can still defer publication.
    NativeOrderDispatchStarted,
    /// Adapter starts submitting its existing handler command, before any command-queue wait.
    AdapterEnqueueStarted,
    /// Submitted to the native writer queue.
    Enqueued,
    /// Writer started the actual transport send future.
    WriterStarted,
    /// Transport send/flush returned successfully.
    FlushReturned,
    /// Writer rejected or failed the send; the caller retains the native outcome semantics.
    Failed,
    /// Raw response frame reached the adapter's native reader callback.
    RawResponseReceived,
    /// Adapter decoded the response on its handler task.
    AdapterResponseDecoded,
}

/// A timestamp captured at the actual observation point.
/// Monotonic nanoseconds are relative to one telemetry collector's creation instant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketObservationTime {
    pub ts_init: u64,
    /// `None` explicitly marks a historical timestamp supplied with no monotonic counterpart.
    pub monotonic_ns: Option<u64>,
}

/// Timestamped diagnostic record scoped to one adapter generation and transport epoch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketObservation {
    /// Owner assigned by the live node socket registry (zero for standalone clients).
    pub owner_id: u64,
    /// Existing logical endpoint transport generation.
    pub generation: u64,
    pub connection_epoch: u64,
    /// Receipt timestamp in Unix nanoseconds, retained only for catalog/wall-clock correlation.
    pub ts_init: u64,
    /// Process-local collector ID. Compare monotonic timestamps only within this ID and run.
    pub monotonic_clock_id: u64,
    /// Nanoseconds from this collector's creation instant, shared by clones and generations.
    /// `None` means the original observation supplied only UTC; no later value is fabricated.
    #[serde(default)]
    pub monotonic_ns: Option<u64>,
    pub event: WebSocketObservationEvent,
}

#[derive(Debug)]
struct TelemetryInner {
    monotonic_origin: dst::time::Instant,
    monotonic_clock_id: u64,
    events: Mutex<VecDeque<WebSocketObservation>>,
    dropped: AtomicU64,
}

/// Nonblocking, bounded diagnostic collector shared with a runtime evidence consumer.
///
/// Hot paths use `try_lock`; contention and capacity exhaustion increment `dropped`. Consumers
/// must report that counter and must not use this lossy stream for trading or replay state.
#[derive(Clone, Debug)]
pub struct WebSocketTelemetry {
    inner: Arc<TelemetryInner>,
    owner_id: u64,
    generation: u64,
}

impl Default for WebSocketTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketTelemetry {
    /// Creates a collector with room for 4,096 observations.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TelemetryInner {
                monotonic_origin: dst::time::Instant::now(),
                monotonic_clock_id: NEXT_TELEMETRY_CLOCK_ID.fetch_add(1, Ordering::Relaxed),
                events: Mutex::new(VecDeque::with_capacity(TELEMETRY_CAPACITY)),
                dropped: AtomicU64::new(0),
            }),
            owner_id: 0,
            generation: 0,
        }
    }

    /// Captures both clocks now, before queueing, decoding or other work being measured.
    #[must_use]
    pub fn capture_time(&self) -> WebSocketObservationTime {
        let monotonic_ns = nanos(self.inner.monotonic_origin.elapsed());
        WebSocketObservationTime {
            ts_init: now_ns(),
            monotonic_ns: Some(monotonic_ns),
        }
    }

    /// Shares this collector while labeling records with the adapter's existing generation.
    #[must_use]
    pub fn with_generation(&self, generation: u64) -> Self {
        self.with_identity(self.owner_id, generation)
    }

    /// Shares this collector with the existing live socket owner and endpoint generation.
    #[must_use]
    pub fn with_identity(&self, owner_id: u64, generation: u64) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            owner_id,
            generation,
        }
    }

    /// Drains all available records on the consumer thread.
    ///
    /// Producers never wait for this lock; contention is counted as dropped evidence.
    #[must_use]
    pub fn drain(&self) -> Vec<WebSocketObservation> {
        self.inner.events.lock().drain(..).collect()
    }

    /// Returns the cumulative number of records dropped due to contention or capacity.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// Records a transaction stage using a nonsecret correlation identifier such as a tx hash.
    pub fn record_send(&self, connection_epoch: u64, request_id: &str, stage: WebSocketSendStage) {
        self.record_send_at_time(connection_epoch, request_id, stage, self.capture_time());
    }

    /// Records a historical UTC-only timestamp. Its monotonic time remains explicitly unknown.
    pub fn record_send_at(
        &self,
        connection_epoch: u64,
        request_id: &str,
        stage: WebSocketSendStage,
        ts_init: u64,
    ) {
        self.record_send_at_time(
            connection_epoch,
            request_id,
            stage,
            WebSocketObservationTime {
                ts_init,
                monotonic_ns: None,
            },
        );
    }

    /// Records a stage at the clocks captured at its actual observation point.
    pub fn record_send_at_time(
        &self,
        connection_epoch: u64,
        request_id: &str,
        stage: WebSocketSendStage,
        observed: WebSocketObservationTime,
    ) {
        // Bound caller-controlled identifiers and never retain a request body.
        if request_id.len() > 128 {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.record_at_time(
            connection_epoch,
            observed,
            WebSocketObservationEvent::Send {
                request_id: request_id.to_owned(),
                stage,
            },
        );
    }

    /// Links existing nonsecret adapter identities without retaining an order shadow map.
    pub fn record_correlation(&self, connection_epoch: u64, origin_id: &str, request_id: &str) {
        if origin_id.len() > 128 || request_id.len() > 128 {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.record(
            connection_epoch,
            WebSocketObservationEvent::Correlation {
                origin_id: origin_id.to_owned(),
                request_id: request_id.to_owned(),
            },
        );
    }

    /// Records an already applied native order event. `terminal` comes from the native cache.
    /// This observation never asserts that the current connection caused the event.
    pub fn record_native_order_applied(
        &self,
        connection_epoch: u64,
        client_order_id: &str,
        event_id: &str,
        event_type: &str,
        terminal: bool,
    ) {
        if client_order_id.len() > 128 || event_id.len() > 128 || event_type.len() > 64 {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.record(
            connection_epoch,
            WebSocketObservationEvent::NativeOrderApplied {
                client_order_id: client_order_id.to_owned(),
                event_id: event_id.to_owned(),
                event_type: event_type.to_owned(),
                terminal,
            },
        );
    }

    /// Records an uncorrelated response using the clocks captured by the native reader.
    pub fn record_unmatched_response_at_time(
        &self,
        connection_epoch: u64,
        stage: WebSocketSendStage,
        observed: WebSocketObservationTime,
    ) {
        self.record_at_time(
            connection_epoch,
            observed,
            WebSocketObservationEvent::UnmatchedResponse { stage },
        );
    }

    pub(crate) fn record(&self, connection_epoch: u64, event: WebSocketObservationEvent) {
        self.record_at_time(connection_epoch, self.capture_time(), event);
    }

    fn record_at_time(
        &self,
        connection_epoch: u64,
        observed: WebSocketObservationTime,
        event: WebSocketObservationEvent,
    ) {
        if let Some(mut events) = self.inner.events.try_lock()
            && events.len() < TELEMETRY_CAPACITY
        {
            events.push_back(WebSocketObservation {
                owner_id: self.owner_id,
                generation: self.generation,
                connection_epoch,
                ts_init: observed.ts_init,
                monotonic_clock_id: self.inner.monotonic_clock_id,
                monotonic_ns: observed.monotonic_ns,
                event,
            });
            return;
        }
        self.inner.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn now_ns() -> u64 {
    get_atomic_clock_realtime().get_time_ns().as_u64()
}

pub(crate) fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
struct PendingProbe {
    epoch: u64,
    token: u64,
    started: dst::time::Instant,
    started_ns: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ProbeState {
    pending: Option<PendingProbe>,
    next_token: u64,
    samples: VecDeque<(u64, dst::time::Instant, u64)>,
}

impl ProbeState {
    pub(crate) fn start(&mut self, epoch: u64) -> (u64, Vec<u8>) {
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let token = self.next_token;
        self.pending = Some(PendingProbe {
            epoch,
            token,
            started: dst::time::Instant::now(),
            started_ns: now_ns(),
        });
        (token, probe_payload(epoch, token))
    }

    pub(crate) fn pong(
        &mut self,
        epoch: u64,
        payload: &[u8],
        max_age: Duration,
    ) -> Option<WebSocketObservationEvent> {
        let pending = self.pending.as_ref()?;
        if epoch != pending.epoch || payload != probe_payload(epoch, pending.token) {
            return None;
        }
        let pending = self.pending.take()?;
        let elapsed = pending.started.elapsed();
        if elapsed > max_age {
            return None;
        }

        let received = dst::time::Instant::now();
        let round_trip_ns = nanos(elapsed);
        self.samples.push_back((epoch, received, round_trip_ns));
        while self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }

        Some(WebSocketObservationEvent::RoundTrip {
            token: pending.token,
            writer_started_ns: pending.started_ns,
            pong_received_ns: now_ns(),
            round_trip_ns,
        })
    }

    pub(crate) fn slow_median(
        &self,
        epoch: u64,
        policy: &WebSocketRoutePolicy,
        max_age: Duration,
    ) -> Option<u64> {
        let values: Vec<u64> = self
            .samples
            .iter()
            .rev()
            .take(policy.samples)
            .filter(|(owner, when, _)| *owner == epoch && when.elapsed() <= max_age)
            .map(|(_, _, value)| *value)
            .collect();
        (values.len() == policy.samples
            && values
                .iter()
                .all(|v| *v >= policy.slow_round_trip_ms.saturating_mul(1_000_000)))
        .then(|| median(values))
    }

    pub(crate) fn reset(&mut self) {
        self.pending = None;
        self.samples.clear();
    }
}

pub(crate) fn probe_payload(epoch: u64, token: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(20);
    payload.extend_from_slice(PROBE_PREFIX);
    payload.extend_from_slice(&epoch.to_be_bytes());
    payload.extend_from_slice(&token.to_be_bytes());
    payload
}

pub(crate) fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

pub(crate) fn edge_label(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'))
    .then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn telemetry_is_bounded_and_never_waits_on_consumer() {
        let telemetry = WebSocketTelemetry::new().with_generation(17);
        for _ in 0..=TELEMETRY_CAPACITY {
            telemetry.record(
                3,
                WebSocketObservationEvent::Connected {
                    details: WebSocketConnectionDetails::default(),
                },
            );
        }
        assert_eq!(telemetry.dropped(), 1);
        let events = telemetry.drain();
        assert_eq!(events.len(), TELEMETRY_CAPACITY);
        assert!(
            events
                .iter()
                .all(|e| e.generation == 17 && e.connection_epoch == 3)
        );
        let guard = telemetry.inner.events.lock();
        telemetry.record_send(3, "tx-1", WebSocketSendStage::Enqueued);
        assert_eq!(telemetry.dropped(), 2);
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn probe_rejects_wrong_owner_token_expiry_and_duplicates() {
        let mut probes = ProbeState::default();
        let (_, payload) = probes.start(5);
        dst::time::sleep(Duration::from_millis(48)).await;
        assert!(probes.pong(4, &payload, Duration::from_secs(1)).is_none());
        assert!(probes.pong(5, b"wrong", Duration::from_secs(1)).is_none());
        let event = probes.pong(5, &payload, Duration::from_secs(1)).unwrap();
        assert!(matches!(
            event,
            WebSocketObservationEvent::RoundTrip {
                round_trip_ns: 48_000_000,
                ..
            }
        ));
        assert!(probes.pong(5, &payload, Duration::from_secs(1)).is_none());
        let (_, payload) = probes.start(5);
        dst::time::sleep(Duration::from_secs(2)).await;
        assert!(probes.pong(5, &payload, Duration::from_secs(1)).is_none());
        assert!(probes.pending.is_none());
    }

    #[rstest]
    #[case(60, 40, true)]
    #[case(60, 51, false)]
    #[case(60, 46, false)]
    fn improvement_requires_absolute_and_relative_margin(
        #[case] current: u64,
        #[case] candidate: u64,
        #[case] improves: bool,
    ) {
        assert_eq!(
            WebSocketRoutePolicy::default().improves(current * 1_000_000, candidate * 1_000_000),
            improves
        );
    }

    #[rstest]
    fn edge_diagnostics_reject_arbitrary_header_content() {
        assert_eq!(edge_label("NRT20-P1"), Some("NRT20-P1".into()));
        assert_eq!(edge_label("secret=value; authorization=token"), None);
        assert_eq!(edge_label(&"a".repeat(65)), None);
    }
}
