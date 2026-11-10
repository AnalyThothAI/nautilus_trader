// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the License
//  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//  or implied. See the License for the specific language governing permissions and limitations.
// -------------------------------------------------------------------------------------------------

//! Request and connection timing evidence without URLs, headers, or payloads.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use nautilus_core::time::get_atomic_clock_realtime;
use parking_lot::Mutex;
use serde::Serialize;

const SOCKET_EVENT_CAPACITY: usize = 128;
static CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Local UTC timestamps for one request, retained even if its future is canceled.
///
/// Connection acquisition is observed by the request task after Hyper selects a connection;
/// it is not a socket-write timestamp. Socket events describe the shared connection, including
/// other HTTP/2 streams. They prove local transport activity, not remote receipt or processing.
#[derive(Clone, Debug, Serialize)]
pub struct HttpRequestTiming {
    /// Time the trace was created, before polling the request.
    pub started_ns: u64,
    /// Time request construction and client rate limiting completed.
    pub prepared_ns: Option<u64>,
    /// Transport attempts, including protocol retries and followed redirects.
    pub attempts: Vec<HttpAttemptTiming>,
    /// Time the complete encoded response body was read, before content decoding.
    pub body_received_ns: Option<u64>,
    /// Time the request future completed or was dropped.
    pub completed_ns: Option<u64>,
}

/// One call into Hyper, which may itself retry an unstarted pooled request.
#[derive(Clone, Debug, Serialize)]
pub struct HttpAttemptTiming {
    /// Time this transport attempt started.
    pub started_ns: u64,
    /// Connection observations before or after the response; different IDs expose replacement.
    pub connections: Vec<HttpConnectionTiming>,
    /// Time the parsed HTTP response headers became available.
    pub headers_received_ns: Option<u64>,
    /// HTTP status, also retained when reading the body fails.
    pub status: Option<u16>,
    /// Time the attempt returned a response or transport error.
    pub completed_ns: Option<u64>,
    /// Whether this attempt caused the transport's bounded protocol retry.
    pub protocol_retry: bool,
}

/// A connection selected for a request; addresses identify its TCP flow for kernel diagnostics.
#[derive(Clone, Debug, Serialize)]
pub struct HttpConnectionTiming {
    /// Process-local identity, stable across requests that reuse this connection.
    pub connection_id: u64,
    /// Time the request task first observed this selected connection.
    pub observed_ns: u64,
    /// Time connection establishment started, including name resolution.
    pub connect_started_ns: u64,
    /// Time TCP or the configured proxy tunnel became available.
    pub transport_connected_ns: u64,
    /// Time destination TLS completed (or the plain HTTP connection became available).
    pub established_ns: u64,
    /// Whether the connection negotiated HTTP/2.
    pub http2: bool,
    /// Whether transport activity passes through a configured proxy.
    pub proxied: bool,
    /// Local TCP address, when the connector supplies it.
    pub local_addr: Option<SocketAddr>,
    /// Remote TCP address, when the connector supplies it.
    pub remote_addr: Option<SocketAddr>,
    /// Bounded connection activity during this attempt, never payload contents.
    pub socket_events: Vec<HttpSocketEvent>,
    /// Whether the bounded connection history discarded activity in this attempt's window.
    pub socket_events_truncated: bool,
}

/// A successful transport read or write, including TLS records on direct HTTPS connections.
///
/// A write means bytes were accepted by the underlying transport, not acknowledged by the peer.
/// With a proxy, that transport can itself buffer TLS; HTTP/2 events cannot be assigned to a
/// particular stream. TCP acknowledgments and retransmissions require kernel or packet evidence.
#[derive(Clone, Debug, Serialize)]
pub struct HttpSocketEvent {
    /// Local UTC time of the completed poll.
    pub ts_ns: u64,
    /// Whether this is a write; false denotes a read.
    pub write: bool,
    /// Number of bytes accepted or read by this poll.
    pub bytes: usize,
}

/// A per-request timing collector, containing no request or response contents.
///
/// Create a fresh collector for each `request_with_trace` call. Clones share evidence so a caller
/// can inspect a snapshot after a timeout drops the request future.
#[derive(Clone, Debug)]
pub struct HttpRequestTrace(Arc<Mutex<RequestState>>);

#[derive(Debug)]
struct RequestState {
    used: bool,
    timing: HttpRequestTiming,
    probes: Vec<Vec<ConnectionProbe>>,
}

impl Default for HttpRequestTrace {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRequestTrace {
    /// Creates a fresh timing collector.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(RequestState {
            used: false,
            timing: HttpRequestTiming {
                started_ns: now(),
                prepared_ns: None,
                attempts: Vec::new(),
                body_received_ns: None,
                completed_ns: None,
            },
            probes: Vec::new(),
        })))
    }

    /// Returns the recorded phases and the bounded socket history for their time windows.
    #[must_use]
    pub fn snapshot(&self) -> HttpRequestTiming {
        let state = self.0.lock();
        let mut timing = state.timing.clone();
        collect_events(&mut timing, &state.probes);
        timing
    }

    pub(super) fn prepared(&self) {
        self.0.lock().timing.prepared_ns = Some(now());
    }

    pub(super) fn begin(&self) -> bool {
        !std::mem::replace(&mut self.0.lock().used, true)
    }

    pub(super) fn attempt(&self) -> usize {
        let mut state = self.0.lock();
        let index = state.timing.attempts.len();
        state.timing.attempts.push(HttpAttemptTiming {
            started_ns: now(),
            connections: Vec::new(),
            headers_received_ns: None,
            status: None,
            completed_ns: None,
            protocol_retry: false,
        });
        state.probes.push(Vec::new());
        index
    }

    pub(super) fn connection(
        &self,
        index: usize,
        probe: ConnectionProbe,
        http2: bool,
        local_addr: Option<SocketAddr>,
        remote_addr: Option<SocketAddr>,
    ) {
        let mut state = self.0.lock();
        let data = probe.0.lock();
        if state.timing.attempts[index]
            .connections
            .iter()
            .any(|connection| connection.connection_id == data.id)
        {
            return;
        }
        state.timing.attempts[index]
            .connections
            .push(HttpConnectionTiming {
                connection_id: data.id,
                observed_ns: now(),
                connect_started_ns: data.started_ns,
                transport_connected_ns: data.connected_ns,
                established_ns: data.established_ns,
                http2,
                proxied: data.proxied,
                local_addr,
                remote_addr,
                socket_events: Vec::new(),
                socket_events_truncated: false,
            });
        drop(data);
        state.probes[index].push(probe);
    }

    pub(super) fn attempt_finished(&self, index: usize, status: Option<u16>, retry: bool) {
        let mut state = self.0.lock();
        let attempt = &mut state.timing.attempts[index];
        let timestamp = now();
        attempt.status = status;
        attempt.headers_received_ns = status.map(|_| timestamp);
        attempt.completed_ns = Some(timestamp);
        attempt.protocol_retry = retry;
    }

    pub(super) fn body_received(&self) {
        self.0.lock().timing.body_received_ns = Some(now());
    }

    pub(super) fn finish(&self) {
        let mut state = self.0.lock();
        state.timing.completed_ns = Some(now());
        let probes = std::mem::take(&mut state.probes);
        collect_events(&mut state.timing, &probes);
    }
}

fn collect_events(timing: &mut HttpRequestTiming, probes: &[Vec<ConnectionProbe>]) {
    let completed = timing.completed_ns.unwrap_or_else(now);
    for (index, probes) in probes.iter().enumerate() {
        // Earlier attempts end before the next retry or redirect; the last includes its body
        let until = timing
            .attempts
            .get(index + 1)
            .map_or(completed, |next| next.started_ns);
        let attempt = &mut timing.attempts[index];
        for (connection, probe) in attempt.connections.iter_mut().zip(probes) {
            let data = probe.0.lock();
            connection.socket_events = data
                .events
                .iter()
                .filter(|event| event.ts_ns >= attempt.started_ns && event.ts_ns <= until)
                .cloned()
                .collect();
            connection.socket_events_truncated = data
                .dropped_through_ns
                .is_some_and(|dropped| dropped >= attempt.started_ns);
        }
    }
}

pub(super) struct FinishTrace(pub(super) HttpRequestTrace);

impl Drop for FinishTrace {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[derive(Clone, Debug)]
pub(super) struct ConnectionProbe(Arc<Mutex<ConnectionState>>);

#[derive(Debug)]
struct ConnectionState {
    id: u64,
    started_ns: u64,
    connected_ns: u64,
    established_ns: u64,
    proxied: bool,
    events: VecDeque<HttpSocketEvent>,
    dropped_through_ns: Option<u64>,
}

impl ConnectionProbe {
    pub(super) fn new(started_ns: u64, proxied: bool) -> Self {
        let connected_ns = now();
        Self(Arc::new(Mutex::new(ConnectionState {
            id: CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            started_ns,
            connected_ns,
            established_ns: connected_ns,
            proxied,
            events: VecDeque::new(),
            dropped_through_ns: None,
        })))
    }

    pub(super) fn established(&self) {
        self.0.lock().established_ns = now();
    }

    pub(super) fn record(&self, write: bool, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let event = HttpSocketEvent {
            ts_ns: now(),
            write,
            bytes,
        };
        let mut state = self.0.lock();
        if state.events.len() == SOCKET_EVENT_CAPACITY {
            state.dropped_through_ns = state.events.pop_front().map(|event| event.ts_ns);
        }
        state.events.push_back(event);
    }
}

pub(super) fn now() -> u64 {
    get_atomic_clock_realtime().get_time_ns().as_u64()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn completed_trace_is_frozen_and_reports_dropped_socket_history() {
        let trace = HttpRequestTrace::new();
        let index = trace.attempt();
        let probe = ConnectionProbe::new(now(), false);
        trace.connection(index, probe.clone(), true, None, None);
        for _ in 0..=SOCKET_EVENT_CAPACITY {
            probe.record(true, 10);
        }
        trace.finish();
        let timing = trace.snapshot();
        let connection = &timing.attempts[0].connections[0];
        assert!(connection.socket_events_truncated);
        assert_eq!(connection.socket_events.len(), SOCKET_EVENT_CAPACITY);
        for _ in 0..SOCKET_EVENT_CAPACITY {
            probe.record(false, 20);
        }
        assert_eq!(
            serde_json::to_value(trace.snapshot()).unwrap(),
            serde_json::to_value(timing).unwrap()
        );
    }
}
