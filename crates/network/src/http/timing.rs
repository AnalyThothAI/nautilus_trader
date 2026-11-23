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

//! Request-local phase evidence without URLs, headers, payloads, or socket observations.

use std::sync::Arc;

use nautilus_core::time::get_atomic_clock_realtime;
use parking_lot::Mutex;
use serde::Serialize;

use crate::dst::time::Instant;

/// A phase's UTC receipt and elapsed time within this collector's monotonic scope.
///
/// Elapsed values can only be subtracted within the same collector, never across requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct HttpPhaseTime {
    /// UTC nanoseconds observed by the local request task.
    pub utc_ns: u64,
    /// Nanoseconds elapsed since this collector's private monotonic origin.
    pub elapsed_ns: u64,
}

/// The local buffered network future's terminal state, before adapter response parsing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpRequestCompletion {
    /// Returned an HTTP response; an adapter may still reject its status or body.
    Returned,
    /// Returned a preparation, transport, timeout, or body-processing error.
    Failed,
    /// Was dropped after its first poll; this does not prove remote cancellation.
    Cancelled,
}

/// Recorded phases of one opt-in buffered request.
///
/// `transport_invoked` means the request task entered Hyper, not that bytes were written or
/// received by a peer. Connection acquisition, DNS, TLS, HTTP/2 queues, network latency and remote
/// processing can all contribute to the interval before headers. Missing phases stay absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HttpRequestTiming {
    /// Stable schema identifier for serialized phase evidence.
    pub schema: &'static str,
    /// Collector construction, which can precede first poll and quota admission.
    pub created: HttpPhaseTime,
    /// First poll of the native traced request, not collector construction.
    pub first_polled: Option<HttpPhaseTime>,
    /// Entry into the native quota wait.
    pub limiter_started: Option<HttpPhaseTime>,
    /// All native quotas required by this call have been acquired.
    pub limiter_acquired: Option<HttpPhaseTime>,
    /// Request URL, query, headers and body have been constructed.
    pub prepared: Option<HttpPhaseTime>,
    /// Entry into Hyper's request future; no wire transmission is implied.
    pub transport_invoked: Option<HttpPhaseTime>,
    /// Counts calls into Hyper, not requests proven to have reached the server.
    pub transport_invocations: u32,
    /// Parsed response headers became available to the request task.
    pub headers_received: Option<HttpPhaseTime>,
    /// HTTP status retained even when body processing is incomplete or fails.
    pub status: Option<u16>,
    /// The encoded response body has been read, before content decoding.
    pub body_received: Option<HttpPhaseTime>,
    /// Local completion or drop of the polled buffered network future.
    pub completed: Option<HttpPhaseTime>,
    /// The local future's terminal state, absent until it terminates.
    pub completion: Option<HttpRequestCompletion>,
}

/// A single-use collector retained by the caller when its request future is cancelled.
///
/// Clones share evidence. An unpolled future records no first poll or completion. The collector
/// does not change request policy, deadlines, quotas, redirects, or retries.
#[derive(Clone, Debug)]
pub struct HttpRequestTrace(Arc<Mutex<RequestState>>);

#[derive(Debug)]
struct RequestState {
    used: bool,
    origin: Instant,
    timing: HttpRequestTiming,
}

impl Default for HttpRequestTrace {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRequestTrace {
    /// Creates a fresh collector without polling or submitting a request.
    #[must_use]
    pub fn new() -> Self {
        let origin = Instant::now();
        let created = stamp(origin);
        Self(Arc::new(Mutex::new(RequestState {
            used: false,
            origin,
            timing: HttpRequestTiming {
                schema: "native_http_buffered_phases_v1",
                created,
                first_polled: None,
                limiter_started: None,
                limiter_acquired: None,
                prepared: None,
                transport_invoked: None,
                transport_invocations: 0,
                headers_received: None,
                status: None,
                body_received: None,
                completed: None,
                completion: None,
            },
        })))
    }

    /// Returns the actual phase prefix, including local cancellation when already observed.
    #[must_use]
    pub fn snapshot(&self) -> HttpRequestTiming {
        self.0.lock().timing.clone()
    }

    pub(super) fn begin(&self) -> bool {
        let mut state = self.0.lock();
        if std::mem::replace(&mut state.used, true) {
            return false;
        }

        state.timing.first_polled = Some(stamp(state.origin));
        true
    }

    pub(super) fn limiter_started(&self) {
        let mut state = self.0.lock();
        state.timing.limiter_started = Some(stamp(state.origin));
    }

    pub(super) fn limiter_acquired(&self) {
        let mut state = self.0.lock();
        state.timing.limiter_acquired = Some(stamp(state.origin));
    }

    pub(super) fn prepared(&self) {
        let mut state = self.0.lock();
        state.timing.prepared = Some(stamp(state.origin));
    }

    #[cfg(not(all(feature = "simulation", madsim)))]
    pub(super) fn transport_invoked(&self) {
        let mut state = self.0.lock();
        state.timing.transport_invoked = Some(stamp(state.origin));
        state.timing.transport_invocations += 1;
    }

    #[cfg(not(all(feature = "simulation", madsim)))]
    pub(super) fn headers_received(&self, status: u16) {
        let mut state = self.0.lock();
        state.timing.headers_received = Some(stamp(state.origin));
        state.timing.status = Some(status);
    }

    pub(super) fn body_received(&self) {
        let mut state = self.0.lock();
        state.timing.body_received = Some(stamp(state.origin));
    }

    fn finish(&self, completion: HttpRequestCompletion) {
        let mut state = self.0.lock();
        state.timing.completed = Some(stamp(state.origin));
        state.timing.completion = Some(completion);
    }
}

fn stamp(origin: Instant) -> HttpPhaseTime {
    HttpPhaseTime {
        utc_ns: get_atomic_clock_realtime().get_time_ns().as_u64(),
        elapsed_ns: origin.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
    }
}

pub(super) struct FinishTrace {
    trace: HttpRequestTrace,
    completion: HttpRequestCompletion,
}

impl FinishTrace {
    pub(super) const fn new(trace: HttpRequestTrace) -> Self {
        Self {
            trace,
            completion: HttpRequestCompletion::Cancelled,
        }
    }

    pub(super) const fn returned(&mut self, success: bool) {
        self.completion = if success {
            HttpRequestCompletion::Returned
        } else {
            HttpRequestCompletion::Failed
        };
    }
}

impl Drop for FinishTrace {
    fn drop(&mut self) {
        self.trace.finish(self.completion);
    }
}
