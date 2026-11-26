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

//! Opt-in monotonic measurements of the existing single-create send path.

use nautilus_model::identifiers::{ClientOrderId, InstrumentId};

pub(super) struct CreateSendTrace {
    start: std::time::Instant,
    start_utc_ns: Option<u64>,
    command_ts_init_ns: u64,
    pub(super) market_index: Option<i64>,
    marks_ns: [Option<u64>; 11],
}

pub(super) struct CreateSendReturn {
    pub(super) elapsed_ns: Option<u64>,
    pub(super) utc_ns: Option<u64>,
    pub(super) ok: bool,
}

#[repr(usize)]
pub(super) enum CreateSendMark {
    PlanReady,
    SignStart,
    SignDone,
    Spawn,
    FirstPoll,
    SubmittedEmitted,
    NonceWaitStart,
    NonceReady,
    QuotaWaitStart,
    QuotaReady,
    WsCall,
}

impl CreateSendTrace {
    pub(super) fn utc_ns() -> Option<u64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?;
        u64::try_from(now.as_nanos()).ok()
    }

    pub(super) fn enabled() -> bool {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| {
            std::env::var_os("LIGHTER_EXECUTION_TIMING").is_some_and(|value| value == "1")
        })
    }

    pub(super) fn begin(command_ts_init_ns: u64) -> Option<Self> {
        Self::enabled().then(|| Self {
            start: std::time::Instant::now(),
            start_utc_ns: Self::utc_ns(),
            command_ts_init_ns,
            market_index: None,
            marks_ns: [None; 11],
        })
    }

    pub(super) fn elapsed_ns(&self) -> Option<u64> {
        u64::try_from(self.start.elapsed().as_nanos()).ok()
    }

    pub(super) fn mark(trace: &mut Option<Self>, mark: CreateSendMark) {
        if let Some(trace) = trace.as_mut() {
            trace.marks_ns[mark as usize] = trace.elapsed_ns();
        }
    }

    pub(super) fn emit(
        self,
        client_order_id: ClientOrderId,
        client_order_index: i64,
        instrument_id: InstrumentId,
        connection_epoch: u64,
        nonce: i64,
        observation: CreateSendReturn,
    ) {
        // Only after the existing reservation release; never log signed data or credentials.
        log::info!(
            target: "nautilus_lighter::latency",
            "LIGHTER_CREATE_SEND_TIMING_V2 {}",
            serde_json::json!({
                "client_order_id": client_order_id.to_string(),
                "client_order_index": client_order_index,
                "market_index": self.market_index,
                "instrument_id": instrument_id.to_string(),
                "connection_epoch": connection_epoch,
                "nonce": nonce,
                "command_ts_init_ns": self.command_ts_init_ns,
                "adapter_start_utc_ns": self.start_utc_ns,
                "plan_ready_elapsed_ns": self.marks_ns[CreateSendMark::PlanReady as usize],
                "sign_start_elapsed_ns": self.marks_ns[CreateSendMark::SignStart as usize],
                "sign_done_elapsed_ns": self.marks_ns[CreateSendMark::SignDone as usize],
                "spawn_elapsed_ns": self.marks_ns[CreateSendMark::Spawn as usize],
                "first_poll_elapsed_ns": self.marks_ns[CreateSendMark::FirstPoll as usize],
                "submitted_emitted_elapsed_ns": self.marks_ns[CreateSendMark::SubmittedEmitted as usize],
                "nonce_wait_start_elapsed_ns": self.marks_ns[CreateSendMark::NonceWaitStart as usize],
                "nonce_ready_elapsed_ns": self.marks_ns[CreateSendMark::NonceReady as usize],
                "quota_wait_start_elapsed_ns": self.marks_ns[CreateSendMark::QuotaWaitStart as usize],
                "quota_ready_elapsed_ns": self.marks_ns[CreateSendMark::QuotaReady as usize],
                "ws_call_elapsed_ns": self.marks_ns[CreateSendMark::WsCall as usize],
                "ws_return_elapsed_ns": observation.elapsed_ns,
                "ws_return_utc_ns": observation.utc_ns,
                "ws_return_ok": observation.ok,
            })
        );
    }
}
