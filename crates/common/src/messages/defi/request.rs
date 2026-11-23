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

use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_model::{
    defi::data::canonical::CanonicalAnchor,
    identifiers::{ClientId, InstrumentId},
};
use serde::{Deserialize, Serialize};

/// Represents a request for a pool snapshot from a specific AMM pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestPoolSnapshot {
    pub instrument_id: InstrumentId,
    pub client_id: Option<ClientId>,
    pub request_id: UUID4,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
    /// Explicit hash-bound canonical state required by this request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_anchor: Option<CanonicalAnchor>,
}

impl RequestPoolSnapshot {
    /// Creates a new [`RequestPoolSnapshot`] instance.
    #[must_use]
    pub fn new(
        instrument_id: InstrumentId,
        client_id: Option<ClientId>,
        request_id: UUID4,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            instrument_id,
            client_id,
            request_id,
            ts_init,
            params,
            canonical_anchor: None,
        }
    }

    /// Attaches the explicit canonical identity of this hydration.
    #[must_use]
    pub fn with_canonical_anchor(mut self, anchor: CanonicalAnchor) -> Self {
        self.canonical_anchor = Some(anchor);
        self
    }
}
