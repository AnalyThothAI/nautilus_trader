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

use nautilus_core::UUID4;
use nautilus_model::defi::{data::canonical::CanonicalAnchor, pool_analysis::PoolSnapshot};

/// Represents a pool snapshot sent in reply to a [`RequestPoolSnapshot`](super::RequestPoolSnapshot).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PoolSnapshotResponse {
    pub correlation_id: UUID4,
    pub data: PoolSnapshot,
    /// The validated canonical identity requested by the caller.
    pub canonical_anchor: Option<CanonicalAnchor>,
}

impl PoolSnapshotResponse {
    /// Creates a new [`PoolSnapshotResponse`] instance.
    #[must_use]
    pub fn new(correlation_id: UUID4, data: PoolSnapshot) -> Self {
        Self {
            correlation_id,
            data,
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
