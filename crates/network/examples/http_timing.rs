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

//! Samples one public GET endpoint using a persistent client, without authentication or payload logs.

use std::time::Duration;

use nautilus_network::http::{HttpClient, HttpRedirectPolicy, HttpRequestTrace, Method};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: http_timing PUBLIC_URL [COUNT]")?;
    let count: usize = std::env::args().nth(2).map_or(Ok(5), |s| s.parse())?;
    if !(1..=100).contains(&count) {
        return Err("count must be between 1 and 100".into());
    }
    let client = HttpClient::builder()
        .redact_url(true)
        .connection_timing(true)
        .redirect_policy(HttpRedirectPolicy::Reject)
        .header_keys(vec!["cf-ray".into()])
        .timeout_secs(10)
        .build()?;
    for _ in 0..count {
        let trace = HttpRequestTrace::new();
        let response = client
            .request_with_trace(
                Method::GET,
                url.clone(),
                None,
                None,
                None,
                None,
                None,
                &trace,
            )
            .await;
        println!(
            "{}",
            serde_json::json!({
                "ok": response.is_ok(),
                "status": response.as_ref().ok().map(|r| r.status.as_u16()),
                "cf_ray": response.as_ref().ok().and_then(|r| r.headers.get("cf-ray")),
                "transport": trace.snapshot(),
            })
        );
        response?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}
