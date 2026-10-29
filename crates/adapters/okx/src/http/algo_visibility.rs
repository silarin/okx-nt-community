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

//! Bounded, account-wide read-only algo visibility, independent of execution support.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::OnceLock,
    time::{Duration, Instant},
};

use super::{
    client::OKXRawHttpClient,
    error::OKXHttpError,
    models::OKXOrderAlgo,
    query::{GetAlgoOrdersParams, GetAlgoOrdersParamsBuilder},
};
use crate::common::enums::{OKXAlgoOrderStatus, OKXAlgoOrderType, OKXInstrumentType};

const PAGE_SIZE: usize = 100;
const MAX_PAGES: usize = 10;
// Current OKX algo pending/history reads are limited to 20 requests per two
// seconds per User ID. A 110 ms shared interval is deliberately conservative
// and applies to every page across all native algo reads and provider instances.
const ALGO_VISIBILITY_REQUEST_INTERVAL: Duration = Duration::from_millis(110);
static ALGO_VISIBILITY_DISPATCH_GATE: OnceLock<tokio::sync::Mutex<Instant>> = OnceLock::new();
const TYPED_ALGO_ORDER_TYPES: [OKXAlgoOrderType; 7] = [
    OKXAlgoOrderType::Oco,
    OKXAlgoOrderType::Conditional,
    OKXAlgoOrderType::Trigger,
    OKXAlgoOrderType::MoveOrderStop,
    OKXAlgoOrderType::Iceberg,
    OKXAlgoOrderType::Twap,
    OKXAlgoOrderType::SmartIceberg,
];

/// Stable identity evidence returned by an account-wide algo route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OKXAlgoOrderIdentity {
    /// Venue algo ID.
    pub algo_id: String,
    /// Client algo ID, if supplied.
    pub algo_client_order_id: String,
    /// Child client order ID, if supplied.
    pub client_order_id: String,
    /// Native OKX instrument ID.
    pub instrument_id: String,
    /// Venue-reported state.
    pub state: OKXAlgoOrderStatus,
}

/// Complete bounded visibility batch; pending evidence is retained separately.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OKXAlgoOrderVisibility {
    /// Number of completed pending/history routes.
    pub queried_routes: usize,
    /// Distinct pending identities.
    pub pending_orders: Vec<OKXAlgoOrderIdentity>,
    /// Distinct terminal identities.
    pub terminal_orders: Vec<OKXAlgoOrderIdentity>,
}

impl OKXRawHttpClient {
    /// Collects all seven cash algo types across pending and terminal SPOT/MARGIN routes.
    ///
    /// Every route is account-wide (no instrument ID), bounded to ten pages of 100 rows.
    /// A conflict, repeated row, exhausted bound, or failed route rejects the whole batch.
    /// Sequential reads are not an atomic venue snapshot. This method enables no execution types.
    /// Callers can place an overall timeout around the collection future.
    ///
    /// # Errors
    ///
    /// Returns an error if transport fails or evidence is invalid or incomplete.
    pub async fn get_algo_order_visibility(
        &self,
        instrument_types: &[OKXInstrumentType],
    ) -> anyhow::Result<OKXAlgoOrderVisibility, OKXHttpError> {
        let mut seen = std::collections::HashSet::new();

        if instrument_types.is_empty()
            || instrument_types.iter().any(|t| {
                !matches!(t, OKXInstrumentType::Spot | OKXInstrumentType::Margin)
                    || !seen.insert(*t)
            })
        {
            return Err(OKXHttpError::ValidationError(
                "Algo visibility requires distinct SPOT/MARGIN instrument types".into(),
            ));
        }

        self.collect_algo_order_visibility(instrument_types)
            .await
            .map_err(|e| match e.downcast::<OKXHttpError>() {
                Ok(e) => e,
                Err(e) => OKXHttpError::ValidationError(e.to_string()),
            })
    }

    async fn collect_algo_order_visibility(
        &self,
        instrument_types: &[OKXInstrumentType],
    ) -> anyhow::Result<OKXAlgoOrderVisibility> {
        let mut reconciliation = OKXAlgoOrderVisibility::default();
        let mut seen = BTreeMap::new();
        let mut pending_seen = BTreeSet::new();
        let mut terminal_seen = BTreeSet::new();

        for &query_type in instrument_types {
            for order_type in TYPED_ALGO_ORDER_TYPES {
                let base = algo_params(query_type, order_type, None)?;
                let pending = self.paginate_pending(&base).await?;
                reconciliation.queried_routes += 1;

                for order in pending {
                    let identity = record_algo_identity(order, &mut seen)?;
                    if pending_seen.insert(identity.algo_id.clone()) {
                        reconciliation.pending_orders.push(identity);
                    }
                }

                for state in [
                    OKXAlgoOrderStatus::Effective,
                    OKXAlgoOrderStatus::Canceled,
                    OKXAlgoOrderStatus::OrderFailed,
                ] {
                    let base = algo_params(query_type, order_type, Some(state))?;
                    for order in self.paginate_history(&base).await? {
                        let identity = record_algo_identity(order, &mut seen)?;
                        if terminal_seen.insert(identity.algo_id.clone()) {
                            reconciliation.terminal_orders.push(identity);
                        }
                    }
                    reconciliation.queried_routes += 1;
                }
            }
        }

        Ok(reconciliation)
    }

    async fn paginate_pending(
        &self,
        base: &GetAlgoOrdersParams,
    ) -> anyhow::Result<Vec<OKXOrderAlgo>> {
        let mut all = Vec::new();
        let mut cursor = None;
        let mut seen = BTreeSet::new();

        for page_index in 0..MAX_PAGES {
            let mut params = base.clone();
            params.after = cursor.take();
            params.limit = Some(PAGE_SIZE as u32);
            let page = self.get_order_algo_pending(params).await?;
            validate_algo_page(&page, base, &mut seen)?;
            let page_len = page.len();
            cursor = page.last().map(|order| order.algo_id.clone());
            all.extend(page);
            if page_len < PAGE_SIZE {
                return Ok(all);
            }
            anyhow::ensure!(
                page_index + 1 < MAX_PAGES,
                "OKX pending algo-order visibility exceeded the pagination bound"
            );
        }
        unreachable!("bounded pagination returns or fails on its final page")
    }

    async fn paginate_history(
        &self,
        base: &GetAlgoOrdersParams,
    ) -> anyhow::Result<Vec<OKXOrderAlgo>> {
        let mut all = Vec::new();
        let mut cursor = None;
        let mut seen = BTreeSet::new();

        for page_index in 0..MAX_PAGES {
            let mut params = base.clone();
            params.after = cursor.take();
            params.limit = Some(PAGE_SIZE as u32);
            let page = self.get_order_algo_history(params).await?;
            validate_algo_page(&page, base, &mut seen)?;
            let page_len = page.len();
            cursor = page.last().map(|order| order.algo_id.clone());
            all.extend(page);
            if page_len < PAGE_SIZE {
                return Ok(all);
            }
            anyhow::ensure!(
                page_index + 1 < MAX_PAGES,
                "OKX terminal algo-order visibility exceeded the pagination bound"
            );
        }
        unreachable!("bounded pagination returns or fails on its final page")
    }
}

pub(super) async fn pace_algo_visibility_request<F, R>(request: F) -> R::Output
where
    F: FnOnce() -> R,
    R: Future,
{
    let dispatch_gate =
        ALGO_VISIBILITY_DISPATCH_GATE.get_or_init(|| tokio::sync::Mutex::new(Instant::now()));
    let mut next_dispatch_at = dispatch_gate.lock().await;
    let delay = next_dispatch_at.saturating_duration_since(Instant::now());
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    // Keep the process-wide gate locked through the actual HTTP future so delayed
    // tasks cannot wake together and bunch requests after merely reserving slots.
    // The pre-dispatch deadline also remains conservative if this future is canceled.
    *next_dispatch_at = Instant::now() + ALGO_VISIBILITY_REQUEST_INTERVAL;
    let output = request().await;
    *next_dispatch_at = Instant::now() + ALGO_VISIBILITY_REQUEST_INTERVAL;
    output
}

fn algo_params(
    instrument_type: OKXInstrumentType,
    order_type: OKXAlgoOrderType,
    state: Option<OKXAlgoOrderStatus>,
) -> anyhow::Result<GetAlgoOrdersParams> {
    let mut builder = GetAlgoOrdersParamsBuilder::default();
    builder
        .inst_type(instrument_type)
        .ord_type(order_type)
        .limit(PAGE_SIZE as u32);

    if let Some(state) = state {
        builder.state(state);
    }
    builder
        .build()
        .map_err(|e| anyhow::anyhow!("invalid algo visibility request: {e}"))
}

fn record_algo_identity(
    order: OKXOrderAlgo,
    seen: &mut BTreeMap<String, (OKXAlgoOrderType, OKXAlgoOrderIdentity)>,
) -> anyhow::Result<OKXAlgoOrderIdentity> {
    let order_type = order.ord_type;
    let identity = OKXAlgoOrderIdentity {
        algo_id: order.algo_id,
        algo_client_order_id: order.algo_cl_ord_id,
        client_order_id: order.cl_ord_id,
        instrument_id: order.inst_id.to_string(),
        state: order.state,
    };

    if let Some((previous_type, previous)) = seen.get(&identity.algo_id) {
        // Reads are sequential, not atomic. Even a real state transition makes
        // this batch inconsistent; never choose a winner across routes/streams.
        anyhow::ensure!(
            *previous_type == order_type && *previous == identity,
            "OKX algo visibility returned conflicting evidence for an algo ID"
        );
    } else {
        seen.insert(identity.algo_id.clone(), (order_type, identity.clone()));
    }
    Ok(identity)
}

fn validate_algo_page(
    page: &[OKXOrderAlgo],
    request: &GetAlgoOrdersParams,
    seen: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        page.len() <= PAGE_SIZE,
        "OKX algo visibility returned more rows than the requested page size"
    );

    for order in page {
        validate_algo_order(order, request)?;
        anyhow::ensure!(
            seen.insert(order.algo_id.clone()),
            "OKX algo visibility repeated an algo ID within a paginated route"
        );
    }
    Ok(())
}

fn validate_algo_order(raw: &OKXOrderAlgo, request: &GetAlgoOrdersParams) -> anyhow::Result<()> {
    anyhow::ensure!(
        !raw.algo_id.is_empty() && raw.algo_id.bytes().all(|byte| byte.is_ascii_digit()),
        "OKX algo response returned an invalid algo ID"
    );
    anyhow::ensure!(
        raw.inst_type == request.inst_type,
        "OKX algo response instrument type does not match the requested account-wide scope"
    );
    anyhow::ensure!(
        is_query_safe_instrument_id(raw.inst_id.as_str()),
        "OKX algo response returned an invalid instrument ID"
    );
    anyhow::ensure!(
        Some(raw.ord_type) == request.ord_type,
        "OKX algo response returned a different order type"
    );
    let state = raw.state;
    anyhow::ensure!(
        algo_state_matches_route(state, request.state),
        "OKX algo response state does not match the requested pending/history route"
    );
    Ok(())
}

fn algo_state_matches_route(
    actual: OKXAlgoOrderStatus,
    expected_history_state: Option<OKXAlgoOrderStatus>,
) -> bool {
    match expected_history_state {
        None => matches!(
            actual,
            OKXAlgoOrderStatus::Live
                | OKXAlgoOrderStatus::Pause
                | OKXAlgoOrderStatus::PartiallyEffective
        ),
        Some(OKXAlgoOrderStatus::OrderFailed) => matches!(
            actual,
            OKXAlgoOrderStatus::OrderFailed | OKXAlgoOrderStatus::PartiallyFailed
        ),
        Some(OKXAlgoOrderStatus::Effective) => matches!(
            actual,
            OKXAlgoOrderStatus::Effective | OKXAlgoOrderStatus::PartiallyEffective
        ),
        Some(expected) => actual == expected,
    }
}

fn is_query_safe_instrument_id(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
        })
}

#[cfg(test)]
#[path = "tests/algo_visibility.rs"]
mod tests;
