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

//! Provenance and currency-specific timestamps for OKX `AccountState` events.

use std::collections::BTreeMap;

use anyhow::Context;
use nautilus_core::{UnixNanos, params::Params};
use nautilus_model::{
    events::AccountState,
    types::{AccountBalance, Currency},
};
use serde::{Deserialize, Serialize};

/// Origin of an account update. A snapshot page does not prove omitted balances are zero.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OKXAccountUpdate {
    /// Origin was not supplied (for example, a legacy event).
    #[default]
    Unknown,
    /// An unfiltered REST account-balance response.
    Rest,
    /// Initial or periodic WebSocket snapshot, possibly paginated.
    WebSocketSnapshot {
        /// Venue page number, when supplied.
        page: Option<u32>,
        /// Venue last-page flag, when supplied.
        last_page: Option<bool>,
    },
    /// Event-driven, potentially partial WebSocket update.
    WebSocketUpdate,
}

/// Adapter-owned metadata stored under `AccountState.info["okx"]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OKXAccountStateInfo {
    /// Source of this event; never a claim that omitted currencies have zero balances.
    pub update: OKXAccountUpdate,
    /// Per-currency venue update times, independent of the account-wide timestamp.
    pub balance_timestamps: BTreeMap<String, UnixNanos>,
    /// Local time captured before requesting this REST snapshot, including any retries.
    /// This is observation provenance, not a replacement for venue update times.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_request_started: Option<UnixNanos>,
}

impl OKXAccountStateInfo {
    /// Converts this metadata to native account event parameters.
    ///
    /// # Panics
    ///
    /// Panics if serialization fails (the metadata contains only JSON-compatible values).
    #[must_use]
    pub fn into_params(self) -> Params {
        Params::new(
            [(
                "okx".to_string(),
                serde_json::to_value(self).expect("OKX account metadata is JSON serializable"),
            )]
            .into_iter()
            .collect(),
        )
    }
}

/// Returns a currency balance explicitly reported in this event and its own update time.
///
/// An omitted currency returns `None`; neither cached balances nor snapshot completeness
/// can manufacture a fresh zero. Synthetic and legacy events are not reported evidence.
///
/// # Errors
///
/// Returns an error if the event is unreported, metadata is absent/malformed, balances are
/// duplicated, or a currency timestamp is missing, zero or in the future.
pub fn reported_currency_balance(
    state: &AccountState,
    currency: Currency,
) -> anyhow::Result<Option<(AccountBalance, UnixNanos)>> {
    anyhow::ensure!(
        state.is_reported,
        "OKX balance evidence requires a reported AccountState"
    );
    let value = state
        .info
        .as_ref()
        .and_then(|info| info.get("okx"))
        .context("OKX AccountState is missing balance provenance")?;
    let info: OKXAccountStateInfo = serde_json::from_value(value.clone())
        .context("Invalid OKX AccountState balance provenance")?;
    let mut balances = state
        .balances
        .iter()
        .filter(|balance| balance.currency == currency);
    let Some(balance) = balances.next() else {
        return Ok(None);
    };
    anyhow::ensure!(
        balances.next().is_none(),
        "OKX AccountState reports duplicate currency balances"
    );
    let timestamp = info
        .balance_timestamps
        .get(currency.code.as_str())
        .context("OKX AccountState is missing currency update time")?;
    anyhow::ensure!(
        *timestamp != UnixNanos::default() && *timestamp <= state.ts_event,
        "OKX AccountState has an invalid currency update time"
    );
    Ok(Some((*balance, *timestamp)))
}

/// Returns an explicitly reported balance from a REST request started after `after`.
///
/// `after` must be a nonzero local initialization timestamp (for example, a native
/// order event's `ts_init`), never a venue timestamp. A late response to an earlier
/// request cannot satisfy this boundary. Legacy events and WebSocket updates do
/// not supply REST observation evidence. Currency update times remain unchanged;
/// callers must still validate venue chronology, balances and lifecycle state.
///
/// # Errors
///
/// Returns an error if reported balance evidence or request chronology is invalid.
pub fn reported_currency_balance_after(
    state: &AccountState,
    currency: Currency,
    after: UnixNanos,
) -> anyhow::Result<Option<(AccountBalance, UnixNanos)>> {
    anyhow::ensure!(
        after != UnixNanos::default(),
        "Missing local observation boundary"
    );
    let balance = reported_currency_balance(state, currency)?;
    let info: OKXAccountStateInfo = serde_json::from_value(
        state
            .info
            .as_ref()
            .and_then(|info| info.get("okx"))
            .context("OKX AccountState is missing balance provenance")?
            .clone(),
    )?;
    let Some(started) = info.rest_request_started else {
        return Ok(None);
    };
    anyhow::ensure!(
        info.update == OKXAccountUpdate::Rest
            && started != UnixNanos::default()
            && started <= state.ts_init,
        "Invalid OKX REST snapshot request chronology"
    );
    Ok(balance.filter(|_| started > after))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::AccountType, identifiers::AccountId};

    use super::*;
    use crate::{common::parse::parse_account_state_with_source, http::models::OKXAccount};

    #[rstest::rstest]
    fn rest_observation_preserves_currency_time_and_rejects_replayed_snapshot() {
        let response: serde_json::Value = serde_json::from_str(include_str!(
            "../../test_data/http_get_account_balance.json"
        ))
        .unwrap();
        let raw: OKXAccount = serde_json::from_value(response["data"][0].clone()).unwrap();
        let currency = Currency::get_or_create_crypto(raw.details[0].ccy.as_str());
        let state = parse_account_state_with_source(
            &raw,
            AccountId::from("OKX-001"),
            AccountType::Cash,
            300.into(),
            OKXAccountUpdate::Rest,
            Some(200.into()),
        )
        .unwrap();
        let state: AccountState =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert_eq!(
            reported_currency_balance_after(&state, currency, 199.into()).unwrap(),
            reported_currency_balance(&state, currency).unwrap()
        );

        for boundary in [200, 201, 400] {
            assert!(
                reported_currency_balance_after(&state, currency, boundary.into())
                    .unwrap()
                    .is_none()
            );
        }
        let mut delayed = state.clone();
        delayed.ts_init = 500.into();
        assert!(
            reported_currency_balance_after(&delayed, currency, 400.into())
                .unwrap()
                .is_none()
        );
        assert!(
            reported_currency_balance_after(
                &state,
                Currency::get_or_create_crypto("ABSENT"),
                199.into()
            )
            .unwrap()
            .is_none()
        );
        assert!(reported_currency_balance_after(&state, currency, 0.into()).is_err());
        let mut invalid = state.clone();
        invalid.is_reported = false;
        assert!(reported_currency_balance_after(&invalid, currency, 199.into()).is_err());
        invalid = state.clone();
        invalid.balances.push(invalid.balances[0]);
        assert!(reported_currency_balance_after(&invalid, currency, 199.into()).is_err());
        invalid = state;
        invalid.info = None;
        assert!(reported_currency_balance_after(&invalid, currency, 199.into()).is_err());
    }

    #[rstest::rstest]
    #[case(OKXAccountUpdate::Rest, None, false)]
    #[case(OKXAccountUpdate::Unknown, None, false)]
    #[case(OKXAccountUpdate::WebSocketUpdate, None, false)]
    #[case(OKXAccountUpdate::WebSocketSnapshot { page: Some(1), last_page: Some(true) }, None, false)]
    #[case(OKXAccountUpdate::Rest, Some(0), true)]
    #[case(OKXAccountUpdate::Rest, Some(301), true)]
    #[case(OKXAccountUpdate::WebSocketUpdate, Some(200), true)]
    #[case(OKXAccountUpdate::Unknown, Some(200), true)]
    fn rest_observation_requires_native_request_provenance(
        #[case] update: OKXAccountUpdate,
        #[case] started: Option<u64>,
        #[case] error: bool,
    ) {
        let response: serde_json::Value = serde_json::from_str(include_str!(
            "../../test_data/http_get_account_balance.json"
        ))
        .unwrap();
        let raw: OKXAccount = serde_json::from_value(response["data"][0].clone()).unwrap();
        let currency = Currency::get_or_create_crypto(raw.details[0].ccy.as_str());
        let state = parse_account_state_with_source(
            &raw,
            AccountId::from("OKX-001"),
            AccountType::Cash,
            300.into(),
            update,
            started.map(UnixNanos::from),
        )
        .unwrap();
        let result = reported_currency_balance_after(&state, currency, 199.into());
        if error {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_none());
        }
    }

    #[rstest::rstest]
    fn currency_freshness_survives_account_event_round_trip() {
        let response: serde_json::Value = serde_json::from_str(include_str!(
            "../../test_data/http_get_account_balance.json"
        ))
        .unwrap();
        let mut raw: OKXAccount = serde_json::from_value(response["data"][0].clone()).unwrap();
        raw.u_time = 2000;
        raw.details.truncate(1);
        raw.details[0].u_time = 1000;
        let currency = Currency::get_or_create_crypto(raw.details[0].ccy.as_str());
        let state = parse_account_state_with_source(
            &raw,
            AccountId::from("OKX-001"),
            AccountType::Cash,
            UnixNanos::from(3_000_000_000),
            OKXAccountUpdate::WebSocketUpdate,
            None,
        )
        .unwrap();
        let encoded = serde_json::to_vec(&state).unwrap();
        let mut state: AccountState = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            reported_currency_balance(&state, currency)
                .unwrap()
                .unwrap()
                .1,
            UnixNanos::from(1_000_000_000)
        );
        assert!(
            reported_currency_balance(&state, Currency::get_or_create_crypto("ABSENT"))
                .unwrap()
                .is_none()
        );
        state.balances.push(state.balances[0]);
        assert!(reported_currency_balance(&state, currency).is_err());
        state.balances.pop();
        state.is_reported = false;
        assert!(reported_currency_balance(&state, currency).is_err());
        state.is_reported = true;
        state.info = None;
        assert!(reported_currency_balance(&state, currency).is_err());
        raw.details[0].u_time = 0;
        let state = parse_account_state_with_source(
            &raw,
            AccountId::from("OKX-001"),
            AccountType::Cash,
            UnixNanos::default(),
            OKXAccountUpdate::Rest,
            None,
        )
        .unwrap();
        assert!(reported_currency_balance(&state, currency).is_err());
    }
}
