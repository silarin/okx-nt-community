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
use nautilus_model::{enums::AccountType, identifiers::AccountId, types::Money};
use rstest::rstest;

use super::*;

fn state(update: OKXAccountUpdate, started: Option<u64>, initialized: u64) -> AccountState {
    let currency = Currency::ETH();
    let mut state = AccountState::new(
        AccountId::from("OKX-001"),
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("10 ETH"),
            Money::from("0 ETH"),
            Money::from("10 ETH"),
        )],
        vec![],
        true,
        UUID4::new(),
        100.into(),
        initialized.into(),
        None,
    );
    state.info = Some(
        OKXAccountStateInfo {
            update,
            balance_timestamps: BTreeMap::from([(currency.to_string(), 100.into())]),
            rest_request_started: started.map(UnixNanos::from),
            exact_balance_currencies: BTreeSet::from([currency.to_string()]),
        }
        .into_params(),
    );
    state
}

fn observed(
    account: &AccountAny,
    after: u64,
) -> anyhow::Result<Option<(AccountBalance, UnixNanos)>> {
    reported_account_currency_balance_after(account, Currency::ETH(), after.into())
}

fn anchored(rest: AccountState) -> AccountAny {
    let mut account = AccountAny::from(state(OKXAccountUpdate::WebSocketUpdate, None, 100));
    account.apply(rest).unwrap();
    account
}

#[rstest]
#[case(OKXAccountUpdate::WebSocketUpdate)]
#[case(OKXAccountUpdate::WebSocketSnapshot { page: Some(1), last_page: Some(true) })]
fn retained_rest_observation_survives_identical_websocket_events(#[case] update: OKXAccountUpdate) {
    let rest = state(OKXAccountUpdate::Rest, Some(250), 300);
    let expected = reported_currency_balance(&rest, Currency::ETH()).unwrap();
    let mut account = anchored(rest);
    assert_eq!(observed(&account, 249).unwrap(), expected);

    for initialized in [400, 500] {
        let websocket = state(update.clone(), None, initialized);
        assert!(
            reported_currency_balance_after(&websocket, Currency::ETH(), 249.into())
                .unwrap()
                .is_none()
        );
        account.apply(websocket).unwrap();
        assert_eq!(observed(&account, 249).unwrap(), expected);
    }
    assert_eq!(observed(&account.clone(), 249).unwrap(), expected);
    for later_fill in [250, 251, 600] {
        assert!(observed(&account, later_fill).unwrap().is_none());
    }
    assert!(observed(&account, 0).is_err());
}

#[rstest]
#[case("total", false)]
#[case("locked", false)]
#[case("older_currency", false)]
#[case("newer_currency", false)]
#[case("omitted", false)]
#[case("unknown", false)]
#[case("late_rest", false)]
#[case("legacy_rest", false)]
#[case("normalized", false)]
#[case("unreported", true)]
#[case("duplicate", true)]
#[case("missing_metadata", true)]
#[case("malformed_metadata", true)]
#[case("missing_currency_time", true)]
#[case("zero_currency_time", true)]
#[case("future_currency_time", true)]
#[case("websocket_rest_time", true)]
#[case("invalid_rest_time", true)]
#[case("zero_init", true)]
#[case("reversed_init", true)]
#[case("account_type", true)]
#[case("base_currency", true)]
fn invalidating_event_cannot_be_skipped_even_if_balance_returns(
    #[case] mutation: &str,
    #[case] error: bool,
) {
    let mut account = anchored(state(OKXAccountUpdate::Rest, Some(250), 300));
    let mut changed = state(OKXAccountUpdate::WebSocketUpdate, None, 400);
    changed.ts_event = 200.into();
    let mut info: OKXAccountStateInfo =
        serde_json::from_value(changed.info.as_ref().unwrap().get("okx").unwrap().clone()).unwrap();

    match mutation {
        "total" => {
            changed.balances[0] = AccountBalance::new(
                Money::from("11 ETH"),
                Money::from("0 ETH"),
                Money::from("11 ETH"),
            );
        }
        "locked" => {
            changed.balances[0] = AccountBalance::new(
                Money::from("10 ETH"),
                Money::from("1 ETH"),
                Money::from("9 ETH"),
            );
        }
        "older_currency" => {
            info.balance_timestamps.insert("ETH".into(), 99.into());
        }
        "newer_currency" => {
            info.balance_timestamps.insert("ETH".into(), 101.into());
        }
        "omitted" => changed.balances.clear(),
        "unknown" => info.update = OKXAccountUpdate::Unknown,
        "late_rest" => {
            info.update = OKXAccountUpdate::Rest;
            info.rest_request_started = Some(200.into());
        }
        "legacy_rest" => info.update = OKXAccountUpdate::Rest,
        "normalized" => info.exact_balance_currencies.clear(),
        "unreported" => changed.is_reported = false,
        "duplicate" => changed.balances.push(changed.balances[0]),
        "missing_metadata" | "malformed_metadata" => {}
        "missing_currency_time" => info.balance_timestamps.clear(),
        "zero_currency_time" => {
            info.balance_timestamps.insert("ETH".into(), 0.into());
        }
        "future_currency_time" => {
            info.balance_timestamps.insert("ETH".into(), 201.into());
        }
        "websocket_rest_time" => info.rest_request_started = Some(350.into()),
        "invalid_rest_time" => {
            info.update = OKXAccountUpdate::Rest;
            info.rest_request_started = Some(401.into());
        }
        "zero_init" => changed.ts_init = 0.into(),
        "reversed_init" => changed.ts_init = 299.into(),
        "account_type" => changed.account_type = AccountType::Margin,
        "base_currency" => changed.base_currency = Some(Currency::ETH()),
        _ => unreachable!(),
    }
    changed.info = Some(info.into_params());
    if mutation == "missing_metadata" {
        changed.info = None;
    }

    if mutation == "malformed_metadata" {
        changed.info = Some(Params::new(
            [("okx".to_string(), serde_json::json!({"update":false}))]
                .into_iter()
                .collect(),
        ));
    }
    account.apply(changed).unwrap();

    for restore_balance in [false, true] {
        if restore_balance {
            account
                .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 500))
                .unwrap();
        }
        let result = observed(&account, 200);
        if error {
            assert!(result.is_err(), "{mutation}");
        } else {
            assert!(result.unwrap().is_none(), "{mutation}");
        }
    }
    account
        .apply(state(OKXAccountUpdate::Rest, Some(550), 600))
        .unwrap();
    assert_eq!(
        observed(&account, 200).unwrap().is_some(),
        mutation != "zero_init"
    );
}

#[rstest]
fn purge_cannot_hide_a_contradiction_or_replace_latest_event() {
    let mut rest = state(OKXAccountUpdate::Rest, Some(250), 300);
    rest.ts_event = 300.into();
    let mut account = anchored(rest.clone());
    let mut conflict = state(OKXAccountUpdate::WebSocketUpdate, None, 400);
    conflict.balances[0] = AccountBalance::new(
        Money::from("11 ETH"),
        Money::from("0 ETH"),
        Money::from("11 ETH"),
    );
    account.apply(conflict.clone()).unwrap();
    account.purge_account_events(200.into(), 0);
    assert_eq!(account.last_event(), Some(conflict.clone()));
    assert!(observed(&account, 200).unwrap().is_none());

    let mut account = anchored(rest);
    account.apply(conflict).unwrap();
    let mut websocket = state(OKXAccountUpdate::WebSocketUpdate, None, 500);
    websocket.ts_event = 400.into();
    account.apply(websocket.clone()).unwrap();
    assert!(observed(&account, 200).unwrap().is_none());
    account.purge_account_events(200.into(), 0);
    assert_eq!(account.events().len(), 2);
    assert_eq!(account.contiguous_events().0, &[websocket]);
    assert!(observed(&account, 200).unwrap().is_none());
    account.purge_account_events(200.into(), 0);
    assert!(observed(&account, 200).unwrap().is_none());
    account
        .apply(state(OKXAccountUpdate::Rest, Some(550), 600))
        .unwrap();
    account
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 700))
        .unwrap();
    assert!(observed(&account, 200).unwrap().is_some());
}

#[rstest]
fn prefix_and_full_purge_keep_only_available_observation_proof() {
    let mut account = AccountAny::from(state(OKXAccountUpdate::Rest, Some(50), 100));
    let mut rest = state(OKXAccountUpdate::Rest, Some(250), 300);
    rest.ts_event = 300.into();
    account.apply(rest).unwrap();
    let mut websocket = state(OKXAccountUpdate::WebSocketUpdate, None, 400);
    websocket.ts_event = 400.into();
    account.apply(websocket).unwrap();
    account.purge_account_events(200.into(), 0);
    assert_eq!(account.contiguous_events().0.len(), 2);
    // The pre-request anchor was purged, so this request cannot bridge the lost prefix.
    assert!(observed(&account, 200).unwrap().is_none());
    account
        .apply(state(OKXAccountUpdate::Rest, Some(450), 500))
        .unwrap();
    assert!(observed(&account, 200).unwrap().is_some());
    account.purge_account_events(1000.into(), 0);
    assert_eq!(account.contiguous_events().0.len(), 1);
    assert!(observed(&account, 200).unwrap().is_none());
}

#[rstest]
#[case("serde")]
#[case("events")]
#[case("without_events")]
fn restored_history_requires_new_observation(#[case] restore: &str) {
    let mut original = AccountAny::from(state(OKXAccountUpdate::Rest, Some(250), 300));
    original
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 400))
        .unwrap();
    let mut account = match restore {
        "serde" => {
            serde_json::from_slice::<AccountAny>(&serde_json::to_vec(&original).unwrap()).unwrap()
        }
        "events" => AccountAny::from_events(&original.events()).unwrap(),
        "without_events" => original.clone_without_events(),
        _ => unreachable!(),
    };
    assert!(account.contiguous_events().0.is_empty());
    account.purge_account_events(0.into(), 0);
    assert!(account.contiguous_events().0.is_empty());
    account
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 500))
        .unwrap();
    assert!(observed(&account, 200).unwrap().is_none());
    account
        .apply(state(OKXAccountUpdate::Rest, Some(550), 600))
        .unwrap();
    account
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 700))
        .unwrap();
    assert_eq!(
        observed(&account, 200).unwrap().is_some(),
        restore != "events"
    );
}

#[rstest]
#[case("purge", 180, true)]
#[case("clone", 180, true)]
#[case("serde", 180, true)]
#[case("legacy", 180, false)]
#[case("zero", 0, false)]
fn excluded_history_cannot_hide_activity_during_a_rest_request(
    #[case] operation: &str,
    #[case] earlier_init: u64,
    #[case] can_recover: bool,
) {
    // An older retained record also precedes the purge-created frontier.
    let mut prefix = state(OKXAccountUpdate::WebSocketUpdate, None, earlier_init);
    prefix.ts_event = 400.into();
    let mut account = AccountAny::from(prefix);
    let mut barrier = state(OKXAccountUpdate::Unknown, None, 150);
    barrier.is_reported = false;
    barrier.balances.clear();
    account.apply(barrier).unwrap();
    let mut delayed = state(OKXAccountUpdate::WebSocketUpdate, None, 50);
    delayed.ts_event = 400.into();
    account.apply(delayed.clone()).unwrap();
    account.purge_account_events(200.into(), 0);
    assert_eq!(account.contiguous_events().0, &[delayed]);
    assert_eq!(
        account.contiguous_events().1,
        (earlier_init != 0).then(|| 180.into())
    );

    if operation == "clone" {
        account = account.clone_without_events();
    } else if matches!(operation, "serde" | "legacy") {
        let mut value = serde_json::to_value(&account).unwrap();
        if operation == "legacy" {
            value["Cash"]["base"]
                .as_object_mut()
                .unwrap()
                .remove("event_history_cutoff");
        }
        account = serde_json::from_value(value).unwrap();
    }
    // A delayed old source event cannot anchor a request which overlapped the lost barrier.
    account
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 60))
        .unwrap();
    account
        .apply(state(OKXAccountUpdate::Rest, Some(100), 200))
        .unwrap();
    assert!(observed(&account, 90).unwrap().is_none());
    account
        .apply(state(OKXAccountUpdate::Rest, Some(250), 300))
        .unwrap();
    assert_eq!(observed(&account, 90).unwrap().is_some(), can_recover);
}

#[rstest]
#[case(250)]
#[case(275)]
fn rest_request_overlapping_an_account_event_cannot_establish_proof(#[case] initialized: u64) {
    let mut account = AccountAny::from(state(OKXAccountUpdate::WebSocketUpdate, None, initialized));
    // A delayed predecessor must not hide the overlapping event.
    account
        .apply(state(OKXAccountUpdate::WebSocketUpdate, None, 100))
        .unwrap();
    account
        .apply(state(OKXAccountUpdate::Rest, Some(250), 300))
        .unwrap();
    assert!(observed(&account, 200).unwrap().is_none());
    account
        .apply(state(OKXAccountUpdate::Rest, Some(350), 400))
        .unwrap();
    assert!(observed(&account, 200).unwrap().is_some());
}

#[rstest]
#[case("10", "11", false)]
#[case("10.0000000001", "10", cfg!(feature = "high-precision"))]
#[case("10", "9.9999999999", cfg!(feature = "high-precision"))]
#[case("10.0000000000000000000000000000000001", "10", false)]
#[case("10", "9.9999999999999999999999999999999999", false)]
fn changed_source_values_cannot_prove_an_unchanged_balance(
    #[case] total: &str,
    #[case] free: &str,
    #[case] exact_change: bool,
) {
    use crate::{common::parse::parse_account_state_with_source, http::models::OKXAccount};
    let mut payload: serde_json::Value = serde_json::from_str(include_str!(
        "../../../test_data/http_get_account_balance.json"
    ))
    .unwrap();
    let raw = &mut payload["data"][0];
    raw["uTime"] = serde_json::json!("1");
    let mut detail = raw["details"][0].clone();
    detail["ccy"] = serde_json::json!("ETH");
    detail["cashBal"] = serde_json::json!("10");
    detail["availBal"] = serde_json::json!("10");
    detail["uTime"] = serde_json::json!("1");
    raw["details"] = serde_json::json!([detail]);
    let initial: OKXAccount = serde_json::from_value(raw.clone()).unwrap();
    let rest = parse_account_state_with_source(
        &initial,
        AccountId::from("OKX-001"),
        AccountType::Cash,
        300.into(),
        OKXAccountUpdate::Rest,
        Some(250.into()),
    )
    .unwrap();
    let expected = reported_currency_balance(&rest, Currency::ETH()).unwrap();
    let mut account = anchored(rest);
    assert_eq!(observed(&account, 200).unwrap(), expected);
    raw["details"][0]["cashBal"] = serde_json::json!(total);
    raw["details"][0]["availBal"] = serde_json::json!(free);
    let changed: OKXAccount = serde_json::from_value(raw.clone()).unwrap();
    let websocket = parse_account_state_with_source(
        &changed,
        AccountId::from("OKX-001"),
        AccountType::Cash,
        400.into(),
        OKXAccountUpdate::WebSocketUpdate,
        None,
    )
    .unwrap();
    let reported = reported_currency_balance(&websocket, Currency::ETH()).unwrap();
    let info: OKXAccountStateInfo =
        serde_json::from_value(websocket.info.as_ref().unwrap()["okx"].clone()).unwrap();
    assert_eq!(info.exact_balance_currencies.contains("ETH"), exact_change);
    if exact_change {
        let (balance, _) = reported.unwrap();
        assert_eq!(balance.total.as_decimal(), total.parse().unwrap());
        assert_eq!(balance.free.as_decimal(), free.parse().unwrap());
        assert_ne!(reported, expected);
    } else {
        // Rounded or clamped equality still cannot certify unchanged source balances
        assert_eq!(reported, expected);
    }
    account.apply(websocket).unwrap();
    assert!(observed(&account, 200).unwrap().is_none());
}
