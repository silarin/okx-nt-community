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

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    accounts::{Account, BettingAccount, CashAccount, MarginAccount, WalletAccount},
    events::AccountState,
};

/// Constructs a `CashAccount` from a list of Python dict events.
///
/// # Errors
///
/// Returns an error if an event cannot be converted or the input `events` list is empty.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.model")]
#[pyo3(signature = (events, calculate_account_state, allow_borrowing = false))]
pub fn cash_account_from_account_events(
    events: Vec<Bound<'_, PyDict>>,
    calculate_account_state: bool,
    allow_borrowing: bool,
) -> PyResult<CashAccount> {
    let account_events = events
        .into_iter()
        .map(|obj| AccountState::py_from_dict(&obj))
        .collect::<PyResult<Vec<AccountState>>>()?;

    if account_events.is_empty() {
        return Err(to_pyvalue_err("No account events"));
    }
    let init_event = account_events[0].clone();
    let mut cash_account = CashAccount::new(init_event, calculate_account_state, allow_borrowing);
    for event in account_events.iter().skip(1) {
        cash_account
            .apply(event.clone())
            .map_err(to_pyruntime_err)?;
    }
    cash_account.invalidate_event_history();
    Ok(cash_account)
}

/// Constructs a `BettingAccount` from a list of Python dict events.
///
/// # Errors
///
/// Returns an error if the input `events` list is empty.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.model")]
pub fn betting_account_from_account_events(
    events: Vec<Bound<'_, PyDict>>,
    calculate_account_state: bool,
) -> PyResult<BettingAccount> {
    let account_events = events
        .into_iter()
        .map(|obj| AccountState::py_from_dict(&obj))
        .collect::<PyResult<Vec<AccountState>>>()?;

    if account_events.is_empty() {
        return Err(to_pyvalue_err("No account events"));
    }
    let init_event = account_events[0].clone();
    let mut betting_account = BettingAccount::new(init_event, calculate_account_state);
    for event in account_events.iter().skip(1) {
        betting_account
            .apply(event.clone())
            .map_err(to_pyruntime_err)?;
    }
    betting_account.invalidate_event_history();
    Ok(betting_account)
}

/// Constructs a `WalletAccount` from a list of Python dict events.
///
/// # Errors
///
/// Returns an error if an event cannot be converted or the input `events` list is empty.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.model")]
#[pyo3(signature = (events, calculate_account_state))]
pub fn wallet_account_from_account_events(
    events: Vec<Bound<'_, PyDict>>,
    calculate_account_state: bool,
) -> PyResult<WalletAccount> {
    let account_events = events
        .into_iter()
        .map(|obj| AccountState::py_from_dict(&obj))
        .collect::<PyResult<Vec<AccountState>>>()?;

    let Some((init_event, remaining_events)) = account_events.split_first() else {
        return Err(to_pyvalue_err("No account events"));
    };

    let mut wallet_account =
        WalletAccount::new_checked(init_event.clone(), calculate_account_state)
            .map_err(to_pyvalue_err)?;

    for event in remaining_events {
        wallet_account
            .apply(event.clone())
            .map_err(to_pyruntime_err)?;
    }

    wallet_account.invalidate_event_history();
    Ok(wallet_account)
}

/// Constructs a `MarginAccount` from a list of Python dict events.
///
/// # Errors
///
/// Returns an error if an event cannot be converted or the input `events` list is empty.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.model")]
pub fn margin_account_from_account_events(
    events: Vec<Bound<'_, PyDict>>,
    calculate_account_state: bool,
) -> PyResult<MarginAccount> {
    let account_events = events
        .into_iter()
        .map(|obj| AccountState::py_from_dict(&obj))
        .collect::<PyResult<Vec<AccountState>>>()?;

    if account_events.is_empty() {
        return Err(to_pyvalue_err("No account events"));
    }
    let init_event = account_events[0].clone();
    let mut margin_account = MarginAccount::new(init_event, calculate_account_state);
    for event in account_events.iter().skip(1) {
        margin_account
            .apply(event.clone())
            .map_err(to_pyruntime_err)?;
    }
    margin_account.invalidate_event_history();
    Ok(margin_account)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        accounts::AccountAny, enums::AccountType, events::account::stubs::cash_account_state,
    };

    #[rstest]
    #[case(AccountType::Cash)]
    #[case(AccountType::Margin)]
    #[case(AccountType::Betting)]
    #[case(AccountType::Wallet)]
    fn reconstructed_python_events_do_not_assert_history_continuity(#[case] kind: AccountType) {
        Python::initialize();
        Python::attach(|py| {
            let mut event = cash_account_state();
            event.account_type = kind;
            event.base_currency = None;
            let object = event.py_to_dict(py).unwrap();
            let events = vec![object.bind(py).cast::<PyDict>().unwrap().clone()];
            let mut account = match kind {
                AccountType::Cash => AccountAny::Cash(
                    cash_account_from_account_events(events, false, false).unwrap(),
                ),
                AccountType::Margin => {
                    AccountAny::Margin(margin_account_from_account_events(events, false).unwrap())
                }
                AccountType::Betting => {
                    AccountAny::Betting(betting_account_from_account_events(events, false).unwrap())
                }
                AccountType::Wallet => {
                    AccountAny::Wallet(wallet_account_from_account_events(events, false).unwrap())
                }
            };
            assert_eq!(account.events().len(), 1);
            assert!(account.contiguous_events().0.is_empty());
            account.apply(event.clone()).unwrap();
            assert_eq!(account.contiguous_events().0, &[event]);
        });
    }
}
