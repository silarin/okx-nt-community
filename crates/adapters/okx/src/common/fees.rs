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

//! Instrument-specific fee group selection and OKX fee sign normalization.

use anyhow::Context;
use nautilus_model::fees::MakerTakerFeeRates;
use rust_decimal::Decimal;

use super::{enums::OKXInstrumentType, models::OKXInstrument};
use crate::http::models::OKXFeeRate;

/// Selects the unique fee group for one SPOT/MARGIN instrument, converting OKX's negative charges
/// to Nautilus positive charges. Missing rates are never interpreted as zero.
///
/// # Errors
///
/// Returns an error if evidence is missing, ambiguous, invalid, or outside SPOT/MARGIN scope.
pub fn select_instrument_fee_rates(
    inst_type: OKXInstrumentType,
    symbol: &str,
    instruments: &[OKXInstrument],
    fees: &[OKXFeeRate],
) -> anyhow::Result<MakerTakerFeeRates> {
    anyhow::ensure!(
        matches!(
            inst_type,
            OKXInstrumentType::Spot | OKXInstrumentType::Margin
        ),
        "fee selection requires SPOT or MARGIN percentage rates"
    );
    anyhow::ensure!(
        instruments.len() == 1,
        "fee metadata requires exactly one instrument"
    );
    let instrument = &instruments[0];
    anyhow::ensure!(
        instrument.inst_type == inst_type && instrument.inst_id.as_str() == symbol,
        "fee metadata does not match the configured instrument scope"
    );
    let group_id = instrument
        .group_id
        .as_ref()
        .filter(|id| !id.as_str().is_empty())
        .context("instrument metadata is missing its fee group")?;
    anyhow::ensure!(
        fees.len() == 1,
        "account fees require exactly one scoped response"
    );
    let fee = &fees[0];
    anyhow::ensure!(
        fee.inst_type == inst_type && fee.ts > 0,
        "account fee response has invalid scope or timestamp"
    );
    let mut groups = fee
        .fee_group
        .iter()
        .filter(|group| group.group_id == group_id.as_str());
    let group = groups
        .next()
        .context("account fee response is missing the instrument fee group")?;
    anyhow::ensure!(
        groups.next().is_none(),
        "account fee response has duplicate instrument fee groups"
    );
    let maker = group
        .maker
        .context("account fee group is missing the maker rate")?;
    let taker = group
        .taker
        .context("account fee group is missing the taker rate")?;
    anyhow::ensure!(
        maker.abs() < Decimal::ONE && taker.abs() < Decimal::ONE,
        "account fee rates must have magnitude below one"
    );
    // OKX charges are negative; Nautilus charges are positive. Explicit zero
    // remains valid evidence. Never substitute missing rates with zero or use
    // legacy category/scalar fees. These strategies do not submit RPI orders.
    Ok(MakerTakerFeeRates::new(-maker, -taker))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn evidence() -> (Vec<OKXInstrument>, Vec<OKXFeeRate>) {
        // Synthetic native response, with category deliberately different from
        // groupId and conflicting deprecated scalar fees to catch fallback use.
        let instrument = json!({
            "instType":"SPOT", "instId":"ETH-USDT", "groupId":"4", "category":"1",
            "uly":"", "instFamily":"", "baseCcy":"ETH", "quoteCcy":"USDT",
            "settleCcy":"", "ctVal":"", "ctMult":"", "ctValCcy":"", "optType":"",
            "stk":"", "listTime":"", "expTime":"", "lever":"", "tickSz":"0.01",
            "lotSz":"0.000001", "minSz":"0.0001", "ctType":"", "state":"live", "ruleType":"normal"
        });
        let fee = json!({
            "instType":"SPOT", "level":"Lv1", "category":"1", "ts":"1763979985847",
            "maker":"-0.8", "taker":"-0.9",
            "feeGroup":[{"groupId":"1", "maker":"-0.7", "taker":"-0.8"},
                {"groupId":"4", "maker":"-0.0008", "taker":"-0.001", "rpiMaker":"0.2", "elpMaker":"0.3"}]
        });
        (
            vec![serde_json::from_value(instrument).unwrap()],
            vec![serde_json::from_value(fee).unwrap()],
        )
    }

    #[rstest::rstest]
    fn account_fee_selection_matches_product_and_group_and_inverts_okx_signs() {
        for inst_type in [OKXInstrumentType::Spot, OKXInstrumentType::Margin] {
            for rate in [Decimal::new(-1, 3), Decimal::ZERO, Decimal::new(1, 3)] {
                let (mut instruments, mut fees) = evidence();
                instruments[0].inst_type = inst_type;
                fees[0].inst_type = inst_type;
                fees[0].fee_group[1].maker = Some(rate);
                fees[0].fee_group[1].taker = Some(rate);
                assert_eq!(
                    select_instrument_fee_rates(inst_type, "ETH-USDT", &instruments, &fees)
                        .unwrap(),
                    MakerTakerFeeRates::new(-rate, -rate)
                );
            }
        }
    }

    #[rstest::rstest]
    fn fee_selection_rejects_missing_ambiguous_or_out_of_scope_evidence() {
        for case in 0..13 {
            let (mut instruments, mut fees) = evidence();
            match case {
                0 => instruments.clear(),
                1 => instruments.push(instruments[0].clone()),
                2 => instruments[0].inst_id = "BTC-USDT".into(),
                3 => instruments[0].inst_type = OKXInstrumentType::Margin,
                4 => instruments[0].group_id = None,
                5 => fees.clear(),
                6 => fees.push(fees[0].clone()),
                7 => fees[0].inst_type = OKXInstrumentType::Margin,
                8 => fees[0].ts = 0,
                9 => fees[0].fee_group.truncate(1),
                10 => {
                    let group = fees[0].fee_group[1].clone();
                    fees[0].fee_group.push(group);
                }
                11 => fees[0].fee_group[1].maker = Some(Decimal::ONE),
                12 => fees[0].fee_group[1].taker = Some(-Decimal::ONE),
                _ => unreachable!(),
            }
            assert!(
                select_instrument_fee_rates(
                    OKXInstrumentType::Spot,
                    "ETH-USDT",
                    &instruments,
                    &fees
                )
                .is_err(),
                "case {case}"
            );
        }
    }

    #[rstest::rstest]
    fn missing_or_empty_rates_never_fall_back_to_legacy_scalars_or_zero() {
        for key in ["maker", "taker"] {
            for value in [None, Some(Value::String(String::new()))] {
                let (instruments, mut fees) = evidence();
                let mut group = json!({"groupId":"4", "maker":"0", "taker":"0"});
                if let Some(value) = value {
                    group[key] = value;
                } else {
                    group.as_object_mut().unwrap().remove(key);
                }
                fees[0].fee_group = vec![serde_json::from_value(group).unwrap()];
                assert!(
                    select_instrument_fee_rates(
                        OKXInstrumentType::Spot,
                        "ETH-USDT",
                        &instruments,
                        &fees
                    )
                    .is_err()
                );
            }
        }
    }
    #[rstest::rstest]
    #[case(OKXInstrumentType::Any)]
    #[case(OKXInstrumentType::Swap)]
    #[case(OKXInstrumentType::Futures)]
    #[case(OKXInstrumentType::Option)]
    #[case(OKXInstrumentType::Events)]
    fn fee_selection_rejects_products_outside_cash_percentage_contract(
        #[case] inst_type: OKXInstrumentType,
    ) {
        let (mut instruments, mut fees) = evidence();
        instruments[0].inst_type = inst_type;
        fees[0].inst_type = inst_type;
        let error =
            select_instrument_fee_rates(inst_type, "ETH-USDT", &instruments, &fees).unwrap_err();
        assert!(error.to_string().contains("SPOT or MARGIN"));
    }
}
