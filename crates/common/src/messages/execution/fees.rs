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

//! Cumulative commission observations, without authority to apply executions.

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, VenueOrderId},
    types::Currency,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Selects independently reported components of native commissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommissionBasis {
    /// Positive commissions, excluding rebates.
    ChargesOnly,
    /// Negative commissions, excluding charges.
    RebatesOnly,
    /// Signed commissions including both charges and rebates.
    Net,
    /// Charges for an all-taker prefix, net for an all-maker prefix.
    /// Mixed maker/taker liquidity resolves only when charges and net agree;
    /// unknown liquidity remains unresolved.
    UniformLiquidity,
}

/// A cumulative amount and currency are independent facts, never execution inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CumulativeCommissionConstraint {
    /// Venue-documented aggregation; unknown semantics cannot establish recovery.
    #[serde(deserialize_with = "Option::deserialize")]
    pub basis: Option<CommissionBasis>,
    /// Exact signed amount in native commission convention.
    #[serde(deserialize_with = "Option::deserialize")]
    pub amount: Option<Decimal>,
    /// Independently reported currency, without inferred conversion.
    #[serde(deserialize_with = "Option::deserialize")]
    pub currency: Option<Currency>,
}

/// Source authority for a cumulative commission observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderFeeOrigin {
    /// A private stream update does not establish complete queried history.
    Stream,
    /// An order snapshot may be accompanied by complete authoritative history.
    Query,
}

/// Immutable cumulative commission facts which constrain native execution recovery.
/// This record cannot apply a fill, amend an order, or change an account balance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderFeeEvidence {
    /// Account whose order was observed.
    pub account_id: AccountId,
    /// Native instrument identity.
    pub instrument_id: InstrumentId,
    /// Venue order identity.
    pub venue_order_id: VenueOrderId,
    /// Independently known client order identity.
    #[serde(deserialize_with = "Option::deserialize")]
    pub client_order_id: Option<ClientOrderId>,
    /// Native venue position mapping.
    #[serde(deserialize_with = "Option::deserialize")]
    pub venue_position_id: Option<PositionId>,
    /// Observed order side.
    pub order_side: OrderSide,
    /// Exact original cumulative quantity, without instrument-grid rounding.
    #[serde(deserialize_with = "Option::deserialize")]
    pub boundary_quantity: Option<Decimal>,
    /// Original venue snapshot boundary, never replaced by a newer query's boundary.
    #[serde(deserialize_with = "Option::deserialize")]
    pub boundary_time: Option<UnixNanos>,
    /// Independently known execution inside the original prefix.
    #[serde(deserialize_with = "Option::deserialize")]
    pub anchor_trade_id: Option<TradeId>,
    /// Execution boundary for tied-time prefix selection, when independently known.
    #[serde(deserialize_with = "Option::deserialize")]
    pub anchor_trade_time: Option<UnixNanos>,
    /// Distinguishes stream observations from query snapshots.
    pub origin: OrderFeeOrigin,
    /// Independently reported cumulative components.
    pub constraints: Vec<CumulativeCommissionConstraint>,
    /// Numeric facts exceed exact representation and cannot be silently recovered.
    pub unrepresentable_values: bool,
    /// Incomplete or malformed fields require later authoritative query evidence.
    pub requires_fresh_query: bool,
    /// Observation time, or originating query start time.
    pub ts_init: UnixNanos,
}

impl OrderFeeEvidence {
    /// Compares fully specified cumulative query facts independently of receipt time.
    /// This does not establish complete execution history or transfer recovery proof.
    #[must_use]
    fn same_complete_query_facts(&self, other: &Self) -> bool {
        if self.origin != OrderFeeOrigin::Query
            || self.requires_fresh_query
            || self.unrepresentable_values
            || self.boundary_quantity.is_none()
            || self.boundary_time.is_none()
            || self.constraints.is_empty()
            || self.constraints.iter().any(|constraint| {
                constraint.basis.is_none()
                    || constraint.amount.is_none()
                    || constraint.currency.is_none()
            })
        {
            return false;
        }
        let mut comparable = other.clone();
        comparable.ts_init = self.ts_init;
        self == &comparable
    }

    /// Returns redundant indices among identical, fully specified query observations.
    /// Input order is receipt/capture order. First, last, minimum query time and maximum
    /// query time retain their original records; completion proof is never transferred.
    #[must_use]
    pub fn redundant_query_observation_indices<'a>(
        &self,
        observations: impl Iterator<Item = (usize, &'a Self)>,
    ) -> Vec<usize> {
        if !self.same_complete_query_facts(self) {
            return Vec::new();
        }
        let matching: Vec<_> = observations
            .filter(|(_, previous)| previous.same_complete_query_facts(self))
            .map(|(index, previous)| (index, previous.ts_init))
            .collect();
        let retained = [
            matching.first().map(|(index, _)| *index),
            matching.last().map(|(index, _)| *index),
            matching
                .iter()
                .min_by_key(|(_, time)| time)
                .map(|(index, _)| *index),
            matching
                .iter()
                .max_by_key(|(_, time)| time)
                .map(|(index, _)| *index),
        ];
        matching
            .into_iter()
            .filter(|(index, _)| !retained.contains(&Some(*index)))
            .map(|(index, _)| index)
            .collect()
    }

    /// Compares semantic retransmissions while retaining their original time boundary.
    #[must_use]
    pub fn same_observation(&self, other: &Self) -> bool {
        if self.requires_fresh_query || other.requires_fresh_query {
            return false;
        }
        let mut comparable = other.clone();
        if self.origin == OrderFeeOrigin::Stream {
            comparable.ts_init = self.ts_init;
        }

        if self.origin == OrderFeeOrigin::Stream && self.anchor_trade_id.is_some() {
            comparable.boundary_time = self.boundary_time;
        }
        self == &comparable
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn observation() -> OrderFeeEvidence {
        OrderFeeEvidence {
            account_id: AccountId::from("OKX-001"),
            instrument_id: InstrumentId::from("BTC-USD.OKX"),
            venue_order_id: VenueOrderId::from("V1"),
            client_order_id: None,
            venue_position_id: None,
            order_side: OrderSide::Buy,
            boundary_quantity: Some(Decimal::ONE),
            boundary_time: Some(UnixNanos::from(10)),
            anchor_trade_id: Some(TradeId::from("T1")),
            anchor_trade_time: Some(UnixNanos::from(10)),
            origin: OrderFeeOrigin::Stream,
            constraints: vec![CumulativeCommissionConstraint {
                basis: Some(CommissionBasis::ChargesOnly),
                amount: Some(Decimal::ONE),
                currency: Some(Currency::USD()),
            }],
            unrepresentable_values: false,
            requires_fresh_query: false,
            ts_init: UnixNanos::from(100),
        }
    }

    #[rstest]
    fn cumulative_commission_duplicates_preserve_boundary_and_query_freshness() {
        let first = observation();
        let mut duplicate = first.clone();
        duplicate.ts_init = UnixNanos::from(101);
        duplicate.boundary_time = Some(UnixNanos::from(11));
        assert!(first.same_observation(&duplicate));
        duplicate.constraints[0].amount = Some(Decimal::from(2));
        assert!(!first.same_observation(&duplicate));
        let mut query = first;
        query.origin = OrderFeeOrigin::Query;
        let mut repeated = query.clone();
        repeated.ts_init = UnixNanos::from(101);
        assert!(!query.same_observation(&repeated));
    }

    #[rstest]
    fn complete_query_equivalence_excludes_missing_changed_and_malformed_facts() {
        let mut first = observation();
        first.origin = OrderFeeOrigin::Query;
        let mut repeated = first.clone();
        repeated.ts_init = UnixNanos::from(200);
        assert!(first.same_complete_query_facts(&repeated));
        assert!(!first.same_observation(&repeated));

        for field in [
            "fresh",
            "unrepresentable",
            "currency",
            "amount",
            "basis",
            "quantity",
            "time",
            "client",
            "boundary",
            "commission",
        ] {
            let mut changed = first.clone();
            match field {
                "fresh" => changed.requires_fresh_query = true,
                "unrepresentable" => changed.unrepresentable_values = true,
                "currency" => changed.constraints[0].currency = None,
                "amount" => changed.constraints[0].amount = None,
                "basis" => changed.constraints[0].basis = None,
                "quantity" => changed.boundary_quantity = None,
                "time" => changed.boundary_time = None,
                "client" => changed.client_order_id = Some(ClientOrderId::from("DIFFERENT")),
                "boundary" => changed.boundary_quantity = Some(Decimal::from(2)),
                "commission" => changed.constraints[0].amount = Some(Decimal::from(2)),
                _ => unreachable!(),
            }
            assert!(!first.same_complete_query_facts(&changed), "{field}");
            assert!(!changed.same_complete_query_facts(&first), "{field}");
        }
    }

    #[rstest]
    fn cumulative_commission_schema_requires_explicit_unknowns() {
        let first = observation();
        let value = serde_json::to_value(&first).unwrap();
        assert_eq!(
            serde_json::from_value::<OrderFeeEvidence>(value.clone()).unwrap(),
            first
        );

        for field in [
            "boundary_quantity",
            "boundary_time",
            "anchor_trade_id",
            "client_order_id",
        ] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<OrderFeeEvidence>(missing).is_err());
        }
        let mut unknown = value;
        unknown["boundary_quantity"] = serde_json::Value::Null;
        assert_eq!(
            serde_json::from_value::<OrderFeeEvidence>(unknown)
                .unwrap()
                .boundary_quantity,
            None
        );
    }
}
