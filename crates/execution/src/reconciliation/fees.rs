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

//! Exact cumulative commission constraints over independently complete execution prefixes.

use std::collections::HashSet;

use nautilus_common::messages::execution::{
    CommissionBasis, CumulativeCommissionConstraint, OrderFeeEvidence, OrderFeeOrigin,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::LiquiditySide,
    events::OrderEventAny,
    identifiers::ClientOrderId,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
};
use rust_decimal::Decimal;

/// Result of checking immutable cumulative facts, without applying accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeEvidenceState {
    /// One complete execution prefix satisfies every independently known fact.
    Satisfied,
    /// Evidence, authority, exact arithmetic, or bounded search is incomplete.
    Pending,
    /// Complete independently known executions contradict retained facts.
    Conflict,
}

/// Evaluates one original quantity and commission boundary using a joint exact witness.
///
/// `complete` must prove the supplied executions cover the original boundary, independently
/// of this subset search. `query_ts` identifies the complete query supplying that authority.
/// Native contiguous stream history may omit it. Corroborating query observations can supply
/// missing fields only for the same original execution boundary; they cannot replace facts.
#[must_use]
pub fn evaluate_order_fee_evidence(
    evidence: &OrderFeeEvidence,
    fills: &[FillReport],
    canonical_client_order_id: Option<ClientOrderId>,
    complete: bool,
    query_ts: Option<UnixNanos>,
    corroborating: &[OrderFeeEvidence],
) -> FeeEvidenceState {
    const MAX_REACHABLE_PREFIXES: usize = 4_096;

    if evidence.unrepresentable_values {
        return FeeEvidenceState::Pending;
    }

    if evidence
        .client_order_id
        .is_some_and(|id| canonical_client_order_id != Some(id))
    {
        return FeeEvidenceState::Conflict;
    }
    let peers: Vec<_> = corroborating
        .iter()
        .filter(|peer| {
            !peer.unrepresentable_values
                && peer.origin == OrderFeeOrigin::Query
                && peer.account_id == evidence.account_id
                && peer.instrument_id == evidence.instrument_id
                && peer.venue_order_id == evidence.venue_order_id
                && peer.order_side == evidence.order_side
                && peer.venue_position_id == evidence.venue_position_id
                && peer
                    .client_order_id
                    .is_none_or(|id| canonical_client_order_id == Some(id))
                && (peer.ts_init > evidence.ts_init
                    || (!evidence.requires_fresh_query && peer.ts_init == evidence.ts_init))
                && evidence
                    .anchor_trade_id
                    .is_none_or(|id| peer.anchor_trade_id == Some(id))
                && if let Some(quantity) = evidence.boundary_quantity {
                    peer.boundary_quantity == Some(quantity)
                } else {
                    evidence.boundary_time.is_some() && peer.boundary_time == evidence.boundary_time
                }
        })
        .collect();
    let target = evidence
        .boundary_quantity
        .or_else(|| peers.first().and_then(|peer| peer.boundary_quantity));
    let boundary = evidence.boundary_time.or_else(|| {
        evidence
            .anchor_trade_id
            .and_then(|_| peers.first().and_then(|peer| peer.boundary_time))
    });
    let (Some(target), Some(boundary)) = (target, boundary) else {
        return FeeEvidenceState::Pending;
    };

    if target.is_sign_negative() {
        return FeeEvidenceState::Conflict;
    }
    let mut constraints = evidence.constraints.clone();
    let mut unresolved =
        evidence.requires_fresh_query && query_ts.is_none_or(|time| time <= evidence.ts_init);

    for constraint in &mut constraints {
        if constraint.currency.is_none() && constraint.basis.is_some() {
            let mut currencies = peers
                .iter()
                .flat_map(|peer| &peer.constraints)
                .filter(|known| {
                    known.basis == constraint.basis
                        && constraint
                            .amount
                            .is_none_or(|amount| known.amount == Some(amount))
                })
                .filter_map(|known| known.currency);
            if let Some(currency) = currencies.next()
                && currencies.all(|other| other == currency)
            {
                constraint.currency = Some(currency);
            }
        }

        if constraint.amount.is_none() {
            let mut amounts = peers
                .iter()
                .flat_map(|peer| &peer.constraints)
                .filter(|known| {
                    known.basis == constraint.basis && known.currency == constraint.currency
                })
                .filter_map(|known| known.amount);
            if let Some(amount) = amounts.next()
                && amounts.all(|other| other == amount)
            {
                constraint.amount = Some(amount);
            }
        }
        unresolved |= (constraint.amount.is_none()
            && query_ts.is_none_or(|time| time <= evidence.ts_init))
            || constraint.basis.is_none()
            || (constraint.currency.is_none()
                && !(target.is_zero() && constraint.amount == Some(Decimal::ZERO)));
    }
    let mut identities = HashSet::new();

    for fill in fills {
        if fill.account_id != evidence.account_id
            || fill.instrument_id != evidence.instrument_id
            || fill.venue_order_id != evidence.venue_order_id
            || fill.order_side != evidence.order_side
            || fill.venue_position_id != evidence.venue_position_id
            || fill
                .client_order_id
                .is_some_and(|id| canonical_client_order_id != Some(id))
            || !fill.last_qty.is_positive()
            || !identities.insert(fill.trade_id)
        {
            return FeeEvidenceState::Conflict;
        }
    }
    let anchor = evidence
        .anchor_trade_id
        .and_then(|id| fills.iter().find(|fill| fill.trade_id == id));
    if evidence.anchor_trade_id.is_some() && anchor.is_none() {
        return if complete {
            FeeEvidenceState::Conflict
        } else {
            FeeEvidenceState::Pending
        };
    }

    if anchor.is_some_and(|fill| {
        fill.ts_event > boundary
            || evidence
                .anchor_trade_time
                .is_some_and(|time| time != fill.ts_event)
    }) {
        return FeeEvidenceState::Conflict;
    }
    let cutoff = anchor.map_or_else(
        || evidence.anchor_trade_time.unwrap_or(boundary),
        |fill| fill.ts_event,
    );

    if cutoff > boundary {
        return FeeEvidenceState::Conflict;
    }
    let mut mandatory = (
        Decimal::ZERO,
        vec![[Decimal::ZERO; 3]; constraints.len()],
        0u8,
        false,
    );
    let mut tied = Vec::new();

    for fill in fills {
        if fill.ts_event > boundary {
            continue;
        }

        if evidence.anchor_trade_id.is_none()
            || fill.ts_event < cutoff
            || Some(fill.trade_id) == evidence.anchor_trade_id
        {
            let Some(next) = add_commission_fill(&mandatory, fill, &constraints) else {
                return FeeEvidenceState::Pending;
            };
            mandatory = next;
        } else if fill.ts_event == cutoff {
            tied.push(fill);
        } else if evidence.origin == OrderFeeOrigin::Query && evidence.anchor_trade_id.is_some() {
            return FeeEvidenceState::Conflict;
        }
    }

    if mandatory.0 > target {
        return FeeEvidenceState::Conflict;
    }
    let mut reachable = HashSet::from([mandatory]);

    for fill in tied {
        let mut added = Vec::new();

        for previous in &reachable {
            let Some(next) = add_commission_fill(previous, fill, &constraints) else {
                return FeeEvidenceState::Pending;
            };

            if next.0 <= target {
                added.push(next);
            }
        }

        for next in added {
            reachable.insert(next);
            if reachable.len() > MAX_REACHABLE_PREFIXES {
                return FeeEvidenceState::Pending;
            }
        }
    }

    if !complete {
        return FeeEvidenceState::Pending;
    }
    let mut pending_witness = false;

    for (quantity, amounts, roles, uncovered) in &reachable {
        if *quantity != target {
            continue;
        }
        let mut unknown = unresolved;
        let mut mismatch = *uncovered;

        for (constraint, totals) in constraints.iter().zip(amounts) {
            let index = match constraint.basis {
                Some(CommissionBasis::ChargesOnly) => Some(0),
                Some(CommissionBasis::RebatesOnly) => Some(1),
                Some(CommissionBasis::Net) => Some(2),
                Some(CommissionBasis::UniformLiquidity) => match *roles {
                    0 | 2 => Some(0),
                    1 => Some(2),
                    // Mixed maker/taker fees are unambiguous when both aggregates agree
                    3 if totals[0] == totals[2] => Some(0),
                    _ => None,
                },
                None => None,
            };

            if let (Some(index), Some(expected), Some(_)) =
                (index, constraint.amount, constraint.currency)
            {
                mismatch |= expected != totals[index];
            } else if index.is_some()
                && constraint.amount.is_none()
                && constraint.currency.is_some()
            {
                // A fresh complete query supplies the missing amount in the known
                // denomination; no reported numeric fact is replaced or set to zero.
                unknown |= query_ts.is_none_or(|time| time <= evidence.ts_init);
            } else if !(target.is_zero() && constraint.amount == Some(Decimal::ZERO)) {
                unknown = true;
            }
        }

        if !mismatch {
            if !unknown {
                return FeeEvidenceState::Satisfied;
            }
            pending_witness = true;
        }
    }

    if pending_witness {
        FeeEvidenceState::Pending
    } else {
        FeeEvidenceState::Conflict
    }
}

type CommissionPrefix = (Decimal, Vec<[Decimal; 3]>, u8, bool);

fn add_commission_fill(
    previous: &CommissionPrefix,
    fill: &FillReport,
    constraints: &[CumulativeCommissionConstraint],
) -> Option<CommissionPrefix> {
    let quantity = previous.0.checked_add(fill.last_qty.as_decimal())?;
    let mut amounts = previous.1.clone();
    let commission = fill.commission.as_decimal();
    for (constraint, totals) in constraints.iter().zip(&mut amounts) {
        if constraint.currency == Some(fill.commission.currency) {
            let index = usize::from(commission.is_sign_negative());
            totals[index] = totals[index].checked_add(commission)?;
            totals[2] = totals[2].checked_add(commission)?;
        }
    }
    let role = match fill.liquidity_side {
        LiquiditySide::Maker => 1,
        LiquiditySide::Taker => 2,
        LiquiditySide::NoLiquiditySide => 4,
    };
    // A known denomination is a constraint too: a positive charge in another
    // currency cannot disappear merely because the reported bucket sums to zero.
    // Separate charge/rebate denominations remain valid when their own fields cover them.
    let uncovered = !commission.is_zero()
        && !constraints.is_empty()
        && !constraints.iter().any(|constraint| {
            constraint
                .currency
                .is_none_or(|currency| currency == fill.commission.currency)
                && match constraint.basis {
                    Some(CommissionBasis::ChargesOnly) => commission.is_sign_positive(),
                    Some(CommissionBasis::RebatesOnly) => commission.is_sign_negative(),
                    Some(CommissionBasis::Net | CommissionBasis::UniformLiquidity) | None => true,
                }
        });
    Some((
        quantity,
        amounts,
        previous.2 | role,
        previous.3 || uncovered,
    ))
}

/// Collects active, exact venue executions without synthesizing fees or identities.
/// The boolean identifies a contiguous native stream history, not query fragments.
#[must_use]
pub fn committed_commission_fills(
    order: &OrderAny,
    evidence: &OrderFeeEvidence,
) -> Option<(Vec<FillReport>, bool)> {
    if order.account_id() != Some(evidence.account_id)
        || order.instrument_id() != evidence.instrument_id
        || order.venue_order_id() != Some(evidence.venue_order_id)
        || order.order_side() != evidence.order_side
        || evidence
            .client_order_id
            .is_some_and(|id| id != order.client_order_id())
        || super::inferred_reconciliation_trade_ids(order).map_or(true, |ids| !ids.is_empty())
    {
        return None;
    }
    let active = order.trade_ids();
    let mut fills = Vec::new();
    let mut contiguous = true;

    for event in order.events() {
        let OrderEventAny::Filled(fill) = event else {
            continue;
        };

        if !active.contains(&&fill.trade_id) {
            continue;
        }

        if fill.trade_id.as_str().starts_with("synth-")
            || evidence
                .venue_position_id
                .is_some_and(|id| fill.position_id != Some(id))
        {
            return None;
        }
        contiguous &= !fill.reconciliation;
        fills.push(FillReport::new(
            fill.account_id,
            fill.instrument_id,
            fill.venue_order_id,
            fill.trade_id,
            fill.order_side,
            fill.last_qty,
            fill.last_px,
            fill.commission?,
            fill.liquidity_side,
            Some(fill.client_order_id),
            evidence.venue_position_id,
            fill.ts_event,
            fill.ts_init,
            Some(fill.event_id),
        ));
    }
    Some((fills, contiguous))
}

/// Proves original fee constraints using committed native executions and complete query prefixes.
/// Every reconciled fragment needs independent, fully committed order-history authority.
#[must_use]
pub fn order_fee_recovery_complete(
    evidence: &OrderFeeEvidence,
    order: &OrderAny,
    queries: &[(OrderStatusReport, Vec<FillReport>)],
    peers: &[OrderFeeEvidence],
    stream_fills: &[FillReport],
) -> bool {
    let complete_queries: Vec<_> = queries
        .iter()
        .filter(|(status, fills)| super::query_fill_recovery_complete(order, status, fills, false))
        .collect();

    if complete_queries.iter().any(|(status, fills)| {
        evidence
            .boundary_time
            .is_some_and(|time| status.ts_last >= time)
            && evaluate_order_fee_evidence(
                evidence,
                fills,
                Some(order.client_order_id()),
                true,
                Some(status.ts_init),
                peers,
            ) == FeeEvidenceState::Satisfied
    }) {
        return true;
    }
    let Some((fills, _)) = committed_commission_fills(order, evidence) else {
        return false;
    };
    let complete = commission_history_has_authority(order, queries, stream_fills)
        && evidence.boundary_quantity.is_some_and(|target| {
            fills
                .iter()
                .try_fold(Decimal::ZERO, |sum, fill| {
                    sum.checked_add(fill.last_qty.as_decimal())
                })
                .is_some_and(|total| total >= target)
        });
    evaluate_order_fee_evidence(
        evidence,
        &fills,
        Some(order.client_order_id()),
        complete,
        None,
        peers,
    ) == FeeEvidenceState::Satisfied
}

/// Distinguishes committed actual stream evidence and complete queries from inferred fragments.
#[must_use]
pub fn commission_history_has_authority(
    order: &OrderAny,
    queries: &[(OrderStatusReport, Vec<FillReport>)],
    stream_fills: &[FillReport],
) -> bool {
    let complete_queries: Vec<_> = queries
        .iter()
        .filter(|(status, fills)| super::query_fill_recovery_complete(order, status, fills, false))
        .collect();
    let active = order.trade_ids();
    order.events().iter().all(|event| {
        let OrderEventAny::Filled(fill) = event else {
            return true;
        };
        !active.contains(&&fill.trade_id)
            || !fill.reconciliation
            || stream_fills
                .iter()
                .chain(
                    complete_queries
                        .iter()
                        .flat_map(|(_, history)| history.iter()),
                )
                .any(|report| {
                    report.trade_id == fill.trade_id
                        && super::fill_evidence_is_committed(order, report)
                })
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{AccountId, InstrumentId, TradeId, VenueOrderId},
        types::{Currency, Money, Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn fill(
        id: &str,
        quantity: &str,
        commission: &str,
        time: u64,
        liquidity: LiquiditySide,
    ) -> FillReport {
        FillReport::new(
            AccountId::from("OKX-001"),
            InstrumentId::from("BTC-USD.OKX"),
            VenueOrderId::from("V1"),
            TradeId::from(id),
            OrderSide::Buy,
            Quantity::from(quantity),
            Price::from("50000.0"),
            Money::from(commission),
            liquidity,
            Some(ClientOrderId::from("C1")),
            None,
            UnixNanos::from(time),
            UnixNanos::from(100),
            None,
        )
    }

    fn evidence(
        quantity: &str,
        anchor: Option<(&str, u64)>,
        constraints: &[(CommissionBasis, &str, Currency)],
    ) -> OrderFeeEvidence {
        OrderFeeEvidence {
            account_id: AccountId::from("OKX-001"),
            instrument_id: InstrumentId::from("BTC-USD.OKX"),
            venue_order_id: VenueOrderId::from("V1"),
            client_order_id: Some(ClientOrderId::from("C1")),
            venue_position_id: None,
            order_side: OrderSide::Buy,
            boundary_quantity: Some(quantity.parse().unwrap()),
            boundary_time: Some(UnixNanos::from(20)),
            anchor_trade_id: anchor.map(|(id, _)| TradeId::from(id)),
            anchor_trade_time: anchor.map(|(_, time)| UnixNanos::from(time)),
            origin: OrderFeeOrigin::Stream,
            constraints: constraints
                .iter()
                .map(|(basis, amount, currency)| CumulativeCommissionConstraint {
                    basis: Some(*basis),
                    amount: Some(amount.parse().unwrap()),
                    currency: Some(*currency),
                })
                .collect(),
            unrepresentable_values: false,
            requires_fresh_query: false,
            ts_init: UnixNanos::from(100),
        }
    }

    fn check(
        observed: &OrderFeeEvidence,
        fills: &[FillReport],
        complete: bool,
    ) -> FeeEvidenceState {
        evaluate_order_fee_evidence(
            observed,
            fills,
            Some(ClientOrderId::from("C1")),
            complete,
            None,
            &[],
        )
    }

    #[rstest]
    #[case("0.02", FeeEvidenceState::Satisfied)]
    #[case("0.04", FeeEvidenceState::Conflict)]
    fn cumulative_commission_first_fill(#[case] total: &str, #[case] expected: FeeEvidenceState) {
        let observed = evidence(
            "0.001",
            Some(("T1", 10)),
            &[(CommissionBasis::ChargesOnly, total, Currency::USD())],
        );
        let fills = [fill("T1", "0.001", "0.02 USD", 10, LiquiditySide::Taker)];
        assert_eq!(check(&observed, &fills, true), expected);
        assert_eq!(check(&observed, &fills, false), FeeEvidenceState::Pending);
    }

    #[rstest]
    fn cumulative_commission_preserves_charge_and_rebate_buckets() {
        let fills = [
            fill("T1", "1", "2 USD", 1, LiquiditySide::Taker),
            fill("T2", "1", "-2 USD", 2, LiquiditySide::Maker),
        ];
        let observed = evidence(
            "2",
            Some(("T2", 2)),
            &[
                (CommissionBasis::ChargesOnly, "2", Currency::USD()),
                (CommissionBasis::RebatesOnly, "-2", Currency::USD()),
            ],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Satisfied);
        let mut contradictory = observed;
        contradictory.constraints[0].amount = Some(Decimal::ZERO);
        contradictory.constraints[1].amount = Some(Decimal::ZERO);
        assert_eq!(
            check(&contradictory, &fills, true),
            FeeEvidenceState::Conflict
        );
    }

    #[rstest]
    fn cumulative_commission_known_currency_cannot_drop_foreign_charge() {
        let fills = [fill("T1", "1", "2 BTC", 1, LiquiditySide::Taker)];
        let observed = evidence(
            "1",
            Some(("T1", 1)),
            &[
                (CommissionBasis::ChargesOnly, "0", Currency::USD()),
                (CommissionBasis::RebatesOnly, "0", Currency::BTC()),
            ],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Conflict);
        let rebate = [fill("T1", "1", "-2 BTC", 1, LiquiditySide::Maker)];
        let mut matching = observed;
        matching.constraints[1].amount = Some(Decimal::from(-2));
        assert_eq!(check(&matching, &rebate, true), FeeEvidenceState::Satisfied);
    }

    #[rstest]
    fn cumulative_commission_requires_joint_tied_witness() {
        let fills = [
            fill("T1", "1", "0 USD", 10, LiquiditySide::Taker),
            fill("T2", "1", "2 USD", 10, LiquiditySide::Taker),
            fill("T3", "1", "3 BTC", 10, LiquiditySide::Taker),
        ];
        let observed = evidence(
            "2",
            Some(("T1", 10)),
            &[
                (CommissionBasis::ChargesOnly, "2", Currency::USD()),
                (CommissionBasis::ChargesOnly, "3", Currency::BTC()),
            ],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Conflict);
        let mut matching = observed;
        matching.boundary_quantity = Some(Decimal::from(3));
        assert_eq!(check(&matching, &fills, true), FeeEvidenceState::Satisfied);
    }

    #[rstest]
    fn cumulative_commission_mixed_liquidity_remains_unknown() {
        let fills = [
            fill("T1", "1", "2 USD", 1, LiquiditySide::Taker),
            fill("T2", "1", "-2 USD", 2, LiquiditySide::Maker),
        ];
        let observed = evidence(
            "2",
            Some(("T2", 2)),
            &[(CommissionBasis::UniformLiquidity, "0", Currency::USD())],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Pending);
        assert_eq!(
            check(&observed, &[fills[1].clone(), fills[0].clone()], true),
            FeeEvidenceState::Pending
        );
        let mut pure = observed;
        pure.boundary_quantity = Some(Decimal::ONE);
        pure.anchor_trade_id = Some(fills[0].trade_id);
        pure.anchor_trade_time = Some(fills[0].ts_event);
        pure.constraints[0].amount = Some(Decimal::from(2));
        assert_eq!(check(&pure, &fills, true), FeeEvidenceState::Satisfied);
    }

    #[rstest]
    #[case::positive("0.02 USD", "0.03 USD", "0.05")]
    #[case::zero_maker("0.02 USD", "0 USD", "0.02")]
    #[case::zero_taker("0 USD", "0.03 USD", "0.03")]
    #[case::zero_fees("0 USD", "0 USD", "0")]
    fn cumulative_commission_mixed_liquidity_requires_identical_fee_interpretations(
        #[case] taker_fee: &str,
        #[case] maker_fee: &str,
        #[case] total: &str,
        #[values(false, true)] reverse: bool,
    ) {
        let mut fills = [
            fill("T1", "1", taker_fee, 1, LiquiditySide::Taker),
            fill("T2", "1", maker_fee, 2, LiquiditySide::Maker),
        ];

        for fill in &mut fills {
            fill.order_side = OrderSide::Sell;
        }

        if reverse {
            fills.reverse();
        }
        let mut observed = evidence(
            "2",
            Some(("T2", 2)),
            &[
                (CommissionBasis::UniformLiquidity, total, Currency::USD()),
                (CommissionBasis::RebatesOnly, "0", Currency::USD()),
            ],
        );
        observed.order_side = OrderSide::Sell;
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Satisfied);
        assert_eq!(check(&observed, &fills, false), FeeEvidenceState::Pending);

        let mut conflicting = observed.clone();
        conflicting.constraints[0].amount = Some(total.parse::<Decimal>().unwrap() + Decimal::ONE);
        assert_eq!(
            check(&conflicting, &fills, true),
            FeeEvidenceState::Conflict
        );
        conflicting = observed.clone();
        conflicting.constraints[1].amount = Some(-Decimal::ONE);
        assert_eq!(
            check(&conflicting, &fills, true),
            FeeEvidenceState::Conflict
        );

        let mut missing = observed.clone();
        missing.constraints[1].amount = None;
        assert_eq!(check(&missing, &fills, true), FeeEvidenceState::Pending);
        missing = observed.clone();
        missing.constraints[0].currency = None;
        assert_eq!(check(&missing, &fills, true), FeeEvidenceState::Pending);

        fills[0].liquidity_side = LiquiditySide::NoLiquiditySide;
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Pending);
    }

    #[rstest]
    fn cumulative_commission_extended_query_keeps_original_boundary() {
        let fills = [
            fill("T1", "1", "2 USD", 1, LiquiditySide::Taker),
            fill("T2", "1", "3 USD", 2, LiquiditySide::Taker),
        ];
        let observed = evidence(
            "1",
            Some(("T1", 1)),
            &[(CommissionBasis::ChargesOnly, "2", Currency::USD())],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Satisfied);
        let mut wrong = observed;
        wrong.constraints[0].amount = Some(Decimal::from(5));
        assert_eq!(check(&wrong, &fills, true), FeeEvidenceState::Conflict);
    }

    #[rstest]
    fn cumulative_commission_missing_currency_needs_same_boundary_authority() {
        let fills = [fill("T1", "1", "2 USD", 1, LiquiditySide::Taker)];
        let mut observed = evidence(
            "1",
            Some(("T1", 1)),
            &[(CommissionBasis::ChargesOnly, "2", Currency::USD())],
        );
        let mut peer = observed.clone();
        peer.origin = OrderFeeOrigin::Query;
        peer.ts_init = UnixNanos::from(101);
        observed.constraints[0].currency = None;
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Pending);
        assert_eq!(
            evaluate_order_fee_evidence(
                &observed,
                &fills,
                Some(ClientOrderId::from("C1")),
                true,
                Some(peer.ts_init),
                &[peer.clone()]
            ),
            FeeEvidenceState::Satisfied
        );
        peer.boundary_quantity = Some(Decimal::from(2));
        assert_eq!(
            evaluate_order_fee_evidence(
                &observed,
                &fills,
                Some(ClientOrderId::from("C1")),
                true,
                Some(peer.ts_init),
                &[peer]
            ),
            FeeEvidenceState::Pending
        );
    }

    #[rstest]
    fn cumulative_commission_missing_amount_needs_fresh_complete_history() {
        let fills = [
            fill("T1", "1", "2 USD", 1, LiquiditySide::Taker),
            fill("T2", "1", "3 USD", 2, LiquiditySide::Taker),
        ];
        let mut observed = evidence(
            "1",
            Some(("T1", 1)),
            &[(CommissionBasis::ChargesOnly, "2", Currency::USD())],
        );
        observed.constraints[0].amount = None;
        observed.requires_fresh_query = true;
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Pending);
        assert_eq!(
            evaluate_order_fee_evidence(
                &observed,
                &fills,
                Some(ClientOrderId::from("C1")),
                true,
                Some(UnixNanos::from(101)),
                &[]
            ),
            FeeEvidenceState::Satisfied
        );
        assert_eq!(
            evaluate_order_fee_evidence(
                &observed,
                &fills,
                Some(ClientOrderId::from("C1")),
                false,
                Some(UnixNanos::from(101)),
                &[]
            ),
            FeeEvidenceState::Pending
        );
        observed.unrepresentable_values = true;
        assert_eq!(
            evaluate_order_fee_evidence(
                &observed,
                &fills,
                Some(ClientOrderId::from("C1")),
                true,
                Some(UnixNanos::from(101)),
                &[]
            ),
            FeeEvidenceState::Pending
        );
    }

    #[rstest]
    fn cumulative_commission_cumulative_only_includes_boundary_ties() {
        let fills = [
            fill("T1", "1", "2 USD", 20, LiquiditySide::Taker),
            fill("T2", "1", "3 USD", 20, LiquiditySide::Taker),
        ];
        let observed = evidence(
            "1",
            None,
            &[(CommissionBasis::ChargesOnly, "2", Currency::USD())],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Conflict);
    }

    #[rstest]
    fn cumulative_commission_search_exhaustion_remains_pending() {
        let mut fills = vec![fill("anchor", "1", "0 USD", 10, LiquiditySide::Taker)];
        for index in 0..13 {
            fills.push(fill(
                &format!("T{index}"),
                &(1u64 << index).to_string(),
                "0 USD",
                10,
                LiquiditySide::Taker,
            ));
        }
        let observed = evidence(
            "8192",
            Some(("anchor", 10)),
            &[(CommissionBasis::ChargesOnly, "0", Currency::USD())],
        );
        assert_eq!(check(&observed, &fills, true), FeeEvidenceState::Pending);
    }
}
