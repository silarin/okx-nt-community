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

//! Immutable terminal observations retained until native execution accounting is complete.

use nautilus_core::UnixNanos;
use nautilus_model::{
    events::{OrderCanceled, OrderDenied, OrderEventAny, OrderExpired, OrderRejected},
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::Quantity,
};
use serde::{Deserialize, Serialize};

/// A venue terminal observation without inferred fills or cumulative quantities.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OrderTerminalEvidence {
    Canceled(OrderCanceled),
    Rejected(OrderRejected),
    Denied(OrderDenied),
    Expired(OrderExpired),
    /// A cumulative execution snapshot from an authoritative fill-recovery source.
    Report(Box<OrderStatusReport>),
    /// A status delta whose quantities do not establish cumulative execution evidence.
    Status(Box<OrderStatusReport>),
}

impl OrderTerminalEvidence {
    /// Compares venue facts, ignoring identifiers and times assigned by local polling.
    #[must_use]
    pub fn same_observation(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Report(left), Self::Report(right))
            | (Self::Status(left), Self::Status(right)) => {
                let mut right = right.clone();
                right.report_id = left.report_id;
                right.ts_init = left.ts_init;
                left == &right
            }
            (Self::Canceled(left), Self::Canceled(right)) => {
                let mut right = *right;
                right.event_id = left.event_id;
                right.ts_init = left.ts_init;
                left == &right
            }
            (Self::Expired(left), Self::Expired(right)) => {
                let mut right = *right;
                right.event_id = left.event_id;
                right.ts_init = left.ts_init;
                left == &right
            }
            (Self::Rejected(left), Self::Rejected(right)) => {
                let mut right = *right;
                right.event_id = left.event_id;
                right.ts_init = left.ts_init;
                left == &right
            }
            (Self::Denied(left), Self::Denied(right)) => {
                let mut right = *right;
                right.event_id = left.event_id;
                right.ts_init = left.ts_init;
                left == &right
            }
            _ => self == other,
        }
    }

    /// Returns the original event when the observation supplied one.
    #[must_use]
    pub fn event(&self) -> Option<OrderEventAny> {
        match self {
            Self::Canceled(event) => Some(OrderEventAny::Canceled(*event)),
            Self::Rejected(event) => Some(OrderEventAny::Rejected(event.clone())),
            Self::Denied(event) => Some(OrderEventAny::Denied(event.clone())),
            Self::Expired(event) => Some(OrderEventAny::Expired(*event)),
            Self::Report(_) | Self::Status(_) => None,
        }
    }

    /// Returns the supplied client identity, without inventing an owner.
    #[must_use]
    pub fn client_order_id(&self) -> Option<ClientOrderId> {
        match self {
            Self::Canceled(event) => Some(event.client_order_id),
            Self::Expired(event) => Some(event.client_order_id),
            Self::Rejected(event) => Some(event.client_order_id),
            Self::Denied(event) => Some(event.client_order_id),
            Self::Report(report) | Self::Status(report) => report.client_order_id,
        }
    }

    /// Returns the supplied venue identity.
    #[must_use]
    pub fn venue_order_id(&self) -> Option<VenueOrderId> {
        match self {
            Self::Canceled(event) => event.venue_order_id,
            Self::Expired(event) => event.venue_order_id,
            Self::Rejected(_) => None,
            Self::Denied(_) => None,
            Self::Report(report) | Self::Status(report) => Some(report.venue_order_id),
        }
    }

    /// Returns the supplied venue scope when all components were observed.
    #[must_use]
    pub fn order_key(&self) -> Option<(AccountId, InstrumentId, VenueOrderId)> {
        match self {
            Self::Canceled(event) => Some((
                event.account_id?,
                event.instrument_id,
                event.venue_order_id?,
            )),
            Self::Expired(event) => Some((
                event.account_id?,
                event.instrument_id,
                event.venue_order_id?,
            )),
            Self::Report(report) | Self::Status(report) => Some((
                report.account_id,
                report.instrument_id,
                report.venue_order_id,
            )),
            Self::Rejected(_) | Self::Denied(_) => None,
        }
    }

    /// Returns the original venue timestamp.
    #[must_use]
    pub fn timestamp(&self) -> UnixNanos {
        match self {
            Self::Canceled(event) => event.ts_event,
            Self::Expired(event) => event.ts_event,
            Self::Rejected(event) => event.ts_event,
            Self::Denied(event) => event.ts_event,
            Self::Report(report) | Self::Status(report) => report.ts_last,
        }
    }

    /// Checks every supplied identity against the native order.
    #[must_use]
    pub fn matches_order(&self, order: &OrderAny) -> bool {
        if self
            .client_order_id()
            .is_some_and(|id| id != order.client_order_id())
            || self
                .venue_order_id()
                .is_some_and(|id| order.venue_order_id().is_some_and(|known| known != id))
        {
            return false;
        }

        match self {
            Self::Report(report) | Self::Status(report) => {
                report.order_status.is_closed()
                    && report.instrument_id == order.instrument_id()
                    && order
                        .account_id()
                        .is_none_or(|account| account == report.account_id)
                    && report
                        .order_side
                        .is_none_or(|side| side == order.order_side())
            }
            _ => {
                let Some(event) = self.event() else {
                    return false;
                };
                event.trader_id() == order.trader_id()
                    && event.strategy_id() == order.strategy_id()
                    && event.instrument_id() == order.instrument_id()
                    && event
                        .account_id()
                        .is_none_or(|id| order.account_id() == Some(id))
            }
        }
    }

    /// Checks exact executions through the snapshot boundary without double-counting trades.
    #[must_use]
    pub fn conflicts_with_fills<'a>(
        &self,
        fills: impl IntoIterator<Item = &'a FillReport>,
    ) -> bool {
        let Self::Report(report) = self else {
            return false;
        };
        self.executions_exceed(
            fills
                .into_iter()
                .filter(|fill| {
                    fill.account_id == report.account_id
                        && fill.instrument_id == report.instrument_id
                        && fill.venue_order_id == report.venue_order_id
                })
                .map(|fill| (fill.trade_id, fill.last_qty, fill.ts_event)),
        )
    }

    /// Checks already committed executions against the original cumulative boundary.
    #[must_use]
    pub fn conflicts_with_order(&self, order: &OrderAny) -> bool {
        self.executions_exceed(order.events().into_iter().filter_map(|event| match event {
            OrderEventAny::Filled(fill) if order.trade_ids().contains(&&fill.trade_id) => {
                Some((fill.trade_id, fill.last_qty, fill.ts_event))
            }
            _ => None,
        }))
    }

    fn executions_exceed(
        &self,
        fills: impl IntoIterator<Item = (TradeId, Quantity, UnixNanos)>,
    ) -> bool {
        let Self::Report(report) = self else {
            return false;
        };
        let mut seen = std::collections::HashSet::new();
        let mut quantity = rust_decimal::Decimal::ZERO;

        for (trade_id, size, timestamp) in fills {
            if timestamp < report.ts_last && seen.insert(trade_id) {
                let Some(total) = quantity.checked_add(size.as_decimal()) else {
                    return true;
                };
                quantity = total;
            }
        }
        quantity > report.filled_qty.as_decimal()
    }

    /// Checks cumulative execution chronology against another authentic snapshot.
    #[must_use]
    pub fn conflicts_with_report(&self, other: &OrderStatusReport) -> bool {
        let Self::Report(report) = self else {
            return false;
        };
        report.account_id == other.account_id
            && report.instrument_id == other.instrument_id
            && report.venue_order_id == other.venue_order_id
            && ((report.ts_last > other.ts_last && report.filled_qty < other.filled_qty)
                || (report.ts_last < other.ts_last && report.filled_qty > other.filled_qty))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UUID4;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
        types::{Money, Price},
    };
    use rstest::rstest;

    use super::*;

    fn report() -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("BTCUSDT.BINANCE"),
            Some(ClientOrderId::from("O-001")),
            VenueOrderId::from("V-001"),
            Some(OrderSide::Buy),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Canceled,
            Quantity::from("1.000"),
            Quantity::from("0.400"),
            1.into(),
            20.into(),
            30.into(),
            None,
        )
    }

    #[rstest]
    fn terminal_event_dedup_ignores_only_local_delivery_identity() {
        use nautilus_model::events::order::spec::{
            OrderCanceledSpec, OrderDeniedSpec, OrderExpiredSpec, OrderRejectedSpec,
        };
        let observations = [
            OrderTerminalEvidence::Canceled(OrderCanceledSpec::builder().build()),
            OrderTerminalEvidence::Expired(OrderExpiredSpec::builder().build()),
            OrderTerminalEvidence::Rejected(OrderRejectedSpec::builder().build()),
            OrderTerminalEvidence::Denied(OrderDeniedSpec::builder().build()),
        ];

        for original in observations {
            let mut duplicate = original.clone();
            macro_rules! redeliver {
                ($event:expr) => {{
                    $event.event_id = UUID4::new();
                    $event.ts_init = 100.into();
                }};
            }

            match &mut duplicate {
                OrderTerminalEvidence::Canceled(event) => redeliver!(event),
                OrderTerminalEvidence::Expired(event) => redeliver!(event),
                OrderTerminalEvidence::Rejected(event) => redeliver!(event),
                OrderTerminalEvidence::Denied(event) => redeliver!(event),
                _ => unreachable!(),
            }
            assert!(original.same_observation(&duplicate));
            match &mut duplicate {
                OrderTerminalEvidence::Canceled(event) => event.ts_event = 200.into(),
                OrderTerminalEvidence::Expired(event) => event.ts_event = 200.into(),
                OrderTerminalEvidence::Rejected(event) => event.ts_event = 200.into(),
                OrderTerminalEvidence::Denied(event) => event.ts_event = 200.into(),
                _ => unreachable!(),
            }
            assert!(!original.same_observation(&duplicate));
        }
    }

    #[rstest]
    fn terminal_observation_dedup_preserves_venue_facts() {
        let original = report();
        let evidence = OrderTerminalEvidence::Report(Box::new(original.clone()));
        let mut repeated = original;
        repeated.report_id = UUID4::new();
        repeated.ts_init = 40.into();
        assert!(
            evidence.same_observation(&OrderTerminalEvidence::Report(Box::new(repeated.clone())))
        );
        repeated.ts_last = 21.into();
        assert!(
            !evidence.same_observation(&OrderTerminalEvidence::Report(Box::new(repeated.clone())))
        );
        repeated.ts_last = 20.into();
        repeated.filled_qty = Quantity::from("0.500");
        assert!(!evidence.same_observation(&OrderTerminalEvidence::Report(Box::new(repeated))));
    }

    #[rstest]
    #[case::earlier_fill(19, true)]
    #[case::same_timestamp(20, false)]
    #[case::later_fill(21, false)]
    fn terminal_snapshot_checks_exact_execution_boundary(
        #[case] time: u64,
        #[case] conflict: bool,
    ) {
        let report = report();
        let fill = FillReport::new(
            report.account_id,
            report.instrument_id,
            report.venue_order_id,
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from("0.500"),
            Price::from("50000.0"),
            Money::from("0 USDT"),
            LiquiditySide::Taker,
            report.client_order_id,
            None,
            time.into(),
            30.into(),
            None,
        );
        let evidence = OrderTerminalEvidence::Report(Box::new(report));
        assert_eq!(evidence.conflicts_with_fills([&fill]), conflict);
        let mut other = fill;
        other.venue_order_id = VenueOrderId::from("V-OTHER");
        assert!(!evidence.conflicts_with_fills([&other]));
    }

    #[rstest]
    fn terminal_snapshot_does_not_count_duplicate_trades_twice() {
        let report = report();
        let fill = FillReport::new(
            report.account_id,
            report.instrument_id,
            report.venue_order_id,
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from("0.400"),
            Price::from("50000.0"),
            Money::from("0 USDT"),
            LiquiditySide::Taker,
            report.client_order_id,
            None,
            10.into(),
            30.into(),
            None,
        );
        let evidence = OrderTerminalEvidence::Report(Box::new(report));
        assert!(!evidence.conflicts_with_fills([&fill, &fill]));
    }
}
