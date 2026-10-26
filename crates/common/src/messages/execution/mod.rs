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

//! Execution specific messages such as order commands.

pub mod cancel;
pub mod fees;
pub mod modify;
pub mod query;
pub mod report;
pub mod submit;
pub mod terminal;

use std::fmt::Display;

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        VenueOrderId,
    },
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::Currency,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use strum::Display as StrumDisplay;

pub use self::{
    cancel::{BatchCancelOrders, CancelAllOrders, CancelOrder},
    fees::{CommissionBasis, CumulativeCommissionConstraint, OrderFeeEvidence, OrderFeeOrigin},
    modify::{BatchModifyOrders, ModifyOrder},
    query::{QueryAccount, QueryOrder},
    report::{
        GenerateExecutionMassStatus, GenerateExecutionMassStatusBuilder, GenerateFillReports,
        GenerateFillReportsBuilder, GenerateOrderStatusReport, GenerateOrderStatusReportBuilder,
        GenerateOrderStatusReports, GenerateOrderStatusReportsBuilder,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder,
    },
    submit::{SubmitOrder, SubmitOrderList},
    terminal::OrderTerminalEvidence,
};

/// Parameter indicating that a conditional order should close the whole position at trigger time.
pub const PARAMS_CLOSE_POSITION: &str = "close_position";

/// Maximum distinct incomplete execution obligations retained for one native venue order.
/// Exhaustion must prevent recovery rather than discard facts and accept later accounting.
pub const MAX_INCOMPLETE_FILL_OBSERVATIONS: usize = 4096;

/// Maximum cumulative fee observations retained for one native venue order.
/// Exhaustion must prevent recovery rather than drop unresolved economic facts.
pub const MAX_ORDER_FEE_OBSERVATIONS: usize = 4096;

/// Maximum retained query snapshots and individual execution facts for one venue order.
/// Each snapshot and each fill it contains consumes one unit; exhaustion holds recovery.
pub const MAX_ORDER_FILL_OBSERVATIONS: usize = 4096;

/// Independently parsed execution facts which require complete native recovery.
/// Missing fields are unknown, never zero-valued economic facts. Without a trade ID,
/// this evidence cannot resolve automatically unless the adapter explicitly recognizes a
/// documented placeholder and complete query history identifies one exact matching execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IncompleteFillReport {
    /// Account which observed the execution.
    pub account_id: AccountId,
    /// Native instrument identity.
    pub instrument_id: InstrumentId,
    /// Venue order whose execution needs recovery.
    pub venue_order_id: VenueOrderId,
    /// Client identity when independently known.
    pub client_order_id: Option<ClientOrderId>,
    /// Venue trade identity when independently known.
    pub trade_id: Option<TradeId>,
    /// The venue documented an unavailable execution ID, rather than omitting it unexpectedly.
    /// Only a unique match in complete authoritative query history can resolve this case.
    #[serde(default)]
    pub trade_id_placeholder: bool,
    /// Observed order side.
    pub order_side: OrderSide,
    /// Exact parsed quantity, including values not representable on the instrument grid.
    pub last_qty: Option<Decimal>,
    /// Exact parsed price, without rounding.
    pub last_px: Option<Decimal>,
    /// Independently known per-fill commission amount, without native money rounding.
    pub commission_amount: Option<Decimal>,
    /// Independently known commission currency; cumulative fees do not qualify.
    pub commission_currency: Option<Currency>,
    /// Numeric facts exceed exact representation; no later query can resolve this evidence.
    pub unrepresentable_values: bool,
    /// Malformed evidence requires a later query; successful query facts allow that query.
    pub requires_fresh_query: bool,
    /// Originating successful REST snapshot, including its execution boundary.
    #[serde(deserialize_with = "Option::deserialize")]
    pub order_snapshot: Option<Box<OrderStatusReport>>,
    /// Independently known liquidity role.
    pub liquidity_side: Option<LiquiditySide>,
    /// Native venue position mapping.
    pub venue_position_id: Option<PositionId>,
    /// Venue execution time when provided.
    pub ts_event: Option<UnixNanos>,
    /// Observation time, or query start time for successful query facts.
    pub ts_init: UnixNanos,
}

impl IncompleteFillReport {
    /// Returns the native order scope independently of optional client identity.
    #[must_use]
    pub fn order_key(&self) -> (AccountId, InstrumentId, VenueOrderId) {
        (self.account_id, self.instrument_id, self.venue_order_id)
    }

    /// Compares retained facts, excluding receipt metadata while preserving snapshot boundaries.
    #[must_use]
    pub fn same_observed_facts(&self, other: &Self) -> bool {
        if !self.snapshot_is_consistent() || !other.snapshot_is_consistent() {
            return false;
        }
        let mut comparable = other.clone();
        comparable.ts_init = self.ts_init;
        if let (Some(snapshot), Some(previous)) =
            (&self.order_snapshot, &mut comparable.order_snapshot)
        {
            previous.ts_init = snapshot.ts_init;
            previous.report_id = snapshot.report_id;
        }
        self == &comparable
    }

    /// Whether this later-received malformed observation implies earlier recovery freshness.
    /// Admission still requires the earliest observation time; this alone cannot authorize removal.
    #[must_use]
    pub fn supersedes_malformed_observation(&self, earlier: &Self) -> bool {
        self.requires_fresh_query
            && self.ts_init >= earlier.ts_init
            && self.same_observed_facts(earlier)
    }

    /// Selects redundant earlier observations while preserving admission and recovery constraints.
    /// The actual minimum-time report still constrains queries started before this new receipt.
    #[must_use]
    pub fn superseded_malformed_observation_indices<'a>(
        &self,
        observations: impl Iterator<Item = (usize, &'a Self)>,
    ) -> Vec<usize> {
        let superseded: Vec<_> = observations
            .filter(|(_, previous)| self.supersedes_malformed_observation(previous))
            .collect();
        let earliest = superseded
            .iter()
            .filter(|(_, previous)| previous.ts_init < self.ts_init)
            .min_by_key(|(_, previous)| previous.ts_init)
            .map(|(index, _)| *index);
        superseded
            .into_iter()
            .filter_map(|(index, _)| (Some(index) != earliest).then_some(index))
            .collect()
    }

    /// Whether a complete trade agrees with every independently retained fact.
    #[must_use]
    pub fn matches_fill(&self, fill: &FillReport) -> bool {
        !self.unrepresentable_values
            && self.account_id == fill.account_id
            && self.instrument_id == fill.instrument_id
            && self.venue_order_id == fill.venue_order_id
            && (self.trade_id == Some(fill.trade_id)
                || (self.trade_id.is_none() && self.trade_id_placeholder))
            && self
                .client_order_id
                .is_none_or(|id| fill.client_order_id.is_none_or(|other| id == other))
            && self.order_side == fill.order_side
            && self
                .last_qty
                .is_none_or(|qty| qty == fill.last_qty.as_decimal())
            && self
                .last_px
                .is_none_or(|px| px == fill.last_px.as_decimal())
            && self
                .commission_amount
                .is_none_or(|fee| fee == fill.commission.as_decimal())
            && self
                .commission_currency
                .is_none_or(|currency| currency == fill.commission.currency)
            && self
                .liquidity_side
                .is_none_or(|side| side == fill.liquidity_side)
            && self.venue_position_id == fill.venue_position_id
            && self.ts_event.is_none_or(|time| time == fill.ts_event)
    }

    /// Compares repeated successful observations, excluding local receipt metadata.
    #[must_use]
    pub fn same_successful_observation(&self, other: &Self) -> bool {
        !self.requires_fresh_query && self.same_observed_facts(other)
    }

    /// Checks that retained successful status context agrees with its execution facts.
    #[must_use]
    pub fn snapshot_is_consistent(&self) -> bool {
        self.order_snapshot
            .as_ref()
            .map_or(self.requires_fresh_query, |snapshot| {
                snapshot.account_id == self.account_id
                    && snapshot.instrument_id == self.instrument_id
                    && snapshot.venue_order_id == self.venue_order_id
                    && snapshot.client_order_id == self.client_order_id
                    && snapshot.order_side == Some(self.order_side)
                    && snapshot.venue_position_id == self.venue_position_id
                    && (snapshot.ts_init == self.ts_init
                        || (self.requires_fresh_query && snapshot.ts_init < self.ts_init))
                    && self.ts_event.is_none_or(|time| time <= snapshot.ts_last)
            })
    }

    /// Checks last-execution ordering inside the originating snapshot, without ordering ties.
    #[must_use]
    pub fn respects_snapshot_boundary(&self, last: UnixNanos, execution_time: UnixNanos) -> bool {
        self.order_snapshot.as_ref().is_none_or(|snapshot| {
            last <= snapshot.ts_last
                && (execution_time <= last || execution_time > snapshot.ts_last)
        })
    }

    /// Checks fresh query identity and observed facts, without asserting accounting.
    /// Callers must additionally prove complete history and committed native execution.
    #[must_use]
    pub fn matches_query(&self, report: &OrderStatusReport, fills: &[FillReport]) -> bool {
        self.snapshot_is_consistent()
            && self.account_id == report.account_id
            && self.instrument_id == report.instrument_id
            && self.venue_order_id == report.venue_order_id
            && self
                .client_order_id
                .is_none_or(|id| report.client_order_id.is_none_or(|other| id == other))
            && (report.ts_init > self.ts_init
                || (!self.requires_fresh_query && report.ts_init == self.ts_init))
            && {
                let mut matching = fills.iter().filter(|last| {
                    self.matches_fill(last)
                        && fills.iter().all(|fill| {
                            self.respects_snapshot_boundary(last.ts_event, fill.ts_event)
                        })
                });
                matching.next().is_some()
                    && (!self.trade_id_placeholder || matching.next().is_none())
            }
    }
}

/// Execution report variants for reconciliation.
#[derive(Clone, Debug)]
pub enum ExecutionReport {
    Order(Box<OrderStatusReport>),
    Fill(Box<FillReport>),
    /// An observed authoritative trade which constrains recovery without applying accounting.
    /// Retain it until matching native execution history proves the exact economic fact.
    FillEvidence(Box<FillReport>),
    /// An observed trade whose cumulative order evidence is unavailable or inexact.
    /// A fresh complete order query is required even when this trade is already committed.
    FillQueryRequired(Box<FillReport>),
    /// Partial execution facts retained without inferred economic values.
    IncompleteFill(Box<IncompleteFillReport>),
    /// Immutable cumulative commission constraints, without accounting authority.
    OrderFeeEvidence(Box<OrderFeeEvidence>),
    OrderWithFills(Box<OrderStatusReport>, Vec<FillReport>),
    /// Order query with authoritative fills, without snapshot correction authority.
    ///
    /// An empty fill list with positive `filled_qty` signals incomplete execution
    /// evidence. Consumers must retain it unresolved until real fills account for it.
    OrderQueryWithFills(Box<OrderStatusReport>, Vec<FillReport>),
    /// Stream execution evidence, without complete order-scoped query authority.
    ///
    /// Companion trades constrain recovery without applying fills. Status-only
    /// projection requires committed native executions to account for the snapshot.
    OrderFillEvidence(Box<OrderStatusReport>, Vec<FillReport>),
    /// Contradictory execution evidence which quantity agreement cannot resolve.
    OrderFillConflict(Box<OrderStatusReport>),
    Position(Box<PositionStatusReport>),
    MassStatus(Box<ExecutionMassStatus>),
}

impl Display for ExecutionReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Order(report) => write!(f, "{report}"),
            Self::Fill(report) => write!(f, "{report}"),
            Self::FillEvidence(report) => write!(f, "FillEvidence({report})"),
            Self::FillQueryRequired(report) => write!(f, "FillQueryRequired({report})"),
            Self::IncompleteFill(report) => write!(f, "IncompleteFill({report:?})"),
            Self::OrderFeeEvidence(report) => write!(f, "OrderFeeEvidence({report:?})"),
            Self::OrderFillConflict(report) => write!(f, "OrderFillConflict({report})"),
            Self::OrderWithFills(report, fills)
            | Self::OrderQueryWithFills(report, fills)
            | Self::OrderFillEvidence(report, fills) => {
                let kind = if matches!(self, Self::OrderQueryWithFills(_, _)) {
                    "OrderQueryWithFills"
                } else if matches!(self, Self::OrderFillEvidence(_, _)) {
                    "OrderFillEvidence"
                } else {
                    "OrderWithFills"
                };
                write!(f, "{kind}(order={report}, fills=[")?;
                for (index, fill) in fills.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{fill}")?;
                }
                f.write_str("])")
            }
            Self::Position(report) => write!(f, "{report}"),
            Self::MassStatus(report) => write!(f, "{report}"),
        }
    }
}

/// An execution command sent to an execution client.
///
/// Serializes as the contained command object. Deserialization requires its string `type` field to
/// select the variant.
#[expect(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, StrumDisplay)]
#[serde(untagged)]
pub enum TradingCommand {
    #[strum(transparent)]
    SubmitOrder(SubmitOrder),
    #[strum(transparent)]
    SubmitOrderList(SubmitOrderList),
    #[strum(transparent)]
    ModifyOrder(ModifyOrder),
    #[strum(transparent)]
    ModifyOrders(BatchModifyOrders),
    #[strum(transparent)]
    CancelOrder(CancelOrder),
    #[strum(transparent)]
    CancelOrders(BatchCancelOrders),
    #[strum(transparent)]
    CancelAllOrders(CancelAllOrders),
    #[strum(transparent)]
    QueryOrder(QueryOrder),
    #[strum(transparent)]
    QueryAccount(QueryAccount),
}

#[derive(Deserialize)]
enum TradingCommandType {
    SubmitOrder,
    SubmitOrderList,
    ModifyOrder,
    BatchModifyOrders,
    CancelOrder,
    BatchCancelOrders,
    CancelAllOrders,
    QueryOrder,
    QueryAccount,
}

#[derive(Deserialize)]
struct TradingCommandHeader {
    #[serde(rename = "type")]
    command_type: TradingCommandType,
}

impl<'de> Deserialize<'de> for TradingCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let command_type = TradingCommandHeader::deserialize(&value)
            .map_err(D::Error::custom)?
            .command_type;

        match command_type {
            TradingCommandType::SubmitOrder => serde_json::from_value(value)
                .map(Self::SubmitOrder)
                .map_err(D::Error::custom),
            TradingCommandType::SubmitOrderList => serde_json::from_value(value)
                .map(Self::SubmitOrderList)
                .map_err(D::Error::custom),
            TradingCommandType::ModifyOrder => serde_json::from_value(value)
                .map(Self::ModifyOrder)
                .map_err(D::Error::custom),
            TradingCommandType::BatchModifyOrders => serde_json::from_value(value)
                .map(Self::ModifyOrders)
                .map_err(D::Error::custom),
            TradingCommandType::CancelOrder => serde_json::from_value(value)
                .map(Self::CancelOrder)
                .map_err(D::Error::custom),
            TradingCommandType::BatchCancelOrders => serde_json::from_value(value)
                .map(Self::CancelOrders)
                .map_err(D::Error::custom),
            TradingCommandType::CancelAllOrders => serde_json::from_value(value)
                .map(Self::CancelAllOrders)
                .map_err(D::Error::custom),
            TradingCommandType::QueryOrder => serde_json::from_value(value)
                .map(Self::QueryOrder)
                .map_err(D::Error::custom),
            TradingCommandType::QueryAccount => serde_json::from_value(value)
                .map(Self::QueryAccount)
                .map_err(D::Error::custom),
        }
    }
}

impl TradingCommand {
    #[must_use]
    pub const fn client_id(&self) -> Option<ClientId> {
        match self {
            Self::SubmitOrder(command) => command.client_id,
            Self::SubmitOrderList(command) => command.client_id,
            Self::ModifyOrder(command) => command.client_id,
            Self::ModifyOrders(command) => command.client_id,
            Self::CancelOrder(command) => command.client_id,
            Self::CancelOrders(command) => command.client_id,
            Self::CancelAllOrders(command) => command.client_id,
            Self::QueryOrder(command) => command.client_id,
            Self::QueryAccount(command) => command.client_id,
        }
    }

    /// Returns the instrument ID for the command.
    ///
    /// # Panics
    ///
    /// Panics if the command is `QueryAccount` which does not have an instrument ID.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::SubmitOrder(command) => command.instrument_id,
            Self::SubmitOrderList(command) => command.instrument_id,
            Self::ModifyOrder(command) => command.instrument_id,
            Self::ModifyOrders(command) => command.instrument_id,
            Self::CancelOrder(command) => command.instrument_id,
            Self::CancelOrders(command) => command.instrument_id,
            Self::CancelAllOrders(command) => command.instrument_id,
            Self::QueryOrder(command) => command.instrument_id,
            Self::QueryAccount(_) => panic!("No instrument ID for command"),
        }
    }

    #[must_use]
    pub const fn ts_init(&self) -> UnixNanos {
        match self {
            Self::SubmitOrder(command) => command.ts_init,
            Self::SubmitOrderList(command) => command.ts_init,
            Self::ModifyOrder(command) => command.ts_init,
            Self::ModifyOrders(command) => command.ts_init,
            Self::CancelOrder(command) => command.ts_init,
            Self::CancelOrders(command) => command.ts_init,
            Self::CancelAllOrders(command) => command.ts_init,
            Self::QueryOrder(command) => command.ts_init,
            Self::QueryAccount(command) => command.ts_init,
        }
    }

    #[must_use]
    pub const fn strategy_id(&self) -> Option<StrategyId> {
        match self {
            Self::SubmitOrder(command) => Some(command.strategy_id),
            Self::SubmitOrderList(command) => Some(command.strategy_id),
            Self::ModifyOrder(command) => Some(command.strategy_id),
            Self::ModifyOrders(command) => Some(command.strategy_id),
            Self::CancelOrder(command) => Some(command.strategy_id),
            Self::CancelOrders(command) => Some(command.strategy_id),
            Self::CancelAllOrders(command) => Some(command.strategy_id),
            Self::QueryOrder(command) => Some(command.strategy_id),
            Self::QueryAccount(_) => None,
        }
    }

    #[must_use]
    pub const fn params(&self) -> Option<&Params> {
        match self {
            Self::SubmitOrder(command) => command.params.as_ref(),
            Self::SubmitOrderList(command) => command.params.as_ref(),
            Self::ModifyOrder(command) => command.params.as_ref(),
            Self::ModifyOrders(command) => command.params.as_ref(),
            Self::CancelOrder(command) => command.params.as_ref(),
            Self::CancelOrders(command) => command.params.as_ref(),
            Self::CancelAllOrders(command) => command.params.as_ref(),
            Self::QueryOrder(command) => command.params.as_ref(),
            Self::QueryAccount(command) => command.params.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
        events::OrderInitialized,
        identifiers::{AccountId, OrderListId, TradeId, TraderId, VenueOrderId},
        orders::OrderList,
        reports::{FillReport, OrderStatusReport},
        types::{Currency, Money, Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn trading_commands() -> Vec<TradingCommand> {
        let trader_id = TraderId::from("TRADER-001");
        let client_id = Some(ClientId::from("EXTERNAL"));
        let strategy_id = StrategyId::from("STRATEGY-001");
        let instrument_id = InstrumentId::from("AUD/USD.SIM");
        let ts_init = UnixNanos::from(1_000_000_000);
        let order_init = OrderInitialized::default();
        let client_order_id = order_init.client_order_id;
        let submit_order = SubmitOrder::new(
            trader_id,
            client_id,
            strategy_id,
            instrument_id,
            client_order_id,
            order_init.clone(),
            None,
            None,
            None,
            UUID4::from("00000000-0000-4000-8000-000000000001"),
            ts_init,
            None,
        );
        let order_list = OrderList::new(
            OrderListId::from("OL-001"),
            instrument_id,
            strategy_id,
            vec![client_order_id],
            ts_init,
        );
        let submit_order_list = SubmitOrderList::new(
            trader_id,
            client_id,
            strategy_id,
            order_list,
            vec![order_init],
            None,
            None,
            None,
            UUID4::from("00000000-0000-4000-8000-000000000002"),
            ts_init,
            None,
        );
        let modify_order = ModifyOrder::new(
            trader_id,
            client_id,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            None,
            None,
            None,
            UUID4::from("00000000-0000-4000-8000-000000000003"),
            ts_init,
            None,
            None,
        );
        let cancel_order = CancelOrder::new(
            trader_id,
            client_id,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            UUID4::from("00000000-0000-4000-8000-000000000005"),
            ts_init,
            None,
            None,
        );

        vec![
            TradingCommand::SubmitOrder(submit_order),
            TradingCommand::SubmitOrderList(submit_order_list),
            TradingCommand::ModifyOrder(modify_order.clone()),
            TradingCommand::ModifyOrders(BatchModifyOrders::new(
                trader_id,
                client_id,
                strategy_id,
                instrument_id,
                vec![modify_order],
                UUID4::from("00000000-0000-4000-8000-000000000004"),
                ts_init,
                None,
                None,
            )),
            TradingCommand::CancelOrder(cancel_order.clone()),
            TradingCommand::CancelOrders(BatchCancelOrders::new(
                trader_id,
                client_id,
                strategy_id,
                instrument_id,
                vec![cancel_order],
                UUID4::from("00000000-0000-4000-8000-000000000006"),
                ts_init,
                None,
                None,
            )),
            TradingCommand::CancelAllOrders(CancelAllOrders::new(
                trader_id,
                client_id,
                strategy_id,
                instrument_id,
                None,
                UUID4::from("00000000-0000-4000-8000-000000000007"),
                ts_init,
                None,
                None,
            )),
            TradingCommand::QueryOrder(QueryOrder::new(
                trader_id,
                client_id,
                strategy_id,
                instrument_id,
                client_order_id,
                None,
                UUID4::from("00000000-0000-4000-8000-000000000008"),
                ts_init,
                None,
                None,
            )),
            TradingCommand::QueryAccount(QueryAccount::new(
                trader_id,
                client_id,
                AccountId::from("SIM-001"),
                UUID4::from("00000000-0000-4000-8000-000000000009"),
                ts_init,
                None,
                None,
            )),
        ]
    }

    fn order_status_report() -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("AUD/USD.SIM"),
            None,
            VenueOrderId::from("V-001"),
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("2"),
            Quantity::from("1"),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            Some(UUID4::from("00000000-0000-4000-8000-000000000010")),
        )
    }

    fn fill_report(trade_id: &str) -> FillReport {
        FillReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("AUD/USD.SIM"),
            VenueOrderId::from("V-001"),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from("1"),
            Price::from("1.25"),
            Money::new(0.01, Currency::USD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(4),
            UnixNanos::from(5),
            Some(UUID4::from("00000000-0000-4000-8000-000000000011")),
        )
    }

    #[rstest]
    fn trading_command_round_trips_each_variant() {
        for command in trading_commands() {
            let json = serde_json::to_vec(&command).expect("command must serialize as JSON");
            let json_decoded =
                serde_json::from_slice::<TradingCommand>(&json).expect("command JSON must decode");
            let msgpack =
                rmp_serde::to_vec_named(&command).expect("command must serialize as MsgPack");
            let msgpack_decoded = rmp_serde::from_slice::<TradingCommand>(&msgpack)
                .expect("command MsgPack must decode");

            assert_eq!(json_decoded, command);
            assert_eq!(msgpack_decoded, command);
        }
    }

    #[rstest]
    fn trading_command_display_delegates_to_inner() {
        for command in trading_commands() {
            let expected = match &command {
                TradingCommand::SubmitOrder(command) => command.to_string(),
                TradingCommand::SubmitOrderList(command) => command.to_string(),
                TradingCommand::ModifyOrder(command) => command.to_string(),
                TradingCommand::ModifyOrders(command) => command.to_string(),
                TradingCommand::CancelOrder(command) => command.to_string(),
                TradingCommand::CancelOrders(command) => command.to_string(),
                TradingCommand::CancelAllOrders(command) => command.to_string(),
                TradingCommand::QueryOrder(command) => command.to_string(),
                TradingCommand::QueryAccount(command) => command.to_string(),
            };

            assert_eq!(command.to_string(), expected);
        }
    }

    #[rstest]
    #[case("complete", true)]
    #[case("missing_trade", false)]
    #[case("placeholder", true)]
    #[case("placeholder_price", false)]
    #[case("placeholder_stale", false)]
    #[case("placeholder_ambiguous", false)]
    #[case("wrong_trade", false)]
    #[case("quantity", false)]
    #[case("price", false)]
    #[case("commission", false)]
    #[case("commission_currency", false)]
    #[case("unrepresentable", false)]
    #[case("money_precision", false)]
    #[case("known_amount", true)]
    #[case("known_currency", true)]
    #[case("old_query", false)]
    #[case("same_time_query", false)]
    #[case("successful_same_query", true)]
    #[case("successful_old_query", false)]
    #[case("snapshot_account", false)]
    #[case("snapshot_instrument", false)]
    #[case("snapshot_order", false)]
    #[case("snapshot_client", false)]
    #[case("snapshot_side", false)]
    #[case("snapshot_position", false)]
    #[case("snapshot_time", false)]
    #[case("snapshot_boundary", false)]
    #[case("snapshot_missing", false)]
    fn incomplete_fill_requires_identity_exact_facts_and_fresh_query(
        #[case] change: &str,
        #[case] expected: bool,
    ) {
        let fill = fill_report("T-001");
        let mut report = order_status_report();
        report.ts_init = UnixNanos::from(6);
        report.ts_last = fill.ts_event;
        let mut evidence = IncompleteFillReport {
            account_id: fill.account_id,
            instrument_id: fill.instrument_id,
            venue_order_id: fill.venue_order_id,
            client_order_id: fill.client_order_id,
            trade_id: Some(fill.trade_id),
            trade_id_placeholder: false,
            order_side: fill.order_side,
            last_qty: Some(fill.last_qty.as_decimal()),
            last_px: Some(fill.last_px.as_decimal()),
            commission_amount: None,
            commission_currency: None,
            unrepresentable_values: false,
            requires_fresh_query: true,
            order_snapshot: None,
            liquidity_side: Some(fill.liquidity_side),
            venue_position_id: fill.venue_position_id,
            ts_event: Some(fill.ts_event),
            ts_init: fill.ts_init,
        };

        match change {
            "missing_trade" => evidence.trade_id = None,
            value if value.starts_with("placeholder") => {
                evidence.trade_id = None;
                evidence.trade_id_placeholder = true;
                if change == "placeholder_price" {
                    evidence.last_px = Some(Decimal::from(2));
                }

                if change == "placeholder_stale" {
                    report.ts_init = evidence.ts_init;
                }
            }
            "wrong_trade" => evidence.trade_id = Some(TradeId::from("OTHER")),
            "quantity" => evidence.last_qty = Some(Decimal::from(2)),
            "price" => evidence.last_px = Some(Decimal::from(2)),
            "commission" => {
                evidence.commission_amount = Some(Decimal::from_str_exact("0.02").unwrap());
            }
            "commission_currency" => evidence.commission_currency = Some(Currency::BTC()),
            "unrepresentable" => evidence.unrepresentable_values = true,
            "money_precision" => {
                evidence.commission_amount =
                    Some(Decimal::from_str_exact("0.01000000000000001").unwrap());
            }
            "known_amount" => evidence.commission_amount = Some(fill.commission.as_decimal()),
            "known_currency" => evidence.commission_currency = Some(fill.commission.currency),
            "old_query" => report.ts_init = UnixNanos::from(4),
            "same_time_query" => report.ts_init = evidence.ts_init,
            "successful_same_query" => {
                evidence.requires_fresh_query = false;
                report.ts_init = evidence.ts_init;
            }
            "successful_old_query" => {
                evidence.requires_fresh_query = false;
                report.ts_init = UnixNanos::from(4);
            }
            value if value.starts_with("snapshot_") => evidence.requires_fresh_query = false,
            "complete" => {}
            _ => unreachable!(),
        }

        if !evidence.requires_fresh_query {
            let mut snapshot = report.clone();
            snapshot.ts_init = evidence.ts_init;
            snapshot.client_order_id = evidence.client_order_id;
            match change {
                "snapshot_account" => snapshot.account_id = AccountId::from("OTHER-001"),
                "snapshot_instrument" => snapshot.instrument_id = InstrumentId::from("BTC/USD.SIM"),
                "snapshot_order" => snapshot.venue_order_id = VenueOrderId::from("OTHER"),
                "snapshot_client" => snapshot.client_order_id = Some(ClientOrderId::from("OTHER")),
                "snapshot_side" => snapshot.order_side = Some(OrderSide::Sell),
                "snapshot_position" => snapshot.venue_position_id = Some(PositionId::from("OTHER")),
                "snapshot_time" => snapshot.ts_init = UnixNanos::from(99),
                "snapshot_boundary" => snapshot.ts_last = UnixNanos::from(1),
                _ => {}
            }

            if change != "snapshot_missing" {
                evidence.order_snapshot = Some(Box::new(snapshot));
            }
        }

        if evidence.snapshot_is_consistent() && !evidence.requires_fresh_query {
            let mut repeated = evidence.clone();
            repeated.ts_init = UnixNanos::from(99);
            let snapshot = repeated.order_snapshot.as_mut().unwrap();
            snapshot.ts_init = repeated.ts_init;
            snapshot.report_id = UUID4::new();
            assert!(evidence.same_successful_observation(&repeated));
            repeated.trade_id = Some(TradeId::from("OTHER"));
            assert!(!evidence.same_successful_observation(&repeated));
        }
        let mut later = evidence.clone();
        later.ts_init = UnixNanos::from(100);
        assert_eq!(
            later.supersedes_malformed_observation(&evidence),
            evidence.requires_fresh_query && evidence.snapshot_is_consistent(),
        );
        assert!(!evidence.supersedes_malformed_observation(&later));
        later.commission_currency = Some(Currency::EUR());
        assert!(!later.supersedes_malformed_observation(&evidence));
        let encoded = rmp_serde::to_vec_named(&evidence).unwrap();
        let restored: IncompleteFillReport = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(restored, evidence);
        for field in [
            "unrepresentable_values",
            "requires_fresh_query",
            "order_snapshot",
        ] {
            let mut old = serde_json::to_value(&evidence).unwrap();
            old.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<IncompleteFillReport>(old.clone()).is_err());
            let old = rmp_serde::to_vec_named(&old).unwrap();
            assert!(rmp_serde::from_slice::<IncompleteFillReport>(&old).is_err());
        }
        let mut fills = vec![fill];
        if change == "placeholder_ambiguous" {
            let mut other = fills[0].clone();
            other.trade_id = TradeId::from("OTHER");
            fills.push(other);
        }
        assert_eq!(restored.matches_query(&report, &fills), expected);
        let mut legacy = serde_json::to_value(&evidence).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("trade_id_placeholder");
        let legacy: IncompleteFillReport = serde_json::from_value(legacy).unwrap();
        assert!(!legacy.trade_id_placeholder);
        if legacy.trade_id.is_none() {
            assert!(!legacy.matches_query(&report, &fills));
        }
    }

    #[rstest]
    fn execution_report_order_with_fills_uses_inner_display() {
        let order_report = order_status_report();
        let fill_report_1 = fill_report("T-001");
        let fill_report_2 = fill_report("T-002");
        let expected = format!(
            "OrderWithFills(order={order_report}, fills=[{fill_report_1}, {fill_report_2}])"
        );
        let report = ExecutionReport::OrderWithFills(
            Box::new(order_report),
            vec![fill_report_1, fill_report_2],
        );

        assert_eq!(report.to_string(), expected);
    }

    #[rstest]
    fn trading_command_rejects_unknown_type() {
        let error = serde_json::from_value::<TradingCommand>(serde_json::json!({
            "type": "UnknownCommand",
        }))
        .expect_err("unknown command type must be rejected");

        assert_eq!(
            error.to_string(),
            "unknown variant `UnknownCommand`, expected one of `SubmitOrder`, `SubmitOrderList`, \
             `ModifyOrder`, `BatchModifyOrders`, `CancelOrder`, `BatchCancelOrders`, \
             `CancelAllOrders`, `QueryOrder`, `QueryAccount`",
        );
    }

    #[rstest]
    fn trading_command_accessors_report_the_inner_identity() {
        let instrument_id = InstrumentId::from("AUD/USD.SIM");
        let strategy_id = StrategyId::from("STRATEGY-001");
        let ts_init = UnixNanos::from(1_000_000_000);

        for command in trading_commands() {
            assert_eq!(command.client_id(), Some(ClientId::from("EXTERNAL")));
            assert_eq!(command.ts_init(), ts_init);
            assert_eq!(command.params(), None);

            if matches!(command, TradingCommand::QueryAccount(_)) {
                assert_eq!(command.strategy_id(), None);
            } else {
                assert_eq!(command.strategy_id(), Some(strategy_id));
                assert_eq!(command.instrument_id(), instrument_id);
            }
        }
    }

    #[rstest]
    fn trading_command_params_expose_the_inner_params() {
        let params = Params::new([("reduce_only".into(), "true".into())].into());

        let command = TradingCommand::QueryOrder(QueryOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("EXTERNAL")),
            StrategyId::from("STRATEGY-001"),
            InstrumentId::from("AUD/USD.SIM"),
            OrderInitialized::default().client_order_id,
            None,
            UUID4::from("00000000-0000-4000-8000-000000000012"),
            UnixNanos::from(1),
            Some(params.clone()),
            None,
        ));

        assert_eq!(command.params(), Some(&params));
    }

    #[rstest]
    #[should_panic(expected = "No instrument ID for command")]
    fn trading_command_instrument_id_panics_for_query_account() {
        let command = trading_commands()
            .into_iter()
            .find(|command| matches!(command, TradingCommand::QueryAccount(_)))
            .expect("query account command must be present");

        let _ = command.instrument_id();
    }
}
