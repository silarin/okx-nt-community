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

//! Integration tests for the bus capture adapter against a real `MessageBus` and the
//! production [`EventStoreWriter`] over a shared [`MemoryBackend`].
//!
//! Exercises the SPEC's "capture before fanout" contract end-to-end with the Phase 6
//! representative samples: a command (`SubmitOrder`), a generated event (`OrderFilled`),
//! and a raw venue report (`OrderStatusReport`).

use std::{any::Any, cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use indexmap::IndexMap;
use nautilus_common::{
    cache::Cache,
    clock::VirtualClock,
    messages::{
        ExecutionReport,
        data::{DataResponse, DataResponseEnvelope, QuotesResponse},
        execution::{SubmitOrder, TradingCommand},
    },
    msgbus::{
        self, BusTap, Endpoint, MStr, MessageBus, MessagingSwitchboard, ShareableMessageHandler,
        TypedHandler,
    },
};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_static};
use nautilus_event_store::{
    AppendEntry, BusCaptureAdapter, EncoderRegistry, EntryDraft, EventStore, EventStoreEntry,
    EventStoreError, EventStoreReader, EventStoreWriter, HaltCallback, Headers, IndexKind,
    MemoryBackend, RegisteredComponents, RunManifest, RunStatus, ScanDirection, SnapshotAnchor,
    Topic, WriterConfig, default_registry, noop_halt, restore_cache_snapshot_and_replay_tail,
};
use nautilus_execution::{engine::ExecutionEngine, reconciliation::RECONCILIATION_ORDER_TAG};
use nautilus_model::{
    accounts::CashAccount,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
        TimeInForce,
    },
    events::{
        AccountState, OrderEventAny, OrderFilled,
        order::spec::{OrderFilledSpec, OrderInitializedSpec},
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
    orders::{Order, builder::OrderTestBuilder, stubs::TestOrderEventStubs},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use parking_lot::Mutex;
use rstest::rstest;
use ustr::Ustr;

const INSTANCE_ID: &str = "trader-001";

fn manifest(run_id: &str) -> RunManifest {
    RunManifest {
        run_id: run_id.to_string(),
        parent_run_id: None,
        instance_id: INSTANCE_ID.to_string(),
        binary_hash: "deadbeef".to_string(),
        schema_version: 1,
        crate_versions: "feedface".to_string(),
        feature_flags: vec!["order-capture=committed-routes-cumulative-fees-v2".to_string()],
        adapter_versions: IndexMap::new(),
        config_hash: "cafebabe".to_string(),
        registered_components: RegisteredComponents::default(),
        seed: None,
        start_ts_init: UnixNanos::from(0),
        end_ts_init: None,
        high_watermark: 0,
        status: RunStatus::Running,
    }
}

/// Wraps `MemoryBackend` so the writer thread and the test thread can read the same
/// backend instance.
#[derive(Debug)]
struct SharedMemory(Arc<Mutex<MemoryBackend>>);

impl EventStore for SharedMemory {
    fn open_run(&mut self, _: RunManifest) -> Result<(), EventStoreError> {
        unreachable!("test wrapper does not forward open_run")
    }

    fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
        self.0.lock().append_batch(entries)
    }

    fn scan_range(
        &self,
        from: u64,
        to: u64,
        direction: ScanDirection,
    ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
        self.0.lock().scan_range(from, to, direction)
    }

    fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
        self.0.lock().scan_seq(seq)
    }

    fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
        self.0.lock().lookup(kind, key)
    }

    fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
        self.0.lock().iter_index_keys(kind)
    }

    fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
        self.0.lock().seal(status)
    }

    fn manifest(&self) -> Result<RunManifest, EventStoreError> {
        self.0.lock().manifest()
    }

    fn high_watermark(&self) -> Result<u64, EventStoreError> {
        self.0.lock().high_watermark()
    }
}

fn writer_with_open_run(
    run_id: &str,
    halt: HaltCallback,
) -> (Arc<EventStoreWriter>, Arc<Mutex<MemoryBackend>>) {
    let backend_arc: Arc<Mutex<MemoryBackend>> = Arc::new(Mutex::new(MemoryBackend::new()));
    backend_arc
        .lock()
        .open_run(manifest(run_id))
        .expect("open run");

    let wrapper = SharedMemory(Arc::clone(&backend_arc));
    let writer = EventStoreWriter::spawn(
        Box::new(wrapper),
        get_atomic_clock_static(),
        halt,
        WriterConfig::default(),
    )
    .expect("spawn");
    (Arc::new(writer), backend_arc)
}

fn drain(writer: &Arc<EventStoreWriter>, target_hwm: u64) {
    let mut waited = Duration::ZERO;
    let deadline = Duration::from_secs(2);
    while writer.high_watermark() < target_hwm && waited < deadline {
        std::thread::sleep(Duration::from_millis(5));
        waited += Duration::from_millis(5);
    }
    assert!(
        writer.high_watermark() >= target_hwm,
        "writer high_watermark {} did not reach {target_hwm} within {:?}",
        writer.high_watermark(),
        deadline,
    );
}

/// Makes a [`SubmitOrder`] command suitable for a representative-end-to-end capture.
fn make_submit_order(client_order_id: ClientOrderId) -> SubmitOrder {
    let order_init = OrderInitializedSpec::builder()
        .instrument_id(InstrumentId::from("ETHUSDT-PERP.BINANCE"))
        .client_order_id(client_order_id)
        .quantity(Quantity::from("1"))
        .time_in_force(TimeInForce::Gtc)
        .ts_event(UnixNanos::from(1))
        .ts_init(UnixNanos::from(2))
        .build();
    SubmitOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("BINANCE")),
        StrategyId::from("S-001"),
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        client_order_id,
        order_init,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(3),
        None, // correlation_id
    )
}

fn make_order_filled(client_order_id: ClientOrderId, venue_order_id: VenueOrderId) -> OrderFilled {
    OrderFilledSpec::builder()
        .instrument_id(InstrumentId::from("ETHUSDT-PERP.BINANCE"))
        .client_order_id(client_order_id)
        .venue_order_id(venue_order_id)
        .account_id(AccountId::from("BINANCE-001"))
        .trade_id(TradeId::from("T-9999"))
        .last_qty(Quantity::from("1"))
        .last_px(Price::from("100.00"))
        .currency(Currency::USDT())
        .ts_event(UnixNanos::from(10))
        .ts_init(UnixNanos::from(11))
        .commission(Money::new(0.10, Currency::USDT()))
        .build()
}

fn make_order_status_report(
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
) -> OrderStatusReport {
    OrderStatusReport::new(
        AccountId::from("BINANCE-001"),
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        Some(client_order_id),
        venue_order_id,
        OrderSide::Buy.into(),
        OrderType::Market,
        TimeInForce::Gtc,
        OrderStatus::Filled,
        Quantity::from("1"),
        Quantity::from("1"),
        UnixNanos::from(20),
        UnixNanos::from(21),
        UnixNanos::from(22),
        Some(UUID4::new()),
    )
}

#[rstest]
fn end_to_end_capture_writes_command_event_and_report() {
    // Construct a real MessageBus on this thread so capture runs in the same
    // single-threaded environment the kernel uses; the writer still owns its dedicated
    // I/O thread.
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, backend_arc) = writer_with_open_run("run-capture-e2e", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = BusCaptureAdapter::new(Arc::clone(&writer), registry, noop_halt());

    let client_order_id = ClientOrderId::from("O-20260510-000001");
    let venue_order_id = VenueOrderId::from("V-12345");

    // Capture the command first, emulating the kernel's send-side dispatch wrapper.
    let cmd = make_submit_order(client_order_id);
    let captured_cmd = adapter
        .capture::<SubmitOrder>(
            Topic::from("exec.command.SubmitOrder"),
            &cmd,
            Headers::empty(),
            cmd.ts_init,
        )
        .expect("capture cmd");
    assert!(captured_cmd, "command must be allow-listed");

    // Capture the generated event, emulating the publish-side wrapper.
    let event = make_order_filled(client_order_id, venue_order_id);
    let captured_event = adapter
        .capture::<OrderFilled>(
            Topic::from("events.order.OrderFilled.S-001"),
            &event,
            Headers::empty(),
            event.ts_init,
        )
        .expect("capture event");
    assert!(captured_event, "event must be allow-listed");

    // Capture the raw venue report, emulating the reconciliation publish boundary.
    let report = make_order_status_report(client_order_id, venue_order_id);
    let captured_report = adapter
        .capture::<OrderStatusReport>(
            Topic::from("reports.OrderStatusReport.BINANCE"),
            &report,
            Headers::empty(),
            report.ts_init,
        )
        .expect("capture report");
    assert!(captured_report, "report must be allow-listed");

    drain(&writer, 3);

    let backend = backend_arc.lock();

    let cmd_entry = backend.scan_seq(1).expect("scan").expect("present");
    assert_eq!(cmd_entry.payload_type.as_str(), "SubmitOrder");
    assert_eq!(cmd_entry.topic.as_ref(), "exec.command.SubmitOrder");
    assert_eq!(cmd_entry.ts_init, cmd.ts_init);

    let event_entry = backend.scan_seq(2).expect("scan").expect("present");
    assert_eq!(event_entry.payload_type.as_str(), "OrderFilled");
    assert_eq!(event_entry.topic.as_ref(), "events.order.OrderFilled.S-001");

    let report_entry = backend.scan_seq(3).expect("scan").expect("present");
    assert_eq!(report_entry.payload_type.as_str(), "OrderStatusReport");
    assert_eq!(
        report_entry.topic.as_ref(),
        "reports.OrderStatusReport.BINANCE"
    );

    // Sidecar indices must point at the entries that mentioned each id; the command's
    // entry is the earliest mention of the client order id, and the event's entry is
    // the earliest mention of the venue order id.
    let by_client = backend
        .lookup(IndexKind::ClientOrderId, client_order_id.as_str())
        .expect("lookup")
        .expect("indexed");
    assert_eq!(
        by_client, 1,
        "client_order_id index must point at the command"
    );
    let by_venue = backend
        .lookup(IndexKind::VenueOrderId, venue_order_id.as_str())
        .expect("lookup")
        .expect("indexed");
    assert_eq!(by_venue, 2, "venue_order_id index must point at the event");

    // Hashes survive a round trip through the backend.
    for entry in [&cmd_entry, &event_entry, &report_entry] {
        assert_eq!(entry.recompute_hash(), entry.entry_hash);
    }

    drop(backend);
    drop(adapter);

    // Recover the writer from the Arc so we can close it cleanly.
    let writer = Arc::try_unwrap(writer).expect("sole writer reference");
    let final_hwm = writer.close(run_ended_draft()).expect("close writer");
    assert_eq!(final_hwm, 4, "three captured entries plus RunEnded");

    let backend = backend_arc.lock();
    let manifest = backend.manifest().expect("manifest");
    assert_eq!(manifest.status, RunStatus::Ended);
    assert_eq!(manifest.high_watermark, 4);
}

#[rstest]
fn capture_preserves_order_routes_and_dedups_same_route() {
    // Ingress and committed publication must both survive capture: replay applies
    // only the latter, which may carry a newly assigned position ID.
    let (writer, backend_arc) = writer_with_open_run("run-capture-dedup", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = BusCaptureAdapter::new(Arc::clone(&writer), registry, noop_halt());

    let fill = make_order_filled(
        ClientOrderId::from("O-20260510-000002"),
        VenueOrderId::from("V-12346"),
    );
    let event = OrderEventAny::Filled(fill);

    let first = adapter
        .capture::<OrderEventAny>(
            Topic::from("Portfolio.update_order"),
            &event,
            Headers::empty(),
            UnixNanos::from(11),
        )
        .expect("first capture");
    let second = adapter
        .capture::<OrderEventAny>(
            Topic::from("events.order.S-001"),
            &event,
            Headers::empty(),
            UnixNanos::from(11),
        )
        .expect("second capture");

    assert!(first, "first dispatch hop must capture");
    assert!(second, "committed publication must survive prior ingress");
    assert!(
        !adapter
            .capture::<OrderEventAny>(
                Topic::from("events.order.S-001"),
                &event,
                Headers::empty(),
                UnixNanos::from(11),
            )
            .unwrap(),
        "repeated publication on the same route is deduplicated"
    );

    // A distinct event still captures.
    let other_fill = make_order_filled(
        ClientOrderId::from("O-20260510-000003"),
        VenueOrderId::from("V-12347"),
    );
    let captured_other = adapter
        .capture::<OrderEventAny>(
            Topic::from("events.order.S-001"),
            &OrderEventAny::Filled(other_fill),
            Headers::empty(),
            UnixNanos::from(12),
        )
        .expect("third capture");
    assert!(captured_other, "distinct event identity must capture");

    drain(&writer, 3);

    let backend = backend_arc.lock();
    assert_eq!(backend.high_watermark().expect("hwm"), 3);

    let entry = backend.scan_seq(1).expect("scan").expect("present");
    assert_eq!(entry.payload_type.as_str(), "OrderFilled");
    assert_eq!(
        entry.topic.as_ref(),
        "Portfolio.update_order",
        "ingress remains available as forensic evidence"
    );
    let committed = backend.scan_seq(2).expect("scan").expect("present");
    assert_eq!(committed.topic.as_ref(), "events.order.S-001");
}

#[rstest]
fn capture_skips_messages_outside_the_allow_list() {
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, _backend) = writer_with_open_run("run-capture-deny", noop_halt());
    let registry = Arc::new(EncoderRegistry::new());
    let adapter = BusCaptureAdapter::new(Arc::clone(&writer), registry, noop_halt());

    // Empty registry: every type is out-of-list, so capture must report `false` rather
    // than surfacing an error. This is the contract that lets the adapter sit on dispatch
    // paths that mix state-affecting and non-state-affecting traffic.
    let cmd = make_submit_order(ClientOrderId::from("O-deny"));
    let captured = adapter
        .capture::<SubmitOrder>(
            Topic::from("exec.command.SubmitOrder"),
            &cmd,
            Headers::empty(),
            cmd.ts_init,
        )
        .expect("capture");
    assert!(!captured);
    assert_eq!(writer.high_watermark(), 0);
}

#[rstest]
fn captured_entry_observed_before_bus_subscriber_dispatch() {
    // Demonstrates the dispatch-boundary ordering contract: the writer high-watermark
    // for the captured entry advances before the bus dispatches to a downstream
    // subscriber. Modeled by capturing first, then publishing, with a subscriber that
    // records the writer's high-watermark at handler-invocation time.
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, _backend) = writer_with_open_run("run-capture-order", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = BusCaptureAdapter::new(Arc::clone(&writer), registry, noop_halt());

    let observed_hwm: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
    let observed_for_handler = Arc::clone(&observed_hwm);
    let writer_for_handler = Arc::clone(&writer);
    let handler = ShareableMessageHandler::from_typed(move |_msg: &SubmitOrder| {
        *observed_for_handler.lock() = Some(writer_for_handler.high_watermark());
    });

    msgbus::subscribe_any("exec.command.*".into(), handler, Some(0));

    let cmd = make_submit_order(ClientOrderId::from("O-ordering"));
    adapter
        .capture::<SubmitOrder>(
            Topic::from("exec.command.SubmitOrder"),
            &cmd,
            Headers::empty(),
            cmd.ts_init,
        )
        .expect("capture");
    drain(&writer, 1);

    msgbus::publish_any("exec.command.SubmitOrder".into(), &cmd);

    let observed = (*observed_hwm.lock()).unwrap_or_default();
    assert!(
        observed >= 1,
        "downstream subscriber observed hwm {observed} but expected the captured entry's seq",
    );
}

fn run_ended_draft() -> EntryDraft {
    EntryDraft {
        headers: Headers::empty(),
        topic: Topic::from("run.lifecycle.RunEnded"),
        payload_type: Ustr::from("RunEnded"),
        payload: Bytes::new(),
        ts_init: UnixNanos::from(99_999),
        index_keys: Vec::new(),
    }
}

/// Minimal [`BusTap`] that forwards every publish to a [`BusCaptureAdapter`].
///
/// Mirrors the kernel's tap so tests can exercise `publish_any -> tap -> writer`
/// without pulling in the kernel lifecycle.
struct AdapterTap {
    adapter: Arc<BusCaptureAdapter>,
}

impl BusTap for AdapterTap {
    fn on_publish(&self, topic: MStr<msgbus::Topic>, message: &dyn Any) {
        let _ = self
            .adapter
            .capture_any(topic, message, Headers::empty(), UnixNanos::from(0));
    }

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) {}
}

fn make_fill_report_for_capture(
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
) -> FillReport {
    FillReport::new(
        AccountId::from("BINANCE-001"),
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        venue_order_id,
        TradeId::from("T-77"),
        OrderSide::Buy,
        Quantity::from("1"),
        Price::from("100.00"),
        Money::new(0.10, Currency::USDT()),
        LiquiditySide::Taker,
        Some(client_order_id),
        None,
        UnixNanos::from(30),
        UnixNanos::from(31),
        Some(UUID4::new()),
    )
}

fn make_position_status_report_for_capture() -> PositionStatusReport {
    PositionStatusReport::new(
        AccountId::from("BINANCE-001"),
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        PositionSide::Long,
        Quantity::from("1"),
        UnixNanos::from(40),
        UnixNanos::from(41),
        Some(UUID4::new()),
        Some(PositionId::from("P-1")),
        None,
    )
}

#[rstest]
fn raw_report_topics_capture_via_publish_any() {
    // The execution engine publishes raw venue reports on the
    // `reconciliation.raw.*` topics before reconciliation mutates local state.
    // Each topic must produce a captured entry whose payload decodes back to the
    // original report. Replay treats these raw inputs as forensic records; it
    // applies the synthesized events captured later rather than running reconciliation.
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, backend_arc) = writer_with_open_run("run-capture-raw-reports", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = Arc::new(BusCaptureAdapter::new(
        Arc::clone(&writer),
        registry,
        noop_halt(),
    ));
    let tap: Rc<dyn BusTap> = Rc::new(AdapterTap {
        adapter: Arc::clone(&adapter),
    });
    msgbus::set_bus_tap(tap);

    let client_order_id = ClientOrderId::from("O-raw-1");
    let venue_order_id = VenueOrderId::from("V-raw-1");

    let order_report = make_order_status_report(client_order_id, venue_order_id);
    msgbus::publish_any(
        MessagingSwitchboard::reconciliation_raw_order_status_report_topic(),
        &order_report,
    );

    let fill_report = make_fill_report_for_capture(client_order_id, venue_order_id);
    msgbus::publish_any(
        MessagingSwitchboard::reconciliation_raw_fill_report_topic(),
        &fill_report,
    );

    let position_report = make_position_status_report_for_capture();
    msgbus::publish_any(
        MessagingSwitchboard::reconciliation_raw_position_status_report_topic(),
        &position_report,
    );

    drain(&writer, 3);

    let backend = backend_arc.lock();

    assert_eq!(
        backend.high_watermark().expect("hwm"),
        3,
        "exactly three raw reports must have been captured",
    );
    assert!(
        backend.scan_seq(4).expect("scan").is_none(),
        "no extra entries beyond the three raw reports",
    );

    let order_entry = backend.scan_seq(1).expect("scan").expect("present");
    assert_eq!(
        order_entry.payload_type.as_str(),
        "OrderStatusReport",
        "raw OrderStatusReport must keep its bare-type payload tag",
    );
    assert_eq!(
        order_entry.topic.as_ref(),
        "reconciliation.raw.OrderStatusReport",
    );
    let decoded_order: OrderStatusReport =
        rmp_serde::from_slice(&order_entry.payload).expect("decode order");
    assert_eq!(decoded_order, order_report);

    let fill_entry = backend.scan_seq(2).expect("scan").expect("present");
    assert_eq!(fill_entry.payload_type.as_str(), "FillReport");
    assert_eq!(fill_entry.topic.as_ref(), "reconciliation.raw.FillReport");
    let decoded_fill: FillReport = rmp_serde::from_slice(&fill_entry.payload).expect("decode fill");
    assert_eq!(decoded_fill, fill_report);

    let position_entry = backend.scan_seq(3).expect("scan").expect("present");
    assert_eq!(position_entry.payload_type.as_str(), "PositionStatusReport");
    assert_eq!(
        position_entry.topic.as_ref(),
        "reconciliation.raw.PositionStatusReport",
    );
    let decoded_position: PositionStatusReport =
        rmp_serde::from_slice(&position_entry.payload).expect("decode position");
    assert_eq!(decoded_position, position_report);

    drop(backend);
    msgbus::clear_bus_tap();
    drop(adapter);
    let writer = Arc::try_unwrap(writer).expect("sole writer reference");
    let _ = writer.close(run_ended_draft()).expect("close writer");
}

#[rstest]
fn default_registry_extracts_headers_from_submit_order_fields() {
    // The bus tap reads headers off the typed message via the registry; the default
    // registry must wire a per-type extractor that surfaces both correlation_id and
    // causation_id so forensics scans inherit the same chain and lineage edge the
    // strategy populated at the minting site.
    let registry = default_registry();
    let correlation = UUID4::new();
    let caused = UUID4::new();
    let mut cmd = make_submit_order(ClientOrderId::from("O-headers"));
    cmd.correlation_id = Some(correlation);
    cmd.causation_id = Some(caused);

    let headers = registry
        .headers_for_any(&cmd as &dyn Any)
        .expect("registered");
    assert_eq!(headers.correlation_id, Some(correlation));
    assert_eq!(headers.causation_id, Some(caused));
}

#[rstest]
fn default_registry_unwraps_trading_command_envelope_headers() {
    // Production code reaches the tap as TradingCommand, not the bare command type;
    // the wrapper extractor must dispatch by variant so the captured entry carries the
    // inner command's headers rather than an empty default.
    let registry = default_registry();
    let correlation = UUID4::new();
    let caused = UUID4::new();
    let mut cmd = make_submit_order(ClientOrderId::from("O-tc-headers"));
    cmd.correlation_id = Some(correlation);
    cmd.causation_id = Some(caused);
    let envelope = TradingCommand::SubmitOrder(cmd);

    let headers = registry
        .headers_for_any(&envelope as &dyn Any)
        .expect("registered");
    assert_eq!(headers.correlation_id, Some(correlation));
    assert_eq!(headers.causation_id, Some(caused));
}

/// Bus tap that mirrors the kernel's [`EventStoreBusTap`]: it consults the registry to
/// derive headers per-message rather than passing [`Headers::empty`].
struct HeadersAwareAdapterTap {
    adapter: Arc<BusCaptureAdapter>,
    registry: Arc<EncoderRegistry>,
}

impl BusTap for HeadersAwareAdapterTap {
    fn on_publish(&self, topic: MStr<msgbus::Topic>, message: &dyn Any) {
        let headers = self
            .registry
            .headers_for_any(message)
            .unwrap_or_else(Headers::empty);
        let _ = self
            .adapter
            .capture_any(topic, message, headers, UnixNanos::from(0));
    }

    fn on_send(&self, endpoint: MStr<Endpoint>, message: &dyn Any) {
        let headers = self
            .registry
            .headers_for_any(message)
            .unwrap_or_else(Headers::empty);
        let topic = Topic::from(*endpoint);
        let _ = self
            .adapter
            .capture_any(topic, message, headers, UnixNanos::from(0));
    }

    fn on_response(&self, _correlation_id: &UUID4, message: &dyn Any) {
        let headers = self
            .registry
            .headers_for_any(message)
            .unwrap_or_else(Headers::empty);
        let topic = MessagingSwitchboard::data_response_topic();
        let _ = self
            .adapter
            .capture_any(topic, message, headers, UnixNanos::from(0));
    }
}

#[rstest]
fn tap_path_writes_headers_from_command_fields() {
    // End-to-end: a SubmitOrder published with populated correlation_id and
    // causation_id must reach the writer with both headers on the entry. This is the
    // contract that makes correlation-keyed forensics filtering and causation-walk
    // tree reconstruction work once strategies populate the fields at minting time.
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, backend_arc) = writer_with_open_run("run-tap-headers", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = Arc::new(BusCaptureAdapter::new(
        Arc::clone(&writer),
        Arc::clone(&registry),
        noop_halt(),
    ));
    let tap: Rc<dyn BusTap> = Rc::new(HeadersAwareAdapterTap {
        adapter: Arc::clone(&adapter),
        registry: Arc::clone(&registry),
    });
    msgbus::set_bus_tap(tap);

    let correlation = UUID4::new();
    let caused = UUID4::new();
    let mut cmd = make_submit_order(ClientOrderId::from("O-tap-hdr"));
    cmd.correlation_id = Some(correlation);
    cmd.causation_id = Some(caused);

    msgbus::publish_any(Topic::from("exec.command.SubmitOrder"), &cmd);

    drain(&writer, 1);

    let backend = backend_arc.lock();
    let entry = backend.scan_seq(1).expect("scan").expect("present");
    assert_eq!(entry.headers.correlation_id, Some(correlation));
    assert_eq!(entry.headers.causation_id, Some(caused));

    drop(backend);
    msgbus::clear_bus_tap();
    drop(adapter);
    let writer = Arc::try_unwrap(writer).expect("sole writer reference");
    let _ = writer.close(run_ended_draft()).expect("close writer");
}

#[rstest]
fn tap_path_writes_headers_from_data_response_correlation_handler() {
    #[derive(serde::Deserialize)]
    struct CapturedResponse {
        correlation_id: UUID4,
        completed: bool,
        sub_correlation_id: Option<UUID4>,
        failure_reason: Option<String>,
        payload_type: Option<String>,
        payload: Option<bytes::Bytes>,
    }

    // Data responses dispatch through the bus correlation-response path rather than a
    // publish topic or endpoint. The tap still captures the DataResponse envelope and
    // forwards its correlation_id into event-store headers.
    let bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None);
    let _bus_rc = bus.register_message_bus();

    let (writer, backend_arc) = writer_with_open_run("run-tap-data-response-headers", noop_halt());
    let registry = Arc::new(default_registry());
    let adapter = Arc::new(BusCaptureAdapter::new(
        Arc::clone(&writer),
        Arc::clone(&registry),
        noop_halt(),
    ));
    let tap: Rc<dyn BusTap> = Rc::new(HeadersAwareAdapterTap {
        adapter: Arc::clone(&adapter),
        registry: Arc::clone(&registry),
    });
    msgbus::set_bus_tap(tap);

    let correlation = UUID4::new();
    let handler_called = Rc::new(RefCell::new(false));
    let handler_called_clone = Rc::clone(&handler_called);
    msgbus::register_response_handler(
        &correlation,
        TypedHandler::from(move |_resp: &DataResponseEnvelope| {
            *handler_called_clone.borrow_mut() = true;
        }),
    );

    let response = QuotesResponse::new(
        correlation,
        ClientId::from("BINANCE"),
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        Vec::new(),
        None,
        None,
        UnixNanos::from(30),
        None,
    );
    msgbus::send_response(&correlation, &DataResponse::Quotes(response.clone()));

    drain(&writer, 1);

    assert!(*handler_called.borrow());
    let backend = backend_arc.lock();
    let entry = backend.scan_seq(1).expect("scan").expect("present");
    assert_eq!(entry.headers.correlation_id, Some(correlation));
    assert_eq!(entry.headers.causation_id, None);
    assert_eq!(entry.payload_type.as_str(), "DataResponseEnvelope");
    assert_eq!(entry.topic, MessagingSwitchboard::data_response_topic());

    let envelope: CapturedResponse =
        rmp_serde::from_slice(&entry.payload).expect("decode response envelope");
    assert_eq!(envelope.correlation_id, correlation);
    assert!(envelope.completed);
    assert_eq!(envelope.sub_correlation_id, None);
    assert_eq!(envelope.failure_reason, None);
    assert_eq!(envelope.payload_type.as_deref(), Some("QuotesResponse"));
    let decoded: QuotesResponse =
        rmp_serde::from_slice(envelope.payload.as_ref().unwrap()).expect("decode quotes payload");
    assert_eq!(decoded.correlation_id, response.correlation_id);
    assert_eq!(decoded.client_id, response.client_id);
    assert_eq!(decoded.instrument_ids, response.instrument_ids);
    assert!(decoded.data.is_empty());

    drop(backend);
    msgbus::clear_bus_tap();
    drop(adapter);
    let writer = Arc::try_unwrap(writer).expect("sole writer reference");
    let _ = writer.close(run_ended_draft()).expect("close writer");
}

#[rstest]
fn order_only_historical_fill_preserves_captured_position_projection(
    #[values(0, 1, 2)] anchor_stage: u8,
) {
    let _bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None)
        .register_message_bus();
    let (writer, backend_arc) = writer_with_open_run("order-only-fill", noop_halt());
    let submitted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let adapter = Arc::new(
        BusCaptureAdapter::new(
            Arc::clone(&writer),
            Arc::new(default_registry()),
            noop_halt(),
        )
        .with_submit_counter(Arc::clone(&submitted)),
    );
    msgbus::set_bus_tap(Rc::new(AdapterTap {
        adapter: Arc::clone(&adapter),
    }));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::clone(&cache),
        None,
    );
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    let account_id = AccountId::from("SIM-001");
    let account = CashAccount::new(
        AccountState::new(
            account_id,
            AccountType::Cash,
            vec![AccountBalance::new(
                Money::from("1000000 USD"),
                Money::from("0 USD"),
                Money::from("1000000 USD"),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(Currency::USD()),
        ),
        true,
        false,
    );
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    cache.borrow_mut().add_account(account.into()).unwrap();
    engine.register_oms_type(StrategyId::external(), OmsType::Netting);
    let order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::external())
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("O-SNAPSHOT"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100_000))
        .tags(vec![Ustr::from(RECONCILIATION_ORDER_TAG)])
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, true)
        .unwrap();
    msgbus::publish_any(
        msgbus::switchboard::get_event_order_topic(order.strategy_id()),
        order.events()[0],
    );
    engine.process(&TestOrderEventStubs::submitted(&order, account_id));
    engine.process(&TestOrderEventStubs::accepted(
        &order,
        account_id,
        VenueOrderId::from("V-SNAPSHOT"),
    ));
    let opening = OrderFilledSpec::builder()
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(instrument.id())
        .client_order_id(order.client_order_id())
        .venue_order_id(VenueOrderId::from("V-SNAPSHOT"))
        .account_id(account_id)
        .trade_id(TradeId::from("T-SNAPSHOT"))
        .order_side(OrderSide::Buy)
        .order_type(OrderType::Market)
        .last_qty(Quantity::from(100_000))
        .last_px(Price::from("1.00000"))
        .currency(Currency::USD())
        .commission(Money::from("0 USD"))
        .liquidity_side(LiquiditySide::Taker)
        .ts_event(UnixNanos::from(9_000_000))
        .build();
    engine.process(&OrderEventAny::Filled(opening));
    let before_orders = cache
        .borrow()
        .orders(None, None, None, None, None)
        .into_iter()
        .map(|order| order.clone())
        .collect::<Vec<_>>();
    let before_position = cache.borrow().positions(None, None, None, None, None)[0].clone();
    assert_eq!(before_position.quantity, Quantity::from(100_000));
    let before_seq = submitted.load(std::sync::atomic::Ordering::Relaxed);
    let history = FillReport::new(
        account_id,
        instrument.id(),
        VenueOrderId::from("V-HISTORY"),
        TradeId::from("T-HISTORY"),
        OrderSide::Buy,
        Quantity::from(100_000),
        Price::from("1.00000"),
        Money::from("0.012345678 USD"),
        LiquiditySide::Taker,
        None,
        None,
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
    );
    let report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        None,
        history.venue_order_id,
        Some(OrderSide::Buy),
        OrderType::Limit,
        TimeInForce::Gtc,
        OrderStatus::Filled,
        history.last_qty,
        history.last_qty,
        history.ts_event,
        history.ts_event,
        history.ts_event,
        None,
    )
    .with_price(history.last_px);
    engine.reconcile_execution_report(&ExecutionReport::OrderQueryWithFills(
        Box::new(report),
        vec![history],
    ));
    let final_orders = cache
        .borrow()
        .orders(None, None, None, None, None)
        .into_iter()
        .map(|order| order.clone())
        .collect::<Vec<_>>();
    assert_eq!(final_orders.len(), 2);
    let final_position = cache.borrow().positions(None, None, None, None, None)[0].clone();
    assert_eq!(final_position, before_position);
    let historical_id = *cache
        .borrow()
        .client_order_id(&VenueOrderId::from("V-HISTORY"))
        .unwrap();
    assert_eq!(
        cache.borrow().order(&historical_id).unwrap().filled_qty(),
        Quantity::from(100_000)
    );
    assert!(
        !final_position
            .trade_ids
            .contains(&TradeId::from("T-HISTORY"))
    );
    let after_seq = submitted.load(std::sync::atomic::Ordering::Relaxed);
    drain(&writer, after_seq);

    if anchor_stage > 0 {
        backend_arc
            .lock()
            .record_snapshot_anchor(SnapshotAnchor::new(
                if anchor_stage == 1 {
                    before_seq
                } else {
                    after_seq
                },
                "cache://fixture",
                "fixture",
            ))
            .unwrap();
    }
    msgbus::clear_bus_tap();
    drop(adapter);
    Arc::try_unwrap(writer)
        .unwrap()
        .close(run_ended_draft())
        .unwrap();
    let backend = Arc::try_unwrap(backend_arc).unwrap().into_inner();
    let entries = backend
        .scan_range(1, after_seq, ScanDirection::Forward)
        .unwrap();
    let marker = entries
        .iter()
        .find(|entry| {
            entry
                .topic
                .as_ref()
                .starts_with("events.order_fill_order_only.")
        })
        .unwrap();
    let marked: OrderFilled = rmp_serde::from_slice(&marker.payload).unwrap();
    assert_eq!(marked.trade_id, TradeId::from("T-HISTORY"));
    assert_eq!(marked.commission, Some(Money::from("0.012345678 USD")));
    let committed = entries
        .iter()
        .find(|entry| {
            entry.topic.as_ref() == "events.order.EXTERNAL"
                && entry.payload_type.as_str() == "OrderFilled"
                && entry.seq > marker.seq
        })
        .unwrap();
    assert_eq!(
        rmp_serde::from_slice::<OrderFilled>(&committed.payload).unwrap(),
        marked
    );
    let mut restored = Cache::default();
    restored.add_instrument(instrument).unwrap();
    let reader = EventStoreReader::new(backend);
    restore_cache_snapshot_and_replay_tail(&mut restored, &reader, |cache, anchor| {
        if anchor.is_some() {
            let orders = if anchor_stage == 1 {
                &before_orders
            } else {
                &final_orders
            };

            for order in orders {
                cache
                    .add_order(order.clone(), order.position_id(), None, false)
                    .unwrap();
            }
            cache
                .add_position(&before_position, OmsType::Netting)
                .unwrap();
        }
        Ok(())
    })
    .unwrap();
    let positions = restored.positions(None, None, None, None, None);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, final_position.quantity);
    assert_eq!(positions[0].commissions(), final_position.commissions());
    assert_eq!(positions[0].realized_pnl, final_position.realized_pnl);
    assert_eq!(positions[0].trade_ids, final_position.trade_ids);
    assert_eq!(
        restored.order(&historical_id).unwrap().events(),
        cache.borrow().order(&historical_id).unwrap().events()
    );
}

#[rstest]
#[case("0.006", "0.0054", "0.0006")]
#[case("-0.006", "-0.0054", "-0.0006")]
fn position_flip_preserves_exact_fees_through_engine_capture_and_replay(
    #[case] total: &str,
    #[case] closing_fee: &str,
    #[case] opening_fee: &str,
    #[values(false, true)] snapshot_before_flip: bool,
) {
    let exact =
        |value: &str| Money::from_decimal_exact(value.parse().unwrap(), Currency::USD()).unwrap();
    let (total, closing_fee, opening_fee) = (exact(total), exact(closing_fee), exact(opening_fee));
    let _bus = MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None)
        .register_message_bus();
    let (writer, backend_arc) = writer_with_open_run("exact-flip-fees", noop_halt());
    let submitted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let adapter = Arc::new(
        BusCaptureAdapter::new(
            Arc::clone(&writer),
            Arc::new(default_registry()),
            noop_halt(),
        )
        .with_submit_counter(Arc::clone(&submitted)),
    );
    msgbus::set_bus_tap(Rc::new(AdapterTap {
        adapter: Arc::clone(&adapter),
    }));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::clone(&cache),
        None,
    );
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    cache
        .borrow_mut()
        .add_account(CashAccount::default().into())
        .unwrap();
    let strategy_id = StrategyId::external();
    engine.register_oms_type(strategy_id, OmsType::Netting);
    let account_id = AccountId::from("SIM-001");
    let position_id = PositionId::new(format!("{}-{strategy_id}", instrument.id()));
    let mut snapshot_orders = Vec::new();
    let mut snapshot_position = None;
    let mut anchor_seq = 0;

    for (id, side, quantity, commission) in [
        ("ENTRY", OrderSide::Sell, 180_000, Money::from("0 USD")),
        ("FLIP", OrderSide::Buy, 200_000, total),
    ] {
        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("TRADER-001"))
            .strategy_id(strategy_id)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::new(id))
            .side(side)
            .quantity(Quantity::from(quantity))
            .tags(vec![Ustr::from(RECONCILIATION_ORDER_TAG)])
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, true)
            .unwrap();
        msgbus::publish_any(
            msgbus::switchboard::get_event_order_topic(strategy_id),
            order.events()[0],
        );
        engine.process(&TestOrderEventStubs::submitted(&order, account_id));
        engine.process(&TestOrderEventStubs::accepted(
            &order,
            account_id,
            VenueOrderId::new(id),
        ));
        let fill = OrderFilledSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(strategy_id)
            .instrument_id(instrument.id())
            .client_order_id(order.client_order_id())
            .venue_order_id(VenueOrderId::new(id))
            .account_id(account_id)
            .trade_id(TradeId::new(id))
            .order_side(side)
            .order_type(OrderType::Market)
            .last_qty(order.quantity())
            .last_px(Price::from("1.00000"))
            .currency(Currency::USD())
            .commission(commission)
            .liquidity_side(LiquiditySide::Taker)
            .ts_event(UnixNanos::from(if id == "ENTRY" { 1 } else { 2 }))
            .build();
        engine.process(&OrderEventAny::Filled(fill.clone()));
        let before = cache.borrow().position(&position_id).unwrap().clone();
        engine.process(&OrderEventAny::Filled(fill));
        assert_eq!(cache.borrow().position(&position_id).unwrap(), &before);
        if id == "ENTRY" {
            snapshot_orders = cache
                .borrow()
                .orders(None, None, None, None, None)
                .into_iter()
                .map(|order| order.clone())
                .collect();
            snapshot_position = Some(before);
            anchor_seq = submitted.load(std::sync::atomic::Ordering::Relaxed);
        }
    }
    let position = cache.borrow().position(&position_id).unwrap().clone();
    let closed = cache.borrow().position_snapshots(Some(&position_id), None);
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].commissions(), vec![closing_fee]);
    assert_eq!(closed[0].realized_pnl, Some(-closing_fee));
    assert_eq!(position.commissions(), vec![opening_fee]);
    assert_eq!(position.realized_pnl, Some(-opening_fee));
    assert_eq!(position.quantity, Quantity::from(20_000));
    assert_eq!(position.side, PositionSide::Long);
    let last_seq = submitted.load(std::sync::atomic::Ordering::Relaxed);
    drain(&writer, last_seq);

    if snapshot_before_flip {
        backend_arc
            .lock()
            .record_snapshot_anchor(SnapshotAnchor::new(anchor_seq, "cache://flip", "fixture"))
            .unwrap();
    }
    msgbus::clear_bus_tap();
    drop(adapter);
    Arc::try_unwrap(writer)
        .unwrap()
        .close(run_ended_draft())
        .unwrap();
    let reader = EventStoreReader::new(Arc::try_unwrap(backend_arc).unwrap().into_inner());
    let mut restored = Cache::default();
    restored.add_instrument(instrument).unwrap();
    restore_cache_snapshot_and_replay_tail(&mut restored, &reader, |cache, anchor| {
        assert_eq!(anchor.is_some(), snapshot_before_flip);
        if snapshot_before_flip {
            for order in &snapshot_orders {
                cache
                    .add_order(order.clone(), order.position_id(), None, false)
                    .unwrap();
            }
            cache
                .add_position(snapshot_position.as_ref().unwrap(), OmsType::Netting)
                .unwrap();
        }
        Ok(())
    })
    .unwrap();
    let restored_position = restored.position(&position_id).unwrap();
    assert_eq!(restored_position.commissions(), position.commissions());
    assert_eq!(restored_position.realized_pnl, position.realized_pnl);
    assert_eq!(restored_position.quantity, position.quantity);
    let restored_closed = restored.position_snapshots(Some(&position_id), None);
    assert_eq!(restored_closed.len(), 1);
    assert_eq!(restored_closed[0].commissions(), closed[0].commissions());
    assert_eq!(restored_closed[0].realized_pnl, closed[0].realized_pnl);
    assert_eq!(
        restored
            .order(&ClientOrderId::from("FLIP"))
            .unwrap()
            .events(),
        cache
            .borrow()
            .order(&ClientOrderId::from("FLIP"))
            .unwrap()
            .events()
    );
}
