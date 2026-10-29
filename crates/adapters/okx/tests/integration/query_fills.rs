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
//! Exercises order queries and stream fills through the native LiveNode and OKX transport.
//! Venue responses use loopback fixtures; explicit legacy-state tests inject native reports.
//! Bounded REST recovery forwards native fill reports through the node's report ingress.

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, registry::try_get_actor_unchecked},
    cache::{Cache, CacheView},
    clients::ExecutionClient,
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, ExecutionClientFactory},
    live::runner::get_exec_event_sender,
    messages::{ExecutionEvent, ExecutionReport, execution::IncompleteFillReport},
    msgbus::{self, MessagingSwitchboard, ShareableMessageHandler},
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_event_store::{
    EventStore, EventStoreConfig, EventStoreLifecycle, RedbBackend, RetentionMode, ScanDirection,
    restore_cache_from_sealed_run,
};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveExecutionEngineConfig, LiveNodeConfig},
    execution::submission::SubmissionRecoveryPolicy,
    node::LiveNode,
    runner::AsyncRunner,
};
use nautilus_model::{
    accounts::{AccountAny, CashAccount},
    data::QuoteTick,
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderEventAny, OrderFilled, order::spec::OrderFilledSpec},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        TraderId, Venue as NativeVenue, VenueOrderId,
    },
    instruments::Instrument,
    orders::{Order, OrderAny, builder::OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};
use nautilus_okx::{
    common::{
        enums::{OKXEnvironment, OKXInstrumentType, OKXRegion},
        models::OKXInstrument,
        parse::{parse_account_state, parse_instrument_any},
    },
    config::OKXExecutionClientConfig,
    factories::OKXExecutionClientFactory,
    http::{
        client::{OKXHttpClient, OKXResponse},
        models::OKXAccount,
    },
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rstest::rstest;
use serde_json::{Value, json};

const CID: &str = "QueryFills1";
const VID: &str = "2497956918703120386";
const INSTRUMENT: &str = "BTC-USD.OKX";
const STRATEGY: &str = "QUERY-001";
const DEADLINE: Duration = Duration::from_secs(35);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuerySource {
    Direct,
    Bulk,
    Targeted,
}

#[derive(Debug)]
struct ProbeStrategy {
    core: StrategyCore,
    market: bool,
    side: OrderSide,
    events: Rc<RefCell<Vec<OrderEventAny>>>,
}

impl DataActor for ProbeStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.cache().order_exists(&ClientOrderId::from(CID)) {
            return Ok(());
        }

        if self.market {
            let order = self.order().market(
                InstrumentId::from(INSTRUMENT),
                self.side,
                Quantity::from("0.00100000"),
                Some(TimeInForce::Gtc),
                None,
                None,
                None,
                None,
                None,
                Some(ClientOrderId::from(CID)),
            );
            return self.submit_order(order, None, Some(ClientId::from("OKX")), None);
        }

        let order = self.order().limit(
            InstrumentId::from(INSTRUMENT),
            self.side,
            Quantity::from("0.00100000"),
            Price::from("50000.0"),
            Some(TimeInForce::Gtc),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(ClientOrderId::from(CID)),
        );
        self.submit_order(order, None, Some(ClientId::from("OKX")), None)
    }
}

nautilus_strategy!(ProbeStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.events.borrow_mut().push(event);
    }
});

#[derive(Debug)]
struct Venue {
    requests: tokio::sync::Mutex<Vec<String>>,
    bulk: std::sync::atomic::AtomicBool,
    submissions: tokio::sync::Mutex<Vec<Value>>,
    omit_submission_ack: std::sync::atomic::AtomicBool,
    queries: tokio::sync::Mutex<Vec<HashMap<String, String>>>,
    instruments: tokio::sync::Mutex<Value>,
    balance: tokio::sync::Mutex<Value>,
    report: tokio::sync::Mutex<Value>,
    order_details: tokio::sync::Mutex<HashMap<String, Value>>,
    fills: tokio::sync::Mutex<Value>,
    historical_fills: tokio::sync::Mutex<Value>,
    next_fill_page: tokio::sync::Mutex<Option<Value>>,
    scoped_fills: tokio::sync::Mutex<Option<Value>>,
    order_fills: tokio::sync::Mutex<HashMap<String, Value>>,
    stalled_request: tokio::sync::Mutex<Option<String>>,
    order_response_gate: tokio::sync::Notify,
    fill_queries: tokio::sync::Mutex<Vec<HashMap<String, String>>>,
    updates: tokio::sync::broadcast::Sender<Value>,
}

fn instruments() -> Value {
    let mut v: Value = serde_json::from_str(include_str!(
        "../../test_data/http_get_instruments_spot.json"
    ))
    .unwrap();
    v["data"].as_array_mut().unwrap().truncate(1);
    v["data"][0]["instIdCode"] = json!(42);
    v
}

fn balance() -> Value {
    let mut v: Value = serde_json::from_str(include_str!(
        "../../test_data/http_get_account_balance.json"
    ))
    .unwrap();
    v["data"][0]["details"][0]["ccy"] = json!("USD");
    v
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<Arc<Venue>>) -> Response {
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(mut socket: WebSocket, state: Arc<Venue>) {
    let mut updates = state.updates.subscribe();
    let mut subscriptions = HashSet::new();

    loop {
        let text = tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(text))) => text,
                Some(Ok(Message::Ping(data))) => {
                    if socket.send(Message::Pong(data)).await.is_err() { break; }
                    continue;
                },
                _ => break,
            },
            update = updates.recv(), if !subscriptions.is_empty() => {
                let update = update.unwrap();
                if update["arg"]["channel"].as_str().is_some_and(|channel| subscriptions.contains(channel))
                    && socket.send(Message::Text(update.to_string().into())).await.is_err() { break; }
                continue;
            },
        };

        if text == "ping" {
            if socket.send(Message::Text("pong".into())).await.is_err() {
                break;
            }
            continue;
        }
        let request: Value = serde_json::from_str(&text).unwrap();
        let response = match request["op"].as_str().unwrap() {
            "login" => json!({"event":"login", "code":"0", "msg":"", "connId":"local"}),
            "subscribe" | "unsubscribe" => {
                for arg in request["args"].as_array().unwrap() {
                    if request["op"] == "subscribe" {
                        subscriptions.insert(arg["channel"].as_str().unwrap().to_string());
                    } else {
                        subscriptions.remove(arg["channel"].as_str().unwrap());
                    }
                    socket
                        .send(Message::Text(
                            json!({"event":request["op"],"arg":arg,"connId":"local"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
                continue;
            }
            "cancel-order" => {
                json!({"id":request["id"],"op":"cancel-order","code":"0","msg":"","data":[{"clOrdId":CID,"ordId":VID,"sCode":"0","sMsg":""}]})
            }
            "order" => {
                let mut submissions = state.submissions.lock().await;
                submissions.push(request.clone());
                let venue_id = if request["args"][0]["clOrdId"] == CID {
                    VID.to_string()
                } else {
                    (VID.parse::<u64>().unwrap() + submissions.len() as u64).to_string()
                };
                drop(submissions);

                if state
                    .omit_submission_ack
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    continue;
                }
                json!({"id":request["id"],"op":"order","code":"0","msg":"","data":[{"clOrdId":request["args"][0]["clOrdId"],"ordId":venue_id,"tag":"","sCode":"0","sMsg":""}]})
            }
            _ => panic!("unexpected fixture command: {request}"),
        };

        if socket
            .send(Message::Text(response.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn start_venue() -> (SocketAddr, Arc<Venue>, tokio::task::JoinHandle<()>) {
    let (updates, _) = tokio::sync::broadcast::channel(32);
    let state = Arc::new(Venue {
        requests: Default::default(),
        bulk: Default::default(),
        submissions: Default::default(),
        omit_submission_ack: Default::default(),
        queries: Default::default(),
        instruments: tokio::sync::Mutex::new(instruments()),
        balance: tokio::sync::Mutex::new(balance()),
        report: tokio::sync::Mutex::new(json!({"code":"0","msg":"","data":[]})),
        order_details: Default::default(),
        fills: tokio::sync::Mutex::new(json!({"code":"0","msg":"","data":[]})),
        historical_fills: tokio::sync::Mutex::new(json!({"code":"0","msg":"","data":[]})),
        next_fill_page: Default::default(),
        scoped_fills: Default::default(),
        order_fills: Default::default(),
        stalled_request: Default::default(),
        order_response_gate: Default::default(),
        fill_queries: Default::default(),
        updates,
    });
    let mut router = Router::new()
        .route("/ws/private", get(websocket))
        .route("/ws/business", get(websocket))
        .route(
            "/api/v5/public/instruments",
            get(|State(s): State<Arc<Venue>>| async move { Json(s.instruments.lock().await.clone()) }),
        )
        .route(
            "/api/v5/account/instruments",
            get(|State(s): State<Arc<Venue>>| async move { Json(s.instruments.lock().await.clone()) }),
        )
        .route(
            "/api/v5/account/balance",
            get(|State(s): State<Arc<Venue>>| async move { Json(s.balance.lock().await.clone()) }),
        )
        .route(
            "/api/v5/trade/order",
            get(
                |State(s): State<Arc<Venue>>, Query(q): Query<HashMap<String, String>>| async move {
                    let response = if let Some(id) = q.get("ordId") {
                        s.order_details.lock().await.get(id).cloned()
                    } else {
                        None
                    };
                    let response = match response {
                        Some(response) => response,
                        None => s.report.lock().await.clone(),
                    };
                    let stalled = s.stalled_request.lock().await.as_deref() == Some("order_status");
                    s.queries.lock().await.push(q);
                    if stalled {
                        s.order_response_gate.notified().await;
                    }
                    Json(response)
                },
            ),
        );
    router = router.route(
        "/api/v5/trade/fills",
        get(
            |State(s): State<Arc<Venue>>, Query(q): Query<HashMap<String, String>>| async move {
                s.requests
                    .lock()
                    .await
                    .push("/api/v5/trade/fills".to_string());
                s.fill_queries.lock().await.push(q.clone());
                let stalled = s.stalled_request.lock().await.clone();
                if (stalled.as_deref() == Some("order_fills") && q.contains_key("ordId"))
                    || (stalled.as_deref() == Some("next_fill_page") && q.contains_key("after"))
                {
                    std::future::pending::<()>().await;
                }
                let mut response = if let Some(order_id) = q.get("ordId") {
                    s.order_fills.lock().await.get(order_id).cloned().or(s
                        .scoped_fills
                        .lock()
                        .await
                        .clone())
                } else {
                    None
                }
                .unwrap_or(s.fills.lock().await.clone());
                if let Some(after) = q.get("after") {
                    if let Some(next) = s.next_fill_page.lock().await.clone() {
                        response = next;
                    } else if let Some(rows) = response["data"].as_array_mut() {
                        let index = rows.iter().position(|row| row["billId"] == *after).unwrap();
                        rows.drain(..=index);
                    }
                }

                if let Some(rows) = response["data"].as_array_mut() {
                    rows.truncate(100);
                }
                Json(response)
            },
        ),
    );
    // Explicit empty startup sources, never a wildcard-success fallback.
    for path in [
        "/api/v5/trade/orders-pending",
        "/api/v5/trade/orders-history",
        "/api/v5/trade/orders-history-archive",
        "/api/v5/trade/orders-algo-pending",
        "/api/v5/trade/orders-algo-history",
        "/api/v5/trade/fills-history",
        "/api/v5/account/positions",
    ] {
        router = router.route(
            path,
            get(move |State(s): State<Arc<Venue>>, Query(q): Query<HashMap<String, String>>| async move {
                if path == "/api/v5/trade/fills-history" {
                    let response = s.historical_fills.lock().await.clone();
                    s.requests.lock().await.push(path.to_string());
                    return Json(response);
                }
                s.requests.lock().await.push(path.to_string());
                let stalled = s.stalled_request.lock().await.clone();
                if stalled.as_deref() == Some(path)
                    || (stalled.as_deref() == Some("later_algo_type")
                        && q.get("ordType").is_some_and(|kind| kind == "move_order_stop"))
                {
                    std::future::pending::<()>().await;
                }

                if stalled.as_deref() == Some("later_algo_type")
                    && path == "/api/v5/trade/orders-algo-history"
                    && q.get("ordType").is_some_and(|kind| kind == "trigger")
                    && q.get("state").is_some_and(|state| state == "effective")
                {
                    return Json(s.report.lock().await.clone());
                }

                if s.bulk.load(std::sync::atomic::Ordering::Relaxed)
                    && matches!(
                        path,
                        "/api/v5/trade/orders-pending" | "/api/v5/trade/orders-history"
                    )
                {
                    let report = s.report.lock().await;
                    let data: Vec<_> = report["data"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|row| {
                            let open =
                                matches!(row["state"].as_str(), Some("live" | "partially_filled"));
                            open == (path == "/api/v5/trade/orders-pending")
                        })
                        .cloned()
                        .collect();
                    return Json(json!({"code":"0","msg":"","data":data}));
                }

                Json(json!({"code":"0","msg":"","data":[]}))
            }),
        );
    }
    let router = router.with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, state, task)
}

fn build_node(
    addr: SocketAddr,
    events: Rc<RefCell<Vec<OrderEventAny>>>,
    source: QuerySource,
) -> LiveNode {
    build_node_with_store(addr, events, source, None, None)
}

fn build_node_with_store(
    addr: SocketAddr,
    events: Rc<RefCell<Vec<OrderEventAny>>>,
    source: QuerySource,
    store: Option<EventStoreConfig>,
    shutdown_evidence: Option<Rc<RefCell<Option<ExecutionReport>>>>,
) -> LiveNode {
    assert!(addr.ip().is_loopback());
    let exec_engine = LiveExecutionEngineConfig {
        submission_recovery_policy: SubmissionRecoveryPolicy::RetainUnresolved,
        open_check_interval_secs: (source != QuerySource::Direct).then_some(1.0),
        open_check_open_only: source == QuerySource::Direct,
        ..Default::default()
    };
    assert!(exec_engine.reconciliation);
    assert!(!exec_engine.allow_overfills);
    assert_eq!(exec_engine.inflight_check_interval_ms, 2000);
    assert_eq!(exec_engine.inflight_check_threshold_ms, 5000);
    assert_eq!(exec_engine.inflight_check_retries, 5);
    let mut builder = LiveNodeBuilder::from_config(LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("TESTER-001"),
        timeout_reconciliation: Duration::from_secs(3),
        delay_post_stop: if shutdown_evidence.is_some() {
            Duration::ZERO
        } else {
            LiveNodeConfig::default().delay_post_stop
        },
        exec_engine,
        ..Default::default()
    })
    .unwrap()
    .with_name("QueryFillsLoopback")
    .add_exec_client(
        None,
        Box::new(OKXExecutionClientFactory),
        Box::new(OKXExecutionClientConfig {
            api_key: Some("dummy-key".into()),
            api_secret: Some("dummy-secret".into()),
            api_passphrase: Some("dummy-passphrase".into()),
            environment: OKXEnvironment::Demo,
            region: OKXRegion::Eea,
            instrument_types: vec![OKXInstrumentType::Spot],
            base_url_http: Some(format!("http://{addr}")),
            base_url_ws_private: Some(format!("ws://{addr}/ws/private")),
            base_url_ws_business: Some(format!("ws://{addr}/ws/business")),
            ..Default::default()
        }),
    )
    .unwrap();

    if let Some(config) = store {
        builder = builder.with_event_store(move |instance_id, clock| {
            Ok(Box::new(EventStoreLifecycle::boot(
                Some(config),
                instance_id,
                clock,
            )?))
        });
    }

    if let Some(pending) = shutdown_evidence {
        let client = ShutdownEvidenceClient {
            connected: false,
            pending,
        };
        builder = builder
            .add_exec_client(
                Some("SHUTDOWN".into()),
                Box::new(client.clone()),
                Box::new(client),
            )
            .unwrap();
    }

    let mut node = builder.build().unwrap();
    let response: OKXResponse<OKXInstrument> = serde_json::from_value(instruments()).unwrap();
    let instrument = parse_instrument_any(&response.data[0], None, None, Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(instrument.id(), InstrumentId::from(INSTRUMENT));
    node.kernel()
        .cache
        .borrow_mut()
        .add_instrument(instrument)
        .unwrap();
    node.add_strategy(ProbeStrategy {
        market: false,
        side: OrderSide::Buy,
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            use_hyphens_in_client_order_ids: false,
            ..Default::default()
        }),
        events,
    })
    .unwrap();
    node
}

fn wire_order(
    status: &str,
    cumulative: &str,
    last: &str,
    trade: &str,
    fee: &str,
    fill_fee: &str,
    submitted_ms: u64,
) -> Value {
    let mut v: Value =
        serde_json::from_str(include_str!("../../test_data/ws_orders_ioc.json")).unwrap();
    let o = &mut v["data"][0];

    for (key, value) in [
        ("clOrdId", CID),
        ("ordId", VID),
        ("instId", "BTC-USD"),
        ("instType", "SPOT"),
        ("side", "buy"),
        ("ordType", "limit"),
        ("px", "50000.0"),
        ("sz", "0.00100000"),
        ("ccy", "USD"),
        ("feeCcy", "USD"),
        ("rebateCcy", "USD"),
        ("state", status),
        ("accFillSz", cumulative),
        ("fillSz", last),
        ("tradeId", trade),
        ("fee", fee),
        ("fillFee", fill_fee),
        ("fillFeeCcy", "USD"),
        ("tdMode", "cash"),
    ] {
        o[key] = json!(value);
    }
    o["cTime"] = json!(submitted_ms.to_string());
    o["uTime"] = json!((submitted_ms + 1).to_string());
    o["fillTime"] = json!(if trade.is_empty() {
        "0".to_string()
    } else {
        (submitted_ms + 1).to_string()
    });
    o["avgPx"] = json!(if status == "live" { "" } else { "50000.0" });
    o["fillPx"] = o["avgPx"].clone();
    v
}

fn cached(cache: &Rc<RefCell<Cache>>) -> OrderAny {
    cache
        .borrow()
        .order(&ClientOrderId::from(CID))
        .unwrap()
        .clone()
}

fn query(cache: &Rc<RefCell<Cache>>) {
    let order = cached(cache);
    let mut strategy =
        try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner()).unwrap();
    strategy
        .query_order(&order, Some(ClientId::from("OKX")), None)
        .unwrap();
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native evidence deadline expired");
}

async fn sentinel(venue: &Venue, cache: &Rc<RefCell<Cache>>, timestamp: u64) {
    let mut b = venue.balance.lock().await.clone();
    b["data"][0]["uTime"] = json!(timestamp.to_string());
    venue
        .updates
        .send(json!({"arg":{"channel":"account"},"data":b["data"]}))
        .unwrap();
    wait_until(|| {
        cache
            .borrow()
            .account(&AccountId::from("OKX-001"))
            .and_then(|a| a.last_event())
            .is_some_and(|e| e.ts_event.as_u64() == timestamp * 1_000_000)
    })
    .await;
}

fn snapshot(cache: &Rc<RefCell<Cache>>) -> Value {
    let order = cached(cache);
    let cache = cache.borrow();
    let positions = cache.positions(None, None, None, None, None);
    json!({
        "status":format!("{:?}",order.status()),"filled_qty":order.filled_qty().to_string(),
        "trade_ids":order.trade_ids().iter().map(ToString::to_string).collect::<Vec<_>>(),
        "fills":order.events().into_iter().filter_map(|e| match e {
            OrderEventAny::Filled(f)=>Some(json!({"trade_id":f.trade_id.to_string(),"qty":f.last_qty.to_string(),"commission":f.commission.map(|m|m.to_string()),"reconciliation":f.reconciliation})),_=>None,
        }).collect::<Vec<_>>(),
        "positions":positions.iter().map(|p|json!({"quantity":p.quantity.to_string(),"commissions":p.commissions().iter().map(ToString::to_string).collect::<Vec<_>>(),"realized_pnl":p.realized_pnl.map(|m|m.to_string())})).collect::<Vec<_>>(),
    })
}

async fn scenario(query_first: bool, partial: bool, skip_first_stream: bool, source: QuerySource) {
    let (addr, venue, server) = start_venue().await;
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node(addr, events.clone(), source);
    let reports = Rc::new(RefCell::new(Vec::new()));
    let captured_reports = reports.clone();
    let report_handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            captured_reports.borrow_mut().push(report.clone());
        }
    });
    let report_topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(report_topic.into(), report_handler.clone(), None);
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|o| {
                    o.status() == OrderStatus::Submitted || o.status() == OrderStatus::Accepted
                })
        })
        .await;
        let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let quantity = if partial { "0.00040000" } else { "0.00100000" };
        let status = if partial {
            "partially_filled"
        } else {
            "filled"
        };
        let fee = if partial { "-0.02" } else { "-0.05" };
        let frame = wire_order(status, quantity, quantity, "90001", fee, fee, submitted_ms);
        *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
        *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":quantity, "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":fee, "feeCcy":"USD",
            "ts":(submitted_ms+5).to_string(), "fillTime":(submitted_ms+1).to_string()
        }]});
        venue.bulk.store(
            source == QuerySource::Bulk,
            std::sync::atomic::Ordering::Relaxed,
        );

        if query_first {
            if source == QuerySource::Direct {
                query(&cache);
            }
            wait_until(|| cached(&cache).filled_qty() == Quantity::from(quantity)).await;
            let first = snapshot(&cache);
            assert!(first["fills"][0]["reconciliation"].as_bool().unwrap());
            assert_eq!(first["fills"][0]["trade_id"], "90001");
            assert_eq!(
                first["fills"][0]["commission"],
                if partial { "0.02 USD" } else { "0.05 USD" }
            );

            if !skip_first_stream {
                venue.updates.send(frame.clone()).unwrap();
            }
        } else {
            venue.updates.send(frame.clone()).unwrap();
            wait_until(|| cached(&cache).filled_qty() == Quantity::from(quantity)).await;
            query(&cache);
        }
        // The native report topic observes ingress without changing lifecycle.
        // Reconciliation is synchronous in the same dispatch, so by our next
        // poll both observation and reconciliation have completed.
        wait_until(|| {
            reports
                .borrow()
                .iter()
                .filter(|r| r.filled_qty == Quantity::from(quantity))
                .count()
                >= 2
        })
        .await;
        // A duplicate and a following account event share the private socket.
        if !skip_first_stream {
            venue.updates.send(frame).unwrap();
        }
        sentinel(&venue, &cache, submitted_ms + 10).await;
        if partial {
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
            venue
                .updates
                .send(wire_order(
                    "filled",
                    "0.00100000",
                    "0.00060000",
                    "90002",
                    "-0.05",
                    "-0.03",
                    submitted_ms + 20,
                ))
                .unwrap();
            sentinel(&venue, &cache, submitted_ms + 30).await;
        }
        let order = cached(&cache);
        assert_eq!(order.filled_qty(), Quantity::from("0.00100000"));
        let positions = cache.borrow();
        let positions = positions.positions(None, None, None, None, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, order.filled_qty());
        assert_eq!(positions[0].commissions(), vec![Money::from("0.05 USD")]);
        assert_eq!(positions[0].realized_pnl, Some(Money::from("-0.05 USD")));
        assert_eq!(order.trade_ids().len(), if partial { 2 } else { 1 });
        assert_eq!(order.status(), OrderStatus::Filled);
        let ids: Vec<_> = order.trade_ids().iter().map(ToString::to_string).collect();
        assert!(ids.iter().any(|id| id == "90001"));
        assert_eq!(ids.iter().any(|id| id == "90002"), partial);
        let callback_fills = events
            .borrow()
            .iter()
            .filter(|e| matches!(e, OrderEventAny::Filled(_)))
            .count();
        assert_eq!(callback_fills, order.trade_ids().len());
        drop(positions);
        // No venue cleanup claim: synthetic open positions intentionally remain
        // available for accounting inspection in this clientless-market fixture.
        let final_state = snapshot(&cache);
        handle.stop();
        final_state
    };
    let (result, final_state) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(snapshot(&cache), final_state);
    assert_eq!(
        reports.borrow().len(),
        2 + 3 * usize::from(!skip_first_stream) + 2 * usize::from(partial)
    );
    msgbus::unsubscribe_any(report_topic.into(), &report_handler);
    let submissions = venue.submissions.lock().await;
    assert_eq!(submissions.len(), 1);
    assert_eq!(submissions[0]["args"][0]["clOrdId"], CID);
    assert_eq!(submissions[0]["args"][0]["tdMode"], "cash");
    assert_eq!(submissions[0]["args"][0]["instIdCode"], 42);
    assert_eq!(submissions[0]["args"][0]["side"], "buy");
    assert_eq!(submissions[0]["args"][0]["ordType"], "limit");
    let queries = venue.queries.lock().await;
    assert_eq!(
        queries.len(),
        if source == QuerySource::Targeted {
            2
        } else {
            1
        }
    );
    assert_eq!(queries[0].get("instId").unwrap(), "BTC-USD");
    assert_eq!(queries[0].get("ordId").unwrap(), VID);
    let fill_queries = venue.fill_queries.lock().await;
    let query = fill_queries
        .iter()
        .find(|q| q.contains_key("ordId"))
        .unwrap();
    assert_eq!(query.get("instId").unwrap(), "BTC-USD");
    assert_eq!(query.get("ordId").unwrap(), VID);
    assert_eq!(query.get("instType").unwrap(), "SPOT");
    assert!(
        venue
            .requests
            .lock()
            .await
            .iter()
            .any(|p| p == "/api/v5/trade/orders-pending")
    );
    assert!(
        venue
            .requests
            .lock()
            .await
            .iter()
            .any(|p| p == "/api/v5/trade/fills")
    );
    server.abort();
}

#[tokio::test]
async fn test_order_query_preserves_authoritative_fills_and_fees() {
    for (query_first, partial) in [(true, false), (false, false), (true, true), (false, true)] {
        scenario(query_first, partial, false, QuerySource::Direct).await;
    }
    scenario(true, true, true, QuerySource::Direct).await;
}

#[rstest]
#[case::canceled_stream(false, true)]
#[case::expired_stream(true, true)]
#[case::canceled_query(false, false)]
#[case::expired_query(true, false)]
#[tokio::test]
async fn test_zero_fill_terminal_callbacks_preserve_wire_origin(
    #[case] expired: bool,
    #[case] stream: bool,
) {
    let (addr, venue, server) = start_venue().await;
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node(addr, events.clone(), QuerySource::Direct);
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            *captured.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|order| {
                    matches!(
                        order.status(),
                        OrderStatus::Submitted | OrderStatus::Accepted
                    )
                })
        })
        .await;
        let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;

        let mut frame = wire_order("canceled", "0", "0", "", "0", "0", submitted_ms);
        frame["data"][0]["avgPx"] = json!("");
        frame["data"][0]["fillPx"] = json!("");
        frame["data"][0]["cancelSource"] = json!(if expired { "5" } else { "" });
        *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
        let expected = if expired {
            OrderStatus::Expired
        } else {
            OrderStatus::Canceled
        };
        let terminal_callbacks = || {
            events
                .borrow()
                .iter()
                .filter_map(|event| match event {
                    OrderEventAny::Canceled(event) => {
                        Some((OrderStatus::Canceled, event.reconciliation))
                    }
                    OrderEventAny::Expired(event) => {
                        Some((OrderStatus::Expired, event.reconciliation))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        if stream {
            venue.updates.send(frame.clone()).unwrap();
        } else {
            query(&cache);
        }
        wait_until(|| cached(&cache).status() == expected && !terminal_callbacks().is_empty())
            .await;
        assert_eq!(terminal_callbacks(), vec![(expected, !stream)]);
        let before_duplicate = cached(&cache).events().len();
        let callbacks_before_duplicate = events.borrow().len();

        if stream {
            venue.updates.send(frame).unwrap();
            sentinel(&venue, &cache, submitted_ms + 100).await;
        } else {
            let reports_before = *observed.borrow();
            query(&cache);
            wait_until(|| *observed.borrow() > reports_before).await;
        }
        let reports_before = *observed.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() > reports_before).await;
        sentinel(&venue, &cache, submitted_ms + 200).await;

        assert_eq!(cached(&cache).status(), expected);
        assert_eq!(cached(&cache).events().len(), before_duplicate);
        assert_eq!(events.borrow().len(), callbacks_before_duplicate);
        assert_eq!(terminal_callbacks(), vec![(expected, !stream)]);
        assert!(cached(&cache).filled_qty().is_zero());
        assert!(cached(&cache).trade_ids().is_empty());
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert!(
            !events
                .borrow()
                .iter()
                .any(|event| matches!(event, OrderEventAny::Filled(_)))
        );
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(DEADLINE, async { tokio::join!(node.run(), driver) })
        .await
        .unwrap();
    result.unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[case("live", OrderStatus::Accepted)]
#[case("canceled", OrderStatus::Canceled)]
#[tokio::test]
async fn test_zero_fill_query_recovers_missing_acceptance(
    #[case] state: &str,
    #[case] expected: OrderStatus,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    venue
        .omit_submission_ack
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node_with_store(
        addr,
        events.clone(),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            *captured.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|order| order.status() == OrderStatus::Submitted)
        })
        .await;
        let order = cached(&cache);
        assert!(order.venue_order_id().is_none());
        let timestamp = order.ts_submitted().unwrap().as_u64() / 1_000_000;
        let mut frame = wire_order(state, "0", "0", "", "0", "0", timestamp);
        frame["data"][0]["avgPx"] = json!("");
        frame["data"][0]["fillPx"] = json!("");
        *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
        query(&cache);
        wait_until(|| cached(&cache).status() == expected).await;
        assert_eq!(
            cached(&cache).venue_order_id(),
            Some(VenueOrderId::from(VID))
        );
        assert_eq!(
            cache.borrow().client_order_id(&VenueOrderId::from(VID)),
            Some(&ClientOrderId::from(CID))
        );
        let before_duplicate = cached(&cache).events().len();
        let reports_before = *observed.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() > reports_before).await;
        assert_eq!(cached(&cache).events().len(), before_duplicate);
        assert!(cached(&cache).filled_qty().is_zero());
        assert!(cached(&cache).trade_ids().is_empty());
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| matches!(event, OrderEventAny::Accepted(_)))
                .count(),
            1
        );
        assert!(
            !events
                .borrow()
                .iter()
                .any(|event| matches!(event, OrderEventAny::Filled(_)))
        );
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(DEADLINE, async { tokio::join!(node.run(), driver) })
        .await
        .unwrap();
    // Native shutdown must confirm submission recovery, despite no submission acknowledgement.
    result.unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);
    assert_eq!(venue.submissions.lock().await.len(), 1);
    let queries = venue.queries.lock().await;
    assert_eq!(queries[0].get("clOrdId").unwrap(), CID);
    assert!(!queries[0].contains_key("ordId"));
    assert!(
        venue
            .fill_queries
            .lock()
            .await
            .iter()
            .all(|query| !query.contains_key("ordId"))
    );
    drop(queries);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let mut restored = restarted.kernel().cache.borrow_mut();
    restore_cache_from_sealed_run(
        &mut restored,
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap();
    let restored_order = restored.order(&ClientOrderId::from(CID)).unwrap();
    assert_eq!(restored_order.status(), expected);
    assert_eq!(
        restored_order.venue_order_id(),
        Some(VenueOrderId::from(VID))
    );
    assert!(restored_order.filled_qty().is_zero());
    assert!(restored_order.trade_ids().is_empty());
    assert!(restored.positions(None, None, None, None, None).is_empty());
    server.abort();
}

#[rstest]
#[case(QuerySource::Bulk, true)]
#[case(QuerySource::Targeted, false)]
#[tokio::test]
async fn test_periodic_queries_preserve_authoritative_fills(
    #[case] source: QuerySource,
    #[case] partial: bool,
) {
    scenario(true, partial, true, source).await;
}

#[rstest]
#[tokio::test]
async fn test_periodic_fill_obligation_persists_before_retry(
    #[values(QuerySource::Bulk, QuerySource::Targeted)] source: QuerySource,
    #[values(false, true)] partial: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    // The empty shutdown source selects zero grace without supplying any evidence.
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        source,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if *captured.borrow() > 0 {
            return;
        }

        if let ExecutionReport::IncompleteFill(observed) = report
            && observed.client_order_id == Some(ClientOrderId::from(CID))
        {
            assert!(!observed.requires_fresh_query);
            assert!(
                observed
                    .order_snapshot
                    .as_ref()
                    .unwrap()
                    .filled_qty
                    .is_positive()
            );
            *captured.borrow_mut() += 1;
            handle.stop();
        } else if let ExecutionReport::OrderQueryWithFills(report, fills) = report
            && report.client_order_id == Some(ClientOrderId::from(CID))
            && report.filled_qty.is_positive()
        {
            assert!(fills.is_empty());
            *captured.borrow_mut() += 1;
            handle.stop();
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let status = if partial {
            "partially_filled"
        } else {
            "filled"
        };
        let quantity = if partial { "0.00040000" } else { "0.00100000" };
        let frame = wire_order(
            status, quantity, quantity, "90001", "-0.05", "-0.05", timestamp,
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":frame["data"]});
        venue.bulk.store(
            source == QuerySource::Bulk,
            std::sync::atomic::Ordering::Relaxed,
        );
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    assert_eq!(*observed.borrow(), 1);
    assert!(cached(&cache).filled_qty().is_zero());
    assert!(cached(&cache).trade_ids().is_empty());
    // No order-scoped retry supplied evidence: only the initial targeted query is allowed.
    assert_eq!(
        venue.queries.lock().await.len(),
        usize::from(source == QuerySource::Targeted)
    );
    assert!(
        venue
            .fill_queries
            .lock()
            .await
            .iter()
            .all(|q| !q.contains_key("ordId"))
    );
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );
    assert!(
        restored
            .unwrap_err()
            .to_string()
            .contains("Unresolved persisted incomplete fill recovery")
    );
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_incomplete_query_prevents_successful_shutdown_until_recovered(
    #[values(QuerySource::Direct, QuerySource::Bulk, QuerySource::Targeted)] source: QuerySource,
) {
    for (recover, block_fills) in [(false, false), (true, false), (false, true)] {
        let (addr, venue, server) = start_venue().await;
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut node = build_node(addr, events.clone(), source);
        let observed = Rc::new(RefCell::new(0));
        let captured = observed.clone();
        let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
            if report.client_order_id == Some(ClientOrderId::from(CID))
                && !report.filled_qty.is_zero()
            {
                *captured.borrow_mut() += 1;
            }
        });
        let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
        msgbus::subscribe_any(topic.into(), handler.clone(), None);
        let cache = node.kernel().cache.clone();
        let handle = node.handle();
        let mut blocked_fills = None;
        let driver = async {
            wait_until(|| {
                cache
                    .borrow()
                    .order(&ClientOrderId::from(CID))
                    .is_some_and(|o| {
                        o.status() == OrderStatus::Submitted || o.status() == OrderStatus::Accepted
                    })
            })
            .await;
            let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
            venue
                .updates
                .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
                .unwrap();
            wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
            {
                let mut strategy =
                    try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner())
                        .unwrap();
                strategy
                    .cancel_order(ClientOrderId::from(CID), Some(ClientId::from("OKX")), None)
                    .unwrap();
            }
            wait_until(|| cached(&cache).status() == OrderStatus::PendingCancel).await;
            let filled = wire_order(
                "filled",
                "0.00100000",
                "0.00100000",
                "90001",
                "-0.05",
                "-0.05",
                submitted_ms,
            );
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":filled["data"]});
            venue.bulk.store(
                source == QuerySource::Bulk,
                std::sync::atomic::Ordering::Relaxed,
            );

            if block_fills {
                blocked_fills = Some(venue.fills.lock().await);
            }

            if source == QuerySource::Direct {
                query(&cache);
            }
            wait_until(|| *observed.borrow() > 0).await;
            assert_eq!(cached(&cache).status(), OrderStatus::PendingCancel);
            assert!(cached(&cache).filled_qty().is_zero());
            assert!(cached(&cache).trade_ids().is_empty());

            if recover {
                venue.updates.send(filled).unwrap();
                sentinel(&venue, &cache, submitted_ms + 20).await;
                assert_eq!(cached(&cache).status(), OrderStatus::Filled);
                assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00100000"));
                assert_eq!(
                    snapshot(&cache)["positions"][0]["commissions"],
                    json!(["0.05 USD"])
                );
                *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[{
                    "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                    "tradeId":"90001", "billId":"80001", "fillSz":"0.00100000", "fillPx":"50000.0",
                    "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
                    "ts":(submitted_ms+5).to_string(), "fillTime":(submitted_ms+1).to_string()
                }]});
                *observed.borrow_mut() = 0;
                query(&cache);
                wait_until(|| *observed.borrow() >= 2).await;
            }
            handle.stop();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
            tokio::join!(node.run(), driver)
        })
        .await
        .unwrap();
        drop(blocked_fills);

        if block_fills {
            assert!(!venue.fill_queries.lock().await.is_empty());
        }

        if recover {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Order fill recovery incomplete at shutdown")
            );
            assert_eq!(cached(&cache).status(), OrderStatus::PendingCancel);
            assert!(events.borrow().iter().all(|event| !matches!(
                event,
                OrderEventAny::Filled(_) | OrderEventAny::Canceled(_) | OrderEventAny::Rejected(_)
            )));
        }
        msgbus::unsubscribe_any(topic.into(), &handler);
        server.abort();
    }
}

#[rstest]
#[tokio::test]
async fn test_query_rejects_unproven_cached_executions(
    #[values(false, true)] synthetic: bool,
    #[values(false, true)] complete_history: bool,
) {
    let (addr, venue, server) = start_venue().await;
    let store = tempfile::TempDir::new().unwrap();
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instance = node.kernel().instance_id.to_string();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) && !report.filled_qty.is_zero()
        {
            *captured.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|o| {
                    o.status() == OrderStatus::Submitted || o.status() == OrderStatus::Accepted
                })
        })
        .await;
        let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        if synthetic {
            // Model a legacy cached native execution; current stream admission never
            // fabricates this ID. A later venue history must reject the mismatch.
            get_exec_event_sender()
                .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                    FillReport::new(
                        AccountId::from("OKX-001"),
                        InstrumentId::from(INSTRUMENT),
                        VenueOrderId::from(VID),
                        TradeId::from("synth-0123456789abcdef"),
                        OrderSide::Buy,
                        Quantity::from("0.00040000"),
                        Price::from("50000.0"),
                        Money::from("0.02 USD"),
                        LiquiditySide::Taker,
                        Some(ClientOrderId::from(CID)),
                        None,
                        UnixNanos::from((submitted_ms + 1) * 1_000_000),
                        UnixNanos::default(),
                        None,
                    ),
                ))))
                .unwrap();
            wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
        } else {
            venue
                .updates
                .send(wire_order(
                    "partially_filled",
                    "0.00040000",
                    "0.00040000",
                    "80000",
                    "-0.02",
                    "",
                    submitted_ms,
                ))
                .unwrap();
        }
        sentinel(&venue, &cache, submitted_ms + 10).await;
        assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
        assert_eq!(cached(&cache).trade_ids().len(), 1);
        assert_eq!(
            cached(&cache).trade_ids()[0].as_str().starts_with("synth-"),
            synthetic
        );
        let before = snapshot(&cache);
        let queried = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00040000",
            "90001",
            "-0.02",
            "-0.02",
            submitted_ms,
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":queried["data"]});
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[{
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(submitted_ms+5).to_string(), "fillTime":(submitted_ms+1).to_string()
        }]});

        if !complete_history {
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[]});
        }
        let expected = *observed.borrow() + if complete_history { 2 } else { 1 };
        query(&cache);
        wait_until(|| *observed.borrow() >= expected).await;
        assert_eq!(snapshot(&cache), before);
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    // The REST last-trade identity contradicts the committed execution even with empty history
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete at shutdown")
    );
    assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    assert!(
        restored
            .unwrap_err()
            .to_string()
            .contains("Unresolved persisted incomplete fill recovery")
    );
    server.abort();
}

#[rstest]
#[case::manual_query(None)]
#[case::missing_stream_fee_recovers(Some(true))]
#[case::missing_stream_fee_stays_pending(Some(false))]
#[tokio::test]
async fn test_bounded_rest_recovery_then_order_query_preserves_execution_times(
    #[case] stream_recovery: Option<bool>,
) {
    let (addr, venue, server) = start_venue().await;
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node(addr, events, QuerySource::Direct);
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = Box::pin(async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|o| {
                    o.status() == OrderStatus::Submitted || o.status() == OrderStatus::Accepted
                })
        })
        .await;
        let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let first = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(submitted_ms+5).to_string(), "fillTime":(submitted_ms+1).to_string()
        });
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[first.clone()]});
        let http = OKXHttpClient::with_credentials(
            Some("dummy-key".into()),
            Some("dummy-secret".into()),
            Some("dummy-passphrase".into()),
            Some(format!("http://{addr}")),
            60,
            0,
            1000,
            10_000,
            OKXEnvironment::Demo,
            None,
        )
        .unwrap();
        http.cache_instrument(
            cache
                .borrow()
                .instrument(&InstrumentId::from(INSTRUMENT))
                .unwrap()
                .clone(),
        );
        let boundary = Timestamp::from_millisecond((submitted_ms + 5) as i64).unwrap();
        let reports = http
            .request_fill_reports(
                cached(&cache).account_id().unwrap(),
                Some(OKXInstrumentType::Spot),
                Some(InstrumentId::from(INSTRUMENT)),
                Some(boundary),
                Some(boundary),
                None,
            )
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].ts_event.as_u64(), (submitted_ms + 1) * 1_000_000);
        assert!(reports[0].ts_event.as_u64() < (submitted_ms + 5) * 1_000_000);
        let request = venue.fill_queries.lock().await.last().unwrap().clone();
        assert_eq!(request.get("begin"), Some(&(submitted_ms + 5).to_string()));
        assert_eq!(request.get("end"), Some(&(submitted_ms + 5).to_string()));
        get_exec_event_sender()
            .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                reports.into_iter().next().unwrap(),
            ))))
            .unwrap();
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
        let second = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
            "ts":(submitted_ms+25).to_string(), "fillTime":(submitted_ms+21).to_string()
        });
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":

            if stream_recovery == Some(false) { vec![first] } else { vec![second, first] }
        });
        let mut final_frame = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            submitted_ms + 20,
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":final_frame["data"]});
        if stream_recovery.is_some() {
            final_frame["data"][0]["fillFee"] = json!("");
            final_frame["data"][0]["fillFeeCcy"] = json!("");
            venue.updates.send(final_frame.clone()).unwrap();
        } else {
            query(&cache);
        }

        if stream_recovery == Some(false) {
            tokio::time::timeout(DEADLINE, async {
                loop {
                    if venue
                        .fill_queries
                        .lock()
                        .await
                        .iter()
                        .any(|query| query.contains_key("ordId"))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            sentinel(&venue, &cache, submitted_ms + 30).await;
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.02 USD"])
            );
            handle.stop();
            return;
        }
        wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
        assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00100000"));
        assert_eq!(cached(&cache).trade_ids().len(), 2);
        let state = snapshot(&cache);
        assert_eq!(state["positions"][0]["commissions"], json!(["0.05 USD"]));
        venue
            .updates
            .send(wire_order(
                "partially_filled",
                "0.00040000",
                "0.00040000",
                "90001",
                "-0.02",
                "-0.02",
                submitted_ms,
            ))
            .unwrap();
        venue.updates.send(final_frame).unwrap();
        sentinel(&venue, &cache, submitted_ms + 30).await;
        assert_eq!(snapshot(&cache), state);
        handle.stop();
    });
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();

    if stream_recovery == Some(false) {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete at shutdown")
        );
    } else {
        result.unwrap();
    }
    server.abort();
}

#[cfg(feature = "high-precision")]
#[rstest]
#[case::complete(true, false, false)]
#[case::incomplete_after_restart(false, false, false)]
#[case::unresolved_before_restart(false, true, false)]
#[case::identityless_stream_cannot_resolve(false, true, true)]
#[tokio::test]
async fn test_persisted_restart_preserves_exact_fees_and_fill_authority(
    #[case] complete: bool,
    #[case] pending_before_stop: bool,
    #[case] identityless_stream: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node_with_store(
        addr,
        events,
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = Box::pin(async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let first = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00040000",
            "90001",
            "-0.01199569384",
            "-0.01199569384",
            timestamp,
        );
        *venue.report.lock().await = json!({"code":"0","msg":"","data":first["data"]});
        *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.01199569384", "feeCcy":"USD",
            "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
        }]});
        query(&cache);
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
        venue.updates.send(first.clone()).unwrap();
        sentinel(&venue, &cache, timestamp + 10).await;
        let state = snapshot(&cache);
        assert_eq!(state["fills"][0]["commission"], "0.01199569384 USD");
        assert_eq!(state["positions"][0]["realized_pnl"], "-0.01199569384 USD");

        if pending_before_stop {
            let missing = wire_order(
                "filled",
                "0.00100000",
                "0.00060000",
                "90002",
                "-0.02428441256",
                "-0.01228871872",
                timestamp + 20,
            );
            *venue.report.lock().await = json!({"code":"0","msg":"","data":missing["data"]});
            query(&cache);

            loop {
                if venue
                    .fill_queries
                    .lock()
                    .await
                    .iter()
                    .filter(|q| q.contains_key("ordId"))
                    .count()
                    >= 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }

            if identityless_stream {
                let mut update = missing.clone();
                for field in ["tradeId", "fillSz", "fillPx", "fillFee", "fillFeeCcy"] {
                    update["data"][0][field] = json!("");
                }
                venue.updates.send(update).unwrap();
            }
            sentinel(&venue, &cache, timestamp + 40).await;
            assert_eq!(snapshot(&cache), state);
        }
        handle.stop();
        (timestamp, first, state)
    });
    let (result, (timestamp, first, persisted)) =
        tokio::time::timeout(Duration::from_secs(70), async {
            tokio::join!(node.run(), driver)
        })
        .await
        .unwrap();

    if pending_before_stop {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);

    let manifests = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(manifests.len(), 1);
    let recovered_events = Rc::new(RefCell::new(Vec::new()));
    let mut restarted = build_node(addr, recovered_events.clone(), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &manifests[0].run_id,
    );

    if pending_before_stop {
        let error = restored.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unresolved persisted incomplete fill recovery"),
            "{error:#}"
        );
        assert_eq!(venue.submissions.lock().await.len(), 1);
        server.abort();
        return;
    }
    let restored = restored.unwrap();
    assert!(restored.cache.applied_entries > 0);
    assert_eq!(snapshot(&cache), persisted);
    let second = wire_order(
        "filled",
        "0.00100000",
        "0.00060000",
        "90002",
        "-0.02428441256",
        "-0.01228871872",
        timestamp + 20,
    );
    *venue.report.lock().await = json!({"code":"0","msg":"","data":second["data"]});
    venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);
    if complete {
        venue.fills.lock().await["data"].as_array_mut().unwrap().push(json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.01228871872", "feeCcy":"USD",
            "ts":(timestamp+25).to_string(), "fillTime":(timestamp+21).to_string()
        }));
    }
    let handle = restarted.handle();
    let driver = Box::pin(async {
        if complete {
            wait_until(|| handle.is_running()).await;
            query(&cache);
            wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00100000")).await;
            venue.updates.send(first).unwrap();
            venue.updates.send(second.clone()).unwrap();
            venue.updates.send(second).unwrap();
            sentinel(&venue, &cache, timestamp + 40).await;
            let state = snapshot(&cache);
            assert_eq!(state["trade_ids"], json!(["90001", "90002"]));
            assert_eq!(state["fills"][0]["commission"], "0.01199569384 USD");
            assert_eq!(state["fills"][1]["commission"], "0.01228871872 USD");
            assert_eq!(state["positions"][0]["quantity"], "0.00100000");
            assert_eq!(
                state["positions"][0]["commissions"],
                json!(["0.02428441256 USD"])
            );
            assert_eq!(state["positions"][0]["realized_pnl"], "-0.02428441256 USD");
        } else {
            // The query itself leaves native evidence pending; wait for native HTTP
            // receipt and an ordered private-stream sentinel before checking accounting.
            loop {
                if venue
                    .fill_queries
                    .lock()
                    .await
                    .iter()
                    .filter(|q| q.contains_key("ordId"))
                    .count()
                    >= 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                !handle.is_running(),
                "incomplete startup must not start strategies"
            );
            assert_eq!(snapshot(&cache), persisted);
            return;
        }
        handle.stop();
    });
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(restarted.run(), driver)
    })
    .await
    .unwrap();
    if complete {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Startup order fill recovery incomplete")
        );
    }
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[case::canceled(false, false)]
#[case::expired(true, false)]
#[case::mixed_canceled(false, true)]
#[case::mixed_expired(true, true)]
#[tokio::test]
async fn test_managed_exit_waits_for_canceled_fill_recovery(
    #[case] expired: bool,
    #[case] mixed_liquidity: bool,
    #[values(false, true)] recover: bool,
    #[values(false, true)] missing_status: bool,
) {
    let (addr, venue, server) = start_venue().await;
    {
        let mut balance = venue.balance.lock().await;
        let mut base = balance["data"][0]["details"][0].clone();
        base["ccy"] = json!("BTC");

        for field in ["availBal", "availEq", "cashBal", "eq"] {
            base[field] = json!("0.00100000");
        }
        base["frozenBal"] = json!("0");
        balance["data"][0]["details"]
            .as_array_mut()
            .unwrap()
            .push(base);
    }
    let mut node = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    node.kernel()
        .exec_engine
        .borrow_mut()
        .register_oms_type(StrategyId::from(STRATEGY), OmsType::Netting);
    let now = node.kernel().clock().borrow().timestamp_ns();
    node.kernel()
        .cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            InstrumentId::from(INSTRUMENT),
            Price::from("50000.0"),
            Price::from("50000.0"),
            Quantity::from("1.00000000"),
            Quantity::from("1.00000000"),
            now,
            now,
        ))
        .unwrap();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    {
        let mut strategy =
            try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner()).unwrap();
        strategy.side = OrderSide::Sell;
        strategy.core.config.manage_stop = true;
        strategy.core.config.market_exit_reduce_only = false;

        if missing_status {
            strategy.core.config.market_exit_max_attempts = 400;
        }
    }
    let driver = Box::pin(async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        let mut accepted = wire_order("live", "0", "0", "", "0", "0", timestamp);
        accepted["data"][0]["side"] = json!("sell");
        venue.updates.send(accepted).unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let instrument = cache
            .borrow()
            .instrument(&InstrumentId::from(INSTRUMENT))
            .unwrap()
            .clone();
        let position_id = PositionId::from("BTC-USD.OKX-QUERY-001");
        let position = Position::new(
            &instrument,
            OrderFilledSpec::builder()
                .trader_id(TraderId::from("TESTER-001"))
                .strategy_id(StrategyId::from(STRATEGY))
                .instrument_id(instrument.id())
                .client_order_id(ClientOrderId::from("OPENING"))
                .account_id(AccountId::from("OKX-001"))
                .position_id(position_id)
                .order_side(OrderSide::Buy)
                .last_qty(Quantity::from("0.00100000"))
                .last_px(Price::from("50000.0"))
                .currency(instrument.quote_currency())
                .commission(Money::zero(instrument.quote_currency()))
                .ts_event(UnixNanos::from((timestamp - 1) * 1_000_000))
                .build(),
        );
        cache
            .borrow_mut()
            .add_position(&position, OmsType::Netting)
            .unwrap();
        let mut canceled = wire_order(
            "canceled",
            "0.00040000",
            "0",
            "",
            "-0.02",
            "0",
            timestamp + 20,
        );
        canceled["data"][0]["side"] = json!("sell");
        canceled["data"][0]["cTime"] = json!(timestamp.to_string());
        canceled["data"][0]["fillPx"] = json!("0");
        canceled["data"][0]["rebate"] = json!("0");

        if expired {
            canceled["data"][0]["cancelSource"] = json!("5");
        }

        if !missing_status {
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":canceled["data"]});
            venue.updates.send(canceled.clone()).unwrap();
            sentinel(&venue, &cache, timestamp + 40).await;
        }
        assert!(!cached(&cache).is_closed());
        assert!(cached(&cache).filled_qty().is_zero());
        {
            let mut strategy =
                try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner())
                    .unwrap();
            assert!(!Strategy::stop(&mut *strategy));
            assert!(strategy.is_exiting());
        }
        // Let the real native managed-exit timer run while history is unavailable.
        tokio::time::sleep(if missing_status {
            Duration::from_secs(33)
        } else {
            Duration::from_millis(350)
        })
        .await;

        if missing_status {
            // Four queries precede the fifth check, which exhausts the native budget.
            assert_eq!(venue.queries.lock().await.len(), 4);
        }
        assert_eq!(venue.submissions.lock().await.len(), 1);
        assert!(!cached(&cache).is_closed());
        assert_eq!(
            cache.borrow().position(&position_id).unwrap().quantity,
            Quantity::from("0.00100000")
        );

        if recover {
            if missing_status {
                *venue.report.lock().await = json!({"code":"0", "msg":"", "data":canceled["data"]});
                venue.updates.send(canceled).unwrap();
            }

            let mut history = json!({"code":"0", "msg":"", "data":[{
                "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"sell", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+10).to_string(), "fillTime":(timestamp+5).to_string()
            }]});

            if mixed_liquidity {
                history["data"][0]["fillSz"] = json!("0.00020000");
                history["data"][0]["fee"] = json!("-0.01");
                let mut maker = history["data"][0].clone();
                maker["tradeId"] = json!("90002");
                maker["billId"] = json!("80002");
                maker["execType"] = json!("M");
                maker["fillTime"] = json!((timestamp + 6).to_string());
                history["data"].as_array_mut().unwrap().push(maker);
            }
            *venue.fills.lock().await = history;
            query(&cache);
            wait_until(|| cached(&cache).is_closed()).await;
            assert_eq!(
                cached(&cache).status(),
                if expired {
                    OrderStatus::Expired
                } else {
                    OrderStatus::Canceled
                }
            );
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
            assert_eq!(
                cached(&cache).trade_ids().len(),
                if mixed_liquidity { 2 } else { 1 }
            );
            tokio::time::timeout(DEADLINE, async {
                while venue.submissions.lock().await.len() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let submissions = venue.submissions.lock().await.clone();
            assert_eq!(submissions.len(), 2);
            let close = &submissions[1]["args"][0];
            assert_eq!(close["side"], "sell");
            assert_eq!(close["ordType"], "market");
            assert_eq!(
                close["sz"].as_str().unwrap().parse::<Quantity>().unwrap(),
                Quantity::from("0.00060000")
            );
            let close_id = ClientOrderId::from(close["clOrdId"].as_str().unwrap());
            wait_until(|| {
                cache
                    .borrow()
                    .order(&close_id)
                    .is_some_and(|o| o.ts_submitted().is_some())
            })
            .await;
            let close_order = cache.borrow().order(&close_id).unwrap().clone();
            let mut filled = wire_order(
                "filled",
                "0.00060000",
                "0.00060000",
                if mixed_liquidity { "90003" } else { "90002" },
                "-0.03",
                "-0.03",
                close_order.ts_submitted().unwrap().as_u64() / 1_000_000 + 1,
            );
            filled["data"][0]["side"] = json!("sell");
            filled["data"][0]["ordType"] = json!("market");
            filled["data"][0]["sz"] = json!("0.00060000");
            filled["data"][0]["clOrdId"] = json!(close_id.as_str());
            filled["data"][0]["ordId"] = json!((VID.parse::<u64>().unwrap() + 2).to_string());
            filled["data"][0]["px"] = json!("0");
            venue.updates.send(filled).unwrap();
            wait_until(|| cache.borrow().position(&position_id).unwrap().is_closed()).await;
            wait_until(|| {
                let strategy =
                    try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner())
                        .unwrap();
                !strategy.is_exiting()
            })
            .await;
            assert_eq!(venue.submissions.lock().await.len(), 2);
            assert_eq!(
                cache.borrow().positions(None, None, None, None, None).len(),
                1
            );
            assert_eq!(
                cache.borrow().position(&position_id).unwrap().commissions(),
                vec![Money::from("0.05 USD")]
            );
        }
        handle.stop();
    });
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();

    if recover {
        result.unwrap();
    } else {
        assert!(result.unwrap_err().to_string().contains(if missing_status {
            "Command recovery incomplete"
        } else {
            "Order fill recovery incomplete"
        }));
        assert!(!cached(&cache).is_closed());
        assert_eq!(venue.submissions.lock().await.len(), 1);
    }
    node.dispose();
    server.abort();
}

#[rstest]
#[case::full("filled", "0.00100000", "0.00060000")]
#[case::partial("partially_filled", "0.00080000", "0.00040000")]
#[case::short_final("filled", "0.00080000", "0.00040000")]
#[case::canceled("canceled", "0.00080000", "0.00040000")]
#[tokio::test]
async fn test_latest_stream_fill_retains_missed_execution_across_restart(
    #[case] status: &str,
    #[case] cumulative: &str,
    #[case] last: &str,
    #[values(false, true)] recover: bool,
    #[values(false, true)] same_time: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = Box::pin(async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let latest = wire_order(
            status,
            cumulative,
            last,
            "90002",
            "-0.05",
            "-0.03",
            timestamp + if same_time { 0 } else { 20 },
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":latest["data"]});
        let first = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
        });
        let second = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90002", "billId":"80002", "fillSz":last, "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
            "ts":(timestamp+25).to_string(), "fillTime":(timestamp+if same_time { 1 } else { 21 }).to_string()
        });
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":

            if recover { vec![second, first] } else { vec![second] }
        });
        venue.updates.send(latest.clone()).unwrap();

        tokio::time::timeout(DEADLINE, async {
            loop {
                if venue
                    .fill_queries
                    .lock()
                    .await
                    .iter()
                    .any(|q| q.contains_key("ordId"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        if recover {
            wait_until(|| cached(&cache).filled_qty() == Quantity::from(cumulative)).await;
            assert_eq!(cached(&cache).trade_ids().len(), 2);
        } else {
            assert!(cached(&cache).filled_qty().is_zero());
            assert!(cached(&cache).trade_ids().is_empty());
            assert!(!cached(&cache).is_closed());
        }
        let before = snapshot(&cache);
        venue.updates.send(latest).unwrap();
        sentinel(&venue, &cache, timestamp + 40).await;
        assert_eq!(snapshot(&cache), before);
        if recover {
            assert_eq!(before["positions"][0]["commissions"], json!(["0.05 USD"]));
        } else {
            assert!(before["positions"].as_array().unwrap().is_empty());
        }
        handle.stop();
        before
    });
    let (result, persisted) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    if recover {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if recover {
        restored.unwrap();
        assert_eq!(snapshot(&cache), persisted);
    } else {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted incomplete fill recovery")
        );
    }
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[case::query_before_stream(None, false, false)]
#[case::earlier_empty_constraint(None, true, false)]
#[case::partial_then_complete_query(None, false, true)]
#[case::invalid_partial_then_complete_query(None, true, true)]
#[case::conflicting_fee(Some(("fee", "-0.03")), false, false)]
#[case::conflicting_price(Some(("fillPx", "50001.0")), false, false)]
#[case::conflicting_identity(Some(("tradeId", "90009")), false, false)]
#[tokio::test]
async fn test_stream_progress_preserves_query_history_across_restart(
    #[case] conflict: Option<(&'static str, &'static str)>,
    #[case] earlier_empty: bool,
    #[case] complete_query: bool,
    #[values(false, true)] same_time: bool,
) {
    let expected_hold = conflict.is_some() || earlier_empty;
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let observed = Rc::new(RefCell::new(Vec::new()));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            captured.borrow_mut().push(report.filled_qty);
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let observed_fills = Rc::new(RefCell::new(Vec::new()));
    let captured_fills = observed_fills.clone();
    let fill_handler = ShareableMessageHandler::from_typed(move |fill: &FillReport| {
        captured_fills.borrow_mut().push(fill.clone());
    });
    let fill_topic = MessagingSwitchboard::reconciliation_fill_evidence_topic();
    msgbus::subscribe_any(fill_topic.into(), fill_handler.clone(), None);
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let first = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00040000",
            "90001",
            "-0.02",
            "-0.02",
            timestamp,
        );
        let second = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            timestamp + if same_time { 0 } else { 20 },
        );

        if earlier_empty || complete_query {
            let mut early = first.clone();

            if earlier_empty {
                early["data"][0]["uTime"] = json!(timestamp.to_string());
            }
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":early["data"]});
            query(&cache);
            wait_until(|| observed.borrow().contains(&Quantity::from("0.00040000"))).await;
            sentinel(&venue, &cache, timestamp + 10).await;
            assert!(cached(&cache).filled_qty().is_zero());
        }

        if let Some((field, value)) = conflict {
            venue.updates.send(first.clone()).unwrap();
            wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
            let mut history = json!({
                "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
            });
            history[field] = json!(value);
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":first["data"]});
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[history]});
            let before_query = observed.borrow().len();
            query(&cache);
            wait_until(|| observed.borrow().len() > before_query).await;
            // The detail report precedes the HTTP fill request. Keep this response
            // installed until native ingress has retained the contradictory fill.
            wait_until(|| {
                observed_fills.borrow().iter().any(|fill| {
                    fill.trade_id == TradeId::from(if field == "tradeId" { value } else { "90001" })
                        && fill.last_px
                            == Price::from(if field == "fillPx" { value } else { "50000.0" })
                        && fill.commission
                            == Money::from(if field == "fee" {
                                "0.03 USD"
                            } else {
                                "0.02 USD"
                            })
                })
            })
            .await;
            sentinel(&venue, &cache, timestamp + 10).await;
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.02 USD"])
            );
        }
        // Supply the later executions in one REST bundle or contiguous stream fragments.
        // An earlier observation must be satisfied at its own native committed prefix.
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":second["data"]});
        let history = if complete_query {
            vec![
                json!({
                    "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                    "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                    "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                    "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
                }),
                json!({
                    "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                    "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
                    "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
                    "ts":(timestamp+25).to_string(),
                    "fillTime":(timestamp + if same_time { 1 } else { 21 }).to_string()
                }),
            ]
        } else {
            Vec::new()
        };
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":history});
        query(&cache);
        wait_until(|| observed.borrow().contains(&Quantity::from("0.00100000"))).await;
        sentinel(&venue, &cache, timestamp + 30).await;

        if earlier_empty {
            if !complete_query {
                venue.updates.send(first).unwrap();
                venue.updates.send(second).unwrap();
                sentinel(&venue, &cache, timestamp + 40).await;
            }
            // The retained last execution is after its reported snapshot boundary;
            // neither query nor stream delivery may admit that contradictory prefix.
            assert!(cached(&cache).filled_qty().is_zero());
            handle.stop();
            return snapshot(&cache);
        }

        if !complete_query {
            if conflict.is_none() {
                assert!(cached(&cache).filled_qty().is_zero());
                venue.updates.send(first.clone()).unwrap();
                wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
            }
            venue.updates.send(second.clone()).unwrap();
        }
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00100000")).await;
        let before = snapshot(&cache);
        assert_eq!(before["trade_ids"], json!(["90001", "90002"]));
        assert_eq!(before["positions"][0]["quantity"], "0.00100000");
        assert_eq!(before["positions"][0]["commissions"], json!(["0.05 USD"]));
        assert_eq!(before["positions"][0]["realized_pnl"], "-0.05 USD");
        venue.updates.send(first).unwrap();
        venue.updates.send(second).unwrap();
        sentinel(&venue, &cache, timestamp + 40).await;
        assert_eq!(snapshot(&cache), before);
        handle.stop();
        before
    };
    let (result, persisted) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);
    msgbus::unsubscribe_any(fill_topic.into(), &fill_handler);

    if expected_hold {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored_cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut restored_cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if expected_hold {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains(if conflict.is_some() {
                    "Unresolved persisted standalone fill recovery"
                } else {
                    "Unresolved persisted incomplete fill recovery"
                })
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&restored_cache), persisted);
    }
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[derive(Clone, Debug)]
struct ShutdownEvidenceClient {
    connected: bool,
    pending: Rc<RefCell<Option<ExecutionReport>>>,
}

impl ClientConfig for ShutdownEvidenceClient {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ExecutionClientFactory for ShutdownEvidenceClient {
    fn create(
        &self,
        _: TraderId,
        _: &str,
        config: &dyn ClientConfig,
        _: CacheView,
        _: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config.as_any().downcast_ref::<Self>().unwrap();
        Ok(Box::new(config.clone()))
    }
    fn name(&self) -> &'static str {
        "ShutdownEvidenceClient"
    }
    fn config_type(&self) -> &'static str {
        "ShutdownEvidenceClient"
    }
}

#[async_trait::async_trait(?Send)]
impl ExecutionClient for ShutdownEvidenceClient {
    fn is_connected(&self) -> bool {
        self.connected
    }
    fn client_id(&self) -> ClientId {
        ClientId::from("SHUTDOWN")
    }
    fn account_id(&self) -> AccountId {
        AccountId::from("SHUTDOWN-001")
    }
    fn venue(&self) -> NativeVenue {
        NativeVenue::from("SIM")
    }
    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }
    fn get_account(&self) -> Option<AccountAny> {
        None
    }
    fn generate_account_state(
        &self,
        _: Vec<AccountBalance>,
        _: Vec<MarginBalance>,
        _: bool,
        _: UnixNanos,
        _: Option<Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;

        if let Some(report) = self.pending.borrow_mut().take() {
            get_exec_event_sender().send(ExecutionEvent::Report(report))?;
        }
        Ok(())
    }
    async fn generate_mass_status(
        &self,
        _: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        Ok(None)
    }
}

#[rstest]
#[case::healthy_query(false, "query")]
#[case::failed_query(true, "query")]
#[case::failed_standalone(true, "fill")]
#[case::healthy_status(false, "status")]
#[case::failed_status(true, "status")]
#[case::healthy_bundle(false, "bundle")]
#[case::failed_bundle(true, "bundle")]
#[case::healthy_observation(false, "observation")]
#[case::failed_observation(true, "observation")]
#[case::healthy_incomplete(false, "incomplete")]
#[case::failed_incomplete(true, "incomplete")]
#[case::healthy_fresh_query(false, "fresh_query")]
#[case::failed_fresh_query(true, "fresh_query")]
#[tokio::test]
async fn test_shutdown_captures_new_fill_obligation_before_sealing(
    #[values(false, true)] queued_before_stop: bool,
    #[case] callback_failed: bool,
    #[case] envelope_kind: &str,
) {
    let standalone = matches!(
        envelope_kind,
        "fill" | "observation" | "fresh_query" | "incomplete"
    );
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let pending = Rc::new(RefCell::new(None));
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(pending.clone()),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = Box::pin(async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap();
        venue
            .updates
            .send(wire_order(
                "live",
                "0",
                "0",
                "",
                "0",
                "0",
                timestamp.as_u64() / 1_000_000,
            ))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let order = cached(&cache);
        let report = OrderStatusReport::new(
            order.account_id().unwrap(),
            order.instrument_id(),
            Some(order.client_order_id()),
            order.venue_order_id().unwrap(),
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            OrderStatus::PartiallyFilled,
            order.quantity(),
            Quantity::from("0.00040000"),
            timestamp,
            timestamp,
            timestamp,
            None,
        );
        let envelope = if standalone {
            ExecutionReport::Fill(Box::new(FillReport::new(
                order.account_id().unwrap(),
                order.instrument_id(),
                order.venue_order_id().unwrap(),
                TradeId::from("90001"),
                order.order_side(),
                Quantity::from("0.00040000"),
                Price::from("50000.0"),
                Money::from("0.02 USD"),
                LiquiditySide::Taker,
                Some(order.client_order_id()),
                None,
                timestamp,
                timestamp,
                None,
            )))
        } else {
            match envelope_kind {
                "status" => ExecutionReport::Order(Box::new(report)),
                "bundle" => ExecutionReport::OrderWithFills(Box::new(report), Vec::new()),
                "query" => ExecutionReport::OrderQueryWithFills(Box::new(report), Vec::new()),
                _ => unreachable!(),
            }
        };

        let envelope = match envelope {
            ExecutionReport::Fill(fill) if envelope_kind == "observation" => {
                ExecutionReport::FillEvidence(fill)
            }
            ExecutionReport::Fill(fill) if envelope_kind == "incomplete" => {
                ExecutionReport::IncompleteFill(Box::new(IncompleteFillReport {
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
                }))
            }
            ExecutionReport::Fill(fill) if envelope_kind == "fresh_query" => {
                ExecutionReport::FillQueryRequired(fill)
            }
            envelope => envelope,
        };

        if queued_before_stop {
            // No yield between enqueue and stop: the biased zero-grace deadline wins.
            get_exec_event_sender()
                .send(ExecutionEvent::Report(envelope))
                .unwrap();
        } else {
            *pending.borrow_mut() = Some(envelope);
        }

        if callback_failed {
            struct DrainOnDrop;
            impl Drop for DrainOnDrop {
                fn drop(&mut self) {
                    assert_eq!(nautilus_common::actor::drain_callbacks(1), Ok(false));
                }
            }
            let result = std::panic::catch_unwind(|| {
                let _drain = DrainOnDrop;
                panic!("simulated callback unwind");
            });
            assert!(result.is_err());
            assert!(nautilus_common::actor::callback_failure().is_some());
        }
        handle.stop();
    });
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    assert!(cached(&cache).filled_qty().is_zero());
    assert!(cached(&cache).trade_ids().is_empty());
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );
    assert!(
        restored
            .unwrap_err()
            .to_string()
            .contains(if envelope_kind == "incomplete" {
                "Unresolved persisted incomplete fill recovery"
            } else if standalone {
                "Unresolved persisted standalone fill recovery"
            } else {
                "Unresolved persisted order fill recovery"
            })
    );
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[case::identical_duplicates(false, None, false)]
#[case::duplicate_cumulative_fee(false, Some(("fee", "-0.04")), true)]
#[case::duplicate_fill_fee(false, Some(("fillFee", "-0.04")), true)]
#[case::duplicate_price(false, Some(("fillPx", "50001.0")), true)]
#[case::duplicate_position(false, Some(("posSide", "short")), true)]
#[case::duplicate_liquidity(false, Some(("execType", "M")), true)]
#[case::rest_overlap_fee(true, Some(("fee", "-0.04")), true)]
#[case::rest_overlap_price(true, Some(("fillPx", "50001.0")), true)]
#[case::rest_overlap_time(true, Some(("fillTime", "1")), true)]
#[case::rest_overlap_liquidity(true, Some(("execType", "M")), true)]
#[case::duplicate_quantity_precision(false, Some(("fillSz", "0.000400000000000000000000000001")), true)]
#[case::duplicate_price_precision(false, Some(("fillPx", "50000.000000000000000000000000001")), true)]
#[case::duplicate_fee_precision(false, Some(("fillFee", "-0.020000000000000000000000000001")), true)]
#[case::duplicate_cumulative_precision(false, Some(("accFillSz", "0.000400000000000000000000000001")), true)]
#[case::duplicate_rebate(false, Some(("rebate", "0.01")), true)]
#[case::duplicate_rebate_currency(false, Some(("rebateCcy", "BTC")), true)]
#[case::equivalent_decimal(false, Some(("fillFee", "-0.020000")), false)]
#[tokio::test]
async fn test_conflicting_execution_evidence_survives_quantity_agreement_and_restart(
    #[case] rest: bool,
    #[case] conflict: Option<(&'static str, &'static str)>,
    #[case] expected_hold: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let conflicts = Rc::new(RefCell::new(0));
    let observed = conflicts.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if matches!(report, ExecutionReport::OrderFillConflict(_)) {
            *observed.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let first = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00040000",
            "90001",
            "-0.02",
            if conflict.is_none_or(|(field, _)| field.starts_with("rebate")) {
                ""
            } else {
                "-0.02"
            },
            timestamp,
        );
        let second = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "",
            timestamp + 20,
        );
        venue.updates.send(first.clone()).unwrap();
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;

        if !rest {
            let mut duplicate = first.clone();
            if let Some((key, value)) = conflict {
                duplicate["data"][0][key] = json!(value);
            }
            venue.updates.send(duplicate).unwrap();
            sentinel(&venue, &cache, timestamp + 10).await;
        }
        venue.updates.send(second.clone()).unwrap();
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00100000")).await;
        let before = snapshot(&cache);
        assert_eq!(before["trade_ids"], json!(["90001", "90002"]));
        assert_eq!(before["positions"][0]["commissions"], json!(["0.05 USD"]));
        assert_eq!(before["positions"][0]["realized_pnl"], "-0.05 USD");
        assert_eq!(before["fills"][1]["commission"], "0.03 USD");
        if rest {
            let first_fill = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()});
            let second_fill = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
                "ts":(timestamp+25).to_string(), "fillTime":(timestamp+21).to_string()});
            let mut changed = first_fill.clone();
            let (key, value) = conflict.unwrap();
            changed[key] = json!(value);
            *venue.report.lock().await = json!({"code":"0","msg":"","data":second["data"]});
            *venue.fills.lock().await = json!({"code":"0","msg":"","data":[first_fill]});
            *venue.historical_fills.lock().await =
                json!({"code":"0","msg":"","data":[changed, second_fill]});
            query(&cache);
        }

        if expected_hold {
            wait_until(|| *conflicts.borrow() > 0).await;
        }
        venue.updates.send(first).unwrap();
        venue.updates.send(second).unwrap();
        sentinel(&venue, &cache, timestamp + 40).await;
        assert_eq!(snapshot(&cache), before);
        handle.stop();
        before
    };
    let (result, persisted) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);

    if expected_hold {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if expected_hold {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted order fill conflict")
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&cache), persisted);
    }
    server.abort();
}

#[rstest]
#[case::sequential(false, true)]
#[case::missing_predecessor_recovered(true, true)]
#[case::missing_predecessor_held(true, false)]
#[tokio::test]
async fn test_external_order_without_client_id_preserves_size_and_fill_chronology(
    #[case] missing: bool,
    #[case] recover: bool,
) {
    const EXTERNAL_VID: &str = "2497956918703120399";
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let external = || {
        cache
            .borrow()
            .client_order_id(&VenueOrderId::from(EXTERNAL_VID))
            .and_then(|id| cache.borrow().order(id).map(|o| o.clone()))
    };
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut first = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00040000",
            "90001",
            "-0.02",
            "-0.02",
            timestamp,
        );
        let mut second = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            timestamp + 20,
        );

        for frame in [&mut first, &mut second] {
            frame["data"][0]["ordId"] = json!(EXTERNAL_VID);
            frame["data"][0]["clOrdId"] = json!("");
        }

        if !missing {
            venue.updates.send(first.clone()).unwrap();
            wait_until(|| {
                external().is_some_and(|o| o.filled_qty() == Quantity::from("0.00040000"))
            })
            .await;
            assert_eq!(external().unwrap().quantity(), Quantity::from("0.00100000"));
            assert_eq!(external().unwrap().status(), OrderStatus::PartiallyFilled);
        }
        venue.updates.send(second.clone()).unwrap();
        sentinel(&venue, &cache, timestamp + 30).await;
        assert_eq!(external().unwrap().quantity(), Quantity::from("0.00100000"));
        if missing {
            assert!(external().unwrap().filled_qty().is_zero());
            assert!(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .is_empty()
            );
            *venue.report.lock().await = json!({"code":"0","msg":"","data":second["data"]});

            if recover {
                let fills: Vec<_> = [("90001", "80001", "0.00040000", "-0.02", timestamp+1), ("90002", "80002", "0.00060000", "-0.03", timestamp+21)]
                    .into_iter().map(|(trade, bill, qty, fee, time)| json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":EXTERNAL_VID, "clOrdId":"",
                        "tradeId":trade, "billId":bill, "fillSz":qty, "fillPx":"50000.0", "side":"buy", "posSide":"net", "execType":"T",
                        "fee":fee, "feeCcy":"USD", "ts":time.to_string(), "fillTime":time.to_string()})).collect();
                *venue.fills.lock().await = json!({"code":"0","msg":"","data":fills});
            }
        }

        if recover {
            wait_until(|| {
                external().is_some_and(|o| o.filled_qty() == Quantity::from("0.00100000"))
            })
            .await;
            assert_eq!(
                external()
                    .unwrap()
                    .trade_ids()
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                vec!["90001", "90002"]
            );
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.05 USD"])
            );
            venue.updates.send(first).unwrap();
            venue.updates.send(second).unwrap();
            sentinel(&venue, &cache, timestamp + 40).await;
        }
        let before = snapshot(&cache);
        handle.stop();
        before
    };
    let (result, persisted) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    if recover {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if recover {
        restored.unwrap();
        assert_eq!(snapshot(&cache), persisted);
    } else {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted standalone fill recovery")
        );
    }
    server.abort();
}

#[rstest]
#[case::earlier_uncovered(false, false)]
#[case::earlier_snapshot(true, false)]
#[case::tied_uncovered(false, true)]
#[tokio::test]
async fn test_snapshot_covered_history_preserves_exposure_with_native_okx_client(
    #[values("startup", "query", "report", "standalone")] source: &str,
    #[case] snapshot_covered: bool,
    #[case] tied: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        (!snapshot_covered).then(|| EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let account_id = AccountId::from("OKX-001");
    let instrument_id = InstrumentId::from(INSTRUMENT);
    let instrument = cache.borrow().instrument(&instrument_id).unwrap().clone();
    let snapshot_ms = node.kernel().clock().borrow().timestamp_ns().as_u64() / 1_000_000;
    let execution_ms = snapshot_ms - if tied { 1 } else { 1000 };
    let account: OKXResponse<OKXAccount> = serde_json::from_value(balance()).unwrap();
    let account = parse_account_state(
        &account.data[0],
        account_id,
        AccountType::Cash,
        UnixNanos::default(),
    )
    .unwrap();
    cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    let mut builder = OrderTestBuilder::new(OrderType::Market);
    builder
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::external())
        .client_order_id(ClientOrderId::from("SNAPSHOT-OPEN"))
        .instrument_id(instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.00100000"));

    if snapshot_covered {
        builder.tags(vec!["RECONCILIATION".into()]);
    }
    let mut opening = builder.build();
    opening
        .apply(TestOrderEventStubs::submitted(&opening, account_id))
        .unwrap();
    let opening_venue_id = VenueOrderId::from("SNAPSHOT-VENUE");
    opening
        .apply(TestOrderEventStubs::accepted(
            &opening,
            account_id,
            opening_venue_id,
        ))
        .unwrap();
    let opening_fill = OrderFilled::new(
        opening.trader_id(),
        opening.strategy_id(),
        instrument_id,
        opening.client_order_id(),
        opening_venue_id,
        account_id,
        TradeId::from("SNAPSHOT-TRADE"),
        OrderSide::Buy,
        OrderType::Market,
        opening.quantity(),
        Price::from("50000.0"),
        instrument.quote_currency(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from(snapshot_ms * 1_000_000),
        UnixNanos::from(snapshot_ms * 1_000_000),
        snapshot_covered,
        Some(PositionId::from("SNAPSHOT-POSITION")),
        Some(Money::zero(instrument.quote_currency())),
        None,
    );
    opening
        .apply(OrderEventAny::Filled(opening_fill.clone()))
        .unwrap();
    let position = Position::new(&instrument, opening_fill);
    cache
        .borrow_mut()
        .add_order(
            opening,
            Some(position.id),
            Some(ClientId::from("OKX")),
            true,
        )
        .unwrap();
    cache
        .borrow_mut()
        .add_position(&position, OmsType::Netting)
        .unwrap();

    let wire = wire_order(
        "filled",
        "0.00100000",
        "0.00100000",
        "90001",
        "-0.05",
        "-0.05",
        execution_ms,
    );
    let history = json!({"code":"0", "msg":"", "data":[{
        "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
        "tradeId":"90001", "billId":"80001", "fillSz":"0.00100000", "fillPx":"50000.0",
        "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
        "ts":(execution_ms+5).to_string(), "fillTime":(execution_ms+1).to_string()
    }]});

    if source == "startup" {
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":wire["data"]});
        *venue.fills.lock().await = history.clone();
        venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    let handle = node.handle();
    let driver = async {
        if source == "startup" && !snapshot_covered {
            loop {
                if !venue.fill_queries.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            return;
        }
        wait_until(|| handle.is_running()).await;

        if source != "startup" {
            wait_until(|| cached(&cache).ts_submitted().is_some()).await;
            let submitted_ms = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
            venue
                .updates
                .send(wire_order("live", "0", "0", "", "0", "0", submitted_ms))
                .unwrap();
            wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
            let status = OrderStatusReport::new(
                account_id,
                instrument_id,
                Some(ClientOrderId::from(CID)),
                VenueOrderId::from(VID),
                Some(OrderSide::Buy),
                OrderType::Limit,
                TimeInForce::Gtc,
                OrderStatus::Filled,
                Quantity::from("0.00100000"),
                Quantity::from("0.00100000"),
                UnixNanos::from(execution_ms * 1_000_000),
                UnixNanos::from((execution_ms + 1) * 1_000_000),
                UnixNanos::from((execution_ms + 1) * 1_000_000),
                None,
            )
            .with_price(Price::from("50000.0"));
            let fill = FillReport::new(
                account_id,
                instrument_id,
                VenueOrderId::from(VID),
                TradeId::from("90001"),
                OrderSide::Buy,
                status.filled_qty,
                Price::from("50000.0"),
                Money::from("0.05 USD"),
                LiquiditySide::Taker,
                Some(ClientOrderId::from(CID)),
                None,
                status.ts_last,
                status.ts_init,
                None,
            );
            let sender = get_exec_event_sender();
            if source != "standalone" {
                sender
                    .send(ExecutionEvent::Report(
                        ExecutionReport::OrderQueryWithFills(
                            Box::new(status.clone()),
                            if source == "query" {
                                vec![fill.clone()]
                            } else {
                                Vec::new()
                            },
                        ),
                    ))
                    .unwrap();
            }

            if matches!(source, "report" | "standalone") {
                sender
                    .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                        fill.clone(),
                    ))))
                    .unwrap();
            }

            if snapshot_covered && source == "report" {
                // A lone reconciled fragment can project snapshot-covered history,
                // but only complete query history retires the empty query obligation.
                sender
                    .send(ExecutionEvent::Report(
                        ExecutionReport::OrderQueryWithFills(Box::new(status), vec![fill]),
                    ))
                    .unwrap();
            }
            sentinel(&venue, &cache, submitted_ms + 100).await;
        }

        if snapshot_covered {
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            assert_eq!(cached(&cache).trade_ids(), vec![&TradeId::from("90001")]);
            assert_eq!(
                cached(&cache)
                    .commissions()
                    .values()
                    .copied()
                    .collect::<Vec<_>>(),
                vec![Money::from("0.05 USD")]
            );
        } else {
            assert!(cached(&cache).filled_qty().is_zero());
        }
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();

    if snapshot_covered {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("fill recovery incomplete")
        );
    }
    let retained = cache.borrow().position(&position.id).unwrap().clone();
    assert_eq!(retained.quantity, position.quantity);
    assert_eq!(retained.commissions(), position.commissions());
    assert_eq!(retained.trade_ids, position.trade_ids);
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None).len(),
        1
    );

    if snapshot_covered {
        server.abort();
        return;
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );
    assert!(
        restored
            .unwrap_err()
            .to_string()
            .contains("Unresolved persisted")
    );
    server.abort();
}

#[rstest]
#[case::forward([0, 1, 2])]
#[case::reverse([2, 1, 0])]
#[case::middle_first([1, 0, 2])]
#[case::first_last([0, 2, 1])]
#[case::middle_last([1, 2, 0])]
#[case::last_first([2, 0, 1])]
#[tokio::test]
async fn test_complete_query_before_delayed_tied_stream_and_restart(
    #[case] permutation: [usize; 3],
    #[values(false, true)] restart: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let executions = [
            (
                "90001",
                "80001",
                "0.00020000",
                "0.00020000",
                "-0.01",
                "-0.01",
            ),
            (
                "90002",
                "80002",
                "0.00030000",
                "0.00050000",
                "-0.015",
                "-0.025",
            ),
            (
                "90003",
                "80003",
                "0.00050000",
                "0.00100000",
                "-0.025",
                "-0.05",
            ),
        ];
        let frames: Vec<_> = executions
            .iter()
            .enumerate()
            .map(|(index, (trade, _, qty, cumulative, fee, total_fee))| {
                wire_order(
                    if index == 2 {
                        "filled"
                    } else {
                        "partially_filled"
                    },
                    cumulative,
                    qty,
                    trade,
                    total_fee,
                    fee,
                    timestamp,
                )
            })
            .collect();
        let fills: Vec<_> = permutation
            .iter()
            .map(|index| {
                let (trade, bill, quantity, _, fee, _) = executions[*index];
                json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":trade, "billId":bill, "fillSz":quantity, "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":fee, "feeCcy":"USD",
                "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()})
            })
            .collect();
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":frames[2]["data"]});
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":fills});
        // No earlier stream or partial query can pre-establish a native prefix.
        query(&cache);
        wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00100000")).await;
        let before = snapshot(&cache);
        assert_eq!(cached(&cache).trade_ids().len(), 3);
        assert_eq!(before["positions"][0]["commissions"], json!(["0.05 USD"]));
        assert_eq!(before["positions"][0]["realized_pnl"], "-0.05 USD");

        if !restart {
            for frame in frames.iter().chain(frames.iter()) {
                venue.updates.send(frame.clone()).unwrap();
            }
            sentinel(&venue, &cache, timestamp + 20).await;
            assert_eq!(snapshot(&cache), before);
        }
        handle.stop();
        (timestamp, frames, before)
    };
    let (result, (timestamp, frames, before)) =
        tokio::time::timeout(Duration::from_secs(70), async {
            tokio::join!(node.run(), driver)
        })
        .await
        .unwrap();
    result.unwrap();
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let mut restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap();
    assert_eq!(snapshot(&cache), before);

    if restart {
        venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);
        let handle = restarted.handle();
        let driver = async {
            wait_until(|| handle.is_running()).await;

            for frame in frames.iter().chain(frames.iter()) {
                venue.updates.send(frame.clone()).unwrap();
            }
            sentinel(&venue, &cache, timestamp + 20).await;
            assert_eq!(snapshot(&cache), before);
            handle.stop();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
            tokio::join!(restarted.run(), driver)
        })
        .await
        .unwrap();
        result.unwrap();
    }
    assert_eq!(venue.submissions.lock().await.len(), 1);
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_tied_standalone_fragments_cannot_prove_missing_predecessor(
    #[values(false, true)] empty_query: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID))
            && report.filled_qty.is_positive()
        {
            *captured.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;

        let fragments = if empty_query {
            vec![
                ("90003", "0.00030000", "0.00030000"),
                ("90004", "0.00010000", "0.00040000"),
            ]
        } else {
            vec![
                ("90003", "0.00030000", "0.00030000"),
                ("90004", "0.00020000", "0.00050000"),
                ("90002", "0.00010000", "0.00060000"),
            ]
        };

        for (index, (trade, quantity, _)) in fragments.into_iter().enumerate() {
            get_exec_event_sender()
                .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                    FillReport::new(
                        AccountId::from("OKX-001"),
                        InstrumentId::from(INSTRUMENT),
                        VenueOrderId::from(VID),
                        TradeId::from(trade),
                        OrderSide::Buy,
                        Quantity::from(quantity),
                        Price::from("50000.0"),
                        Money::from("0.01 USD"),
                        LiquiditySide::Taker,
                        Some(ClientOrderId::from(CID)),
                        None,
                        UnixNanos::from((timestamp + 1) * 1_000_000),
                        UnixNanos::default(),
                        None,
                    ),
                ))))
                .unwrap();
            sentinel(&venue, &cache, timestamp + 2 + index as u64).await;
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00030000"));
        }
        let before = snapshot(&cache);
        let fragment = wire_order(
            "partially_filled",
            "0.00040000",
            "0.00010000",
            "90002",
            "-0.02",
            "-0.01",
            timestamp,
        );

        if empty_query {
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":fragment["data"]});
            query(&cache);
            wait_until(|| *observed.borrow() > 0).await;
        } else {
            // A=0.0003 is absent, but known C+B happens to equal B's cumulative.
            venue.updates.send(fragment.clone()).unwrap();
            venue.updates.send(fragment).unwrap();
        }
        sentinel(&venue, &cache, timestamp + 20).await;
        if empty_query {
            assert_eq!(snapshot(&cache), before);
        } else {
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
            assert_eq!(
                cached(&cache)
                    .trade_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                vec!["90003", "90002"]
            );
        }
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let error = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap_err();
    assert!(error.to_string().contains(if empty_query {
        "Unresolved persisted incomplete fill recovery"
    } else {
        "Unresolved persisted standalone fill recovery"
    }));
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_rejected_fill_evidence_survives_complete_query_and_sealed_restart(
    #[values(false, true)] liquidity_conflict: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let counter = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID))
            && report.filled_qty.is_positive()
        {
            *counter.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let time = UnixNanos::from((timestamp + 1) * 1_000_000);
        let order = cached(&cache);

        if !liquidity_conflict {
            let status = OrderStatusReport::new(
                order.account_id().unwrap(),
                order.instrument_id(),
                Some(order.client_order_id()),
                order.venue_order_id().unwrap(),
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Filled,
                order.quantity(),
                order.quantity(),
                time,
                time,
                time,
                None,
            );
            get_exec_event_sender()
                .send(ExecutionEvent::Report(
                    ExecutionReport::OrderQueryWithFills(Box::new(status), Vec::new()),
                ))
                .unwrap();
            wait_until(|| *observed.borrow() == 1).await;
            // This fragment cannot be admitted under the preceding cumulative quantity.
            get_exec_event_sender()
                .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                    FillReport::new(
                        order.account_id().unwrap(),
                        order.instrument_id(),
                        order.venue_order_id().unwrap(),
                        TradeId::from("90001"),
                        OrderSide::Buy,
                        Quantity::from("0.00040000"),
                        Price::from("50000.0"),
                        Money::from("0.02 USD"),
                        LiquiditySide::Taker,
                        Some(order.client_order_id()),
                        None,
                        time,
                        time,
                        None,
                    ),
                ))))
                .unwrap();
            sentinel(&venue, &cache, timestamp + 10).await;
            assert!(cached(&cache).trade_ids().is_empty());
        }
        let wire = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            "90002",
            "-0.05",
            "-0.05",
            timestamp,
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":wire["data"]});
        let fill = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90002", "billId":"80002", "fillSz":"0.00100000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
            "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()});
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[fill.clone()]});
        let before_query = snapshot(&cache);
        let count = *observed.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() >= count + 2).await;
        let before = snapshot(&cache);

        if liquidity_conflict {
            assert_eq!(cached(&cache).status(), OrderStatus::Filled);
            assert_eq!(before["trade_ids"], json!(["90002"]));
        } else {
            // Complete history cannot omit an individually observed execution.
            assert_eq!(before, before_query);
            assert!(cached(&cache).filled_qty().is_zero());
            assert!(cached(&cache).trade_ids().is_empty());
        }

        if liquidity_conflict {
            let count = *observed.borrow();
            let mut changed = fill;
            changed["execType"] = json!("M");
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[changed]});
            query(&cache);
            wait_until(|| *observed.borrow() >= count + 2).await;
            sentinel(&venue, &cache, timestamp + 20).await;
        }
        assert_eq!(snapshot(&cache), before);
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let error = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains(if liquidity_conflict {
            "Unresolved persisted standalone fill recovery"
        } else {
            "Unresolved persisted incomplete fill recovery"
        }),
        "{error:#}"
    );
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_tied_reversal_query_does_not_choose_position_accounting(
    #[values(false, true)] reverse: bool,
) {
    use nautilus_common::messages::execution::{QueryOrder, TradingCommand};
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let counter = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.venue_order_id == VenueOrderId::from("REVERSE-VENUE") {
            *counter.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        venue
            .updates
            .send(wire_order(
                "filled",
                "0.00100000",
                "0.00100000",
                "90000",
                "-0.05",
                "-0.05",
                timestamp,
            ))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
        let before = serde_json::to_value(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .into_iter()
                .map(|position| position.cloned())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut wire = wire_order(
            "filled",
            "0.00200000",
            "0.00100000",
            "90002",
            "-0.10",
            "-0.05",
            timestamp + 10,
        );

        for (key, value) in [
            ("side", "sell"),
            ("sz", "0.00200000"),
            ("clOrdId", ""),
            ("ordId", "REVERSE-VENUE"),
            ("avgPx", "50015.0"),
        ] {
            wire["data"][0][key] = json!(value);
        }
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":wire["data"]});
        let mut fills = vec![];
        for (trade, price) in [("90001", "50010.0"), ("90002", "50020.0")] {
            fills.push(
                json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":"REVERSE-VENUE", "clOrdId":"",
                "tradeId":trade, "billId":trade, "fillSz":"0.00100000", "fillPx":price,
                "side":"sell", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
                "ts":(timestamp+15).to_string(), "fillTime":(timestamp+11).to_string()}),
            );
        }

        if reverse {
            fills.reverse();
        }
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":fills});
        AsyncRunner::handle_exec_command(TradingCommand::QueryOrder(QueryOrder::new(
            TraderId::from("TESTER-001"),
            Some(ClientId::from("OKX")),
            StrategyId::external(),
            InstrumentId::from(INSTRUMENT),
            ClientOrderId::from("REVERSE-VENUE"),
            Some(VenueOrderId::from("REVERSE-VENUE")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )));
        wait_until(|| *observed.borrow() >= 2).await;
        sentinel(&venue, &cache, timestamp + 30).await;
        assert_eq!(
            serde_json::to_value(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .into_iter()
                    .map(|position| position.cloned())
                    .collect::<Vec<_>>()
            )
            .unwrap(),
            before
        );
        assert!(
            cache
                .borrow()
                .order(&ClientOrderId::from("REVERSE-VENUE"))
                .unwrap()
                .filled_qty()
                .is_zero()
        );
        let requests = venue.queries.lock().await;
        assert!(
            requests
                .iter()
                .any(|q| q.get("ordId").is_some_and(|id| id == "REVERSE-VENUE")
                    && !q.contains_key("clOrdId"))
        );
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    assert!(
        restore_cache_from_sealed_run(
            &mut cache.borrow_mut(),
            store.path().to_path_buf(),
            &instance,
            &runs[0].run_id
        )
        .unwrap_err()
        .to_string()
        .contains("Unresolved persisted incomplete fill recovery")
    );
    server.abort();
}

#[rstest]
#[case::quantity("fillSz", "0.001000000000000000000000000001")]
#[case::price("fillPx", "50000.000000000000000000000000001")]
#[case::fee("fillFee", "-0.050000000000000000000000000001")]
#[tokio::test]
async fn test_unrepresentable_stream_execution_remains_pending_after_restart(
    #[case] field: &str,
    #[case] value: &str,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let before = snapshot(&cache);
        let mut frame = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            "90001",
            "-0.05",
            "-0.05",
            timestamp,
        );
        frame["data"][0][field] = json!(value);
        venue.updates.send(frame).unwrap();
        sentinel(&venue, &cache, timestamp + 10).await;
        assert_eq!(snapshot(&cache), before);
        assert!(cached(&cache).trade_ids().is_empty());
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Order fill recovery incomplete")
    );
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );
    assert!(
        restored
            .unwrap_err()
            .to_string()
            .contains("Unresolved persisted incomplete fill recovery")
    );
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_external_algo_status_requires_authoritative_child_executions(
    #[values("held", "stream", "query", "malformed")] recovery: &str,
    #[values(false, true)] larger_earlier_history: bool,
    #[values("", "ChildClient1")] child_client_order_id: &str,
) {
    use nautilus_common::messages::execution::{QueryOrder, TradingCommand};

    const EXTERNAL_CID: &str = "ExternalAlgo1";
    const CHILD_VID: &str = "2497956918703120499";
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.venue_order_id == VenueOrderId::from(CHILD_VID) {
            *captured.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let external = || {
        cache
            .borrow()
            .order(&ClientOrderId::from(EXTERNAL_CID))
            .map(|o| o.clone())
    };
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut fixture: Value =
            serde_json::from_str(include_str!("../../test_data/ws_orders_algo.json")).unwrap();
        let mut algo = fixture["data"].as_array_mut().unwrap().remove(0);
        for (key, value) in [
            ("algoClOrdId", EXTERNAL_CID),
            ("clOrdId", ""),
            ("ordId", CHILD_VID),
            ("instId", "BTC-USD"),
            ("instType", "SPOT"),
            ("side", "buy"),
            ("posSide", "net"),
            ("sz", "0.00100000"),
            ("actualSz", "0.00100000"),
            ("ordPx", "50001.0"),
            ("triggerPx", "50000.0"),
            ("actualPx", "50000.0"),
            ("state", "filled"),
            ("tdMode", "cash"),
            ("reduceOnly", "false"),
        ] {
            algo[key] = json!(value);
        }

        if larger_earlier_history {
            algo["actualSz"] = json!("0.00040000");
        }
        algo["cTime"] = json!(timestamp.to_string());
        algo["uTime"] = json!((timestamp + 2).to_string());
        let frame = json!({"arg":{"channel":"orders-algo","instType":"SPOT"},"data":[algo]});
        venue.updates.send(frame.clone()).unwrap();
        wait_until(|| *observed.borrow() >= 1).await;
        assert!(external().unwrap().filled_qty().is_zero());
        assert!(external().unwrap().trade_ids().is_empty());
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert_ne!(external().unwrap().status(), OrderStatus::Filled);
        let pending = serde_json::to_value(external().unwrap()).unwrap();
        let count = *observed.borrow();
        venue.updates.send(frame).unwrap();
        wait_until(|| *observed.borrow() > count).await;
        assert_eq!(serde_json::to_value(external().unwrap()).unwrap(), pending);
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );

        let mut child = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            "90001",
            "-0.05",
            "-0.05",
            timestamp,
        );
        child["data"][0]["clOrdId"] = json!(child_client_order_id);
        child["data"][0]["algoClOrdId"] = json!(EXTERNAL_CID);
        child["data"][0]["ordId"] = json!(CHILD_VID);

        if recovery != "held" {
            *venue.report.lock().await = json!({"code":"0","msg":"","data":child["data"]});
            *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
                "instType":"SPOT", "instId":"BTC-USD", "ordId":CHILD_VID, "clOrdId":child_client_order_id,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00100000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
                "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
            }]});
        }

        if recovery == "malformed" {
            let mut malformed = child.clone();
            malformed["data"][0]["fillFee"] = json!("invalid");
            venue.updates.send(malformed).unwrap();
            sentinel(&venue, &cache, timestamp + 10).await;
            assert!(external().unwrap().filled_qty().is_zero());
        }

        if matches!(recovery, "query" | "malformed" | "fills") {
            let order = external().unwrap();
            AsyncRunner::handle_exec_command(TradingCommand::QueryOrder(QueryOrder::new(
                order.trader_id(),
                Some(ClientId::from("OKX")),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                order.venue_order_id(),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            )));
        } else if recovery == "stream" {
            venue.updates.send(child.clone()).unwrap();
        }

        if recovery != "held" {
            wait_until(|| external().unwrap().status() == OrderStatus::Filled).await;
            // A provisional query report precedes its asynchronous fill-history bundle.
            // Snapshot only after that bundle has committed the native price update.
            if !larger_earlier_history {
                wait_until(|| external().unwrap().price() == Some(Price::from("50000.0"))).await;
                wait_until(|| {
                    !cache.borrow().has_pending_order_executions(
                        external().unwrap().strategy_id(),
                        Some(external().unwrap().instrument_id()),
                    )
                })
                .await;
            }
            assert_eq!(
                external().unwrap().trade_ids(),
                vec![&TradeId::from("90001")]
            );
            let positions: Vec<_> = cache
                .borrow()
                .positions(None, None, None, None, None)
                .into_iter()
                .map(|position| position.cloned())
                .collect();
            assert_eq!(positions.len(), 1);
            assert_eq!(positions[0].quantity, Quantity::from("0.00100000"));
            assert_eq!(positions[0].avg_px_open, rust_decimal_macros::dec!(50000.0));
            assert_eq!(positions[0].commissions(), vec![Money::from("0.05 USD")]);
            assert_eq!(positions[0].realized_pnl, Some(Money::from("-0.05 USD")));
        }
        let before = serde_json::to_value(external().unwrap()).unwrap();
        let positions = snapshot(&cache)["positions"].clone();

        if recovery != "held" {
            venue.updates.send(child).unwrap();
            sentinel(&venue, &cache, timestamp + 10).await;
        }
        assert_eq!(serde_json::to_value(external().unwrap()).unwrap(), before);
        assert_eq!(snapshot(&cache)["positions"], positions);
        handle.stop();
        (before, positions)
    };
    let (result, (order, positions)) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);

    if recovery == "held" || larger_earlier_history {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if recovery == "held" || larger_earlier_history {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted order fill recovery")
        );
    } else {
        restored.unwrap();
        assert_eq!(
            serde_json::to_value(
                cache
                    .borrow()
                    .order(&ClientOrderId::from(EXTERNAL_CID))
                    .unwrap()
                    .clone()
            )
            .unwrap(),
            order
        );
        assert_eq!(snapshot(&cache)["positions"], positions);
    }
    server.abort();
}

#[rstest]
#[case::failed_extended_replaced("error", false)]
#[case::failed_extended_recovered("error", true)]
#[case::extended_omits_observed_trade("omission", false)]
#[case::failed_next_page_replaced("page_error", false)]
#[case::failed_next_page_recovered("page_error", true)]
#[tokio::test]
async fn test_partial_rest_observations_survive_later_history_and_restart(
    #[case] first_result: &str,
    #[case] recover: bool,
    #[values(false, true)] algo_child: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &FillReport| {
        if report.trade_id == TradeId::from("90001") {
            *captured.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_fill_evidence_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let reports = Rc::new(RefCell::new(0));
    let captured = reports.clone();
    let status_handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.venue_order_id == VenueOrderId::from(VID) {
            *captured.borrow_mut() += 1;
        }
    });
    let status_topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(status_topic.into(), status_handler.clone(), None);
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut full = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            timestamp + 20,
        );

        if algo_child {
            full["data"][0]["clOrdId"] = json!("ChildClient1");
            full["data"][0]["algoClOrdId"] = json!(CID);
        }
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
        let first = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":if algo_child { "ChildClient1" } else { CID },
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()
        });
        let second = json!({
            "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":if algo_child { "ChildClient1" } else { CID },
            "tradeId":"90002", "billId":"80002", "fillSz": if recover { "0.00060000" } else { "0.00100000" }, "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee": if recover { "-0.03" } else { "-0.05" }, "feeCcy":"USD",
            "ts":(timestamp+25).to_string(), "fillTime":(timestamp+21).to_string()
        });
        let mut first_history = vec![first.clone()];

        if first_result == "page_error" {
            first_history = (0..100)
                .map(|index| {
                    let mut fill = first.clone();
                    if index > 0 {
                        fill["tradeId"] = json!((91000 + index).to_string());
                    }
                    fill["billId"] = json!((81000 + index).to_string());
                    fill["fillSz"] = json!("0.00000400");
                    fill["fee"] = json!("-0.0002");
                    fill
                })
                .collect();
            *venue.next_fill_page.lock().await = Some(json!({
                "code":"51000", "msg":"fixture pagination failure", "data":[]
            }));
        }
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":first_history});
        *venue.historical_fills.lock().await = if first_result == "error" {
            json!({"code":"51000", "msg":"fixture extended-history failure", "data":[]})
        } else {
            json!({"code":"0", "msg":"", "data":[second.clone()]})
        };
        let before = snapshot(&cache);
        let previous = venue
            .requests
            .lock()
            .await
            .iter()
            .filter(|path| *path == "/api/v5/trade/fills-history")
            .count();
        query(&cache);
        wait_until(|| *observed.borrow() > 0).await;

        if first_result != "page_error" {
            tokio::time::timeout(DEADLINE, async {
                loop {
                    if venue
                        .requests
                        .lock()
                        .await
                        .iter()
                        .filter(|path| *path == "/api/v5/trade/fills-history")
                        .count()
                        > previous
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(
            snapshot(&cache),
            before,
            "Observations cannot apply partial accounting"
        );
        let expected_trades = first_history.len() + 1;
        let mut history = if recover { first_history } else { Vec::new() };
        history.push(second);
        *venue.next_fill_page.lock().await = None;
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":history});
        *venue.historical_fills.lock().await = json!({"code":"0", "msg":"", "data":history});
        let count = *reports.borrow();
        query(&cache);
        wait_until(|| *reports.borrow() >= count + 2).await;

        if recover {
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            let state = snapshot(&cache);
            assert_eq!(cached(&cache).trade_ids().len(), expected_trades);
            assert!(
                cached(&cache)
                    .trade_ids()
                    .contains(&&TradeId::from("90001"))
            );
            assert!(
                cached(&cache)
                    .trade_ids()
                    .contains(&&TradeId::from("90002"))
            );
            assert_eq!(state["positions"][0]["commissions"], json!(["0.05 USD"]));
            assert_eq!(state["positions"][0]["realized_pnl"], "-0.05 USD");
        } else {
            assert_eq!(
                snapshot(&cache),
                before,
                "Conflicting history cannot replace an observed trade"
            );
        }
        let persisted = snapshot(&cache);
        handle.stop();
        persisted
    };
    let (result, persisted) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);
    msgbus::unsubscribe_any(status_topic.into(), &status_handler);

    if recover {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if recover {
        restored.unwrap();
        assert_eq!(snapshot(&cache), persisted);
    } else {
        assert!(restored.is_err());
    }
    server.abort();
}

#[rstest]
#[case::missing(None, false, false, false)]
#[case::empty(Some(""), false, false, false)]
#[case::malformed(Some("invalid"), false, false, false)]
#[case::inexact(Some("0.001000000000000000000000000001"), false, false, false)]
#[case::combined_missing(None, true, false, false)]
#[case::combined_empty(Some(""), true, false, false)]
#[case::combined_malformed(Some("invalid"), true, false, false)]
#[case::combined_inexact(Some("0.001000004"), true, false, false)]
#[case::rest_precision(Some("0.001000004"), false, true, false)]
#[case::rest_decimal_precision(Some("0.001000000000000000000000000001"), false, true, false)]
#[case::rest_empty(Some(""), false, true, false)]
#[case::zero_cumulative(Some("0"), false, false, false)]
#[case::combined_zero_cumulative(Some("0"), true, false, false)]
#[case::invalid_fee_with_valid_cumulative(Some("0.00100000"), true, false, false)]
#[case::stale_query_with_valid_cumulative(Some("0.00100000"), true, false, true)]
#[case::stale_query(Some("invalid"), false, false, true)]
#[case::stale_combined_query(Some("invalid"), true, false, true)]
#[tokio::test]
async fn test_unknown_cumulative_fill_requires_fresh_query_through_replay(
    #[case] cumulative: Option<&str>,
    #[case] invalid_fee: bool,
    #[case] rest: bool,
    #[case] late_query: bool,
    #[values("tracked", "queried", "restored", "terminal", "algo_child")] route: &str,
    #[values(false, true)] recover: bool,
) {
    // An exact contradictory quantity, or numeric precision beyond Decimal, is
    // retained as evidence. A later valid query cannot erase that original fact.
    let contradictory_quantity = matches!(cumulative, Some("0.001000004" | "0"));
    let unrepresentable_quantity = cumulative == Some("0.001000000000000000000000000001");
    let expected_recovery = recover && !contradictory_quantity && !unrepresentable_quantity;
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut previous_run: Option<(String, String)> = None;
    let mut timestamp = 0;
    let mut persisted = None;
    let stages = if route == "restored" { 2 } else { 1 };

    for stage in 0..stages {
        let mut node = build_node_with_store(
            addr,
            Rc::new(RefCell::new(Vec::new())),
            QuerySource::Direct,
            Some(EventStoreConfig {
                base_dir: store.path().to_path_buf(),
                retention: RetentionMode::Full,
                ..Default::default()
            }),
            Some(Rc::new(RefCell::new(None))),
        );
        let instance = node.kernel().instance_id.to_string();
        let cache = node.kernel().cache.clone();

        if let Some((instance, run)) = &previous_run {
            restore_cache_from_sealed_run(
                &mut cache.borrow_mut(),
                store.path().to_path_buf(),
                instance,
                run,
            )
            .unwrap();
            assert_eq!(Some(snapshot(&cache)), persisted);
        }
        let completed_queries = Rc::new(RefCell::new(0));
        let captured = completed_queries.clone();
        let handler = ShareableMessageHandler::from_typed(move |report: &FillReport| {
            if report.trade_id == TradeId::from("90002") {
                *captured.borrow_mut() += 1;
            }
        });
        let topic = MessagingSwitchboard::reconciliation_raw_fill_report_topic();
        msgbus::subscribe_any(topic.into(), handler.clone(), None);
        let incomplete_observations = Rc::new(RefCell::new(0));
        let captured = incomplete_observations.clone();
        let incomplete_handler = ShareableMessageHandler::from_typed(
            move |report: &ExecutionReport| {
                if matches!(report, ExecutionReport::IncompleteFill(fill) if fill.venue_order_id == VenueOrderId::from(VID))
                {
                    *captured.borrow_mut() += 1;
                }
            },
        );
        let incomplete_topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
        msgbus::subscribe_any(incomplete_topic.into(), incomplete_handler.clone(), None);
        let received_statuses = Rc::new(RefCell::new(0));
        let captured_statuses = received_statuses.clone();
        let status_handler =
            ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
                if report.venue_order_id == VenueOrderId::from(VID)
                    && report.filled_qty.is_positive()
                {
                    *captured_statuses.borrow_mut() += 1;
                }
            });
        let status_topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
        msgbus::subscribe_any(status_topic.into(), status_handler.clone(), None);
        let handle = node.handle();
        let seed_restart = route == "restored" && stage == 0;
        let driver = async {
            wait_until(|| handle.is_running()).await;
            if stage == 0 {
                wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
                wait_until(|| cached(&cache).ts_submitted().is_some()).await;
                timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
                venue
                    .updates
                    .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
                    .unwrap();
                wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
            }
            let mut full = wire_order(
                "filled",
                "0.00100000",
                "0.00060000",
                "90002",
                "-0.05",
                "-0.03",
                timestamp + 20,
            );
            let mut fills = json!({"code":"0", "msg":"", "data":[
                {"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()},
                {"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
                "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
                "ts":(timestamp+25).to_string(), "fillTime":(timestamp+21).to_string()}
            ]});

            if route == "algo_child" {
                full["data"][0]["clOrdId"] = json!("");
                full["data"][0]["algoClOrdId"] = json!(CID);

                for fill in fills["data"].as_array_mut().unwrap() {
                    fill["clOrdId"] = json!("");
                }
            }

            if stage == 0 && matches!(route, "queried" | "restored") {
                *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
                *venue.fills.lock().await = fills.clone();
                query(&cache);
                wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            }

            if seed_restart {
                persisted = Some(snapshot(&cache));
                handle.stop();
                return;
            }

            if route == "terminal" {
                venue
                    .updates
                    .send(wire_order("canceled", "0", "0", "", "0", "0", timestamp))
                    .unwrap();
                wait_until(|| cached(&cache).status() == OrderStatus::Canceled).await;
            }
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":[]});
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[]});
            if late_query {
                *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
                *venue.fills.lock().await = fills.clone();
                *venue.stalled_request.lock().await = Some("order_status".to_string());
                let before_queries = venue.queries.lock().await.len();
                query(&cache);
                tokio::time::timeout(DEADLINE, async {
                    while venue.queries.lock().await.len() == before_queries {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            let before = snapshot(&cache);
            let mut malformed = full.clone();
            if let Some(value) = cumulative {
                malformed["data"][0]["accFillSz"] = json!(value);
            } else {
                malformed["data"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("accFillSz");
            }

            if invalid_fee {
                malformed["data"][0]["fillFee"] = json!("invalid");
                malformed["data"][0]["fee"] = json!("invalid");
            }

            if rest {
                *venue.report.lock().await =
                    json!({"code":"0", "msg":"", "data":malformed["data"]});
                *venue.fills.lock().await = fills.clone();
                let previous = *incomplete_observations.borrow();
                query(&cache);
                wait_until(|| *incomplete_observations.borrow() > previous).await;
            } else {
                venue.updates.send(malformed).unwrap();
            }
            sentinel(&venue, &cache, timestamp + 40).await;
            assert_eq!(snapshot(&cache), before);

            if late_query {
                let previous = *completed_queries.borrow();
                *venue.stalled_request.lock().await = None;
                venue.order_response_gate.notify_one();
                wait_until(|| *completed_queries.borrow() > previous).await;
                wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
                sentinel(&venue, &cache, timestamp + 50).await;
                *venue.report.lock().await = json!({"code":"0", "msg":"", "data":[]});
                *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[]});
            }

            if recover {
                *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
                *venue.fills.lock().await = fills;
                let previous = *completed_queries.borrow();
                let previous_statuses = *received_statuses.borrow();
                query(&cache);

                if contradictory_quantity {
                    wait_until(|| *received_statuses.borrow() > previous_statuses).await;
                    sentinel(&venue, &cache, timestamp + 60).await;
                    assert_eq!(
                        snapshot(&cache),
                        before,
                        "contradictory cumulative evidence must block economic admission"
                    );
                    handle.stop();
                    return;
                }
                wait_until(|| *completed_queries.borrow() > previous).await;
                // Ordered stream progress checks duplicate application after query dispatch;
                // shutdown independently requires completion of the fresh query witness.
                wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
                venue.updates.send(full.clone()).unwrap();
                venue.updates.send(full).unwrap();
                sentinel(&venue, &cache, timestamp + 60).await;
                let after = snapshot(&cache);
                assert_eq!(after["trade_ids"], json!(["90001", "90002"]));
                assert_eq!(after["positions"][0]["quantity"], "0.00100000");
                assert_eq!(after["positions"][0]["commissions"], json!(["0.05 USD"]));
                assert_eq!(after["positions"][0]["realized_pnl"], "-0.05 USD");
                persisted = Some(after);
            }
            handle.stop();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
            tokio::join!(node.run(), driver)
        })
        .await
        .unwrap();
        if expected_recovery || seed_restart {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Order fill recovery incomplete")
            );
        }
        msgbus::unsubscribe_any(status_topic.into(), &status_handler);
        msgbus::unsubscribe_any(topic.into(), &handler);
        msgbus::unsubscribe_any(incomplete_topic.into(), &incomplete_handler);
        node.dispose();
        drop(node);
        drop(cache);
        let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
        assert_eq!(runs.len(), 1);

        if !seed_restart {
            let restarted =
                build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
            let cache = restarted.kernel().cache.clone();

            if let Some((seed_instance, seed_run)) = &previous_run {
                restore_cache_from_sealed_run(
                    &mut cache.borrow_mut(),
                    store.path().to_path_buf(),
                    seed_instance,
                    seed_run,
                )
                .unwrap();
            }
            let result = restore_cache_from_sealed_run(
                &mut cache.borrow_mut(),
                store.path().to_path_buf(),
                &instance,
                &runs[0].run_id,
            );

            if expected_recovery {
                result.unwrap();
                assert_eq!(Some(snapshot(&cache)), persisted);
            } else {
                assert!(result.unwrap_err().to_string().contains(
                    if contradictory_quantity || unrepresentable_quantity {
                        "Unresolved persisted"
                    } else if invalid_fee || rest {
                        "Unresolved persisted incomplete fill recovery"
                    } else {
                        "Unresolved persisted standalone fill recovery"
                    }
                ));
            }
        }
        previous_run = Some((instance, runs[0].run_id.clone()));
    }
    server.abort();
}

#[rstest]
#[case::successful_matching("successful", "", None)]
#[case::successful_missing_time("successful", "missing_time", None)]
#[case::successful_different_trade("successful", "", Some("tradeId"))]
#[case::successful_different_quantity("successful", "", Some("fillSz"))]
#[case::successful_different_price("successful", "", Some("fillPx"))]
#[case::successful_different_time("successful", "", Some("fillTime"))]
#[case::successful_not_last("successful", "", Some("not_last"))]
#[case::successful_not_last_missing_time("successful", "", Some("not_last_missing_time"))]
#[case::successful_last_after_snapshot("successful", "", Some("last_after_snapshot"))]
#[case::malformed_successful_price("query.fillPx", "invalid", None)]
#[case::malformed_successful_quantity("query.fillSz", "invalid", None)]
#[case::matching("fillFee", "invalid", None)]
#[case::different_trade("fillFee", "invalid", Some("tradeId"))]
#[case::different_quantity("fillFee", "invalid", Some("fillSz"))]
#[case::different_price("fillFee", "invalid", Some("fillPx"))]
#[case::different_time("fillFee", "invalid", Some("fillTime"))]
#[case::different_liquidity("fillFee", "invalid", Some("execType"))]
#[case::money_precision("fillFee", "-0.03000000000000001", Some("retained"))]
#[case::decimal_fee_underflow("fillFee", "-0.030000000000000000000000000001", Some("retained"))]
#[case::decimal_quantity_underflow("fillSz", "0.000600000000000000000000000001", Some("retained"))]
#[case::decimal_price_underflow("fillPx", "50000.000000000000000000000000001", Some("retained"))]
#[case::decimal_quantity_overflow(
    "fillSz",
    "99999999999999999999999999999999999",
    Some("retained")
)]
#[case::decimal_price_overflow(
    "fillPx",
    "99_999_999_999_999_999_999_999_999_999_999_999",
    Some("retained")
)]
#[case::scientific_fee("fillFee", "-3e-999999999999999999999999999999999", Some("retained"))]
#[case::amount_only_conflict("fillFeeCcy", "", Some("fee"))]
#[case::amount_only_matching("fillFeeCcy", "", None)]
#[case::currency_only_conflict("fillFee", "invalid", Some("feeCcy"))]
#[case::stream_matching_client_with_omitted_query("clOrdId", CID, None)]
#[case::stream_conflicting_client_with_omitted_query("clOrdId", "OTHER", Some("client_order_id"))]
#[case::rest_matching_client_with_omitted_query("rest.clOrdId", CID, None)]
#[case::rest_conflicting_client_with_omitted_query(
    "rest.clOrdId",
    "OTHER",
    Some("client_order_id")
)]
#[case::committed_matching_client_with_omitted_query("rest_late.clOrdId", CID, None)]
#[case::committed_conflicting_client_with_omitted_query(
    "rest_late.clOrdId",
    "OTHER",
    Some("client_order_id")
)]
#[case::rest_quantity_overflow(
    "rest.fillSz",
    "99999999999999999999999999999999999",
    Some("retained")
)]
#[case::rest_price_underflow("rest.fillPx", "50000.000000000000000000000000001", Some("retained"))]
#[case::rest_scientific_quantity(
    "rest.fillSz",
    "+6E-99999999999999999999999999999999",
    Some("retained")
)]
#[tokio::test]
async fn test_unparsed_fill_preserves_known_facts_with_valid_cumulative(
    #[case] evidence_field: &str,
    #[case] evidence_value: &str,
    #[case] conflict: Option<&str>,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let queries = Rc::new(RefCell::new(0));
    let captured = queries.clone();
    let observations = Rc::new(RefCell::new(0));
    let captured_observations = observations.clone();
    let observed_identities = Rc::new(RefCell::new(Vec::new()));
    let captured_identities = observed_identities.clone();
    let omitted_query_identity = evidence_field.ends_with("clOrdId");
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if matches!(report, ExecutionReport::OrderQueryWithFills(status, fills)
            if status.venue_order_id == VenueOrderId::from(VID) && !fills.is_empty())
        {
            *captured.borrow_mut() += 1;

            if omitted_query_identity
                && let ExecutionReport::OrderQueryWithFills(status, fills) = report
            {
                assert!(status.client_order_id.is_none());
                assert!(fills.iter().all(|fill| fill.client_order_id.is_none()));
            }
        }

        if let ExecutionReport::IncompleteFill(fill) = report
            && fill.venue_order_id == VenueOrderId::from(VID)
        {
            *captured_observations.borrow_mut() += 1;
            captured_identities.borrow_mut().push(fill.client_order_id);
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let mut persisted = None;
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut full = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            timestamp + 20,
        );

        if evidence_field.starts_with("rest_late.") {
            venue
                .updates
                .send(wire_order(
                    "partially_filled",
                    "0.00040000",
                    "0.00040000",
                    "90001",
                    "-0.02",
                    "-0.02",
                    timestamp,
                ))
                .unwrap();
            venue.updates.send(full.clone()).unwrap();
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
        }
        let before = snapshot(&cache);

        if matches!(conflict, Some("not_last" | "not_last_missing_time")) {
            full["data"][0]["uTime"] = json!((timestamp + 30).to_string());
        }

        if evidence_value == "missing_time"
            || matches!(
                conflict,
                Some("not_last_missing_time" | "last_after_snapshot")
            )
        {
            full["data"][0]["fillTime"] = json!("0");
        }

        if conflict == Some("last_after_snapshot") {
            full["data"][0]["uTime"] = json!((timestamp + 20).to_string());
        }
        let mut malformed = full.clone();
        let field = evidence_field
            .strip_prefix("rest.")
            .or_else(|| evidence_field.strip_prefix("rest_late."))
            .or_else(|| evidence_field.strip_prefix("query."))
            .unwrap_or(evidence_field);
        malformed["data"][0][field] = json!(evidence_value);

        if field == "clOrdId" {
            malformed["data"][0]["accFillSz"] = json!("invalid");
            malformed["data"][0]["fillFee"] = json!("invalid");
        }

        if evidence_field == "successful" {
            // No preceding stream or malformed observation may supply the REST constraints
        } else if evidence_field.starts_with("rest.")
            || evidence_field.starts_with("rest_late.")
            || evidence_field.starts_with("query.")
        {
            if evidence_field.starts_with("rest.") {
                malformed["data"][0]["accFillSz"] = json!("invalid");
            }
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":malformed["data"]});
            query(&cache);
        } else {
            venue.updates.send(malformed).unwrap();
        }

        if evidence_field != "successful" {
            wait_until(|| *observations.borrow() > 0).await;
        }

        if field == "clOrdId" {
            assert!(
                observed_identities
                    .borrow()
                    .contains(&Some(ClientOrderId::from(evidence_value)))
            );
        }
        sentinel(&venue, &cache, timestamp + 40).await;
        assert_eq!(snapshot(&cache), before);

        let mut fills = json!({"code":"0", "msg":"", "data":[
            {"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(timestamp+5).to_string(), "fillTime":(timestamp+1).to_string()},
            {"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
            "tradeId":"90002", "billId":"80002", "fillSz":"0.00060000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.03", "feeCcy":"USD",
            "ts":(timestamp+25).to_string(), "fillTime":(timestamp+21).to_string()}
        ]});

        match conflict {
            Some("tradeId") => fills["data"][1]["tradeId"] = json!("90003"),
            Some("fillSz") => {
                fills["data"][0]["fillSz"] = json!("0.00050000");
                fills["data"][1]["fillSz"] = json!("0.00050000");
            }
            Some("fillPx") => fills["data"][1]["fillPx"] = json!("50001.0"),
            Some("fillTime") => fills["data"][1]["fillTime"] = json!((timestamp + 20).to_string()),
            Some("execType") => fills["data"][1]["execType"] = json!("M"),
            Some("fee") => fills["data"][1]["fee"] = json!("-0.04"),
            Some("feeCcy") => fills["data"][1]["feeCcy"] = json!("BTC"),
            Some("not_last" | "not_last_missing_time") => {
                fills["data"][0]["fillTime"] = json!((timestamp + 22).to_string());
            }
            Some("retained" | "last_after_snapshot" | "client_order_id") => {}
            None => {}
            Some(other) => panic!("Unknown conflict {other}"),
        }

        if field == "clOrdId" {
            full["data"][0]["clOrdId"] = json!("");
            for fill in fills["data"].as_array_mut().unwrap() {
                fill["clOrdId"] = json!("");
            }
        }

        if conflict == Some("last_after_snapshot") {
            *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
            let previous = *observations.borrow();
            query(&cache);
            wait_until(|| *observations.borrow() > previous).await;
            full["data"][0]["uTime"] = json!((timestamp + 21).to_string());
        }
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":full["data"]});
        *venue.fills.lock().await = fills;
        query(&cache);

        if conflict.is_some() {
            wait_until(|| *queries.borrow() > 0).await;
        } else {
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
        }
        sentinel(&venue, &cache, timestamp + 50).await;

        if conflict.is_some() {
            assert_eq!(snapshot(&cache), before);
        } else {
            let after = snapshot(&cache);
            assert_eq!(after["trade_ids"], json!(["90001", "90002"]));
            assert_eq!(after["positions"][0]["quantity"], "0.00100000");
            assert_eq!(after["positions"][0]["commissions"], json!(["0.05 USD"]));
            assert_eq!(after["positions"][0]["realized_pnl"], "-0.05 USD");

            // REST placeholders do not make a missing WebSocket execution time valid
            full["data"][0]["fillTime"] = json!((timestamp + 21).to_string());
            venue.updates.send(full.clone()).unwrap();
            venue.updates.send(full).unwrap();
            sentinel(&venue, &cache, timestamp + 60).await;
            assert_eq!(snapshot(&cache), after);
            persisted = Some(after);
        }
        handle.stop();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);

    if conflict.is_some() {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let result = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if conflict.is_some() {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted incomplete fill recovery")
        );
    } else {
        result.unwrap();
        assert_eq!(Some(snapshot(&cache)), persisted);
    }
    server.abort();
}

#[rstest]
#[case::order_fill_timeout("order_fills", false)]
#[case::later_algo_timeout("/api/v5/trade/orders-algo-pending", false)]
#[case::pending_sibling_timeout("/api/v5/trade/orders-pending", false)]
#[case::later_fill_page_timeout("next_fill_page", true)]
#[case::parsed_algo_timeout("later_algo_type", false)]
#[tokio::test]
async fn test_startup_timeout_retains_observed_fill_evidence_in_sealed_run(
    #[case] stalled_request: &str,
    #[case] page_observation: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let timestamp = node.kernel().clock.borrow().timestamp_ns().as_u64() / 1_000_000;
    let frame = wire_order(
        "filled",
        "0.00100000",
        "0.00100000",
        "90001",
        "-0.05",
        "-0.05",
        timestamp,
    );
    *venue.report.lock().await = json!({"code":"0", "msg":"", "data":frame["data"]});
    *venue.stalled_request.lock().await = Some(stalled_request.to_string());
    venue.bulk.store(
        !page_observation && stalled_request != "later_algo_type",
        std::sync::atomic::Ordering::Relaxed,
    );

    if stalled_request == "later_algo_type" {
        let mut response: Value = serde_json::from_str(include_str!(
            "../../test_data/http_get_orders_algo_history.json"
        ))
        .unwrap();

        for (key, value) in [
            ("algoClOrdId", CID),
            ("clOrdId", ""),
            ("ordId", VID),
            ("instId", "BTC-USD"),
            ("instType", "SPOT"),
            ("side", "buy"),
            ("posSide", "net"),
            ("sz", "0.00100000"),
            ("actualSz", "0.00100000"),
            ("ordPx", "50000.0"),
            ("actualPx", "50000.0"),
            ("state", "filled"),
            ("tdMode", "cash"),
        ] {
            response["data"][0][key] = json!(value);
        }
        response["data"][0]["cTime"] = json!(timestamp.to_string());
        response["data"][0]["uTime"] = json!(timestamp.to_string());
        *venue.report.lock().await = response;
    }

    if page_observation {
        let fills: Vec<_> = (0..100)
            .map(|index| {
                json!({
                    "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                    "tradeId":(90000+index).to_string(), "billId":(80000+index).to_string(),
                    "fillSz":"0.00001000", "fillPx":"50000.0", "side":"buy", "posSide":"net",
                    "execType":"T", "fee":"-0.0005", "feeCcy":"USD", "ts":timestamp.to_string(),
                    "fillTime":timestamp.to_string()
                })
            })
            .collect();
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":fills});
    }
    let error = tokio::time::timeout(Duration::from_secs(70), node.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("reconciliation"), "{error:#}");
    assert!(venue.submissions.lock().await.is_empty());
    assert!(
        node.kernel()
            .cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );

    if stalled_request == "order_fills" {
        assert!(
            venue
                .fill_queries
                .lock()
                .await
                .iter()
                .any(|q| q.contains_key("ordId"))
        );
    } else if page_observation {
        assert!(
            venue
                .fill_queries
                .lock()
                .await
                .iter()
                .any(|q| q.contains_key("after"))
        );
    } else {
        let stalled_request = if stalled_request == "later_algo_type" {
            "/api/v5/trade/orders-algo-pending"
        } else {
            stalled_request
        };
        assert!(
            venue
                .requests
                .lock()
                .await
                .iter()
                .any(|path| path == stalled_request)
        );
    }
    node.dispose();
    drop(node);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    if page_observation {
        let backend = RedbBackend::open_sealed(store.path(), &instance, &runs[0].run_id).unwrap();
        let entries = backend
            .scan_range(1, backend.high_watermark().unwrap(), ScanDirection::Forward)
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.payload_type.as_str() == "FillReport")
                .count(),
            100,
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.payload_type.as_str() == "IncompleteFill")
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.payload_type.as_str() == "OrderQueryWithFills")
        );
    }
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let error = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(if stalled_request == "later_algo_type" {
                "Unresolved persisted order fill recovery"
            } else {
                "Unresolved persisted incomplete fill recovery"
            }),
        "{error:#}"
    );
    server.abort();
}

#[rstest]
#[case::matching("")]
#[case::fee_conflict("fee")]
#[case::price_conflict("fillPx")]
#[case::missing_trade("tradeId")]
#[case::client_conflict("clOrdId")]
#[tokio::test]
async fn test_tracked_stream_companion_constrains_later_query(
    #[case] conflict: &str,
    #[values(None, Some(""), Some("ChildClient1"))] algo_child: Option<&str>,
    #[values(false, true)] stream_first: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let count = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if matches!(
            report,
            ExecutionReport::OrderQueryWithFills(..) | ExecutionReport::OrderFillConflict(..)
        ) {
            *count.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut final_frame = wire_order(
            "filled",
            "0.00100000",
            "0.00060000",
            "90002",
            "-0.05",
            "-0.03",
            timestamp + 10,
        );
        let mut frame = if stream_first {
            wire_order(
                "partially_filled",
                "0.00040000",
                "0.00040000",
                "90001",
                "-0.02",
                "-0.02",
                timestamp,
            )
        } else {
            final_frame.clone()
        };

        if let Some(child_client_id) = algo_child {
            for value in [&mut frame, &mut final_frame] {
                value["data"][0]["clOrdId"] = json!(child_client_id);
                value["data"][0]["algoClOrdId"] = json!(CID);
            }
        }
        frame["data"][0]["execType"] = json!("T");
        venue.updates.send(frame.clone()).unwrap();
        sentinel(&venue, &cache, timestamp + 20).await;
        assert_eq!(
            cached(&cache).filled_qty(),
            Quantity::from(if stream_first {
                "0.00040000"
            } else {
                "0.00000000"
            })
        );
        assert_eq!(cached(&cache).trade_ids().len(), usize::from(stream_first));
        let before = snapshot(&cache);
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":final_frame["data"]});
        let mut first = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":algo_child.unwrap_or(CID),
            "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
            "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
            "ts":(timestamp+1).to_string(), "fillTime":(timestamp+1).to_string()});
        let mut second = first.clone();
        for (key, value) in [
            ("tradeId", "90002"),
            ("billId", "80002"),
            ("fillSz", "0.00060000"),
            ("fee", "-0.03"),
        ] {
            second[key] = json!(value);
        }
        second["ts"] = json!((timestamp + 11).to_string());
        second["fillTime"] = second["ts"].clone();

        if !conflict.is_empty() {
            let conflicting = if stream_first {
                &mut first
            } else {
                &mut second
            };
            conflicting[conflict] = json!(match conflict {
                "fee" => "-0.04",
                "fillPx" => "50001.0",
                "tradeId" => "90003",
                "clOrdId" => "UnrelatedClient1",
                _ => unreachable!(),
            });
        }
        *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[first,second]});
        let count = *observed.borrow();
        query(&cache);

        if conflict.is_empty() {
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00100000"));
            assert_eq!(snapshot(&cache)["trade_ids"], json!(["90001", "90002"]));
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.05 USD"])
            );
            assert_eq!(
                snapshot(&cache)["positions"][0]["realized_pnl"],
                json!("-0.05 USD")
            );
        } else {
            wait_until(|| *observed.borrow() > count).await;
            assert_eq!(snapshot(&cache), before);
        }
        let after = snapshot(&cache);
        venue.updates.send(frame).unwrap();
        sentinel(&venue, &cache, timestamp + 30).await;
        assert_eq!(snapshot(&cache), after);
        handle.stop();
        after
    };
    let (result, state) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();

    if conflict.is_empty() {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    }
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if conflict.is_empty() {
        restored.unwrap();
        assert_eq!(snapshot(&restarted.kernel().cache), state);
    } else {
        let error = restored.unwrap_err();
        assert!(
            error.to_string().contains(if conflict == "clOrdId" {
                "Unresolved persisted order fill conflict"
            } else {
                "Unresolved persisted incomplete fill recovery"
            }),
            "{error:#}"
        );
    }
    server.abort();
}

#[cfg(feature = "high-precision")]
#[rstest]
#[tokio::test]
async fn test_cold_order_only_tied_closing_fills_and_restart(
    #[values(false, true)] reverse: bool,
    #[values(
        "complete",
        "child",
        "child-id",
        "missing-fill",
        "fee",
        "cross-order-tie",
        "retained-close"
    )]
    case: &str,
) {
    use nautilus_model::{accounts::Account, types::Currency};
    use rust_decimal::Decimal;

    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut instrument_response = instruments();
    instrument_response["data"][0]["tickSz"] = json!("0.01");
    *venue.instruments.lock().await = instrument_response.clone();
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        None,
    );
    let instruments: OKXResponse<OKXInstrument> =
        serde_json::from_value(instrument_response).unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_instrument(
            parse_instrument_any(&instruments.data[0], None, None, Default::default())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    let response: OKXResponse<OKXAccount> = serde_json::from_value(balance()).unwrap();
    let account = parse_account_state(
        &response.data[0],
        AccountId::from("OKX-001"),
        AccountType::Cash,
        UnixNanos::default(),
    )
    .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let wallet = cache
        .borrow()
        .account(&AccountId::from("OKX-001"))
        .unwrap()
        .balance_total(Some(Currency::USD()))
        .unwrap();
    let timestamp = node.kernel().clock.borrow().timestamp_ns().as_u64() / 1_000_000 - 2000;
    // Two cash cycles, with the final closing order split at one execution timestamp.
    let mut executions = [
        (
            CID,
            VID,
            "90001",
            "buy",
            "0.002514",
            "2386.62",
            "0.01199992536",
            0,
        ),
        (
            "QueryFills2",
            "2497956918703120387",
            "90002",
            "sell",
            "0.002514",
            "2384.70",
            "0.0119902716",
            400,
        ),
        (
            "QueryFills3",
            "2497956918703120388",
            "90003",
            "buy",
            "0.000838",
            "2387.59",
            "0.00400160084",
            793,
        ),
        (
            "QueryFills4",
            "2497956918703120389",
            "90004",
            "sell",
            "0.000449",
            "2384.70",
            "0.0021414606",
            1191,
        ),
        (
            "QueryFills4",
            "2497956918703120389",
            "90005",
            "sell",
            "0.000389",
            "2384.53",
            "0.00185516434",
            1191,
        ),
    ];

    if case == "cross-order-tie" {
        executions[2].7 = executions[3].7;
    }

    if case == "retained-close" {
        let mut closing = OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::external())
            .client_order_id(ClientOrderId::from("QueryFills4"))
            .instrument_id(InstrumentId::from(INSTRUMENT))
            .side(OrderSide::Sell)
            .quantity(Quantity::from("0.000838"))
            .build();
        closing
            .apply(TestOrderEventStubs::submitted(
                &closing,
                AccountId::from("OKX-001"),
            ))
            .unwrap();
        closing
            .apply(TestOrderEventStubs::accepted(
                &closing,
                AccountId::from("OKX-001"),
                VenueOrderId::from("2497956918703120389"),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(closing, None, Some(ClientId::from("OKX")), true)
            .unwrap();
    }
    let mut reports = Vec::new();
    let mut all_fills = Vec::new();

    for group in [
        &executions[..1],
        &executions[1..2],
        &executions[2..3],
        &executions[3..],
    ] {
        let (cid, vid, trade, side, qty, price, fee, offset) = group.last().unwrap();
        let total: Decimal = group.iter().map(|f| f.4.parse::<Decimal>().unwrap()).sum();
        let commission: Decimal = group.iter().map(|f| f.6.parse::<Decimal>().unwrap()).sum();
        let notional: Decimal = group
            .iter()
            .map(|f| f.4.parse::<Decimal>().unwrap() * f.5.parse::<Decimal>().unwrap())
            .sum();
        let total = total.to_string();
        let mut frame = wire_order(
            "filled",
            &total,
            qty,
            trade,
            &(-commission).to_string(),
            &format!("-{fee}"),
            timestamp + offset - 1,
        );

        for (key, value) in [
            ("clOrdId", cid.to_string()),
            ("ordId", vid.to_string()),
            ("side", side.to_string()),
            ("sz", total.clone()),
            ("px", price.to_string()),
            ("fillPx", price.to_string()),
            (
                "avgPx",
                (notional / total.parse::<Decimal>().unwrap()).to_string(),
            ),
            ("ordType", "ioc".to_string()),
        ] {
            frame["data"][0][key] = json!(value);
        }

        if case.starts_with("child") && *cid == CID {
            frame["data"][0]["clOrdId"] = json!("ChildClient1");
            frame["data"][0]["algoClOrdId"] = json!(CID);
            frame["data"][0]["algoId"] = json!("AlgoParent1");
        }

        if case == "fee" && *cid == "QueryFills4" {
            frame["data"][0]["fee"] = json!("-0.004");
        }
        venue.order_details.lock().await.insert(
            vid.to_string(),
            json!({"code":"0", "msg":"", "data":frame["data"]}),
        );
        reports.push(frame["data"][0].clone());
        let mut fills: Vec<_> = group.iter().map(|(cid, vid, trade, side, qty, px, fee, offset)| {
            json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":vid, "clOrdId":cid,
                "tradeId":trade, "billId":trade, "fillSz":qty, "fillPx":px,
                "side":side, "posSide":"net", "execType":"T", "fee":format!("-{fee}"), "feeCcy":"USD",
                "ts":(timestamp+offset+10).to_string(), "fillTime":(timestamp+offset).to_string()})
        }).collect();

        if case.starts_with("child") && *cid == CID {
            fills[0]["clOrdId"] = json!(if case == "child-id" {
                "UnrelatedClient1"
            } else {
                "ChildClient1"
            });
        }

        if case == "missing-fill" && *cid == "QueryFills4" {
            fills.pop();
        }

        if reverse {
            fills.reverse();
        }
        all_fills.extend(fills.clone());
        venue
            .order_fills
            .lock()
            .await
            .insert(vid.to_string(), json!({"code":"0", "msg":"", "data":fills}));
    }

    if reverse {
        reports.reverse();
        all_fills.reverse();
    }
    *venue.report.lock().await = json!({"code":"0", "msg":"", "data":reports});
    *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":all_fills});
    venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);

    let result = tokio::time::timeout(DEADLINE, node.start()).await.unwrap();
    if !matches!(case, "complete" | "child") {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(if case == "child-id" {
                "Order fill recovery incomplete at shutdown"
            } else {
                "Startup order fill recovery incomplete"
            }),
            "{error:#}"
        );
        assert!(venue.submissions.lock().await.is_empty());
        assert!(
            cache
                .borrow()
                .orders(None, None, None, None, None)
                .iter()
                .all(|order| order.filled_qty().is_zero())
        );
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert_eq!(
            cache
                .borrow()
                .account(&AccountId::from("OKX-001"))
                .unwrap()
                .balance_total(Some(Currency::USD())),
            Some(wallet)
        );
        node.dispose();
        drop(node);
        drop(cache);
        let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
        let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
        let error = restore_cache_from_sealed_run(
            &mut restarted.kernel().cache.borrow_mut(),
            store.path().to_path_buf(),
            &instance,
            &runs[0].run_id,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(if case == "child-id" {
                "Unresolved persisted order fill conflict"
            } else {
                "Unresolved persisted incomplete fill recovery"
            }),
            "{error:#}"
        );
        server.abort();
        return;
    }
    result.unwrap();
    assert!(node.handle().is_running());
    assert!(venue.submissions.lock().await.is_empty());

    for (cid, _, trade, _, qty, px, fee, offset) in executions {
        let cache = cache.borrow();
        let order = cache.order(&ClientOrderId::from(cid)).unwrap();
        assert_eq!(order.status(), OrderStatus::Filled);
        let fills: Vec<_> = order
            .events()
            .into_iter()
            .filter_map(|event| match event {
                OrderEventAny::Filled(fill) if fill.trade_id == TradeId::from(trade) => Some(fill),
                _ => None,
            })
            .collect();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].last_qty, Quantity::from(qty));
        assert_eq!(fills[0].last_px, Price::from(px));
        assert_eq!(
            fills[0].commission,
            Some(Money::from_decimal_exact(fee.parse().unwrap(), Currency::USD()).unwrap())
        );
        assert_eq!(fills[0].ts_event.as_u64(), (timestamp + offset) * 1_000_000);
    }
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        cache
            .borrow()
            .account(&AccountId::from("OKX-001"))
            .unwrap()
            .balance_total(Some(Currency::USD())),
        Some(wallet)
    );
    let before: Vec<_> = cache
        .borrow()
        .orders(None, None, None, None, None)
        .into_iter()
        .map(|order| order.cloned())
        .collect();
    node.stop().await.unwrap();
    node.dispose();
    drop(node);
    drop(cache);

    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let mut restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, restarted.start())
        .await
        .unwrap()
        .unwrap();

    for order in before {
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .events(),
            order.events()
        );
    }
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        cache
            .borrow()
            .account(&AccountId::from("OKX-001"))
            .unwrap()
            .balance_total(Some(Currency::USD())),
        Some(wallet)
    );
    assert!(venue.submissions.lock().await.is_empty());
    restarted.stop().await.unwrap();
    restarted.dispose();
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_startup_bulk_observations_precede_scoped_history(
    #[values(false, true)] start_only: bool,
    #[values("matching", "fee", "tradeId", "fillSz", "fillPx", "fillTime")] conflict_field: &str,
) {
    let conflict = conflict_field != "matching";
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    // start() leaves connection channels undriven; model a restored native account.
    let response: OKXResponse<OKXAccount> = serde_json::from_value(balance()).unwrap();
    let account = parse_account_state(
        &response.data[0],
        AccountId::from("OKX-001"),
        AccountType::Cash,
        UnixNanos::default(),
    )
    .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    let instance = node.kernel().instance_id.to_string();
    let timestamp = node.kernel().clock.borrow().timestamp_ns().as_u64() / 1_000_000 - 100;
    let mut frame = wire_order(
        "filled",
        "0.00100000",
        "0.00100000",
        "90001",
        "-0.05",
        "-0.05",
        timestamp,
    );

    match conflict_field {
        "tradeId" => frame["data"][0]["tradeId"] = json!("99999"),
        "fillSz" => frame["data"][0]["fillSz"] = json!("0.00050000"),
        "fillPx" => frame["data"][0]["fillPx"] = json!("50001.0"),
        "fillTime" => frame["data"][0]["fillTime"] = json!(timestamp.to_string()),
        "matching" | "fee" => {}
        _ => unreachable!(),
    }
    *venue.report.lock().await = json!({"code":"0", "msg":"", "data":frame["data"]});
    let first = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
        "tradeId":"90001", "billId":"80001", "fillSz":"0.00100000", "fillPx":"50000.0",
        "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
        "ts":(timestamp+1).to_string(), "fillTime":(timestamp+1).to_string()});
    let mut second = first.clone();
    if conflict_field == "fee" {
        second["fee"] = json!("-0.06");
    }
    *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[first]});
    *venue.scoped_fills.lock().await = Some(json!({"code":"0", "msg":"", "data":[second]}));
    venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);
    let handle = node.handle();
    let result = tokio::time::timeout(Duration::from_secs(70), async {
        if start_only {
            node.start().await?;
            assert!(handle.is_running());
            node.stop().await
        } else if conflict {
            node.run().await
        } else {
            let driver = async {
                wait_until(|| handle.is_running()).await;
                handle.stop();
            };
            let (result, ()) = tokio::join!(node.run(), driver);
            result
        }
    })
    .await
    .unwrap();
    assert!(venue.submissions.lock().await.is_empty());
    let state = if conflict {
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Startup order fill recovery incomplete"),
            "{error:#}"
        );
        assert!(
            node.kernel()
                .cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert!(
            node.kernel()
                .cache
                .borrow()
                .orders(None, None, None, None, None)
                .iter()
                .all(|order| order.filled_qty().is_zero())
        );
        None
    } else {
        result.unwrap();
        let state = snapshot(&node.kernel().cache);
        assert_eq!(state["trade_ids"], json!(["90001"]));
        assert_eq!(state["fills"][0]["commission"], json!("0.05 USD"));
        // Closed cash history is order-only; venue wallet snapshots already include it.
        assert_eq!(state["positions"], json!([]));
        Some(state)
    };
    node.dispose();
    drop(node);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if let Some(state) = state {
        restored.unwrap();
        assert_eq!(snapshot(&restarted.kernel().cache), state);
    } else {
        let error = restored.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unresolved persisted incomplete fill recovery"),
            "{error:#}"
        );
    }
    server.abort();
}

#[rstest]
#[case::matching("", "")]
#[case::inexact_fee("fee", "-0.05000000000000001")]
#[case::off_grid_quantity("fillSz", "0.001000001")]
#[case::off_grid_price("fillPx", "50000.01")]
#[case::wrong_side("side", "sell")]
#[case::invalid_quantity("fillSz", "invalid")]
#[case::zero_quantity("fillSz", "0")]
#[case::negative_quantity("fillSz", "-0.00100000")]
#[case::empty_quantity("fillSz", "")]
#[tokio::test]
async fn test_startup_fill_conflict_survives_completed_cache_snapshot(
    #[values(false, true)] start_only: bool,
    #[values(false, true)] account_wide: bool,
    #[values(false, true)] discovered: bool,
    #[values("", CID)] client_order_id: &str,
    #[case] field: &str,
    #[case] value: &str,
) {
    let conflict = !field.is_empty();
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let account_id = AccountId::from("OKX-001");
    let timestamp = node.kernel().clock.borrow().timestamp_ns().as_u64() / 1_000_000 - 100;
    let response: OKXResponse<OKXAccount> = serde_json::from_value(balance()).unwrap();
    let account = parse_account_state(
        &response.data[0],
        account_id,
        AccountType::Cash,
        UnixNanos::default(),
    )
    .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    let instrument = node
        .kernel()
        .cache
        .borrow()
        .instrument(&InstrumentId::from(INSTRUMENT))
        .unwrap()
        .clone();

    // Model a completed order restored from a prior native cache snapshot
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .strategy_id(StrategyId::from(STRATEGY))
        .client_order_id(ClientOrderId::from(CID))
        .instrument_id(InstrumentId::from(INSTRUMENT))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.00100000"))
        .price(Price::from("50000.0"))
        .build();
    order
        .apply(TestOrderEventStubs::submitted(&order, account_id))
        .unwrap();
    order
        .apply(TestOrderEventStubs::accepted(
            &order,
            account_id,
            VenueOrderId::from(VID),
        ))
        .unwrap();
    let fill = OrderFilled::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(VID),
        account_id,
        TradeId::from("90001"),
        OrderSide::Buy,
        OrderType::Limit,
        order.quantity(),
        Price::from("50000.0"),
        instrument.quote_currency(),
        LiquiditySide::Taker,
        UUID4::new(),
        UnixNanos::from((timestamp + 1) * 1_000_000),
        UnixNanos::from((timestamp + 1) * 1_000_000),
        false,
        None,
        Some(Money::from_str_exact("0.05 USD").unwrap()),
        None,
    );
    order.apply(OrderEventAny::Filled(fill)).unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("OKX")), true)
        .unwrap();
    let frame = wire_order(
        "filled",
        "0.00100000",
        "0.00100000",
        "90001",
        "-0.05",
        "-0.05",
        timestamp,
    );
    *venue.report.lock().await = json!({"code":"0", "msg":"", "data":frame["data"]});
    if discovered {
        venue.order_details.lock().await.insert(
            VID.to_string(),
            json!({"code":"0", "msg":"", "data":frame["data"]}),
        );
        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":[]});
    }
    let good = json!({"instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":client_order_id,
        "tradeId":"90001", "billId":"80001", "fillSz":"0.00100000", "fillPx":"50000.0",
        "side":"buy", "posSide":"net", "execType":"T", "fee":"-0.05", "feeCcy":"USD",
        "ts":(timestamp+1).to_string(), "fillTime":(timestamp+1).to_string()});
    let mut scoped = good.clone();
    if conflict {
        scoped[field] = json!(value);
    }
    let (bulk, scoped) = if account_wide {
        (scoped, good)
    } else {
        (good, scoped)
    };
    let mut account_rows = vec![bulk];
    for quantity in ["", "0", "-0.001"] {
        let mut non_execution = account_rows[0].clone();
        non_execution["tradeId"] = json!("");
        non_execution["fillSz"] = json!(quantity);
        account_rows.push(non_execution);
    }
    *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":account_rows});
    *venue.scoped_fills.lock().await = Some(json!({"code":"0", "msg":"", "data":[scoped]}));
    venue.bulk.store(true, std::sync::atomic::Ordering::Relaxed);
    let handle = node.handle();
    let result = tokio::time::timeout(Duration::from_secs(70), async {
        if start_only {
            node.start().await?;
            node.stop().await
        } else if conflict {
            node.run().await
        } else {
            let driver = async {
                wait_until(|| handle.is_running()).await;
                handle.stop();
            };
            let (result, ()) = tokio::join!(node.run(), driver);
            result
        }
    })
    .await
    .unwrap();

    if conflict {
        let error = result.unwrap_err();

        if !account_wide {
            assert!(
                error
                    .to_string()
                    .contains("Startup order fill recovery incomplete"),
                "{error:#}"
            );
        }
    } else {
        result.unwrap();
    }
    assert!(venue.submissions.lock().await.is_empty());
    node.dispose();
    drop(node);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let backend = RedbBackend::open_sealed(store.path(), &instance, &runs[0].run_id).unwrap();
    let entries = backend
        .scan_range(1, backend.high_watermark().unwrap(), ScanDirection::Forward)
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.payload_type.as_str() == "OrderFillConflict")
            .count(),
        usize::from(conflict),
    );

    let mut restored = Cache::default();
    restored.add_instrument(instrument).unwrap();
    restored
        .add_order(order, None, Some(ClientId::from("OKX")), true)
        .unwrap();
    let reader = nautilus_event_store::EventStoreReader::new(backend);
    let replay = nautilus_event_store::replay_cache_snapshot_tail(&mut restored, &reader);
    if conflict {
        let error = replay.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unresolved persisted order fill conflict"),
            "{error:#}"
        );
    } else {
        replay.unwrap();
        assert_eq!(
            restored.order(&ClientOrderId::from(CID)).unwrap().status(),
            OrderStatus::Filled,
        );
    }
    server.abort();
}

#[rstest]
#[case::first_matching(false, false, false, false, false)]
#[case::first_conflicting(false, false, false, true, false)]
#[case::contiguous_matching(true, false, false, false, false)]
#[case::contiguous_conflicting(true, false, false, true, false)]
#[case::missed_prefix_matching(false, true, false, false, false)]
#[case::missed_prefix_conflicting(false, true, false, true, false)]
#[case::rest_matching(false, true, true, false, false)]
#[case::rest_conflicting(false, true, true, true, false)]
#[case::mixed_contiguous_matching(true, false, false, false, true)]
#[case::mixed_contiguous_conflicting(true, false, false, true, true)]
#[case::mixed_missed_prefix_matching(false, true, false, false, true)]
#[case::mixed_missed_prefix_conflicting(false, true, false, true, true)]
#[case::mixed_rest_matching(false, true, true, false, true)]
#[case::mixed_rest_conflicting(false, true, true, true, true)]
#[tokio::test]
async fn test_cumulative_commission_admission_and_restart(
    #[case] prefix: bool,
    #[case] recovery: bool,
    #[case] rest: bool,
    #[case] conflict: bool,
    #[case] mixed_sell: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    if mixed_sell {
        let mut funded = venue.balance.lock().await;
        let mut base = funded["data"][0]["details"][0].clone();
        base["ccy"] = json!("BTC");
        for field in ["availBal", "availEq", "cashBal", "eq"] {
            base[field] = json!("1");
        }
        base["frozenBal"] = json!("0");
        funded["data"][0]["details"]
            .as_array_mut()
            .unwrap()
            .push(base);
    }
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node_with_store(
        addr,
        events.clone(),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );

    if mixed_sell {
        let mut strategy =
            try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner()).unwrap();
        strategy.side = OrderSide::Sell;
    }
    let side = if mixed_sell { "sell" } else { "buy" };
    let last_role = if mixed_sell { "M" } else { "T" };
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed_queries = Rc::new(RefCell::new(0));
    let count = observed_queries.clone();
    let observed_fees = Rc::new(RefCell::new(0));
    let fee_count = observed_fees.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if matches!(report, ExecutionReport::OrderQueryWithFills(_, fills) if !fills.is_empty()) {
            *count.borrow_mut() += 1;
        }

        if matches!(report, ExecutionReport::OrderFeeEvidence(_)) {
            *fee_count.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        let mut live = wire_order("live", "0", "0", "", "0", "0", timestamp);
        live["data"][0]["side"] = json!(side);
        venue.updates.send(live).unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;

        if prefix {
            let mut partial = wire_order(
                "partially_filled",
                "0.00040000",
                "0.00040000",
                "90001",
                "-0.02",
                "-0.02",
                timestamp,
            );
            partial["data"][0]["side"] = json!(side);
            partial["data"][0]["execType"] = json!("T");
            partial["data"][0]["rebate"] = json!("0");
            venue.updates.send(partial).unwrap();
            wait_until(|| cached(&cache).filled_qty() == Quantity::from("0.00040000")).await;
        }
        let before = snapshot(&cache);
        let split = prefix || recovery;
        let mut frame = wire_order(
            "filled",
            "0.00100000",
            if split { "0.00060000" } else { "0.00100000" },
            "90002",
            if conflict { "-0.07" } else { "-0.05" },
            if split { "-0.03" } else { "-0.05" },
            timestamp + 20,
        );
        frame["data"][0]["side"] = json!(side);
        frame["data"][0]["execType"] = json!(last_role);
        frame["data"][0]["rebate"] = json!("0");

        if !rest {
            venue.updates.send(frame.clone()).unwrap();
            sentinel(&venue, &cache, timestamp + 40).await;
            if conflict || recovery {
                assert_eq!(snapshot(&cache), before);
            }
        }

        if recovery {
            *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
            *venue.fills.lock().await = json!({"code":"0","msg":"","data":[
                {"instType":"SPOT","instId":"BTC-USD","ordId":VID,"clOrdId":CID,"tradeId":"90001","billId":"80001","fillSz":"0.00040000","fillPx":"50000.0","side":side,"posSide":"net","execType":"T","fee":"-0.02","feeCcy":"USD","ts":(timestamp+5).to_string(),"fillTime":(timestamp+1).to_string()},
                {"instType":"SPOT","instId":"BTC-USD","ordId":VID,"clOrdId":CID,"tradeId":"90002","billId":"80002","fillSz":"0.00060000","fillPx":"50000.0","side":side,"posSide":"net","execType":last_role,"fee":"-0.03","feeCcy":"USD","ts":(timestamp+25).to_string(),"fillTime":(timestamp+21).to_string()}
            ]});
            query(&cache);

            if conflict {
                wait_until(|| *observed_queries.borrow() > 0).await;
            } else {
                wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            }
        }
        sentinel(&venue, &cache, timestamp + 50).await;
        assert!(*observed_fees.borrow() > 0);

        if conflict {
            assert_eq!(snapshot(&cache), before);
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(|event| matches!(event, OrderEventAny::Filled(_)))
                    .count(),
                usize::from(prefix)
            );
        } else {
            assert_eq!(cached(&cache).status(), OrderStatus::Filled);
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.05 USD"])
            );
            assert_eq!(
                snapshot(&cache)["positions"][0]["realized_pnl"],
                "-0.05 USD"
            );
        }
        let persisted = snapshot(&cache);
        venue.updates.send(frame.clone()).unwrap();
        venue.updates.send(frame).unwrap();
        sentinel(&venue, &cache, timestamp + 60).await;
        assert_eq!(snapshot(&cache), persisted);
        handle.stop();
        persisted
    };
    let (result, state) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);

    if conflict {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    assert_eq!(runs.len(), 1);
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let cache = restarted.kernel().cache.clone();
    let restored = restore_cache_from_sealed_run(
        &mut cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if conflict {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted")
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&cache), state);
    }
    server.abort();
}

#[rstest]
#[case::ordinary("normal", "90001", OrderSide::Buy, true)]
#[case::liquidation_buy("full_liquidation", "-90001", OrderSide::Buy, true)]
#[case::liquidation_sell("full_liquidation", "-90001", OrderSide::Sell, true)]
#[case::partial_liquidation_sell("partial_liquidation", "-90001", OrderSide::Sell, true)]
#[case::adl_sell("adl", "-90002", OrderSide::Sell, true)]
#[case::ordinary_limit("normal", "90001", OrderSide::Buy, false)]
#[tokio::test]
async fn test_status_only_order_update_recovers_actual_history_and_replays(
    #[case] category: &str,
    #[case] trade_id: &str,
    #[case] side: OrderSide,
    #[case] market: bool,
    #[values(false, true)] query_first: bool,
    #[values(false, true)] conflicting_last_fill: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    if side == OrderSide::Sell {
        let mut funded = venue.balance.lock().await;
        let mut base = funded["data"][0]["details"][0].clone();
        base["ccy"] = json!("BTC");
        for field in ["availBal", "availEq", "cashBal", "eq"] {
            base[field] = json!("1");
        }
        base["frozenBal"] = json!("0");
        funded["data"][0]["details"]
            .as_array_mut()
            .unwrap()
            .push(base);
    }
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    {
        let mut strategy =
            try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner()).unwrap();
        strategy.market = market;
        strategy.side = side;
    }
    let wire_side = if side == OrderSide::Sell {
        "sell"
    } else {
        "buy"
    };
    let exec_type = if category == "normal" { "T" } else { "" };
    let quote = QuoteTick::new(
        InstrumentId::from(INSTRUMENT),
        Price::from("50000.0"),
        Price::from("50000.0"),
        Quantity::from("1.00000000"),
        Quantity::from("1.00000000"),
        node.kernel().clock().borrow().timestamp_ns(),
        node.kernel().clock().borrow().timestamp_ns(),
    );
    node.kernel().cache.borrow_mut().add_quote(quote).unwrap();
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let captured = observed.clone();
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            *captured.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let completed_queries = Rc::new(RefCell::new(0));
    let captured_queries = completed_queries.clone();
    let query_topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    let query_handler = ShareableMessageHandler::from_any(move |message| {
        if let Some(ExecutionReport::OrderQueryWithFills(report, fills)) =
            message.downcast_ref::<ExecutionReport>()
            && report.client_order_id == Some(ClientOrderId::from(CID))
            && !fills.is_empty()
        {
            *captured_queries.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_any(query_topic.into(), query_handler.clone(), None);
    let driver = async {
        wait_until(|| {
            cache
                .borrow()
                .order(&ClientOrderId::from(CID))
                .is_some_and(|o| o.ts_submitted().is_some())
        })
        .await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        let mut live = wire_order("live", "0", "0", "", "0", "0", timestamp);
        live["data"][0]["side"] = json!(wire_side);
        live["data"][0]["ordType"] = json!(if market { "market" } else { "limit" });
        live["data"][0]["px"] = json!(if market { "" } else { "50000.0" });
        venue.updates.send(live).unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let mut complete = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            if category == "normal" { trade_id } else { "0" },
            "-0.05",
            "-0.05",
            timestamp,
        );
        complete["data"][0]["side"] = json!(wire_side);
        complete["data"][0]["execType"] = json!(exec_type);
        complete["data"][0]["ordType"] = json!(if market { "market" } else { "limit" });
        complete["data"][0]["category"] = json!(category);
        complete["data"][0]["px"] = json!(if market { "" } else { "50000.0" });
        *venue.report.lock().await = json!({"code":"0","msg":"","data":complete["data"]});
        *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
            "instType":"SPOT","instId":"BTC-USD","ordId":VID,"clOrdId":CID,
            "tradeId":trade_id,"billId":"80001","fillSz":"0.00100000","fillPx":"50000.0",
            "side":wire_side,"posSide":"net","execType":exec_type,"fee":"-0.05","feeCcy":"USD",
            "ts":(timestamp+1).to_string(),"fillTime":(timestamp+1).to_string()
        }]});

        if query_first {
            query(&cache);
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
        }
        let before = snapshot(&cache);
        let mut notice = complete;

        for (key, value) in [
            ("tradeId", ""),
            ("fillSz", "0.00000000"),
            ("fillPx", ""),
            ("fillTime", "0"),
            ("fillFee", ""),
            ("fillPnl", ""),
            ("execType", ""),
        ] {
            if category == "normal" {
                notice["data"][0][key] = json!(value);
            }
        }

        if conflicting_last_fill {
            notice["data"][0]["fillSz"] = json!("0.00010000");
        }
        venue.updates.send(notice.clone()).unwrap();
        sentinel(&venue, &cache, timestamp + 10).await;
        assert_eq!(snapshot(&cache), before);
        let count = *observed.borrow();
        let completed_count = *completed_queries.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() > count).await;
        // Raw detail arrives before its fill history. Await native recovery, or
        // the retained conflicting bundle, before the next notice or shutdown.
        if conflicting_last_fill {
            wait_until(|| *completed_queries.borrow() > completed_count).await;
        } else {
            wait_until(|| {
                !cache.borrow().has_pending_order_executions(
                    StrategyId::from(STRATEGY),
                    Some(InstrumentId::from(INSTRUMENT)),
                )
            })
            .await;
        }

        if conflicting_last_fill {
            assert_eq!(snapshot(&cache), before);
        } else {
            wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
            assert_eq!(snapshot(&cache)["trade_ids"], json!([trade_id]));
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.05 USD"])
            );
        }
        let state = snapshot(&cache);
        venue.updates.send(notice).unwrap();
        sentinel(&venue, &cache, timestamp + 20).await;
        assert_eq!(snapshot(&cache), state);
        // A placeholder stream observation requires query authority after its receipt,
        // including a repeated update whose unavailable trade ID cannot prove duplication.
        let count = *observed.borrow();
        let completed_count = *completed_queries.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() > count).await;
        // Raw detail arrives before its fill history. Await native recovery, or
        // the retained conflicting bundle, before the next notice or shutdown.
        if conflicting_last_fill {
            wait_until(|| *completed_queries.borrow() > completed_count).await;
        } else {
            wait_until(|| {
                !cache.borrow().has_pending_order_executions(
                    StrategyId::from(STRATEGY),
                    Some(InstrumentId::from(INSTRUMENT)),
                )
            })
            .await;
        }
        assert_eq!(snapshot(&cache), state);
        handle.stop();
        state
    };
    let (result, state) =
        tokio::time::timeout(DEADLINE, async { tokio::join!(node.run(), driver) })
            .await
            .unwrap();

    if conflicting_last_fill {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    msgbus::unsubscribe_any(topic.into(), &handler);
    msgbus::unsubscribe_any(query_topic.into(), &query_handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if conflicting_last_fill {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted incomplete fill recovery")
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&restarted.kernel().cache), state);
    }
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_individual_fill_admission_preserves_observed_execution_facts(
    #[values("incomplete", "standalone", "companion")] observation: &str,
    #[values(false, true)] report_ingress: bool,
    #[values(false, true)] conflict: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut node = build_node_with_store(
        addr,
        events.clone(),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(0));
    let count = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID)) {
            *count.borrow_mut() += 1;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let before = snapshot(&cache);
        let order = cached(&cache);
        let time = UnixNanos::from((timestamp + 1) * 1_000_000);
        let fill = FillReport::new(
            order.account_id().unwrap(),
            order.instrument_id(),
            order.venue_order_id().unwrap(),
            TradeId::from("90001"),
            order.order_side(),
            order.quantity(),
            Price::from("50000.0"),
            Money::from("0.05 USD"),
            LiquiditySide::Taker,
            Some(order.client_order_id()),
            None,
            time,
            time,
            None,
        );
        let frame = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            "90001",
            "-0.05",
            "-0.05",
            timestamp,
        );

        if observation == "incomplete" {
            let mut malformed = frame.clone();
            malformed["data"][0]["fillFeeCcy"] = json!("");
            if conflict {
                malformed["data"][0]["fillPx"] = json!("50001.0");
            }
            venue.updates.send(malformed).unwrap();
        } else {
            let mut previous = fill.clone();
            if conflict {
                previous.last_px = Price::from("50001.0");
            }
            let evidence = if observation == "standalone" {
                ExecutionReport::FillEvidence(Box::new(previous))
            } else {
                let status = OrderStatusReport::new(
                    order.account_id().unwrap(),
                    order.instrument_id(),
                    Some(order.client_order_id()),
                    order.venue_order_id().unwrap(),
                    Some(order.order_side()),
                    order.order_type(),
                    order.time_in_force(),
                    OrderStatus::Filled,
                    order.quantity(),
                    order.quantity(),
                    time,
                    time,
                    time,
                    None,
                );
                ExecutionReport::OrderFillEvidence(Box::new(status), vec![previous])
            };
            get_exec_event_sender()
                .send(ExecutionEvent::Report(evidence))
                .unwrap();
        }
        sentinel(&venue, &cache, timestamp + 10).await;
        assert_eq!(snapshot(&cache), before);

        if report_ingress {
            get_exec_event_sender()
                .send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
                    fill,
                ))))
                .unwrap();
        } else {
            venue.updates.send(frame.clone()).unwrap();
        }
        sentinel(&venue, &cache, timestamp + 20).await;

        if conflict {
            assert_eq!(
                snapshot(&cache),
                before,
                "conflicting fill must not change orders or positions"
            );
            assert!(
                events
                    .borrow()
                    .iter()
                    .all(|event| !matches!(event, OrderEventAny::Filled(_)))
            );
        } else {
            assert_eq!(cached(&cache).status(), OrderStatus::Filled);
            assert_eq!(snapshot(&cache)["trade_ids"], json!(["90001"]));
            assert_eq!(
                snapshot(&cache)["positions"][0]["commissions"],
                json!(["0.05 USD"])
            );
        }
        *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
        *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
            "instType":"SPOT","instId":"BTC-USD","ordId":VID,"clOrdId":CID,
            "tradeId":"90001","billId":"80001","fillSz":"0.00100000","fillPx":"50000.0",
            "side":"buy","posSide":"net","execType":"T","fee":"-0.05","feeCcy":"USD",
            "ts":(timestamp+1).to_string(),"fillTime":(timestamp+1).to_string()
        }]});
        let state = snapshot(&cache);
        let count = *observed.borrow();
        query(&cache);
        wait_until(|| *observed.borrow() > count).await;
        sentinel(&venue, &cache, timestamp + 30).await;
        assert_eq!(snapshot(&cache), state);
        handle.stop();
        state
    };
    let (result, state) =
        tokio::time::timeout(DEADLINE, async { tokio::join!(node.run(), driver) })
            .await
            .unwrap();

    if conflict {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    msgbus::unsubscribe_any(topic.into(), &handler);
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if conflict {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted")
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&restarted.kernel().cache), state);
    }
    server.abort();
}

#[rstest]
#[tokio::test]
async fn test_cash_missing_rebate_requires_actual_fill_history(
    #[values("omitted", "empty", "zero", "rest_empty", "rest_empty_conflict")] rebate: &str,
    #[values(false, true)] recover: bool,
) {
    let store = tempfile::TempDir::new().unwrap();
    let (addr, venue, server) = start_venue().await;
    let mut node = build_node_with_store(
        addr,
        Rc::new(RefCell::new(Vec::new())),
        QuerySource::Direct,
        Some(EventStoreConfig {
            base_dir: store.path().to_path_buf(),
            retention: RetentionMode::Full,
            ..Default::default()
        }),
        Some(Rc::new(RefCell::new(None))),
    );
    let instance = node.kernel().instance_id.to_string();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    let unknown_rebate = rebate != "zero";
    let rest_empty = rebate.starts_with("rest_empty");
    let conflict = rebate == "rest_empty_conflict";
    let held = (unknown_rebate && !recover) || conflict;
    let queried = Rc::new(RefCell::new(false));
    let observed_query = queried.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &ExecutionReport| {
        if let ExecutionReport::OrderQueryWithFills(report, fills) = report
            && report.client_order_id == Some(ClientOrderId::from(CID))
            && report.filled_qty.is_positive()
            && !fills.is_empty()
        {
            assert_eq!(fills.len(), 1);
            *observed_query.borrow_mut() = true;
        }
    });
    let topic = MessagingSwitchboard::reconciliation_raw_order_query_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let driver = async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        venue
            .updates
            .send(wire_order("live", "0", "0", "", "0", "0", timestamp))
            .unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let before = snapshot(&cache);
        let mut frame = wire_order(
            "filled",
            "0.00100000",
            "0.00100000",
            "90001",
            "0",
            "0",
            timestamp,
        );
        let raw = frame["data"][0].as_object_mut().unwrap();
        raw.remove("fillFee");
        raw.insert("execType".into(), json!("M"));
        if rebate == "omitted" {
            raw.remove("rebate");
            raw.remove("rebateCcy");
        } else {
            raw.insert(
                "rebate".into(),
                json!(if rebate == "empty" || rest_empty {
                    ""
                } else {
                    "0"
                }),
            );
            raw.insert("rebateCcy".into(), json!("USD"));
        }
        venue.updates.send(frame.clone()).unwrap();
        sentinel(&venue, &cache, timestamp + 10).await;

        if unknown_rebate {
            assert_eq!(
                snapshot(&cache),
                before,
                "unavailable rebate is not proof of zero commission"
            );
        } else {
            assert_eq!(cached(&cache).status(), OrderStatus::Filled);
            assert_eq!(
                snapshot(&cache)["fills"][0]["commission"],
                json!("0.00 USD")
            );
        }

        if recover {
            let actual_rebate = if unknown_rebate && (!rest_empty || conflict) {
                "0.01"
            } else {
                "0"
            };
            frame["data"][0]["rebate"] = json!(if rest_empty { "" } else { actual_rebate });
            frame["data"][0]["rebateCcy"] = json!("USD");
            *venue.report.lock().await = json!({"code":"0","msg":"","data":frame["data"]});
            *venue.fills.lock().await = json!({"code":"0","msg":"","data":[{
                "instType":"SPOT","instId":"BTC-USD","ordId":VID,"clOrdId":CID,
                "tradeId":"90001","billId":"80001","fillSz":"0.00100000","fillPx":"50000.0",
                "side":"buy","posSide":"net","execType":"M","fee":actual_rebate,"feeCcy":"USD",
                "ts":(timestamp+1).to_string(),"fillTime":(timestamp+1).to_string()
            }]});
            query(&cache);

            if conflict {
                wait_until(|| *queried.borrow()).await;
                assert_eq!(snapshot(&cache), before);
            }

            if unknown_rebate && !conflict {
                wait_until(|| cached(&cache).status() == OrderStatus::Filled).await;
                assert_eq!(
                    snapshot(&cache)["fills"][0]["commission"],
                    json!(if rest_empty { "0.00 USD" } else { "-0.01 USD" })
                );
                assert_eq!(
                    snapshot(&cache)["positions"][0]["realized_pnl"],
                    json!(if rest_empty { "0.00 USD" } else { "0.01 USD" })
                );
            }
        }
        let state = snapshot(&cache);
        handle.stop();
        state
    };
    let (result, state) =
        tokio::time::timeout(DEADLINE, async { tokio::join!(node.run(), driver) })
            .await
            .unwrap();
    msgbus::unsubscribe_any(topic.into(), &handler);

    if held {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
    } else {
        result.unwrap();
    }
    node.dispose();
    drop(node);
    drop(cache);
    let runs = RedbBackend::list_runs(store.path(), &instance).unwrap();
    let restarted = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    let restored = restore_cache_from_sealed_run(
        &mut restarted.kernel().cache.borrow_mut(),
        store.path().to_path_buf(),
        &instance,
        &runs[0].run_id,
    );

    if held {
        assert!(
            restored
                .unwrap_err()
                .to_string()
                .contains("Unresolved persisted")
        );
    } else {
        restored.unwrap();
        assert_eq!(snapshot(&restarted.kernel().cache), state);
    }
    server.abort();
}

#[rstest]
#[case::canceled(false)]
#[case::expired(true)]
#[tokio::test]
async fn test_terminal_evidence_waits_for_cross_transport_fill_recovery(
    #[case] expired: bool,
    #[values(false, true)] contradictory: bool,
) {
    let recover = !contradictory;
    let (addr, venue, server) = start_venue().await;
    {
        let mut balance = venue.balance.lock().await;
        let mut base = balance["data"][0]["details"][0].clone();
        base["ccy"] = json!("BTC");

        for field in ["availBal", "availEq", "cashBal", "eq"] {
            base[field] = json!("0.00100000");
        }
        base["frozenBal"] = json!("0");
        balance["data"][0]["details"]
            .as_array_mut()
            .unwrap()
            .push(base);
    }
    let mut node = build_node(addr, Rc::new(RefCell::new(Vec::new())), QuerySource::Direct);
    node.kernel()
        .exec_engine
        .borrow_mut()
        .register_oms_type(StrategyId::from(STRATEGY), OmsType::Netting);
    let now = node.kernel().clock().borrow().timestamp_ns();
    node.kernel()
        .cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            InstrumentId::from(INSTRUMENT),
            Price::from("50000.0"),
            Price::from("50000.0"),
            Quantity::from("1.00000000"),
            Quantity::from("1.00000000"),
            now,
            now,
        ))
        .unwrap();
    let cache = node.kernel().cache.clone();
    let handle = node.handle();
    {
        let mut strategy =
            try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner()).unwrap();
        strategy.side = OrderSide::Sell;
        strategy.core.config.manage_stop = true;
        strategy.core.config.market_exit_reduce_only = false;
    }
    let observed = Rc::new(RefCell::new(false));
    let captured = observed.clone();
    let handler = ShareableMessageHandler::from_typed(move |report: &OrderStatusReport| {
        if report.client_order_id == Some(ClientOrderId::from(CID))
            && report.filled_qty == Quantity::from("0.00040000")
        {
            *captured.borrow_mut() = true;
        }
    });
    msgbus::subscribe_any(
        MessagingSwitchboard::reconciliation_raw_order_status_report_topic().into(),
        handler.clone(),
        None,
    );
    let driver = Box::pin(async {
        wait_until(|| cache.borrow().order_exists(&ClientOrderId::from(CID))).await;
        wait_until(|| cached(&cache).ts_submitted().is_some()).await;
        let timestamp = cached(&cache).ts_submitted().unwrap().as_u64() / 1_000_000;
        let mut accepted = wire_order("live", "0", "0", "", "0", "0", timestamp);
        accepted["data"][0]["side"] = json!("sell");
        venue.updates.send(accepted).unwrap();
        wait_until(|| cached(&cache).status() == OrderStatus::Accepted).await;
        let instrument = cache
            .borrow()
            .instrument(&InstrumentId::from(INSTRUMENT))
            .unwrap()
            .clone();
        let position_id = PositionId::from("BTC-USD.OKX-QUERY-001");
        let position = Position::new(
            &instrument,
            OrderFilledSpec::builder()
                .trader_id(TraderId::from("TESTER-001"))
                .strategy_id(StrategyId::from(STRATEGY))
                .instrument_id(instrument.id())
                .client_order_id(ClientOrderId::from("OPENING"))
                .account_id(AccountId::from("OKX-001"))
                .position_id(position_id)
                .order_side(OrderSide::Buy)
                .last_qty(Quantity::from("0.00100000"))
                .last_px(Price::from("50000.0"))
                .currency(instrument.quote_currency())
                .commission(Money::zero(instrument.quote_currency()))
                .ts_event(UnixNanos::from((timestamp - 1) * 1_000_000))
                .build(),
        );
        cache
            .borrow_mut()
            .add_position(&position, OmsType::Netting)
            .unwrap();
        let mut canceled = wire_order(
            "canceled",
            "0.00040000",
            "0",
            "",
            "-0.02",
            "0",
            timestamp + 20,
        );
        canceled["data"][0]["side"] = json!("sell");
        canceled["data"][0]["cTime"] = json!(timestamp.to_string());
        canceled["data"][0]["fillPx"] = json!("0");
        canceled["data"][0]["rebate"] = json!("0");

        if expired {
            canceled["data"][0]["cancelSource"] = json!("5");
        }

        *venue.report.lock().await = json!({"code":"0", "msg":"", "data":canceled["data"]});
        query(&cache);
        wait_until(|| *observed.borrow()).await;
        assert!(!cached(&cache).is_closed());
        let mut zero = wire_order(
            "canceled",
            "0",
            "0",
            "",
            "0",
            "0",
            timestamp + if contradictory { 30 } else { 1 },
        );
        zero["data"][0]["side"] = json!("sell");
        zero["data"][0]["cTime"] = json!(timestamp.to_string());
        zero["data"][0]["fillPx"] = json!("0");

        if expired {
            zero["data"][0]["cancelSource"] = json!("5");
        }
        venue.updates.send(zero).unwrap();
        sentinel(&venue, &cache, timestamp + 40).await;
        assert!(!cached(&cache).is_closed());
        assert!(cached(&cache).filled_qty().is_zero());
        {
            let mut strategy =
                try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner())
                    .unwrap();
            assert!(!Strategy::stop(&mut *strategy));
            assert!(strategy.is_exiting());
        }
        // Let the real native managed-exit timer run while history is unavailable.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(venue.submissions.lock().await.len(), 1);
        assert!(!cached(&cache).is_closed());
        assert_eq!(
            cache.borrow().position(&position_id).unwrap().quantity,
            Quantity::from("0.00100000")
        );

        if contradictory {
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[{
                "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"sell", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+10).to_string(), "fillTime":(timestamp+5).to_string()
            }]});
            query(&cache);
            tokio::time::sleep(Duration::from_millis(350)).await;
            assert!(!cached(&cache).is_closed());
            assert_eq!(venue.submissions.lock().await.len(), 1);
        }

        if recover {
            *venue.fills.lock().await = json!({"code":"0", "msg":"", "data":[{
                "instType":"SPOT", "instId":"BTC-USD", "ordId":VID, "clOrdId":CID,
                "tradeId":"90001", "billId":"80001", "fillSz":"0.00040000", "fillPx":"50000.0",
                "side":"sell", "posSide":"net", "execType":"T", "fee":"-0.02", "feeCcy":"USD",
                "ts":(timestamp+10).to_string(), "fillTime":(timestamp+5).to_string()
            }]});
            query(&cache);
            wait_until(|| cached(&cache).is_closed()).await;
            assert_eq!(
                cached(&cache).status(),
                if expired {
                    OrderStatus::Expired
                } else {
                    OrderStatus::Canceled
                }
            );
            assert_eq!(cached(&cache).filled_qty(), Quantity::from("0.00040000"));
            tokio::time::timeout(DEADLINE, async {
                while venue.submissions.lock().await.len() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let submissions = venue.submissions.lock().await.clone();
            assert_eq!(submissions.len(), 2);
            let close = &submissions[1]["args"][0];
            assert_eq!(close["side"], "sell");
            assert_eq!(close["ordType"], "market");
            assert_eq!(
                close["sz"].as_str().unwrap().parse::<Quantity>().unwrap(),
                Quantity::from("0.00060000")
            );
            let close_id = ClientOrderId::from(close["clOrdId"].as_str().unwrap());
            wait_until(|| {
                cache
                    .borrow()
                    .order(&close_id)
                    .is_some_and(|o| o.ts_submitted().is_some())
            })
            .await;
            let close_order = cache.borrow().order(&close_id).unwrap().clone();
            let mut filled = wire_order(
                "filled",
                "0.00060000",
                "0.00060000",
                "90002",
                "-0.03",
                "-0.03",
                close_order.ts_submitted().unwrap().as_u64() / 1_000_000 + 1,
            );
            filled["data"][0]["side"] = json!("sell");
            filled["data"][0]["ordType"] = json!("market");
            filled["data"][0]["sz"] = json!("0.00060000");
            filled["data"][0]["clOrdId"] = json!(close_id.as_str());
            filled["data"][0]["ordId"] = json!((VID.parse::<u64>().unwrap() + 2).to_string());
            filled["data"][0]["px"] = json!("0");
            venue.updates.send(filled).unwrap();
            wait_until(|| cache.borrow().position(&position_id).unwrap().is_closed()).await;
            wait_until(|| {
                let strategy =
                    try_get_actor_unchecked::<ProbeStrategy>(&StrategyId::from(STRATEGY).inner())
                        .unwrap();
                !strategy.is_exiting()
            })
            .await;
            assert_eq!(venue.submissions.lock().await.len(), 2);
            assert_eq!(
                cache.borrow().positions(None, None, None, None, None).len(),
                1
            );
        }
        handle.stop();
    });
    let (result, ()) = tokio::time::timeout(Duration::from_secs(70), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .unwrap();

    if recover {
        result.unwrap();
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Order fill recovery incomplete")
        );
        assert!(!cached(&cache).is_closed());
        assert_eq!(venue.submissions.lock().await.len(), 1);
    }
    msgbus::unsubscribe_any(
        MessagingSwitchboard::reconciliation_raw_order_status_report_topic().into(),
        &handler,
    );
    node.dispose();
    server.abort();
}
