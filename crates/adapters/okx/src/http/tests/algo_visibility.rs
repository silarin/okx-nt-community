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

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use jiff::Timestamp;
use serde_json::{Value, json};

use super::*;
use crate::common::{credential::Credential, enums::OKXEnvironment};
const ALGO_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(120);
const APPROVED_ALGO_INSTRUMENT_TYPES: [OKXInstrumentType; 2] =
    [OKXInstrumentType::Margin, OKXInstrumentType::Spot];
// Synthetic pending/history responses share these fields across smart_iceberg
// and the other algo types and use native decoding.
// https://my.okx.com/docs-v5/en/#order-book-trading-algo-trading-get-algo-order-list
fn order(instrument_type: &str, order_type: &str, state: &str, id: u64) -> Value {
    json!({
        "algoId": id.to_string(), "algoClOrdId": "parent", "clOrdId": "child",
        "instType": instrument_type, "instId": "BTC-USDT", "ordType": order_type,
        "state": state, "side": "buy", "posSide": "",
        "tdMode": if instrument_type == "MARGIN" { "cross" } else { "cash" },
        "cTime": "1753184812254", "uTime": "1753184812255",
        "triggerPxType": "", "tpTriggerPxType": "", "slTriggerPxType": "",
        "triggerParams": [{"triggerAction": "start", "triggerStrategy": "instant"}],
        "lmtOrderNumber": "5", "aggressiveness": "conservative"
    })
}

fn response(orders: Vec<Value>) -> String {
    json!({"code": "0", "msg": "", "data": Value::Array(orders)}).to_string()
}

fn client_at(base_url: String) -> OKXRawHttpClient {
    client_at_environment(base_url, OKXEnvironment::Demo)
}

fn client_at_environment(base_url: String, environment: OKXEnvironment) -> OKXRawHttpClient {
    OKXRawHttpClient::with_credentials(
        "test-key".into(),
        "test-secret".into(),
        "test-passphrase".into(),
        base_url,
        5,
        0,
        1000,
        10000,
        environment,
        None,
    )
    .unwrap()
}

pub(super) fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

fn assert_signed_get(request: &str, path: &str) {
    assert_signed_get_for_environment(request, path, OKXEnvironment::Demo);
}

pub(super) fn assert_signed_get_for_environment(
    request: &str,
    path: &str,
    environment: OKXEnvironment,
) {
    assert!(
        request.starts_with(&format!("GET {path} HTTP/1.1\r\n")),
        "{request}"
    );
    assert!(!path.contains("instId="));
    assert!(request.ends_with("\r\n\r\n"));
    assert!(header(request, "content-length").is_none());
    assert!(header(request, "transfer-encoding").is_none());
    // OKX requires JSON Content-Type for request bodies. Native bodyless GETs
    // omit it; the four authentication headers and Demo header are mandatory.
    // https://my.okx.com/docs-v5/en/#overview-rest-authentication
    assert_eq!(header(request, "ok-access-key"), Some("test-key"));
    assert_eq!(
        header(request, "ok-access-passphrase"),
        Some("test-passphrase")
    );
    assert_eq!(
        header(request, "x-simulated-trading"),
        (environment == OKXEnvironment::Demo).then_some("1")
    );
    let timestamp = header(request, "ok-access-timestamp").unwrap();
    timestamp.parse::<Timestamp>().unwrap();
    let (_, fraction) = timestamp
        .strip_suffix('Z')
        .unwrap()
        .rsplit_once('.')
        .unwrap();
    assert_eq!(fraction.len(), 3);
    let signature = Credential::new(
        "test-key".to_owned(),
        "test-secret".to_owned(),
        "test-passphrase".to_owned(),
    )
    .sign_bytes(timestamp, "GET", path, None);
    assert_eq!(header(request, "ok-access-sign"), Some(signature.as_str()));
}

pub(super) fn spawn_pages(
    bodies: Vec<String>,
) -> (
    String,
    mpsc::Receiver<(String, Instant)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        for body in bodies {
            let deadline = Instant::now() + ALGO_RECONCILIATION_TIMEOUT;
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing native algo request");
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => panic!("loopback accept failed: {e}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            tx.send((String::from_utf8(request).unwrap(), Instant::now()))
                .unwrap();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });
    (format!("http://{address}"), rx, handle)
}

#[tokio::test]
async fn reconciliation_uses_native_reads_for_all_56_account_wide_routes() {
    for environment in [OKXEnvironment::Demo, OKXEnvironment::Live] {
        let types = [
            "oco",
            "conditional",
            "trigger",
            "move_order_stop",
            "iceberg",
            "twap",
            "smart_iceberg",
        ];
        let routes = [
            ("pending", None, "live"),
            ("history", Some("effective"), "effective"),
            ("history", Some("canceled"), "canceled"),
            ("history", Some("order_failed"), "partially_failed"),
        ];
        let mut bodies = Vec::new();
        let mut paths = Vec::new();

        for instrument_type in ["MARGIN", "SPOT"] {
            for order_type in types {
                for (endpoint, state, actual_state) in routes {
                    let mut path = format!(
                        "/api/v5/trade/orders-algo-{endpoint}?instType={instrument_type}&ordType={order_type}"
                    );

                    if let Some(state) = state {
                        write!(path, "&state={state}").unwrap();
                    }
                    path.push_str("&limit=100");
                    paths.push(path);
                    // Repeated identities across product routes must remain deduplicated.
                    let id = bodies.len() as u64 % 28 + 1;
                    bodies.push(response(vec![order(
                        instrument_type,
                        order_type,
                        actual_state,
                        id,
                    )]));
                }
            }
        }
        let (url, rx, server) = spawn_pages(bodies);
        let result = client_at_environment(url, environment)
            .get_algo_order_visibility(&APPROVED_ALGO_INSTRUMENT_TYPES)
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(result.queried_routes, 56);
        assert_eq!(result.pending_orders.len(), 7);
        assert_eq!(result.terminal_orders.len(), 21);
        assert_eq!(
            result.pending_orders.last().unwrap().instrument_id,
            "BTC-USDT"
        );
        let captured = rx.try_iter().collect::<Vec<_>>();
        assert_eq!(captured.len(), paths.len());
        for ((request, _), path) in captured.iter().zip(&paths) {
            assert_signed_get_for_environment(request, path, environment);
        }

        for pair in captured.windows(2) {
            assert!(pair[1].1.duration_since(pair[0].1) >= ALGO_VISIBILITY_REQUEST_INTERVAL);
        }
    }
}

#[tokio::test]
async fn reconciliation_rejects_conflicting_terminal_states_across_routes() {
    for across_products in [false, true] {
        // First-wins deduplication used to hide one of these terminal states.
        // Each row is valid for its individual request, so scope checks alone
        // cannot detect the contradiction.
        let second_index = if across_products { 29 } else { 2 };
        let mut bodies = vec![response(vec![]); second_index + 1];
        if across_products {
            bodies[2] = response(vec![order("MARGIN", "oco", "canceled", 123)]);
            bodies[29] = response(vec![order("SPOT", "oco", "effective", 123)]);
        } else {
            bodies[1] = response(vec![order("MARGIN", "oco", "effective", 123)]);
            bodies[2] = response(vec![order("MARGIN", "oco", "canceled", 123)]);
        }
        let (url, rx, server) = spawn_pages(bodies);
        let error = client_at(url)
            .get_algo_order_visibility(&APPROVED_ALGO_INSTRUMENT_TYPES)
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(rx.try_iter().count(), second_index + 1);
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(
            error.to_string(),
            "Parameter validation error: OKX algo visibility returned conflicting evidence for an algo ID"
        );
    }
}

#[rstest::rstest]
fn repeated_algo_ids_require_matching_identity_type_and_state() {
    for (field, value) in [
        ("algoClOrdId", "PRIVATE_PARENT"),
        ("clOrdId", "PRIVATE_CHILD"),
        ("instId", "ETH-USDT"),
        ("ordType", "conditional"),
        ("state", "effective"),
    ] {
        let original = order("MARGIN", "oco", "canceled", 123);
        let mut changed = original.clone();
        changed[field] = json!(value);
        let mut seen = BTreeMap::new();
        record_algo_identity(serde_json::from_value(original).unwrap(), &mut seen).unwrap();
        let error =
            record_algo_identity(serde_json::from_value(changed).unwrap(), &mut seen).unwrap_err();
        assert_eq!(
            error.to_string(),
            "OKX algo visibility returned conflicting evidence for an algo ID"
        );
        assert_eq!(seen.len(), 1);
        assert_eq!(seen["123"].1.state, OKXAlgoOrderStatus::Canceled);
    }
}

#[tokio::test]
async fn reconciliation_rejects_conflicts_between_pending_and_history() {
    for (field, value) in [
        ("algoClOrdId", "PRIVATE_PARENT"),
        ("clOrdId", "PRIVATE_CHILD"),
        ("instId", "ETH-USDT"),
        ("ordType", "conditional"),
        ("state", "effective"),
    ] {
        // PartiallyEffective is valid on both routes. Changing identity/type
        // must still fail even when the state itself agrees across streams.
        let pending = order("MARGIN", "oco", "partially_effective", 123);
        let mut history = pending.clone();
        history[field] = json!(value);
        let history_index = if field == "ordType" { 5 } else { 1 };
        let mut bodies = vec![response(vec![]); history_index + 1];
        bodies[0] = response(vec![pending]);
        bodies[history_index] = response(vec![history]);
        let (url, rx, server) = spawn_pages(bodies);
        let error = client_at(url)
            .get_algo_order_visibility(&APPROVED_ALGO_INSTRUMENT_TYPES)
            .await
            .unwrap_err();
        server.join().unwrap();
        assert_eq!(rx.try_iter().count(), history_index + 1);
        assert_eq!(
            error.to_string(),
            "Parameter validation error: OKX algo visibility returned conflicting evidence for an algo ID"
        );
    }
}

#[tokio::test]
async fn matching_partial_history_never_removes_pending_evidence() {
    let mut bodies = vec![response(vec![]); 56];
    for (instrument_type, offset) in [("MARGIN", 0), ("SPOT", 28)] {
        let body = response(vec![order(
            instrument_type,
            "oco",
            "partially_effective",
            123,
        )]);
        bodies[offset] = body.clone();
        bodies[offset + 1] = body;
    }
    let (url, rx, server) = spawn_pages(bodies);
    let result = client_at(url)
        .get_algo_order_visibility(&APPROVED_ALGO_INSTRUMENT_TYPES)
        .await
        .unwrap();
    server.join().unwrap();
    assert_eq!(rx.try_iter().count(), 56);
    assert_eq!(result.queried_routes, 56);
    assert_eq!(result.pending_orders.len(), 1);
    assert_eq!(result.terminal_orders.len(), 1);
    assert_eq!(result.pending_orders, result.terminal_orders);
    assert_eq!(result.pending_orders[0].algo_id, "123");
    assert_eq!(
        result.pending_orders[0].state,
        OKXAlgoOrderStatus::PartiallyEffective
    );
}

#[tokio::test]
async fn algo_pagination_rejects_repeated_oversized_and_invalid_later_pages() {
    for order_type in [OKXAlgoOrderType::Trigger, OKXAlgoOrderType::SmartIceberg] {
        let wire_type = serde_json::to_value(order_type).unwrap();
        let wire_type = wire_type.as_str().unwrap();

        for state in [None, Some(OKXAlgoOrderStatus::Canceled)] {
            let actual_state = if state.is_some() { "canceled" } else { "live" };
            let first: Vec<_> = (101..=200)
                .rev()
                .map(|id| order("SPOT", wire_type, actual_state, id))
                .collect();

            for (pages, expected) in [
                (
                    vec![vec![first[0].clone(), first[0].clone()]],
                    "repeated an algo ID",
                ),
                (
                    vec![first.clone(), vec![first[99].clone()]],
                    "repeated an algo ID",
                ),
                (vec![first.clone(), first.clone()], "repeated an algo ID"),
                (
                    vec![
                        first
                            .iter()
                            .cloned()
                            .chain([order("SPOT", wire_type, actual_state, 100)])
                            .collect(),
                    ],
                    "requested page size",
                ),
                (
                    vec![
                        first.clone(),
                        vec![order("SWAP", wire_type, actual_state, 100)],
                    ],
                    "requested account-wide scope",
                ),
            ] {
                let page_count = pages.len();
                let (url, rx, server) = spawn_pages(pages.into_iter().map(response).collect());
                let client = client_at(url);
                let params = algo_params(OKXInstrumentType::Spot, order_type, state).unwrap();
                let error = if state.is_some() {
                    client.paginate_history(&params).await
                } else {
                    client.paginate_pending(&params).await
                }
                .unwrap_err();
                server.join().unwrap();
                assert_eq!(rx.try_iter().count(), page_count);
                assert!(error.to_string().contains(expected), "{error:#}");
                assert_eq!(error.chain().count(), 1);
            }
        }
    }
}

#[tokio::test]
async fn smart_iceberg_pagination_preserves_cursor_and_both_endpoint_contracts() {
    for (instrument_type, wire_type) in [
        (OKXInstrumentType::Margin, "MARGIN"),
        (OKXInstrumentType::Spot, "SPOT"),
    ] {
        for state in [None, Some(OKXAlgoOrderStatus::Canceled)] {
            let actual_state = if state.is_some() { "canceled" } else { "pause" };
            let first = (101..=200)
                .rev()
                .map(|id| order(wire_type, "smart_iceberg", actual_state, id))
                .collect();
            let last = order(wire_type, "smart_iceberg", actual_state, 100);
            let (url, rx, server) = spawn_pages(vec![response(first), response(vec![last])]);
            let client = client_at(url);
            let params =
                algo_params(instrument_type, OKXAlgoOrderType::SmartIceberg, state).unwrap();
            let result = if state.is_some() {
                client.paginate_history(&params).await
            } else {
                client.paginate_pending(&params).await
            }
            .unwrap();
            server.join().unwrap();
            assert_eq!(result.len(), 101);
            assert_eq!(result[100].algo_id, "100");
            let endpoint = if state.is_some() {
                "history"
            } else {
                "pending"
            };
            let suffix = if state.is_some() {
                "&state=canceled"
            } else {
                ""
            };

            for (index, (request, _)) in rx.try_iter().enumerate() {
                let cursor = if index == 0 { "" } else { "&after=101" };
                assert_signed_get(
                    &request,
                    &format!(
                        "/api/v5/trade/orders-algo-{endpoint}?instType={wire_type}&ordType=smart_iceberg{suffix}{cursor}&limit=100"
                    ),
                );
            }
        }
    }
}

#[tokio::test]
async fn all_algo_types_reject_invalid_response_provenance_and_schema_before_pagination() {
    for order_type in TYPED_ALGO_ORDER_TYPES {
        let wire_type = serde_json::to_value(order_type).unwrap();
        let wire_type = wire_type.as_str().unwrap();

        for state in [None, Some(OKXAlgoOrderStatus::Canceled)] {
            let actual_state = if state.is_some() { "canceled" } else { "live" };
            for (field, value) in [
                ("algoId", json!("")),
                ("algoId", json!("123&instId=ETH-USDT")),
                ("instId", json!("ETH/USDT")),
                ("instType", json!("MARGIN")),
                (
                    "ordType",
                    json!(if wire_type == "iceberg" {
                        "trigger"
                    } else {
                        "iceberg"
                    }),
                ),
                ("ordType", json!("future_type")),
                ("state", json!("effective")),
                ("state", json!("future_state")),
                ("side", json!("private malformed side")),
                ("cTime", json!("invalid")),
                ("uTime", Value::Null),
                ("posSide", Value::Null),
                ("tdMode", Value::Null),
            ] {
                let mut item = order("SPOT", wire_type, actual_state, 123);
                item[field] = value;
                let (url, rx, server) = spawn_pages(vec![response(vec![item])]);
                let client = client_at(url);
                let params = algo_params(OKXInstrumentType::Spot, order_type, state).unwrap();
                let error = if state.is_some() {
                    client.paginate_history(&params).await
                } else {
                    client.paginate_pending(&params).await
                }
                .unwrap_err();
                server.join().unwrap();
                assert_eq!(rx.try_iter().count(), 1);
                assert_eq!(error.chain().count(), 1);

                if field == "side" {
                    assert!(matches!(
                        error.downcast_ref::<OKXHttpError>(),
                        Some(OKXHttpError::ResponseDecoding(_))
                    ));
                }
            }
        }
    }
}

#[rstest::rstest]
fn all_algo_types_validate_pending_and_terminal_route_states() {
    for order_type in TYPED_ALGO_ORDER_TYPES {
        let wire_type = serde_json::to_value(order_type).unwrap();
        let wire_type = wire_type.as_str().unwrap();
        for (route, accepted) in [
            (None, vec!["live", "pause", "partially_effective"]),
            (
                Some(OKXAlgoOrderStatus::Effective),
                vec!["effective", "partially_effective"],
            ),
            (Some(OKXAlgoOrderStatus::Canceled), vec!["canceled"]),
            (
                Some(OKXAlgoOrderStatus::OrderFailed),
                vec!["order_failed", "partially_failed"],
            ),
        ] {
            let request = algo_params(OKXInstrumentType::Spot, order_type, route).unwrap();
            let mut seen = BTreeSet::new();

            for state in [
                "live",
                "pause",
                "partially_effective",
                "effective",
                "canceled",
                "order_failed",
                "partially_failed",
                "order_placed",
                "filled",
                "future_state",
            ] {
                let native = serde_json::from_value(order("SPOT", wire_type, state, 123)).unwrap();
                if validate_algo_order(&native, &request).is_ok() {
                    seen.insert(state);
                }
            }
            assert_eq!(seen, accepted.into_iter().collect());
        }
    }
}

#[tokio::test]
async fn smart_iceberg_fails_closed_on_later_pages_and_pagination_exhaustion() {
    for state in [None, Some(OKXAlgoOrderStatus::Canceled)] {
        for exhausted in [false, true] {
            let actual_state = if state.is_some() { "canceled" } else { "live" };
            let mut bodies = Vec::new();
            let pages = if exhausted { MAX_PAGES } else { 2 };
            for index in 0..pages {
                let page = (0..100)
                    .map(|offset| {
                        order(
                            "SPOT",
                            "smart_iceberg",
                            actual_state,
                            10000 - (index * 100 + offset) as u64,
                        )
                    })
                    .collect();
                bodies.push(response(page));
            }

            if !exhausted {
                bodies[1] =
                    json!({"code":"51000", "msg":"private later page", "data":[]}).to_string();
            }
            let (url, rx, server) = spawn_pages(bodies);
            let client = client_at(url);
            let params = algo_params(
                OKXInstrumentType::Spot,
                OKXAlgoOrderType::SmartIceberg,
                state,
            )
            .unwrap();
            let error = if state.is_some() {
                client.paginate_history(&params).await
            } else {
                client.paginate_pending(&params).await
            }
            .unwrap_err();
            server.join().unwrap();
            assert_eq!(rx.try_iter().count(), pages);
            let message = format!("{error:#}");
            assert!(message.contains(if exhausted {
                "pagination bound"
            } else {
                "OKX error 51000"
            }));

            if !exhausted {
                assert!(error.downcast_ref::<OKXHttpError>().is_some());
            }
        }
    }
}

#[tokio::test]
async fn algo_visibility_pacer_serializes_actual_request_dispatch() {
    let first_started = std::sync::Arc::new(tokio::sync::Notify::new());
    let release_first = std::sync::Arc::new(tokio::sync::Notify::new());

    let first_started_in_request = first_started.clone();
    let release_first_in_request = release_first.clone();

    let first = tokio::spawn(async move {
        pace_algo_visibility_request(|| async move {
            first_started_in_request.notify_one();
            release_first_in_request.notified().await;
            Instant::now()
        })
        .await
    });
    first_started.notified().await;

    let second_request = async { pace_algo_visibility_request(|| async { Instant::now() }).await };

    let second = tokio::spawn(second_request);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !second.is_finished(),
        "a concurrent request must not enter the dispatch closure while another request is in flight"
    );

    release_first.notify_one();
    let first_completed_at = first.await.unwrap();
    let second_started_at = second.await.unwrap();
    assert!(
        second_started_at.duration_since(first_completed_at) >= ALGO_VISIBILITY_REQUEST_INTERVAL,
        "the shared pacer must apply its conservative interval after actual request completion"
    );
}

#[rstest::rstest]
#[case(vec![])]
#[case(vec![OKXInstrumentType::Spot, OKXInstrumentType::Spot])]
#[case(vec![OKXInstrumentType::Swap])]
#[case(vec![OKXInstrumentType::Futures])]
#[case(vec![OKXInstrumentType::Option])]
#[case(vec![OKXInstrumentType::Events])]
#[case(vec![OKXInstrumentType::Any])]
#[tokio::test]
async fn visibility_rejects_unsupported_or_duplicate_scopes_before_network(
    #[case] scopes: Vec<OKXInstrumentType>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = client_at(format!("http://{}", listener.local_addr().unwrap()));
    assert!(matches!(
        client.get_algo_order_visibility(&scopes).await,
        Err(OKXHttpError::ValidationError(_))
    ));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}
