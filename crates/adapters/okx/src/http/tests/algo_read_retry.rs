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

use std::sync::{Arc, Mutex};

use axum::{
    Json, Router, extract::Request, http::StatusCode, response::IntoResponse, routing::any,
};
use rstest::rstest;
use serde_json::{Value, json};

use super::*;

type Requests = Arc<Mutex<Vec<(String, std::time::Instant)>>>;

async fn timeout_server(
    failures: usize,
    status: StatusCode,
    retry_after: Option<&'static str>,
    hang_attempt: Option<usize>,
) -> (String, Requests, tokio::task::JoinHandle<()>) {
    let requests: Requests = Arc::default();
    let captured = Arc::clone(&requests);
    let router = Router::new().fallback_service(any(move |request: Request| {
        let captured = Arc::clone(&captured);
        async move {
            let attempt = {
                let mut requests = captured.lock().unwrap();
                requests.push((
                    format!("{} {}", request.method(), request.uri()),
                    std::time::Instant::now(),
                ));
                requests.len()
            };

            if hang_attempt == Some(attempt) {
                std::future::pending::<()>().await;
            }

            if attempt <= failures {
                let mut response = (
                    status,
                    Json(json!({"code":"51054", "msg":"Request timed out", "data":[]})),
                )
                    .into_response();

                if let Some(value) = retry_after {
                    response
                        .headers_mut()
                        .insert("retry-after", value.parse().unwrap());
                }
                response
            } else {
                Json(json!({"code":"0", "msg":"", "data":[]})).into_response()
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, requests, server)
}

fn client_at(url: String, max_elapsed_ms: u64) -> OKXRawHttpClient {
    let mut client = OKXRawHttpClient::with_credentials(
        "test-key".into(),
        "test-secret".into(),
        "test-passphrase".into(),
        url,
        5,
        2,
        1,
        1,
        OKXEnvironment::Demo,
        None,
    )
    .unwrap();
    client.retry_manager = RetryManager::new(RetryConfig {
        max_retries: 2,
        initial_delay_ms: 1,
        max_delay_ms: 1,
        backoff_factor: 2.0,
        jitter_ms: 0,
        operation_timeout_ms: Some(5_000),
        immediate_first: false,
        max_elapsed_ms: Some(max_elapsed_ms),
    });
    client
}

#[rstest]
#[case(Method::GET, "/api/v5/trade/orders-algo-pending", StatusCode::OK, true)]
#[case(
    Method::GET,
    "/api/v5/trade/orders-algo-pending",
    StatusCode::INTERNAL_SERVER_ERROR,
    true
)]
#[case(Method::GET, "/api/v5/trade/orders-algo-history", StatusCode::OK, true)]
#[case(
    Method::GET,
    "/api/v5/trade/orders-algo-history",
    StatusCode::INTERNAL_SERVER_ERROR,
    true
)]
#[case(
    Method::GET,
    "/api/v5/account/balance",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case(
    Method::POST,
    "/api/v5/trade/orders-algo-pending",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case(
    Method::POST,
    "/api/v5/trade/orders-algo-history",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case(
    Method::POST,
    "/api/v5/trade/order",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case(
    Method::POST,
    "/api/v5/trade/order-algo",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[case(
    Method::POST,
    "/api/v5/trade/amend-algos",
    StatusCode::INTERNAL_SERVER_ERROR,
    false
)]
#[tokio::test]
async fn timeout_retry_is_scoped_and_preserves_query_and_pacing(
    #[case] method: Method,
    #[case] path: &str,
    #[case] status: StatusCode,
    #[case] retry: bool,
) {
    let (url, requests, server) = timeout_server(1, status, None, None).await;
    let client = client_at(url, 5_000);
    let params = [
        ("instType", "SPOT"),
        ("ordType", "smart_iceberg"),
        ("after", "123"),
    ];
    let result = client
        .send_request::<Value, _>(method.clone(), path, Some(&params), None, true)
        .await;
    server.abort();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), if retry { 2 } else { 1 });
    assert!(requests.iter().all(|(request, _)| request
        == &format!(
            "{} {path}?instType=SPOT&ordType=smart_iceberg&after=123",
            method.as_str()
        )));

    if retry {
        assert!(result.unwrap().is_empty());
        assert!(requests[1].1.duration_since(requests[0].1) >= Duration::from_millis(110));
    } else {
        assert!(
            matches!(result, Err(OKXHttpError::OkxError { error_code, .. }) if error_code == "51054")
        );
    }
}

#[rstest]
#[case(StatusCode::OK)]
#[case(StatusCode::INTERNAL_SERVER_ERROR)]
#[tokio::test]
async fn timeout_retry_honors_retry_after(#[case] status: StatusCode) {
    let (url, requests, server) = timeout_server(1, status, Some("1"), None).await;
    let client = client_at(url, 5_000);
    let params = GetAlgoOrdersParamsBuilder::default()
        .inst_type(OKXInstrumentType::Spot)
        .ord_type(OKXAlgoOrderType::SmartIceberg)
        .build()
        .unwrap();
    assert!(
        client
            .get_order_algo_pending(params)
            .await
            .unwrap()
            .is_empty()
    );
    server.abort();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].1.duration_since(requests[0].1) >= Duration::from_secs(1));
}

#[tokio::test]
async fn timeout_retry_exhaustion_keeps_context_and_returns_no_visibility() {
    let (url, requests, server) =
        timeout_server(usize::MAX, StatusCode::INTERNAL_SERVER_ERROR, None, None).await;
    let client = client_at(url, 5_000);
    let error = client
        .get_algo_order_visibility_with_context(&[OKXInstrumentType::Spot])
        .await
        .unwrap_err();
    server.abort();
    let context = error.context().unwrap();
    assert_eq!(
        context.route,
        super::super::algo_visibility::OKXAlgoVisibilityRoute::Pending
    );
    assert_eq!(context.instrument_type, OKXInstrumentType::Spot);
    assert_eq!(context.order_type, OKXAlgoOrderType::Oco);
    assert_eq!(context.page_number, 1);
    assert!(
        matches!(error.into_inner(), OKXHttpError::RetryableOkxError { error_code, .. } if error_code == "51054")
    );
    assert_eq!(requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn timeout_retry_preserves_elapsed_budget() {
    let (url, requests, server) = timeout_server(
        usize::MAX,
        StatusCode::INTERNAL_SERVER_ERROR,
        Some("1"),
        None,
    )
    .await;
    let client = client_at(url, 500);
    let result = client
        .send_request::<Value, ()>(
            Method::GET,
            "/api/v5/trade/orders-algo-history",
            None,
            None,
            true,
        )
        .await;
    server.abort();
    assert!(
        matches!(result, Err(OKXHttpError::RetryableOkxError { error_code, retry_after: Some(delay), .. }) if error_code == "51054" && delay == Duration::from_secs(1))
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn timeout_retry_cancellation_prevents_another_attempt_and_releases_pacer() {
    let (url, requests, server) = timeout_server(
        usize::MAX,
        StatusCode::INTERNAL_SERVER_ERROR,
        Some("1"),
        None,
    )
    .await;
    let client = client_at(url, 5_000);
    let cancellation = client.cancellation_token().clone();
    let request = tokio::spawn(async move {
        client
            .get_algo_order_visibility_with_context(&[OKXInstrumentType::Spot])
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while requests.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(error.into_inner(), OKXHttpError::Canceled(_)));
    tokio::time::timeout(
        Duration::from_secs(5),
        super::super::algo_visibility::pace_algo_visibility_request(|| async {}),
    )
    .await
    .unwrap();
    server.abort();
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn dropping_a_later_attempt_keeps_queued_reader_paced() {
    let (url, requests, server) =
        timeout_server(1, StatusCode::INTERNAL_SERVER_ERROR, None, Some(2)).await;
    let first = client_at(url.clone(), 5_000);
    let second = client_at(url, 5_000);
    let params = GetAlgoOrdersParamsBuilder::default()
        .inst_type(OKXInstrumentType::Spot)
        .ord_type(OKXAlgoOrderType::SmartIceberg)
        .build()
        .unwrap();
    let first_params = params.clone();

    let first_task = tokio::spawn(async move { first.get_order_algo_pending(first_params).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while requests.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let second_task = tokio::spawn(async move { second.get_order_algo_pending(params).await });
    tokio::task::yield_now().await;
    assert_eq!(requests.lock().unwrap().len(), 2);
    first_task.abort();
    let error = tokio::time::timeout(Duration::from_secs(5), first_task)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_secs(5), second_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_empty()
    );
    server.abort();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].1.duration_since(requests[1].1) >= Duration::from_millis(110));
}
