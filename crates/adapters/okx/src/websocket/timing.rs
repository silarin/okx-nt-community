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

//! Redacted timing for the adapter's private queue boundaries.

use std::sync::Arc;

use nautilus_common::logging::execution_timing::{ExecutionTiming, execution_timing_id};
use nautilus_model::identifiers::ClientOrderId;
use nautilus_network::{transport::Message, websocket::EpochMessageHandler};
use tokio_tungstenite::tungstenite::Message as LegacyMessage;

use super::{enums::OKXWsOperation, messages::OKXWsMessage};

#[derive(Debug)]
pub(crate) struct TimingEnvelope<T> {
    pub(crate) message: T,
    pub(crate) timing: Option<ExecutionTiming>,
    pub(crate) epoch: Option<u64>,
}

impl<T> From<T> for TimingEnvelope<T> {
    fn from(message: T) -> Self {
        Self {
            message,
            timing: None,
            epoch: None,
        }
    }
}

impl<T> TimingEnvelope<T> {
    pub(super) fn received(self, stage: &'static str) -> T {
        if let Some(stamp) = ExecutionTiming::capture() {
            stamp.record(stage, self.timing, self.epoch);
        }
        self.message
    }
}

impl TimingEnvelope<OKXWsMessage> {
    pub(crate) fn into_execution_message(self) -> OKXWsMessage {
        record_message("adapter_dispatch", &self.message, self.timing, self.epoch);
        self.message
    }
}

pub(super) fn channel_timing_message_handler() -> (
    EpochMessageHandler,
    tokio::sync::mpsc::UnboundedReceiver<TimingEnvelope<(u64, LegacyMessage)>>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handler: EpochMessageHandler = Arc::new(move |epoch, msg: Message| {
        let timing = ExecutionTiming::capture();

        match LegacyMessage::try_from(msg) {
            Ok(message) => {
                if let Some(stamp) = timing {
                    stamp.record("ws_received", None, Some(epoch));
                }
                // Never format a failed send: it contains the authenticated payload
                if tx
                    .send(TimingEnvelope {
                        message: (epoch, message),
                        timing,
                        epoch: Some(epoch),
                    })
                    .is_err()
                {
                    log::debug!("Failed to send connection message to channel");
                }
            }
            Err(_) => log::debug!("Dropping message that failed legacy conversion"),
        }
    });
    (handler, rx)
}

// Optional diagnostic metadata cannot change whether the trading parser accepts a frame
pub(super) fn record_gateway_timing(
    message: &LegacyMessage,
    parent: Option<ExecutionTiming>,
    epoch: u64,
) {
    let Some(stamp) = ExecutionTiming::capture() else {
        return;
    };
    let LegacyMessage::Text(text) = message else {
        return;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };

    if value.get("op").is_none() {
        return;
    }
    let request = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(execution_timing_id);
    let in_us = timestamp(&value, "inTime");
    let out_us = timestamp(&value, "outTime");
    log::info!(target: "ExecutionTiming",
        "stage=ws_gateway trace={} parent={:?} epoch={epoch} host_mono_ns={} host_unix_ns={} request={request:016x?} venue_in_us={in_us:?} venue_out_us={out_us:?}",
        stamp.id, parent.map(|p| p.id), stamp.monotonic_ns, stamp.unix_ns);
}

fn timestamp(value: &serde_json::Value, key: &str) -> Option<u64> {
    value.get(key)?.as_str()?.parse().ok()
}

pub(super) fn record_command(
    stamp: ExecutionTiming,
    request_id: Option<&str>,
    client_order_ids: &[ClientOrderId],
    operation: Option<&OKXWsOperation>,
) {
    log::info!(target: "ExecutionTiming", "stage=ws_command trace={} request={:016x?} operation={operation:?}", stamp.id, request_id.map(execution_timing_id));
    for client_order_id in client_order_ids.iter().take(64) {
        log::info!(target: "ExecutionTiming", "stage=ws_command_order trace={} client={:016x}", stamp.id, execution_timing_id(client_order_id.as_str()));
    }

    if client_order_ids.len() > 64 {
        log::info!(target: "ExecutionTiming", "stage=ws_command_truncated trace={} omitted={}", stamp.id, client_order_ids.len() - 64);
    }
}

pub(super) fn record_message(
    stage: &'static str,
    message: &OKXWsMessage,
    parent: Option<ExecutionTiming>,
    epoch: Option<u64>,
) {
    let Some(stamp) = ExecutionTiming::capture() else {
        return;
    };

    match message {
        OKXWsMessage::Orders(orders) => {
            stamp.record(stage, parent, epoch);

            for order in orders.iter().take(64) {
                log::info!(target: "ExecutionTiming",
                    "stage=ws_order trace={} client={:016x} venue_order={:016x} state={:?} venue_created_ms={:?} venue_updated_ms={:?}",
                    stamp.id, execution_timing_id(&order.cl_ord_id), execution_timing_id(&order.ord_id),
                    order.state, order.c_time, order.u_time);
            }

            if orders.len() > 64 {
                log::info!(target: "ExecutionTiming", "stage=ws_order_truncated trace={} omitted={}", stamp.id, orders.len() - 64);
            }
        }
        OKXWsMessage::OrderResponse {
            id, op, code, data, ..
        } => {
            stamp.record(stage, parent, epoch);
            log::info!(target: "ExecutionTiming",
                "stage=ws_ack trace={} request={:016x?} operation={op:?} code={:?} items={}",
                stamp.id, id.as_deref().map(execution_timing_id), code.parse::<u64>().ok(), data.len());
            for item in data.iter().take(64) {
                log::info!(target: "ExecutionTiming",
                    "stage=ws_ack_item trace={} client={:016x?} venue_order={:016x?} venue_created_ms={:?} code={:?}",
                    stamp.id, item.get("clOrdId").and_then(serde_json::Value::as_str).map(execution_timing_id),
                    item.get("ordId").and_then(serde_json::Value::as_str).map(execution_timing_id),
                    timestamp(item, "ts"), timestamp(item, "sCode"));
            }

            if data.len() > 64 {
                log::info!(target: "ExecutionTiming", "stage=ws_ack_truncated trace={} omitted={}", stamp.id, data.len() - 64);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, atomic::AtomicBool};

    use nautilus_common::logging::execution_timing::{
        record_order_event, set_execution_timing_enabled,
    };
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_model::{
        events::{OrderEventAny, order::spec::OrderSubmittedSpec},
        identifiers::ClientOrderId,
    };
    use nautilus_network::{
        error::SendError,
        websocket::{AuthTracker, SubscriptionState},
    };
    use rstest::rstest;

    use super::{
        super::{enums::OKXWsOperation, handler::OKXWsFeedHandler},
        *,
    };

    struct TimingLogger(Mutex<Vec<String>>);
    impl log::Log for TimingLogger {
        fn enabled(&self, metadata: &log::Metadata) -> bool {
            metadata.target() == "ExecutionTiming"
        }
        fn log(&self, record: &log::Record) {
            if self.enabled(record.metadata()) {
                self.0.lock().unwrap().push(record.args().to_string());
            }
        }
        fn flush(&self) {}
    }
    static LOGGER: TimingLogger = TimingLogger(Mutex::new(Vec::new()));

    struct TimingGuard;
    impl Drop for TimingGuard {
        fn drop(&mut self) {
            set_execution_timing_enabled(false);
        }
    }

    #[rstest]
    fn gateway_timestamps_keep_microseconds_and_tolerate_missing_or_invalid_metadata() {
        let value = serde_json::json!({"inTime":"1597026383085123", "outTime":"invalid"});
        assert_eq!(timestamp(&value, "inTime"), Some(1_597_026_383_085_123));
        assert_eq!(timestamp(&value, "outTime"), None);
        assert_eq!(timestamp(&value, "missing"), None);
        assert_eq!(
            timestamp(
                &serde_json::json!({"inTime":1_597_026_383_085_123_u64}),
                "inTime"
            ),
            None
        );
    }

    #[tokio::test]
    async fn timing_observations_are_opt_in_redacted_and_preserve_queue_delivery() {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        let (callback, mut raw_rx) = channel_timing_message_handler();
        callback(7, Message::Text("disabled".into()));
        let disabled = raw_rx.recv().await.unwrap();
        assert!(disabled.timing.is_none());
        assert_eq!(
            disabled.received("disabled"),
            (7, LegacyMessage::Text("disabled".into()))
        );
        assert!(LOGGER.0.lock().unwrap().is_empty());

        set_execution_timing_enabled(true);
        let guard = TimingGuard;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut handler = OKXWsFeedHandler::new(
            Arc::new(AtomicBool::new(false)),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            false,
            SubscriptionState::new('|'),
            get_atomic_clock_realtime(),
        );
        let item = serde_json::json!({"clOrdId":"PRIVATE_CLIENT", "ordId":"PRIVATE_ORDER", "ts":"1597026383085", "sCode":"0", "sMsg":"PRIVATE_REASON"});
        let response = serde_json::json!({"id":"PRIVATE_REQUEST", "op":"order", "code":"0", "msg":"PRIVATE_REASON", "apiKey":"PRIVATE_KEY", "passphrase":"PRIVATE_PASSPHRASE", "sign":"PRIVATE_SIGNATURE", "inTime":"1597026383085123", "outTime":"1597026383085999", "data":vec![item; 70]}).to_string();
        callback(7, Message::Text(response.into()));
        callback(8, Message::Text(serde_json::json!({"id":"PRIVATE_SECOND", "op":"amend-order", "code":"0", "msg":"", "data":[]}).to_string().into()));

        for (expected_epoch, expected_op, expected_items) in [
            (7, OKXWsOperation::Order, 70),
            (8, OKXWsOperation::AmendOrder, 0),
        ] {
            let output = handler.next().await.unwrap();
            handler.send(output).unwrap();
            let envelope = out_rx.recv().await.unwrap();
            assert_eq!(envelope.epoch, Some(expected_epoch));
            assert!(envelope.timing.is_some());
            if let Some(stamp) = ExecutionTiming::capture() {
                stamp.record("ws_output_received", envelope.timing, envelope.epoch);
            }
            let output = envelope.into_execution_message();
            let OKXWsMessage::OrderResponse { op, data, .. } = &output else {
                panic!("Expected unchanged acknowledgement");
            };
            assert_eq!(*op, expected_op);
            assert_eq!(data.len(), expected_items);
        }
        cmd_tx
            .send(super::super::handler::HandlerCommand::Send {
                payload: "PRIVATE_UNSENT_PAYLOAD".to_string(),
                rate_limit_keys: None,
                request_id: Some("PRIVATE_UNSENT_REQUEST".to_string()),
                client_order_ids: vec![ClientOrderId::from("PRIVATE_UNSENT_CLIENT")],
                op: Some(OKXWsOperation::Order),
            })
            .unwrap();
        let before_drop = LOGGER.0.lock().unwrap().len();
        drop(handler);
        let failed = out_rx.recv().await.unwrap();
        assert!(failed.epoch.is_none());
        assert!(matches!(failed.message,
            OKXWsMessage::SendFailed { request_id, client_order_ids, error: super::super::error::OKXWsError::TransportSend(SendError::Closed), .. }
            if request_id == "PRIVATE_UNSENT_REQUEST"
                && client_order_ids == vec![ClientOrderId::from("PRIVATE_UNSENT_CLIENT")]));
        assert!(
            LOGGER.0.lock().unwrap()[before_drop..]
                .iter()
                .any(|line| line.contains("stage=ws_output_enqueue_attempt")
                    && line.contains("parent=None epoch=None"))
        );

        let event = OrderEventAny::Submitted(
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("PRIVATE_CLIENT"))
                .build(),
        );
        let original = event.clone();
        record_order_event("execution_enqueue_attempt", &event);
        record_order_event("execution_dequeued", &event);
        record_order_event("strategy_received", &event);
        assert_eq!(event, original);

        let command_stamp = ExecutionTiming::capture().unwrap();
        record_command(
            command_stamp,
            Some("PRIVATE_UNACKNOWLEDGED"),
            &vec![ClientOrderId::from("PRIVATE_CLIENT"); 70],
            Some(&OKXWsOperation::Order),
        );
        let records = LOGGER.0.lock().unwrap().join("\n");

        for stage in [
            "ws_received",
            "ws_handler_dequeued",
            "ws_gateway",
            "ws_parsed",
            "ws_output_enqueue_attempt",
            "ws_output_received",
            "adapter_dispatch",
            "execution_enqueue_attempt",
            "execution_dequeued",
            "strategy_received",
        ] {
            assert!(
                records.contains(&format!("stage={stage}")),
                "Missing {stage}"
            );
        }
        assert!(records.contains("venue_in_us=Some(1597026383085123)"));
        assert!(records.contains("venue_in_us=None venue_out_us=None"));
        assert!(records.contains("venue_created_ms=Some(1597026383085)"));
        assert!(records.contains(&format!(
            "stage=ws_command trace={} request={:016x?}",
            command_stamp.id,
            Some(execution_timing_id("PRIVATE_UNACKNOWLEDGED"))
        )));
        assert!(records.contains(&format!(
            "stage=ws_command_order trace={} client={:016x}",
            command_stamp.id,
            execution_timing_id("PRIVATE_CLIENT")
        )));
        assert_eq!(records.matches("stage=ws_command_order ").count(), 64);
        assert!(records.contains("stage=ws_command_truncated"));
        assert!(records.contains("stage=ws_ack_truncated"));
        assert_eq!(records.matches("stage=ws_ack_item ").count(), 128);
        assert!(!records.contains("PRIVATE"));
        assert!(!records.contains("apiKey"));
        assert!(!records.contains("passphrase"));
        assert!(!records.contains("sMsg"));
        drop(guard);
        assert!(ExecutionTiming::capture().is_none());
    }
}
