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

//! Opt-in execution timing observations, independent of trading state.
//!
//! Enable before connecting clients. Records use the `ExecutionTiming` INFO target,
//! so raw WebSocket TRACE logging is unnecessary. Host monotonic nanoseconds share
//! one process origin; host Unix nanoseconds and venue clocks must not be subtracted
//! to claim network latency. Logging itself adds overhead. Correlation hashes are
//! process-local pseudonyms, not an encryption or anonymization guarantee.

use std::{
    collections::hash_map::RandomState,
    hash::BuildHasher,
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::events::{OrderEventAny, order::OrderEvent};

static ENABLED: AtomicBool = AtomicBool::new(false);
static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static IDS: AtomicU64 = AtomicU64::new(1);
static HASHER: LazyLock<RandomState> = LazyLock::new(RandomState::new);

/// Enables or disables native execution timing records (disabled by default).
///
/// Configure before connecting clients; changing this while running can leave
/// incomplete traces. This does not change logger levels or enable raw payload logs.
pub fn set_execution_timing_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// A host observation with a process-local correlation identity.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionTiming {
    pub id: u64,
    pub monotonic_ns: u128,
    pub unix_ns: u64,
}

impl ExecutionTiming {
    /// Captures a host observation only when explicitly enabled.
    pub fn capture() -> Option<Self> {
        ENABLED.load(Ordering::Relaxed).then(|| Self {
            id: IDS.fetch_add(1, Ordering::Relaxed),
            monotonic_ns: ORIGIN.elapsed().as_nanos(),
            unix_ns: get_atomic_clock_realtime().get_time_ns().as_u64(),
        })
    }

    /// Records a stage and its optional predecessor without retaining message state.
    pub fn record(self, stage: &'static str, parent: Option<Self>, epoch: Option<u64>) {
        log::info!(target: "ExecutionTiming",
            "stage={stage} trace={} parent={:?} epoch={epoch:?} host_mono_ns={} host_unix_ns={} elapsed_ns={:?}",
            self.id, parent.map(|p| p.id), self.monotonic_ns, self.unix_ns,
            parent.and_then(|p| self.monotonic_ns.checked_sub(p.monotonic_ns)));
    }
}

/// Returns a bounded process-local pseudonym without logging the input.
pub fn execution_timing_id(value: &str) -> u64 {
    HASHER.hash_one(value)
}

/// Records native event delivery without order quantities, prices, reasons or account IDs.
pub fn record_order_event(stage: &'static str, event: &OrderEventAny) {
    if let Some(stamp) = ExecutionTiming::capture() {
        let event: &dyn OrderEvent = match event {
            OrderEventAny::Initialized(event) => event,
            OrderEventAny::Denied(event) => event,
            OrderEventAny::Emulated(event) => event,
            OrderEventAny::Released(event) => event,
            OrderEventAny::Submitted(event) => event,
            OrderEventAny::Accepted(event) => event,
            OrderEventAny::Rejected(event) => event,
            OrderEventAny::Canceled(event) => event,
            OrderEventAny::Expired(event) => event,
            OrderEventAny::Triggered(event) => event,
            OrderEventAny::PendingUpdate(event) => event,
            OrderEventAny::PendingCancel(event) => event,
            OrderEventAny::ModifyDenied(event) => event,
            OrderEventAny::ModifyRejected(event) => event,
            OrderEventAny::CancelDenied(event) => event,
            OrderEventAny::CancelRejected(event) => event,
            OrderEventAny::Updated(event) => event,
            OrderEventAny::Filled(event) => event,
            OrderEventAny::FillVoided(event) => event,
        };
        log::info!(target: "ExecutionTiming",
            "stage={stage} trace={} host_mono_ns={} host_unix_ns={} event_id={} client={:016x} event={} event_ts_ns={} init_ts_ns={}",
            stamp.id, stamp.monotonic_ns, stamp.unix_ns, event.id(),
            execution_timing_id(event.client_order_id().as_str()), event.type_name(),
            event.ts_event(), event.ts_init());
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn execution_timing_is_opt_in_and_uses_one_monotonic_origin() {
        assert!(ExecutionTiming::capture().is_none());
        set_execution_timing_enabled(true);
        let first = ExecutionTiming::capture().unwrap();
        let second = ExecutionTiming::capture().unwrap();
        set_execution_timing_enabled(false);
        assert!(ExecutionTiming::capture().is_none());
        assert!(second.monotonic_ns >= first.monotonic_ns);
        assert!(second.id > first.id);
        assert!(first.unix_ns > 0);
        assert_eq!(
            execution_timing_id("private-id"),
            execution_timing_id("private-id")
        );
        assert_ne!(
            execution_timing_id("private-id"),
            execution_timing_id("another-id")
        );
    }
}
