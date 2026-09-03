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

//! Trackers for in-flight submit and cancel commands whose venue order ID is not yet known.

use std::sync::{Arc, Mutex, OnceLock};

use ahash::AHashSet;
use nautilus_common::cache::fifo::FifoCacheMap;
use nautilus_core::MUTEX_POISONED;
use nautilus_model::identifiers::{ClientOrderId, VenueOrderId};

static EXPECTED_VENUE_IDS: OnceLock<Mutex<FifoCacheMap<ClientOrderId, VenueOrderId, 10_000>>> =
    OnceLock::new();

fn expected_venue_ids() -> &'static Mutex<FifoCacheMap<ClientOrderId, VenueOrderId, 10_000>> {
    EXPECTED_VENUE_IDS.get_or_init(|| Mutex::new(FifoCacheMap::default()))
}

/// Returns the deterministic venue hash retained for an ambiguous submit.
///
/// This allows the embedding sidecar to durably persist the exact identity
/// without promoting the order to accepted.
pub fn expected_venue_order_id(client_order_id: &str) -> Option<String> {
    expected_venue_ids()
        .lock()
        .expect(MUTEX_POISONED)
        .get(&ClientOrderId::from(client_order_id))
        .map(ToString::to_string)
}

/// Maps an in-flight submit's expected venue order ID to its local client order ID, so the
/// cache-free WS dispatch can resolve a tracked own order before the submit response lands.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingSubmitTracker {
    venue_to_client: Arc<Mutex<FifoCacheMap<VenueOrderId, ClientOrderId, 10_000>>>,
    client_to_venue: Arc<Mutex<FifoCacheMap<ClientOrderId, VenueOrderId, 10_000>>>,
}

impl PendingSubmitTracker {
    pub(crate) fn insert(&self, venue_order_id: VenueOrderId, client_order_id: ClientOrderId) {
        self.venue_to_client
            .lock()
            .expect(MUTEX_POISONED)
            .insert(venue_order_id, client_order_id);
        self.client_to_venue
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_id, venue_order_id);
        expected_venue_ids()
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_id, venue_order_id);
    }

    pub(crate) fn client_order_id(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.venue_to_client
            .lock()
            .expect(MUTEX_POISONED)
            .get(venue_order_id)
            .copied()
    }

    pub(crate) fn venue_order_id(&self, client_order_id: &ClientOrderId) -> Option<VenueOrderId> {
        self.client_to_venue
            .lock()
            .expect(MUTEX_POISONED)
            .get(client_order_id)
            .copied()
    }
}

/// Tracks client order IDs whose cancel was deferred because the venue order ID was not yet
/// known, so the cancel can be issued once the submit response lands.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingCancelTracker {
    client_order_ids: Arc<Mutex<AHashSet<ClientOrderId>>>,
}

impl PendingCancelTracker {
    pub(crate) fn insert(&self, client_order_id: ClientOrderId) {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_id);
    }

    pub(crate) fn remove(&self, client_order_id: &ClientOrderId) -> bool {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .remove(client_order_id)
    }

    pub(crate) fn contains(&self, client_order_id: &ClientOrderId) -> bool {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .contains(client_order_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_submit_identity_is_resolvable_in_both_directions() {
        let tracker = PendingSubmitTracker::default();
        let client_order_id = ClientOrderId::from("client-1");
        let venue_order_id = VenueOrderId::from("0xvenue-1");

        tracker.insert(venue_order_id, client_order_id);

        assert_eq!(
            tracker.client_order_id(&venue_order_id),
            Some(client_order_id)
        );
        assert_eq!(
            tracker.venue_order_id(&client_order_id),
            Some(venue_order_id)
        );
        assert_eq!(
            expected_venue_order_id(client_order_id.as_str()).as_deref(),
            Some(venue_order_id.as_str())
        );
    }
}
