use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

// Client status
struct ClientState {
    sender: watch::Sender<Option<Value>>,
    // Record which channels the client has subscribed to,
    // and clear them when unsubscribing all channels
    subscribed_channels: HashSet<Bytes>,
    subscribed_patterns: HashSet<Bytes>,
}

#[derive(Default)]
pub struct PubSub {
    /// Precise channel subscription: Channel -> Collection of subscribed client IDs
    subs: Arc<RwLock<HashMap<Bytes, HashSet<u64>>>>,
    /// Mode subscription: Mode -> Collection of subscribed client IDs
    patterns: Arc<RwLock<HashMap<Bytes, HashSet<u64>>>>,
    /// Client State Management: client_id -> ClientState
    clients: Arc<RwLock<HashMap<u64, ClientState>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieve or create status for the client and return a new Receiver
    async fn get_or_create_client(&self, client_id: u64) -> watch::Receiver<Option<Value>> {
        let mut clients = self.clients.write().await;
        let state = clients.entry(client_id).or_insert_with(|| {
            let (tx, _) = watch::channel(None);
            ClientState {
                sender: tx,
                subscribed_channels: HashSet::new(),
                subscribed_patterns: HashSet::new(),
            }
        });
        // watch::Sender can create a new Receiver using the subscribe() method
        // All receivers created through subscribe() will receive subsequent messages
        state.sender.subscribe()
    }

    /// Subscribe to multiple precise channels
    pub async fn subscribe(
        &self,
        channels: Vec<Bytes>,
        client_id: u64,
    ) -> (Value, watch::Receiver<Option<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        let mut responses = Vec::new();
        for channel in &channels {
            let resp = self.subscribe_single(channel.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // Record that the client subscribed to these channels
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in &channels {
                    state.subscribed_channels.insert(channel.clone());
                }
            }
        }

        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx)
    }

    /// Subscribe to a single precise channel
    async fn subscribe_single(&self, channel: Bytes, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        subs.entry(channel.clone()).or_default().insert(client_id);

        let count = subs.get(&channel).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::SimpleString("subscribe".to_string()),
            Value::BulkString(Some(channel)),
            Value::Integer(count),
        ]))
    }

    /// Unsubscribe from multiple precise channels
    pub async fn unsubscribe(&self, channels: Vec<Bytes>, client_id: u64) -> Value {
        let mut responses = Vec::new();
        for channel in &channels {
            let resp = self.unsubscribe_single(channel.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // Remove the records of these channels from the client state and
        // clean them up when there are no subscriptions
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in &channels {
                    state.subscribed_channels.remove(channel);
                }
                // If the client no longer has any subscriptions,
                // send a close signal and clear it
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// Unsubscribe from a single precise channel
    async fn unsubscribe_single(&self, channel: Bytes, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        match subs.get_mut(&channel) {
            Some(set) => {
                let count = set.len() as i64;
                let existed = set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(&channel);
                }

                // Fix: If the client actually subscribed to this channel,
                // return the count before removal.
                // If there is no subscription (which should not occur,
                // but should be protected), return 0.
                Value::Array(Some(vec![
                    Value::SimpleString("unsubscribe".to_string()),
                    Value::BulkString(Some(channel)),
                    Value::Integer(if existed { count } else { 0 }),
                ]))
            }
            None => Value::Array(Some(vec![
                Value::SimpleString("unsubscribe".to_string()),
                Value::BulkString(Some(channel)),
                Value::Integer(0),
            ])),
        }
    }

    /// Unsubscribe all precise channels of the client
    pub async fn unsubscribe_all_channels(&self, client_id: u64) -> Value {
        let mut responses = Vec::new();

        // Get all channels subscribed to by the client
        let channels = {
            let clients = self.clients.read().await;
            clients
                .get(&client_id)
                .map(|state| {
                    state
                        .subscribed_channels
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        // Unsubscribe from all channels
        let mut subs = self.subs.write().await;
        for channel in &channels {
            if let Some(set) = subs.get_mut(channel) {
                let count = set.len() as i64;
                set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(channel);
                }
                responses.push(Value::Array(Some(vec![
                    Value::SimpleString("unsubscribe".to_string()),
                    Value::BulkString(Some(channel.clone())),
                    Value::Integer(count),
                ])));
            }
        }
        drop(subs);

        // Clear the client status and send a close signal if there are no subscriptions
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                state.subscribed_channels.clear();
                // Only clear when the mode is also empty, otherwise keep the mode subscription
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// Subscribe to multiple modes
    pub async fn psubscribe(
        &self,
        patterns: Vec<Bytes>,
        client_id: u64,
    ) -> (Value, watch::Receiver<Option<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        let mut responses = Vec::new();
        for pattern in &patterns {
            let resp = self.psubscribe_single(pattern.clone(), client_id).await;
            // Fix: Use append to expand array elements instead of pushing the entire response
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // Record that the client subscribed to these modes
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in &patterns {
                    state.subscribed_patterns.insert(pattern.clone());
                }
            }
        }

        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx)
    }

    /// Subscription single mode
    async fn psubscribe_single(&self, pattern: Bytes, client_id: u64) -> Value {
        let mut patterns = self.patterns.write().await;
        patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);

        let count = patterns.get(&pattern).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::SimpleString("psubscribe".to_string()),
            Value::BulkString(Some(pattern)),
            Value::Integer(count),
        ]))
    }

    pub async fn publish(&self, channel: Bytes, message_content: Bytes) -> Value {
        let mut delivered_clients = HashSet::new();

        // Precise channel subscribers
        let exact_subs: HashSet<u64> = {
            let subs = self.subs.read().await;
            subs.get(&channel).cloned().unwrap_or_default()
        };

        // Build precise subscription messages
        let exact_msg = Value::Array(Some(vec![
            Value::SimpleString("message".to_string()),
            Value::BulkString(Some(channel.clone())),
            Value::BulkString(Some(message_content.clone())),
        ]));

        // Collect all matching patterns and their subscribers
        let mut pattern_targets: Vec<(Bytes, HashSet<u64>)> = Vec::new();
        {
            let patterns = self.patterns.read().await;
            for (pattern, set) in patterns.iter() {
                if matches_pattern(&channel, pattern) {
                    pattern_targets.push((pattern.clone(), set.clone()));
                }
            }
        }

        let clients = self.clients.read().await;

        // Send to precise subscribers
        for client_id in &exact_subs {
            if let Some(state) = clients.get(client_id)
                && state.sender.send(Some(exact_msg.clone())).is_ok()
            {
                delivered_clients.insert(*client_id);
            }
        }

        // Send to pattern subscribers (each pattern uses a separate pmessage format)
        for (pattern, set) in &pattern_targets {
            let pmessage = Value::Array(Some(vec![
                Value::SimpleString("pmessage".to_string()),
                Value::BulkString(Some(pattern.clone())),
                Value::BulkString(Some(channel.to_vec().into())),
                Value::BulkString(Some(message_content.clone())),
            ]));

            for client_id in set {
                if let Some(state) = clients.get(client_id)
                    && state.sender.send(Some(pmessage.clone())).is_ok()
                {
                    delivered_clients.insert(*client_id);
                }
            }
        }
        drop(clients);

        // Return the number of clients who received the message (deduplicated)
        Value::Integer(delivered_clients.len() as i64)
    }

    /// Completely remove the client (called when the connection is disconnected)
    pub async fn remove_client(&self, client_id: u64) {
        // Clear precise channel subscriptions
        let mut subs = self.subs.write().await;
        subs.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(subs);

        // Clean up mode subscription
        let mut patterns = self.patterns.write().await;
        patterns.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(patterns);

        // Forcefully remove the client state and send an empty message to notify all receivers first
        let mut clients = self.clients.write().await;
        if let Some(state) = clients.get_mut(&client_id) {
            let _ = state.sender.send(None); // Send a shutdown signal
        }
        clients.remove(&client_id);
    }

    /// Unsubscribe from multiple modes (external calls)
    pub async fn punsubscribe(&self, patterns: Vec<Bytes>, client_id: u64) -> Value {
        let mut responses = Vec::new();
        for pattern in &patterns {
            let resp = self.punsubscribe_single(pattern.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // Remove records of these patterns from the client state
        // and clean them up when there are no subscriptions
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in &patterns {
                    state.subscribed_patterns.remove(pattern);
                }
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// Unsubscribe individual mode (internal assistance)
    async fn punsubscribe_single(&self, pattern: Bytes, client_id: u64) -> Value {
        let mut patterns = self.patterns.write().await;
        match patterns.get_mut(&pattern) {
            Some(set) => {
                let count = set.len() as i64;
                let existed = set.remove(&client_id);
                if set.is_empty() {
                    patterns.remove(&pattern);
                }
                // If the subscription is actually cancelled, return the count before removal;
                // Otherwise (not subscribed to this mode), return 0
                Value::Array(Some(vec![
                    Value::SimpleString("punsubscribe".to_string()),
                    Value::BulkString(Some(pattern)),
                    Value::Integer(if existed { count } else { 0 }),
                ]))
            }
            None => Value::Array(Some(vec![
                Value::SimpleString("punsubscribe".to_string()),
                Value::BulkString(Some(pattern)),
                Value::Integer(0),
            ])),
        }
    }

    /// Unsubscribe all modes of this client (external calls)
    pub async fn punsubscribe_all_patterns(&self, client_id: u64) -> Value {
        let mut responses = Vec::new();

        // Get all modes subscribed by the client
        let patterns = {
            let clients = self.clients.read().await;
            clients
                .get(&client_id)
                .map(|state| {
                    state
                        .subscribed_patterns
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        // Unsubscribe from all modes
        let mut pats = self.patterns.write().await;
        for pattern in &patterns {
            if let Some(set) = pats.get_mut(pattern) {
                let count = set.len() as i64;
                set.remove(&client_id);
                if set.is_empty() {
                    pats.remove(pattern);
                }
                responses.push(Value::Array(Some(vec![
                    Value::SimpleString("punsubscribe".to_string()),
                    Value::BulkString(Some(pattern.clone())),
                    Value::Integer(count),
                ])));
            }
        }
        drop(pats);

        // Clear the client status and send a close signal if there are no subscriptions
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                state.subscribed_patterns.clear();
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// PUBSUB CHANNELS [pattern]
    /// List the currently active channels and select patterns for glob filtering
    pub async fn pubsub_channels(&self, pattern: Option<&[u8]>) -> Value {
        let subs = self.subs.read().await;
        let channels: Vec<Value> = subs
            .keys()
            .filter(|ch| {
                if let Some(pat) = pattern {
                    matches_pattern(ch, pat)
                } else {
                    true
                }
            })
            .map(|ch| Value::BulkString(Some(ch.clone())))
            .collect();
        Value::Array(Some(channels))
    }

    /// PUBSUB NUMSUB [channel [channel ...]]
    /// Return the number of subscribers to the specified channel
    pub async fn pubsub_numsub(&self, channels: &[Bytes]) -> Value {
        let subs = self.subs.read().await;
        let mut result = Vec::with_capacity(channels.len() * 2);
        for ch in channels {
            let count = subs.get(ch).map(|s| s.len() as i64).unwrap_or(0);
            result.push(Value::BulkString(Some(ch.clone())));
            result.push(Value::Integer(count));
        }
        Value::Array(Some(result))
    }

    /// PUBSUB NUMPAT
    /// Return the number of modes subscribed by all clients (the number of different modes)
    pub async fn pubsub_numpat(&self) -> Value {
        let patterns = self.patterns.read().await;
        Value::Integer(patterns.len() as i64)
    }

    /// Get the exact number of channels currently subscribed to by the client
    pub async fn client_subscription_count(&self, client_id: u64) -> u64 {
        let clients = self.clients.read().await;
        clients
            .get(&client_id)
            .map(|state| state.subscribed_channels.len() as u64)
            .unwrap_or(0)
    }

    /// Get the current number of subscription modes for the client
    pub async fn client_pattern_count(&self, client_id: u64) -> u64 {
        let clients = self.clients.read().await;
        clients
            .get(&client_id)
            .map(|state| state.subscribed_patterns.len() as u64)
            .unwrap_or(0)
    }

    // ------------------ Private auxiliary function ------------------

    /// When the client no longer has any subscriptions,
    /// send an empty message and remove the client state.
    /// Before calling, it is necessary to ensure that the write lock for 'clients'
    /// is already held and the subscription collection has been updated.
    fn cleanup_client_if_empty(clients: &mut HashMap<u64, ClientState>, client_id: u64) {
        if let Some(state) = clients.get_mut(&client_id)
            && state.subscribed_channels.is_empty()
            && state.subscribed_patterns.is_empty()
        {
            // Send a 'None' as a shutdown signal to notify all receivers
            // that the subscription has ended
            let _ = state.sender.send(None);
            clients.remove(&client_id);
        }
    }
}

/// Simple globe style pattern matching (supports * and?)
pub fn matches_pattern(channel: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return channel.is_empty();
    }
    match pattern[0] {
        b'*' => {
            for i in 0..=channel.len() {
                if matches_pattern(&channel[i..], &pattern[1..]) {
                    return true;
                }
            }
            false
        }
        b'?' => {
            if channel.is_empty() {
                false
            } else {
                matches_pattern(&channel[1..], &pattern[1..])
            }
        }
        c => {
            if channel.is_empty() || channel[0] != c {
                false
            } else {
                matches_pattern(&channel[1..], &pattern[1..])
            }
        }
    }
}
