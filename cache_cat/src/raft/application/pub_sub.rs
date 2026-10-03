use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

// Client status
struct ClientState {
    sender: mpsc::UnboundedSender<Value>,
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

    /// Return the receiver only for the first subscription. Further
    /// subscriptions reuse the queue consumed by the connection's existing loop.
    async fn get_or_create_client(&self, client_id: u64) -> Option<mpsc::UnboundedReceiver<Value>> {
        let mut clients = self.clients.write().await;
        let mut receiver = None;
        clients.entry(client_id).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            receiver = Some(rx);
            ClientState {
                sender: tx,
                subscribed_channels: HashSet::new(),
                subscribed_patterns: HashSet::new(),
            }
        });
        receiver
    }

    /// Build a pub/sub confirmation / notification frame.
    ///
    /// These are Push frames: encoded as `>N` for RESP3 clients and as plain
    /// `*N` arrays for RESP2 clients, exactly like Redis. The message kind is
    /// a bulk string, matching the wire format of real Redis.
    fn push_frame(kind: &'static str, target: Option<Bytes>, count: i64) -> Value {
        Value::Push(vec![
            Value::BulkString(Some(Bytes::from_static(kind.as_bytes()))),
            Value::BulkString(target),
            Value::Integer(count),
        ])
    }

    /// Total number of channels + patterns the client is subscribed to.
    /// Redis reports this as the third element of every
    /// (p)subscribe / (p)unsubscribe confirmation.
    fn client_sub_total(state: &ClientState) -> i64 {
        (state.subscribed_channels.len() + state.subscribed_patterns.len()) as i64
    }

    /// Subscribe to multiple precise channels
    pub async fn subscribe(
        &self,
        channels: Vec<Bytes>,
        client_id: u64,
    ) -> (Value, Option<mpsc::UnboundedReceiver<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        // Register the subscriptions
        {
            let mut subs = self.subs.write().await;
            for channel in &channels {
                subs.entry(channel.clone()).or_default().insert(client_id);
            }
        }

        // One confirmation frame per channel, exactly like Redis. The count
        // is the client's total subscription count after each subscription.
        let mut frames = Vec::with_capacity(channels.len());
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in channels {
                    state.subscribed_channels.insert(channel.clone());
                    frames.push(Self::push_frame(
                        "subscribe",
                        Some(channel),
                        Self::client_sub_total(state),
                    ));
                }
            }
        }

        (Value::Batch(frames), rx)
    }

    /// Unsubscribe from multiple precise channels
    pub async fn unsubscribe(&self, channels: Vec<Bytes>, client_id: u64) -> Value {
        // Remove the client from each channel's subscriber set
        {
            let mut subs = self.subs.write().await;
            for channel in &channels {
                if let Some(set) = subs.get_mut(channel) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        subs.remove(channel);
                    }
                }
            }
        }

        // One confirmation frame per channel, exactly like Redis: the count
        // is the client's remaining subscription count after each removal.
        let mut frames = Vec::with_capacity(channels.len());
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in channels {
                    state.subscribed_channels.remove(&channel);
                    frames.push(Self::push_frame(
                        "unsubscribe",
                        Some(channel),
                        Self::client_sub_total(state),
                    ));
                }
                // Drop the sender when the client has no subscriptions left.
                Self::cleanup_client_if_empty(&mut clients, client_id);
            } else {
                for channel in channels {
                    frames.push(Self::push_frame("unsubscribe", Some(channel), 0));
                }
            }
        }

        Value::Batch(frames)
    }

    /// Unsubscribe all precise channels of the client
    pub async fn unsubscribe_all_channels(&self, client_id: u64) -> Value {
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
        {
            let mut subs = self.subs.write().await;
            for channel in &channels {
                if let Some(set) = subs.get_mut(channel) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        subs.remove(channel);
                    }
                }
            }
        }

        // One confirmation frame per channel; when the client was not
        // subscribed to anything Redis still sends one frame with a null
        // channel name.
        let mut frames = Vec::with_capacity(channels.len().max(1));
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in channels {
                    state.subscribed_channels.remove(&channel);
                    frames.push(Self::push_frame(
                        "unsubscribe",
                        Some(channel),
                        Self::client_sub_total(state),
                    ));
                }
                if frames.is_empty() {
                    frames.push(Self::push_frame(
                        "unsubscribe",
                        None,
                        Self::client_sub_total(state),
                    ));
                }
                // Only clear when the mode is also empty, otherwise keep the mode subscription
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }
        if frames.is_empty() {
            frames.push(Self::push_frame("unsubscribe", None, 0));
        }

        Value::Batch(frames)
    }

    /// Subscribe to multiple modes
    pub async fn psubscribe(
        &self,
        patterns: Vec<Bytes>,
        client_id: u64,
    ) -> (Value, Option<mpsc::UnboundedReceiver<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        // Register the pattern subscriptions
        {
            let mut pats = self.patterns.write().await;
            for pattern in &patterns {
                pats.entry(pattern.clone()).or_default().insert(client_id);
            }
        }

        // One confirmation frame per pattern, exactly like Redis.
        let mut frames = Vec::with_capacity(patterns.len());
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in patterns {
                    state.subscribed_patterns.insert(pattern.clone());
                    frames.push(Self::push_frame(
                        "psubscribe",
                        Some(pattern),
                        Self::client_sub_total(state),
                    ));
                }
            }
        }

        (Value::Batch(frames), rx)
    }

    pub async fn publish(&self, channel: Bytes, message_content: Bytes) -> Value {
        let mut delivered_clients = HashSet::new();

        // Precise channel subscribers
        let exact_subs: HashSet<u64> = {
            let subs = self.subs.read().await;
            subs.get(&channel).cloned().unwrap_or_default()
        };

        // Build precise subscription messages (Push frames:
        // RESP3 `>3`, RESP2 plain `*3` array, like Redis)
        let exact_msg = Value::Push(vec![
            Value::BulkString(Some(Bytes::from_static(b"message"))),
            Value::BulkString(Some(channel.clone())),
            Value::BulkString(Some(message_content.clone())),
        ]);

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
                && state.sender.send(exact_msg.clone()).is_ok()
            {
                delivered_clients.insert(*client_id);
            }
        }

        // Send to pattern subscribers (each pattern uses a separate pmessage format)
        for (pattern, set) in &pattern_targets {
            let pmessage = Value::Push(vec![
                Value::BulkString(Some(Bytes::from_static(b"pmessage"))),
                Value::BulkString(Some(pattern.clone())),
                Value::BulkString(Some(channel.to_vec().into())),
                Value::BulkString(Some(message_content.clone())),
            ]);

            for client_id in set {
                if let Some(state) = clients.get(client_id)
                    && state.sender.send(pmessage.clone()).is_ok()
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

        // Dropping the sender closes the queue after its pending messages.
        let mut clients = self.clients.write().await;
        clients.remove(&client_id);
    }

    /// Unsubscribe from multiple modes (external calls)
    pub async fn punsubscribe(&self, patterns: Vec<Bytes>, client_id: u64) -> Value {
        // Remove the client from each pattern's subscriber set
        {
            let mut pats = self.patterns.write().await;
            for pattern in &patterns {
                if let Some(set) = pats.get_mut(pattern) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        pats.remove(pattern);
                    }
                }
            }
        }

        // One confirmation frame per pattern, exactly like Redis.
        let mut frames = Vec::with_capacity(patterns.len());
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in patterns {
                    state.subscribed_patterns.remove(&pattern);
                    frames.push(Self::push_frame(
                        "punsubscribe",
                        Some(pattern),
                        Self::client_sub_total(state),
                    ));
                }
                Self::cleanup_client_if_empty(&mut clients, client_id);
            } else {
                for pattern in patterns {
                    frames.push(Self::push_frame("punsubscribe", Some(pattern), 0));
                }
            }
        }

        Value::Batch(frames)
    }

    /// Unsubscribe all modes of this client (external calls)
    pub async fn punsubscribe_all_patterns(&self, client_id: u64) -> Value {
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
        {
            let mut pats = self.patterns.write().await;
            for pattern in &patterns {
                if let Some(set) = pats.get_mut(pattern) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        pats.remove(pattern);
                    }
                }
            }
        }

        // One confirmation frame per pattern; with no subscriptions Redis
        // still sends one frame with a null pattern.
        let mut frames = Vec::with_capacity(patterns.len().max(1));
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in patterns {
                    state.subscribed_patterns.remove(&pattern);
                    frames.push(Self::push_frame(
                        "punsubscribe",
                        Some(pattern),
                        Self::client_sub_total(state),
                    ));
                }
                if frames.is_empty() {
                    frames.push(Self::push_frame(
                        "punsubscribe",
                        None,
                        Self::client_sub_total(state),
                    ));
                }
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }
        if frames.is_empty() {
            frames.push(Self::push_frame("punsubscribe", None, 0));
        }

        Value::Batch(frames)
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
    /// remove its state and close the queue without discarding pending messages.
    /// Before calling, it is necessary to ensure that the write lock for 'clients'
    /// is already held and the subscription collection has been updated.
    fn cleanup_client_if_empty(clients: &mut HashMap<u64, ClientState>, client_id: u64) {
        if let Some(state) = clients.get_mut(&client_id)
            && state.subscribed_channels.is_empty()
            && state.subscribed_patterns.is_empty()
        {
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
