use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

// 客户端状态
struct ClientState {
    sender: watch::Sender<Option<Value>>,
    // 记录该客户端订阅了哪些频道，用于退订所有频道时清理
    subscribed_channels: HashSet<Bytes>,
    subscribed_patterns: HashSet<Bytes>,
}

pub struct PubSub {
    /// 精确频道订阅：频道 -> 订阅的客户端ID集合
    subs: Arc<RwLock<HashMap<Bytes, HashSet<u64>>>>,
    /// 模式订阅：模式 -> 订阅的客户端ID集合
    patterns: Arc<RwLock<HashMap<Bytes, HashSet<u64>>>>,
    /// 客户端状态管理：client_id -> ClientState
    clients: Arc<RwLock<HashMap<u64, ClientState>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            subs: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 为客户端获取或创建状态，并返回新的 Receiver
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
        // watch::Sender 可以通过 subscribe() 方法创建新的 Receiver
        // 所有通过 subscribe() 创建的 Receiver 都会收到后续的消息
        state.sender.subscribe()
    }

    /// 订阅多个精确频道
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

        // 记录该客户端订阅了这些频道
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

    /// 订阅单个精确频道
    async fn subscribe_single(&self, channel: Bytes, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        subs.entry(channel.clone()).or_default().insert(client_id);

        let count = subs.get(&channel).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::from_static_string("subscribe"),
            Value::BulkString(Some(channel)),
            Value::Integer(count),
        ]))
    }

    /// 退订多个精确频道
    pub async fn unsubscribe(&self, channels: Vec<Bytes>, client_id: u64) -> Value {
        let mut responses = Vec::new();
        for channel in &channels {
            let resp = self.unsubscribe_single(channel.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // 从客户端状态中移除这些频道的记录，并在无订阅时清理
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in &channels {
                    state.subscribed_channels.remove(channel);
                }
                // 如果该客户端不再有任何订阅，发送关闭信号并清理
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// 退订单个精确频道
    async fn unsubscribe_single(&self, channel: Bytes, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        match subs.get_mut(&channel) {
            Some(set) => {
                let count = set.len() as i64;
                let existed = set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(&channel);
                }

                // 修复：如果客户端实际订阅了这个频道，返回移除前的计数
                // 如果没有订阅（不应该发生，但做防护），返回 0
                Value::Array(Some(vec![
                    Value::from_static_string("unsubscribe"),
                    Value::BulkString(Some(channel)),
                    Value::Integer(if existed { count } else { 0 }),
                ]))
            }
            None => Value::Array(Some(vec![
                Value::from_static_string("unsubscribe"),
                Value::BulkString(Some(channel)),
                Value::Integer(0),
            ])),
        }
    }

    /// 退订客户端的所有精确频道
    pub async fn unsubscribe_all_channels(&self, client_id: u64) -> Value {
        let mut responses = Vec::new();

        // 获取该客户端订阅的所有频道
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

        // 退订所有频道
        let mut subs = self.subs.write().await;
        for channel in &channels {
            if let Some(set) = subs.get_mut(channel) {
                let count = set.len() as i64;
                set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(channel);
                }
                responses.push(Value::Array(Some(vec![
                    Value::from_static_string("unsubscribe"),
                    Value::BulkString(Some(channel.clone())),
                    Value::Integer(count),
                ])));
            }
        }
        drop(subs);

        // 清理客户端状态，如果没有任何订阅则发送关闭信号
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                state.subscribed_channels.clear();
                // 只有在模式也为空时才清理，否则保留模式订阅
                Self::cleanup_client_if_empty(&mut clients, client_id);
            }
        }

        Value::Array(Some(responses))
    }

    /// 订阅多个模式
    pub async fn psubscribe(
        &self,
        patterns: Vec<Bytes>,
        client_id: u64,
    ) -> (Value, watch::Receiver<Option<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        let mut responses = Vec::new();
        for pattern in &patterns {
            let resp = self.psubscribe_single(pattern.clone(), client_id).await;
            // 修复：使用 append 展开数组元素，而不是 push 整个响应
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // 记录该客户端订阅了这些模式
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

    /// 订阅单个模式
    async fn psubscribe_single(&self, pattern: Bytes, client_id: u64) -> Value {
        let mut patterns = self.patterns.write().await;
        patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);

        let count = patterns.get(&pattern).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::from_static_string("psubscribe"),
            Value::BulkString(Some(pattern)),
            Value::Integer(count),
        ]))
    }

    pub async fn publish(&self, channel: &[u8], message_content: Bytes) -> Value {
        let mut delivered_clients = HashSet::new();

        // 精确频道订阅者
        let exact_subs: HashSet<u64> = {
            let subs = self.subs.read().await;
            subs.get(channel).cloned().unwrap_or_default()
        };

        // 构建精确订阅消息
        let exact_msg = Value::Array(Some(vec![
            Value::from_static_string("message"),
            Value::BulkString(Some(Bytes::copy_from_slice(channel))),
            Value::BulkString(Some(message_content.clone())),
        ]));

        // 收集所有匹配的模式及其订阅者
        let mut pattern_targets: Vec<(Bytes, HashSet<u64>)> = Vec::new();
        {
            let patterns = self.patterns.read().await;
            for (pattern, set) in patterns.iter() {
                if matches_pattern(channel, pattern) {
                    pattern_targets.push((pattern.clone(), set.clone()));
                }
            }
        }

        let clients = self.clients.read().await;

        // 发送给精确订阅者
        for client_id in &exact_subs {
            if let Some(state) = clients.get(client_id) {
                if state.sender.send(Some(exact_msg.clone())).is_ok() {
                    delivered_clients.insert(*client_id);
                }
            }
        }

        // 发送给模式订阅者（每个模式使用独立的 pmessage 格式）
        for (pattern, set) in &pattern_targets {
            let pmessage = Value::Array(Some(vec![
                Value::from_static_string("pmessage"),
                Value::BulkString(Some(pattern.clone())),
                Value::BulkString(Some(Bytes::copy_from_slice(channel))),
                Value::BulkString(Some(message_content.clone())),
            ]));
            for client_id in set {
                if let Some(state) = clients.get(client_id) {
                    if state.sender.send(Some(pmessage.clone())).is_ok() {
                        delivered_clients.insert(*client_id);
                    }
                }
            }
        }
        drop(clients);

        // 返回收到消息的客户端数量（去重）
        Value::Integer(delivered_clients.len() as i64)
    }

    /// 完全移除客户端（连接断开时调用）
    pub async fn remove_client(&self, client_id: u64) {
        // 清理精确频道订阅
        let mut subs = self.subs.write().await;
        subs.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(subs);

        // 清理模式订阅
        let mut patterns = self.patterns.write().await;
        patterns.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(patterns);

        // 强制移除客户端状态，并先发送空消息通知所有 Receiver
        let mut clients = self.clients.write().await;
        if let Some(state) = clients.get_mut(&client_id) {
            let _ = state.sender.send(None); // 发送关闭信号
        }
        clients.remove(&client_id);
    }

    /// 退订多个模式（外部调用）
    pub async fn punsubscribe(&self, patterns: Vec<Bytes>, client_id: u64) -> Value {
        let mut responses = Vec::new();
        for pattern in &patterns {
            let resp = self.punsubscribe_single(pattern.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // 从客户端状态中移除这些模式的记录，并在无订阅时清理
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

    /// 退订单个模式（内部辅助）
    async fn punsubscribe_single(&self, pattern: Bytes, client_id: u64) -> Value {
        let mut patterns = self.patterns.write().await;
        match patterns.get_mut(&pattern) {
            Some(set) => {
                let count = set.len() as i64;
                let existed = set.remove(&client_id);
                if set.is_empty() {
                    patterns.remove(&pattern);
                }
                // 如果实际取消了订阅，返回移除前计数；否则（未订阅此模式）返回 0
                Value::Array(Some(vec![
                    Value::from_static_string("punsubscribe"),
                    Value::BulkString(Some(pattern)),
                    Value::Integer(if existed { count } else { 0 }),
                ]))
            }
            None => Value::Array(Some(vec![
                Value::from_static_string("punsubscribe"),
                Value::BulkString(Some(pattern)),
                Value::Integer(0),
            ])),
        }
    }

    /// 退订该客户端的所有模式（外部调用）
    pub async fn punsubscribe_all_patterns(&self, client_id: u64) -> Value {
        let mut responses = Vec::new();

        // 获取该客户端订阅的所有模式
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

        // 退订所有模式
        let mut pats = self.patterns.write().await;
        for pattern in &patterns {
            if let Some(set) = pats.get_mut(pattern) {
                let count = set.len() as i64;
                set.remove(&client_id);
                if set.is_empty() {
                    pats.remove(pattern);
                }
                responses.push(Value::Array(Some(vec![
                    Value::from_static_string("punsubscribe"),
                    Value::BulkString(Some(pattern.clone())),
                    Value::Integer(count),
                ])));
            }
        }
        drop(pats);

        // 清理客户端状态，如果没有任何订阅则发送关闭信号
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
    /// 列出当前活跃的频道，可选 pattern 进行 glob 过滤
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
    /// 返回指定频道的订阅者数量
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
    /// 返回所有客户端订阅的模式数量（不同模式的数量）
    pub async fn pubsub_numpat(&self) -> Value {
        let patterns = self.patterns.read().await;
        Value::Integer(patterns.len() as i64)
    }

    /// 获取客户端当前订阅的精确频道数量
    pub async fn client_subscription_count(&self, client_id: u64) -> u64 {
        let clients = self.clients.read().await;
        clients
            .get(&client_id)
            .map(|state| state.subscribed_channels.len() as u64)
            .unwrap_or(0)
    }

    /// 获取客户端当前订阅的模式数量
    pub async fn client_pattern_count(&self, client_id: u64) -> u64 {
        let clients = self.clients.read().await;
        clients
            .get(&client_id)
            .map(|state| state.subscribed_patterns.len() as u64)
            .unwrap_or(0)
    }

    // ------------------ 私有辅助函数 ------------------

    /// 当客户端不再有任何订阅时，发送空消息并移除客户端状态。
    /// 调用前需保证已持有 `clients` 的写锁，并已更新完订阅集合。
    fn cleanup_client_if_empty(clients: &mut HashMap<u64, ClientState>, client_id: u64) {
        if let Some(state) = clients.get_mut(&client_id) {
            if state.subscribed_channels.is_empty() && state.subscribed_patterns.is_empty() {
                // 发送一个 None 作为关闭信号，通知所有 Receiver 订阅已结束
                let _ = state.sender.send(None);
                clients.remove(&client_id);
            }
        }
    }
}

/// 简单的 glob 风格模式匹配（支持 * 和 ?）
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
