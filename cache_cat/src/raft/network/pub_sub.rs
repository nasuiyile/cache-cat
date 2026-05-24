use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use crate::raft::types::core::response_value::Value;

// 订阅者信息，包含客户端ID和发送端
struct Subscriber {
    client_id: u64,
    sender: watch::Sender<Option<Value>>,
}

pub struct PubSub {
    subs: Arc<RwLock<HashMap<Vec<u8>, Vec<Subscriber>>>>,
    patterns: Arc<RwLock<HashMap<Vec<u8>, Vec<Subscriber>>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            subs: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 订阅多个精确频道
    pub async fn subscribe(&self, channels: Vec<Vec<u8>>, client_id: u64) -> (Value, watch::Receiver<Option<Value>>) {
        let (tx_main, rx_main) = watch::channel(None);
        let mut subs_rx = Vec::new();
        let mut responses = Vec::new();

        for channel in channels {
            let (resp, rx) = self.subscribe_single(channel, client_id).await;
            // 将 subscribe_single 返回的数组元素展开
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
            subs_rx.push(rx);
        }

        tokio::spawn(merge_subscriptions(subs_rx, tx_main));

        // 直接返回包含所有元素的数组
        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx_main)
    }

    async fn subscribe_single(&self, channel: Vec<u8>, client_id: u64) -> (Value, watch::Receiver<Option<Value>>) {
        let (tx, rx) = watch::channel(None);
        let subscriber = Subscriber {
            client_id,
            sender: tx,
        };

        let mut subs = self.subs.write().await;
        subs.entry(channel.clone())
            .or_insert_with(Vec::new)
            .push(subscriber);

        let count = subs.get(&channel)
            .map(|v| v.len())
            .unwrap_or(0) as i64;

        let resp = Value::Array(Some(vec![
            Value::SimpleString("subscribe".to_string()),
            Value::BulkString(Some(channel)),
            Value::Integer(count),
        ]));
        (resp, rx)
    }

    /// 退订多个精确频道
    pub async fn unsubscribe(&self, channels: Vec<Vec<u8>>, client_id: u64) -> Value {
        let mut responses = Vec::new();

        for channel in channels {
            let resp = self.unsubscribe_single(channel, client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        Value::Array(Some(responses))
    }

    /// 退订单个精确频道
    async fn unsubscribe_single(&self, channel: Vec<u8>, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        match subs.get_mut(&channel) {
            Some(subscribers) => {
                // 先记录移除前的数量
                let count = subscribers.len() as i64;

                if let Some(pos) = subscribers.iter().position(|s| s.client_id == client_id) {
                    let subscriber = subscribers.remove(pos);
                    let _ = subscriber.sender.send(None);

                    if subscribers.is_empty() {
                        subs.remove(&channel);
                    }

                    Value::Array(Some(vec![
                        Value::SimpleString("unsubscribe".to_string()),
                        Value::BulkString(Some(channel)),
                        Value::Integer(count),  // 使用移除前的数量
                    ]))
                } else {
                    // 客户端没有订阅这个频道
                    Value::Array(Some(vec![
                        Value::SimpleString("unsubscribe".to_string()),
                        Value::BulkString(Some(channel)),
                        Value::Integer(count),
                    ]))
                }
            }
            None => {
                Value::Array(Some(vec![
                    Value::SimpleString("unsubscribe".to_string()),
                    Value::BulkString(Some(channel)),
                    Value::Integer(0),
                ]))
            }
        }
    }

    /// 退订客户端的所有频道（不包括模式订阅）
    pub async fn unsubscribe_all_channels(&self, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        let mut responses = Vec::new();

        // 收集需要退订的频道
        let channels_to_unsubscribe: Vec<Vec<u8>> = subs.iter()
            .filter(|(_, subscribers)| subscribers.iter().any(|s| s.client_id == client_id))
            .map(|(channel, _)| channel.clone())
            .collect();

        // 退订所有频道
        for channel in channels_to_unsubscribe {
            if let Some(subscribers) = subs.get_mut(&channel) {
                // 先记录移除前的数量
                let count = subscribers.len() as i64;

                if let Some(pos) = subscribers.iter().position(|s| s.client_id == client_id) {
                    let subscriber = subscribers.remove(pos);
                    let _ = subscriber.sender.send(None);

                    if subscribers.is_empty() {
                        subs.remove(&channel);
                    }

                    responses.push(Value::Array(Some(vec![
                        Value::SimpleString("unsubscribe".to_string()),
                        Value::BulkString(Some(channel)),
                        Value::Integer(count),  // 使用移除前的数量
                    ])));
                }
            }
        }

        Value::Array(Some(responses))
    }

    /// 订阅多个模式
    pub async fn psubscribe(&self, patterns: Vec<Vec<u8>>, client_id: u64) -> (Value, watch::Receiver<Option<Value>>) {
        let (tx_main, rx_main) = watch::channel(None);
        let mut patterns_rx = Vec::new();
        let mut responses = Vec::new();

        for pattern in patterns {
            let (resp, rx) = self.psubscribe_single(pattern, client_id).await;
            responses.push(resp);
            patterns_rx.push(rx);
        }

        tokio::spawn(merge_subscriptions(patterns_rx, tx_main));
        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx_main)
    }

    async fn psubscribe_single(&self, pattern: Vec<u8>, client_id: u64) -> (Value, watch::Receiver<Option<Value>>) {
        let (tx, rx) = watch::channel(None);
        let subscriber = Subscriber {
            client_id,
            sender: tx,
        };

        let mut patterns = self.patterns.write().await;
        patterns.entry(pattern.clone())
            .or_insert_with(Vec::new)
            .push(subscriber);

        let count = patterns.get(&pattern)
            .map(|v| v.len())
            .unwrap_or(0) as i64;

        let resp = Value::Array(Some(vec![
            Value::SimpleString("psubscribe".to_string()),
            Value::BulkString(Some(pattern)),
            Value::Integer(count),
        ]));
        (resp, rx)
    }

    pub async fn punsubscribe(&self, pattern: &[u8], client_id: u64) -> Result<Value, Value> {
        let mut patterns = self.patterns.write().await;
        if let Some(subscribers) = patterns.get_mut(pattern) {
            // 先记录移除前的数量
            let count = subscribers.len() as i64;

            // 查找并移除指定客户端的订阅者
            if let Some(pos) = subscribers.iter().position(|s| s.client_id == client_id) {
                let subscriber = subscribers.remove(pos);
                let _ = subscriber.sender.send(None);

                let resp = Value::Array(Some(vec![
                    Value::SimpleString("punsubscribe".to_string()),
                    Value::BulkString(Some(pattern.to_vec())),
                    Value::Integer(count),  // 使用移除前的数量
                ]));

                if subscribers.is_empty() {
                    patterns.remove(pattern);
                }
                return Ok(resp);
            }
        }
        Err(Value::error("no such pattern subscription"))
    }

    // 在 PubSub 中新增一个专门处理用户消息发布的方法
    pub async fn publish_message(&self, channel: &[u8], message: Vec<u8>) -> Value {
        let pub_msg = Value::Array(Some(vec![
            Value::SimpleString("message".to_string()),
            Value::BulkString(Some(channel.to_vec())),
            Value::BulkString(Some(message)),
        ]));
        self.publish(channel, pub_msg).await
    }

    /// 发布消息
    pub async fn publish(&self, channel: &[u8], message: Value) -> Value {
        let mut delivered = 0;

        let subs = self.subs.read().await;
        if let Some(subscribers) = subs.get(channel) {
            for subscriber in subscribers {
                if subscriber.sender.send(Some(message.clone())).is_ok() {
                    delivered += 1;
                }
            }
        }
        drop(subs);

        let patterns = self.patterns.read().await;
        for (pattern, subscribers) in patterns.iter() {
            if matches_pattern(channel, pattern) {
                for subscriber in subscribers {
                    if subscriber.sender.send(Some(message.clone())).is_ok() {
                        delivered += 1;
                    }
                }
            }
        }
        Value::Integer(delivered)
    }

}

// ================== 合并多个订阅流（修复 recv 问题） ==================
async fn merge_subscriptions(
    mut subs: Vec<watch::Receiver<Option<Value>>>,
    tx: watch::Sender<Option<Value>>,
) {
    let mut tasks = vec![];

    for mut rx in subs {
        let tx_clone = tx.clone();
        tasks.push(tokio::spawn(async move {
            // watch::Receiver 使用 changed() 等待变化，然后 borrow() 获取值
            loop {
                // 等待值变化
                match rx.changed().await {
                    Ok(()) => {
                        // 获取当前值
                        let val = rx.borrow().clone();

                        // 如果是 None，表示该订阅已结束
                        if val.is_none() {
                            break;
                        }

                        // 转发消息
                        if tx_clone.send(val).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        // 发送端已关闭
                        break;
                    }
                }
            }
        }));
    }

    // 等待所有内部任务结束
    for task in tasks {
        let _ = task.await;
    }

    // 所有订阅都已结束，通知主 Receiver 关闭
    let _ = tx.send(None);
}

// ================== 模式匹配（支持 * 和 ?） ==================
pub fn matches_pattern(channel: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return channel.is_empty();
    }

    match pattern[0] {
        b'*' => {
            // * 匹配 0 个或多个字节
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