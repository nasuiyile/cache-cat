pub mod psubscribe;
pub mod publish;
pub mod pubsub;
pub mod punsubscribe;
pub mod subscribe;
pub mod unsubscribe;

#[cfg(test)]
mod tests {
    use crate::raft::application::pub_sub::PubSub;
    use crate::raft::types::core::response_value::Value;
    use bytes::Bytes;
    use tokio::sync::mpsc::error::TryRecvError;

    fn frame(parts: &[&str]) -> Vec<u8> {
        Value::Push(
            parts
                .iter()
                .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
                .collect(),
        )
        .encode()
    }

    #[tokio::test]
    async fn pubsub_burst_preserves_every_message_in_order() {
        let pubsub = PubSub::new();
        let (_, receiver) = pubsub.subscribe(vec![Bytes::from_static(b"news")], 1).await;
        let mut receiver = receiver.unwrap();

        // Publish the entire burst before allowing the subscriber to read.
        for number in 0..128 {
            pubsub
                .publish(Bytes::from_static(b"news"), number.to_string().into())
                .await;
        }
        for number in 0..128 {
            let message = receiver.try_recv().expect("published message was lost");
            assert_eq!(
                message.encode(),
                frame(&["message", "news", &number.to_string()])
            );
        }
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn pubsub_additional_subscriptions_share_the_original_queue() {
        let pubsub = PubSub::new();
        let (_, receiver) = pubsub
            .subscribe(vec![Bytes::from_static(b"news:1")], 1)
            .await;
        let mut receiver = receiver.unwrap();
        let (_, additional) = pubsub
            .psubscribe(
                vec![Bytes::from_static(b"news:*"), Bytes::from_static(b"*")],
                1,
            )
            .await;
        assert!(additional.is_none());
        let (_, additional) = pubsub
            .subscribe(vec![Bytes::from_static(b"news:2")], 1)
            .await;
        assert!(additional.is_none());

        for channel in ["news:1", "news:2"] {
            pubsub
                .publish(channel.into(), Bytes::from_static(b"body"))
                .await;
            let mut actual: Vec<_> = (0..3)
                .map(|_| {
                    receiver
                        .try_recv()
                        .expect("subscription message was lost")
                        .encode()
                })
                .collect();
            let mut expected = vec![
                frame(&["message", channel, "body"]),
                frame(&["pmessage", "news:*", channel, "body"]),
                frame(&["pmessage", "*", channel, "body"]),
            ];
            // Pattern iteration order is unspecified, but all frames must arrive.
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        }
    }

    #[tokio::test]
    async fn pubsub_unsubscribe_and_disconnect_preserve_queued_messages() {
        let pubsub = PubSub::new();
        let (_, receiver) = pubsub.subscribe(vec![Bytes::from_static(b"news")], 1).await;
        let mut receiver = receiver.unwrap();
        assert!(
            pubsub
                .psubscribe(vec![Bytes::from_static(b"*")], 1)
                .await
                .1
                .is_none()
        );
        pubsub
            .publish(Bytes::from_static(b"news"), Bytes::from_static(b"before"))
            .await;
        pubsub.unsubscribe_all_channels(1).await;
        assert!(
            !receiver.is_closed(),
            "pattern subscription must remain active"
        );
        pubsub
            .publish(Bytes::from_static(b"news"), Bytes::from_static(b"after"))
            .await;
        pubsub.punsubscribe_all_patterns(1).await;
        assert!(receiver.is_closed());

        for expected in [
            frame(&["message", "news", "before"]),
            frame(&["pmessage", "*", "news", "before"]),
            frame(&["pmessage", "*", "news", "after"]),
        ] {
            assert_eq!(receiver.try_recv().unwrap().encode(), expected);
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));

        // Reusing the connection after the last unsubscribe creates a new queue.
        let (_, receiver) = pubsub.psubscribe(vec![Bytes::from_static(b"*")], 1).await;
        let mut receiver = receiver.unwrap();
        pubsub
            .publish(Bytes::from_static(b"news"), Bytes::from_static(b"last"))
            .await;
        pubsub.remove_client(1).await;
        assert!(receiver.is_closed());
        assert_eq!(
            receiver.try_recv().unwrap().encode(),
            frame(&["pmessage", "*", "news", "last"])
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        assert_eq!(pubsub.client_subscription_count(1).await, 0);
        assert_eq!(pubsub.client_pattern_count(1).await, 0);
    }
}
