use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 1024;

/// Published message.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel: Bytes,
    pub message: Bytes,
}

/// Central pub/sub broker using broadcast channels per topic.
#[derive(Debug, Clone)]
pub struct PubSubBroker {
    channels: Arc<DashMap<Bytes, broadcast::Sender<PubSubMessage>>>,
}

impl PubSubBroker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Subscribe to a channel. Returns a receiver.
    pub fn subscribe(&self, channel: Bytes) -> broadcast::Receiver<PubSubMessage> {
        let entry = self.channels.entry(channel.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });
        entry.subscribe()
    }

    /// Publish a message to a channel. Returns number of receivers.
    pub fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        if let Some(tx) = self.channels.get(&channel) {
            tx.send(PubSubMessage {
                channel,
                message,
            })
            .unwrap_or(0)
        } else {
            0
        }
    }

    /// Get the number of subscribers for a channel.
    pub fn subscriber_count(&self, channel: &Bytes) -> usize {
        self.channels
            .get(channel)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }

    /// Get all active channel names.
    pub fn channels(&self) -> Vec<Bytes> {
        self.channels.iter().map(|r| r.key().clone()).collect()
    }

    /// Get number of active channels with pattern filter.
    pub fn numsub(&self) -> Vec<(Bytes, usize)> {
        self.channels
            .iter()
            .map(|r| (r.key().clone(), r.value().receiver_count()))
            .collect()
    }

    /// Remove channels with no subscribers (cleanup).
    pub fn cleanup(&self) {
        let empty: Vec<Bytes> = self
            .channels
            .iter()
            .filter(|r| r.value().receiver_count() == 0)
            .map(|r| r.key().clone())
            .collect();
        for ch in empty {
            self.channels.remove(&ch);
        }
    }
}

impl Default for PubSubBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pubsub_basic() {
        let broker = PubSubBroker::new();
        let mut rx = broker.subscribe(Bytes::from("ch1"));
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 1);

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, Bytes::from("ch1"));
        assert_eq!(msg.message, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_pubsub_no_subscribers() {
        let broker = PubSubBroker::new();
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_pubsub_multiple_subscribers() {
        let broker = PubSubBroker::new();
        let mut rx1 = broker.subscribe(Bytes::from("ch1"));
        let mut rx2 = broker.subscribe(Bytes::from("ch1"));
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 2);

        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg1.message, Bytes::from("hello"));
        assert_eq!(msg2.message, Bytes::from("hello"));
    }

    #[test]
    fn test_channels_list() {
        let broker = PubSubBroker::new();
        let _rx1 = broker.subscribe(Bytes::from("ch1"));
        let _rx2 = broker.subscribe(Bytes::from("ch2"));
        let channels = broker.channels();
        assert_eq!(channels.len(), 2);
    }
}
