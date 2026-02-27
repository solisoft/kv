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
    /// If this message was delivered via a pattern subscription, the pattern that matched.
    pub pattern: Option<String>,
}

/// Central pub/sub broker using broadcast channels per topic.
#[derive(Debug, Clone)]
pub struct PubSubBroker {
    channels: Arc<DashMap<Bytes, broadcast::Sender<PubSubMessage>>>,
    patterns: Arc<DashMap<String, broadcast::Sender<PubSubMessage>>>,
}

impl PubSubBroker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            patterns: Arc::new(DashMap::new()),
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

    /// Subscribe to a pattern. Returns a receiver.
    pub fn psubscribe(&self, pattern: String) -> broadcast::Receiver<PubSubMessage> {
        let entry = self.patterns.entry(pattern.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });
        entry.subscribe()
    }

    /// Publish a message to a channel. Returns number of receivers that received.
    pub fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        let mut count = 0;

        // Exact channel match
        if let Some(tx) = self.channels.get(&channel) {
            count += tx
                .send(PubSubMessage {
                    channel: channel.clone(),
                    message: message.clone(),
                    pattern: None,
                })
                .unwrap_or(0);
        }

        // Pattern match fan-out
        let channel_str = std::str::from_utf8(&channel).unwrap_or("");
        for entry in self.patterns.iter() {
            let pat = entry.key();
            if glob_match::glob_match(pat, channel_str) {
                count += entry
                    .value()
                    .send(PubSubMessage {
                        channel: channel.clone(),
                        message: message.clone(),
                        pattern: Some(pat.clone()),
                    })
                    .unwrap_or(0);
            }
        }

        count
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

    /// Get pattern subscription counts.
    pub fn pattern_numsub(&self) -> Vec<(String, usize)> {
        self.patterns
            .iter()
            .map(|r| (r.key().clone(), r.value().receiver_count()))
            .collect()
    }

    /// Get total number of active pattern subscriptions.
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Remove channels and patterns with no subscribers (cleanup).
    pub fn cleanup(&self) {
        let empty_channels: Vec<Bytes> = self
            .channels
            .iter()
            .filter(|r| r.value().receiver_count() == 0)
            .map(|r| r.key().clone())
            .collect();
        for ch in empty_channels {
            self.channels.remove(&ch);
        }

        let empty_patterns: Vec<String> = self
            .patterns
            .iter()
            .filter(|r| r.value().receiver_count() == 0)
            .map(|r| r.key().clone())
            .collect();
        for pat in empty_patterns {
            self.patterns.remove(&pat);
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
        assert!(msg.pattern.is_none());
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

    #[tokio::test]
    async fn test_psubscribe_basic() {
        let broker = PubSubBroker::new();
        let mut rx = broker.psubscribe("ch*".to_string());
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 1);

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, Bytes::from("ch1"));
        assert_eq!(msg.message, Bytes::from("hello"));
        assert_eq!(msg.pattern, Some("ch*".to_string()));
    }

    #[tokio::test]
    async fn test_psubscribe_no_match() {
        let broker = PubSubBroker::new();
        let _rx = broker.psubscribe("foo*".to_string());
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_psubscribe_and_subscribe_both_receive() {
        let broker = PubSubBroker::new();
        let mut rx_exact = broker.subscribe(Bytes::from("ch1"));
        let mut rx_pattern = broker.psubscribe("ch*".to_string());
        let count = broker.publish(Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 2);

        let msg1 = rx_exact.recv().await.unwrap();
        assert!(msg1.pattern.is_none());

        let msg2 = rx_pattern.recv().await.unwrap();
        assert_eq!(msg2.pattern, Some("ch*".to_string()));
    }

    #[tokio::test]
    async fn test_psubscribe_keyevent_pattern() {
        let broker = PubSubBroker::new();
        let mut rx = broker.psubscribe("__keyevent@0__:*".to_string());
        let count = broker.publish(Bytes::from("__keyevent@0__:set"), Bytes::from("mykey"));
        assert_eq!(count, 1);

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, Bytes::from("__keyevent@0__:set"));
        assert_eq!(msg.message, Bytes::from("mykey"));
    }

    #[test]
    fn test_pattern_count() {
        let broker = PubSubBroker::new();
        let _rx1 = broker.psubscribe("foo*".to_string());
        let _rx2 = broker.psubscribe("bar*".to_string());
        assert_eq!(broker.pattern_count(), 2);
    }

    #[test]
    fn test_cleanup_removes_patterns() {
        let broker = PubSubBroker::new();
        {
            let _rx = broker.psubscribe("foo*".to_string());
        }
        // rx dropped, cleanup should remove
        broker.cleanup();
        assert_eq!(broker.pattern_count(), 0);
    }
}
