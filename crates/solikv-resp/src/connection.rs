use std::net::SocketAddr;

/// State for a connected Redis client.
#[derive(Debug)]
pub struct ClientConnection {
    pub addr: SocketAddr,
    pub db: usize,
    pub authenticated: bool,
    pub in_transaction: bool,
    pub tx_queue: Vec<Vec<bytes::Bytes>>,
    pub watch_keys: Vec<bytes::Bytes>,
    pub subscriptions: Vec<bytes::Bytes>,
    pub psubscriptions: Vec<String>,
    pub name: Option<String>,
}

impl ClientConnection {
    pub fn new(addr: SocketAddr, requires_auth: bool) -> Self {
        Self {
            addr,
            db: 0,
            authenticated: !requires_auth,
            in_transaction: false,
            tx_queue: Vec::new(),
            watch_keys: Vec::new(),
            subscriptions: Vec::new(),
            psubscriptions: Vec::new(),
            name: None,
        }
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subscriptions.is_empty() || !self.psubscriptions.is_empty()
    }

    pub fn reset_transaction(&mut self) {
        self.in_transaction = false;
        self.tx_queue.clear();
        self.watch_keys.clear();
    }
}
