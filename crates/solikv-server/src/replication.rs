use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Master,
    Replica,
}

#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub role: ReplicationRole,
    pub master_addr: Option<(String, u16)>,
    pub master_connection: Option<MasterConnection>,
    pub replica_connections: Arc<RwLock<Vec<ReplicaConnection>>>,
    pub replication_offset: Arc<RwLock<u64>>,
}

#[derive(Debug)]
pub struct MasterConnection {
    pub replica_addr: SocketAddr,
}

#[derive(Debug)]
pub struct ReplicaConnection {
    pub addr: SocketAddr,
    pub sender: mpsc::Sender<ReplicationCommand>,
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub enum ReplicationCommand {
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
    Hset { key: Bytes, field: Bytes, value: Bytes },
    Hdel { key: Bytes, field: Bytes },
    Rpush { key: Bytes, value: Bytes },
    Lpush { key: Bytes, value: Bytes },
    Sadd { key: Bytes, member: Bytes },
    Srem { key: Bytes, member: Bytes },
    Zadd { key: Bytes, score: f64, member: Bytes },
    Zrem { key: Bytes, member: Bytes },
    Publish { channel: Bytes, message: Bytes },
    FullSync,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            role: ReplicationRole::Master,
            master_addr: None,
            master_connection: None,
            replica_connections: Arc::new(RwLock::new(Vec::new())),
            replication_offset: Arc::new(RwLock::new(0)),
        }
    }

    pub fn set_master(&mut self, host: String, port: u16) {
        self.role = ReplicationRole::Replica;
        self.master_addr = Some((host, port));
    }

    pub fn set_master_mode(&mut self) {
        self.role = ReplicationRole::Master;
        self.master_addr = None;
        self.master_connection = None;
    }

    pub fn is_master(&self) -> bool {
        self.role == ReplicationRole::Master
    }

    pub async fn add_replica(&self, addr: SocketAddr, sender: mpsc::Sender<ReplicationCommand>) {
        let conn = ReplicaConnection {
            addr,
            sender,
            offset: 0,
        };
        let mut replicas = self.replica_connections.write().await;
        replicas.push(conn);
    }

    pub async fn remove_replica(&self, addr: SocketAddr) {
        let mut replicas = self.replica_connections.write().await;
        replicas.retain(|r| r.addr != addr);
    }

    pub async fn broadcast_to_replicas(&self, cmd: ReplicationCommand) {
        let replicas = self.replica_connections.read().await;
        for replica in replicas.iter() {
            let _ = replica.sender.send(cmd.clone()).await;
        }
    }

    pub async fn increment_offset(&self) -> u64 {
        let mut offset = self.replica_connections.write().await;
        let current = *offset.read().await;
        *offset.write().await = current + 1;
        current + 1
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn connect_to_master(
    host: &str,
    port: u16,
) -> Result<(TcpStream, mpsc::Receiver<ReplicationCommand>), std::io::Error> {
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    let (tx, rx) = mpsc::channel(1000);

    Ok((stream, rx))
}

pub async fn replicate_command(
    stream: &mut TcpStream,
    cmd: &ReplicationCommand,
) -> std::io::Result<()> {
    let frame = match cmd {
        ReplicationCommand::Set { key, value } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SET")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(value.clone()),
        ]),
        ReplicationCommand::Del { key } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("DEL")),
            RespFrame::BulkString(key.clone()),
        ]),
        ReplicationCommand::Hset { key, field, value } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("HSET")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(field.clone()),
            RespFrame::BulkString(value.clone()),
        ]),
        ReplicationCommand::Hdel { key, field } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("HDEL")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(field.clone()),
        ]),
        ReplicationCommand::Rpush { key, value } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("RPUSH")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(value.clone()),
        ]),
        ReplicationCommand::Lpush { key, value } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("LPUSH")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(value.clone()),
        ]),
        ReplicationCommand::Sadd { key, member } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SADD")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(member.clone()),
        ]),
        ReplicationCommand::Srem { key, member } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SREM")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(member.clone()),
        ]),
        ReplicationCommand::Zadd { key, score, member } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("ZADD")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(Bytes::from(score.to_string())),
            RespFrame::BulkString(member.clone()),
        ]),
        ReplicationCommand::Zrem { key, member } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("ZREM")),
            RespFrame::BulkString(key.clone()),
            RespFrame::BulkString(member.clone()),
        ]),
        ReplicationCommand::Publish { channel, message } => RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("PUBLISH")),
            RespFrame::BulkString(channel.clone()),
            RespFrame::BulkString(message.clone()),
        ]),
        ReplicationCommand::FullSync => {
            RespFrame::Array(vec![RespFrame::BulkString(Bytes::from("FULLSYNC"))])
        }
    };

    let mut buf = BytesMut::new();
    encode_frame(&frame, &mut buf);
    stream.write_all(&buf).await
}

pub fn is_write_command(name: &str) -> bool {
    matches!(
        name,
        "SET"
            | "MSET"
            | "DEL"
            | "H_SET"
            | "HSET"
            | "HSETNX"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "RPUSH"
            | "RPUSHX"
            | "LPUSH"
            | "LPUSHX"
            | "LREM"
            | "LTRIM"
            | "SADD"
            | "SREM"
            | "SMOVE"
            | "SPOP"
            | "ZADD"
            | "ZINCRBY"
            | "ZREM"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYRANK"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "APPEND"
            | "SETRANGE"
            | "EXPIRE"
            | "EXPIREAT"
            | "PEXPIRE"
            | "PEXPIREAT"
            | "PERSIST"
            | "TTL"
            | "PTTL"
            | "RENAME"
            | "RENAMENX"
            | "FLUSHDB"
            | "FLUSHALL"
            | "XADD"
            | "XDEL"
            | "XTRIM"
            | "XACK"
            | "PUBLISH"
            | "PFADD"
            | "PFMERGE"
            | "SETBIT"
            | "BITOP"
            | "GEOADD"
    )
}
