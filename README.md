# SoliKV

A high-performance in-memory key-value database written in Rust, compatible with the Redis protocol.

## Features

- **Redis RESP Protocol Compatible** - Drop-in replacement for Redis clients
- **REST API** - HTTP-based access on port 5020
- **Sharding** - Horizontal scaling across multiple shards
- **Persistence** - RDB snapshots and AOF (Append-Only File) with configurable fsync policies
- **Redis RDB Import** - Import existing Redis `dump.rdb` files for seamless migration
- **Pub/Sub** - Built-in publish/subscribe messaging
- **Data Types** - Strings, Lists, Sets, Sorted Sets, Hashes with TTL support

## Getting Started

### Build

```bash
cargo build --release
```

### Run

```bash
cargo run --release -- \
  --port 6379 \
  --rest-port 5020 \
  --shards 4 \
  --dir ./data
```

### Docker

```bash
docker run -p 6379:6379 -p 5020:5020 -v ./data:/data solikv:latest
```

## Usage

### Redis CLI

```bash
redis-cli -p 6379
SET mykey "Hello"
GET mykey
```

### REST API

```bash
curl -X GET http://localhost:5020/key/mykey
curl -X POST -H "Content-Type: application/json" -d '{"value": "Hello"}' http://localhost:5020/key/mykey
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 6379 | Redis RESP protocol port |
| `--rest-port` | 5020 | REST API port |
| `--shards` | 0 | Number of shards (0 = auto-detect CPU cores) |
| `--bind` | 0.0.0.0 | Bind address |
| `--log-level` | info | Log level |
| `--dir` | data | Data directory for persistence |
| `--dbfilename` | dump | RDB snapshot filename |
| `--appendonly` | true | Enable AOF persistence |
| `--appendfsync` | everysec | AOF fsync policy: always, everysec, no |
| `--import-redis-rdb` | - | Path to a Redis `dump.rdb` file to import at startup |

## Migrating from Redis

SoliKV can import standard Redis RDB snapshot files. Generate a dump from your Redis instance and load it at startup:

```bash
# On your Redis server
redis-cli BGSAVE
cp /var/lib/redis/dump.rdb /tmp/dump.rdb

# Import into SoliKV
./solikv --import-redis-rdb /tmp/dump.rdb --port 6379
```

Supported Redis data types: String, List, Set, Sorted Set, Hash — including all compact encodings (ziplist, listpack, intset, quicklist, LZF compression). Already-expired keys are automatically filtered out. Multi-database RDB files are flattened into a single namespace.

Unsupported types (Streams, Modules) are skipped with a warning.

## Architecture

SoliKV is built as a Rust workspace with several crates:

- **solikv-core** - Core data types and operations
- **solikv-engine** - Command execution engine with sharding
- **solikv-server** - RESP and REST server implementations
- **solikv-persist** - RDB and AOF persistence
- **solikv-pubsub** - Publish/subscribe broker
- **solikv-resp** - Redis protocol parser
- **solikv-cluster** - Clustering support
- **solikv-replication** - Master/replica replication

## Performance

SoliKV is designed for high performance with:
- Lock-free sharding with crossbeam-skiplist
- MiMalloc memory allocator
- Async I/O with Tokio
- Configurable fsync policies for durability vs performance tradeoffs

## License

MIT License - see [LICENSE](LICENSE) file.
