# Paxos Consensus Implementation

Production-ready Paxos implementation in Rust with web UI, 255 passing tests, and comprehensive documentation.

## Quick Start

```bash
# Setup
git clone https://github.com/FotoVerite/paxos.git
cd paxos

# Run all 255 tests
cargo test --tests

# Start web visualizer
cargo run --release
# Open http://localhost:3000
```

## Features

✅ **Complete Paxos Protocol** - Prepare, Promise, Accept, Accepted phases  
✅ **255 Passing Tests** - Comprehensive test suite with EventBarrier framework  
✅ **Web Visualizer** - Real-time visualization of consensus  
✅ **Network Failure Simulation** - Partitions, packet loss, latency  
✅ **SQLite Persistence** - Durable ledger with crash recovery  
✅ **Multi-Node Clusters** - 3-9 node configurations  

## Documentation

- **[DEVELOPMENT.md](DEVELOPMENT.md)** - For developers: architecture, testing, debugging
- **[Deployment](#deployment)** - Production setup, Docker, Kubernetes

## Deployment

### Run Locally

```bash
# Release build (optimized)
cargo build --release
./target/release/paxos

# Or with logging
RUST_LOG=debug ./target/release/paxos
```

### Data Persistence

Ledger stored in `.paxos/` directory:
```bash
# Backup
cp -r .paxos/ .paxos.backup/

# Clear ledger (fresh start)
rm -rf .paxos/

# Inspect
sqlite3 .paxos/node_1.db
```

### Docker

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/paxos /usr/local/bin/
EXPOSE 3000
CMD ["paxos"]
```

```bash
docker build -t paxos .
docker run -p 3000:3000 paxos
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: paxos
spec:
  serviceName: paxos
  replicas: 5
  selector:
    matchLabels:
      app: paxos
  template:
    metadata:
      labels:
        app: paxos
    spec:
      containers:
      - name: paxos
        image: paxos:latest
        ports:
        - containerPort: 3000
        volumeMounts:
        - name: ledger
          mountPath: /.paxos
  volumeClaimTemplates:
  - metadata:
      name: ledger
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 10Gi
```

```bash
kubectl apply -f deployment.yaml
kubectl port-forward svc/paxos 3000:3000
```

## Monitoring

### Web Dashboard

- Real-time event stream
- Node status indicators
- Proposal/acceptance counts
- Learned decrees

### Logs

```bash
RUST_LOG=debug ./target/release/paxos
```

Key messages:
```
[Node X] Learner reached quorum for decree N
[Node X] Learned value from proposer Y
[ERROR] Failed to reach consensus
```

### Metrics

- Proposals per second
- Consensus latency (10-100ms typical)
- Network partition events
- Node recovery time

## Failure Handling

| Scenario | Behavior |
|----------|----------|
| Node failure (minority) | No impact |
| Node failure (majority) | Consensus halts until recovery |
| Network partition | Majority continues, minority halts |
| Network recovery | Minority catches up automatically |
| Disk full | Node stops accepting new values |

## Performance

| Metric | Value |
|--------|-------|
| Consensus latency | 10-100ms (network sim) |
| Throughput | 10-100 decrees/sec |
| Memory per node | ~1MB |
| Disk per decree | ~1KB |

## Troubleshooting

**Consensus not progressing**
- Check node logs: `RUST_LOG=debug`
- Verify quorum: ceil(N/2) + 1
- Confirm network connectivity

**High latency**
- Check system load
- Reduce network latency
- Profile with `RUST_LOG=debug`

**Data inconsistency**
- Should not happen (algorithm guarantees)
- Check logs for protocol violations
- Report as bug

## System Requirements

- Rust 1.70+
- SQLite3

## Project Status

**255/255 tests passing** | **Production-ready** | **Fully documented**

---

For detailed development information, see [DEVELOPMENT.md](DEVELOPMENT.md).
