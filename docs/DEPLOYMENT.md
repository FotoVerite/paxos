# Deployment & Operations

## Building for Production

```bash
# Release build (optimized)
cargo build --release

# Binary location
./target/release/paxos
```

## Running the Cluster

### Web Visualizer (Recommended)
```bash
./target/release/paxos
# Open http://localhost:3000
```

### Command Line Only
```bash
./target/release/paxos --scenario scenarios/partition_recovery.json
```

### Custom Configuration
Modify `Cluster::new()` in `src/main.rs`:
- Number of nodes
- Failure injection behavior
- Initial scenario

## Data Persistence

Ledger stored in `.paxos/` directory:
```
.paxos/
├── node_1.db (SQLite)
├── node_2.db
└── node_N.db
```

### Backup
```bash
# Backup ledger state
cp -r .paxos/ .paxos.backup/

# Clear ledger (fresh start)
rm -rf .paxos/
```

### Recovery
- Node automatically recovers ledger on startup
- Catches up with cluster's committed values
- No manual intervention needed

## Environment Variables

```bash
# Enable debug logging
RUST_LOG=debug ./target/release/paxos

# Set custom port
export PORT=8000
./target/release/paxos

# Performance tuning
export TOKIO_WORKER_THREADS=4
./target/release/paxos
```

## Monitoring

### Web UI Dashboard
- Real-time event stream
- Node status indicators
- Proposal/acceptance counts
- Learned decrees

### Logs
Look for these key messages:
```
[Node X] Learner reached quorum for decree N
[Node X] Learned value from proposer Y
[ERROR] Failed to reach consensus
```

### Metrics to Track
- Proposals per second
- Consensus latency
- Network partition events
- Node recovery time

## Failure Handling

### Node Failure
- Other nodes detect via timeout
- Cluster continues if quorum available
- Failed node auto-restarts (in simulation)

### Network Partition
- Minority partition: halts
- Majority partition: continues
- On healing: minority catches up

### Disk Full
- Ledger writes will fail
- Node logs error and stops accepting
- Clear space and restart

## High Availability Setup

For production, consider:
1. Run multiple instances
2. Load balance across instances
3. Shared ledger storage (NFS/S3)
4. Monitoring & alerting
5. Graceful shutdown on errors

## Performance Tuning

### Throughput
- Batch proposals if possible
- Increase cluster size for parallelism
- Use pipelining (Phase 2 before Phase 1 complete)

### Latency
- Reduce network latency
- Use local disk for ledger
- Disable debug logging

### Resource Usage
- Monitor memory (usually <100MB per node)
- Disk: ~1KB per decree
- Network: ~100 bytes per message

## Troubleshooting

**Consensus not progressing**:
- Check node logs for errors
- Verify quorum size: ceil(N/2) + 1
- Confirm network connectivity

**High latency**:
- Check network conditions
- Reduce system load
- Profile with `RUST_LOG=debug`

**Data inconsistency**:
- Should not happen (algorithm guarantees)
- If suspected, check logs for protocol violations
- Report as bug

## Docker Deployment

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

Build and run:
```bash
docker build -t paxos .
docker run -p 3000:3000 paxos
```

## Kubernetes Deployment

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

Deploy:
```bash
kubectl apply -f deployment.yaml
kubectl port-forward svc/paxos 3000:3000
```

## Support & Debugging

See `docs/TESTING.md` for test debugging.

For operational issues:
1. Check logs: `RUST_LOG=debug`
2. Inspect ledger: `sqlite3 .paxos/node_1.db`
3. Verify quorum math: ceil(N/2) + 1
4. Review docs/ARCHITECTURE.md
