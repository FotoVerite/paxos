# Paxos Deployment Guide

## Initial Setup (One-time on Linode)

### 1. SSH into your Linode
```bash
ssh deploy@172.104.215.113
```

### 2. Create app directory
```bash
sudo mkdir -p /opt/paxos
sudo chown deploy:deploy /opt/paxos
cd /opt/paxos
```

### 3. Clone the repository
```bash
git clone https://github.com/FotoVerite/paxos.git .
```

### 4. Build the binary (first time)
```bash
cargo build --release
```

### 5. Create systemd service
Create `/etc/systemd/system/paxos.service`:
```bash
sudo tee /etc/systemd/system/paxos.service > /dev/null << 'EOF'
[Unit]
Description=Paxos Web Server
After=network.target

[Service]
Type=simple
User=deploy
WorkingDirectory=/opt/paxos
ExecStart=/opt/paxos/target/release/paxos web
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment="RUST_LOG=info"

[Install]
WantedBy=multi-user.target
EOF
```

### 6. Enable and start service
```bash
sudo systemctl daemon-reload
sudo systemctl enable paxos
sudo systemctl start paxos
sudo systemctl status paxos
```

### 7. Update nginx to port 3001
Edit `/etc/nginx/sites-available/paxos` and change:
```nginx
proxy_pass http://127.0.0.1:3001;
```

Then reload:
```bash
sudo nginx -t
sudo systemctl reload nginx
```

### 8. Verify it's working
```bash
curl -k https://paxos.matthewbergman.com
```

---

## Deploying Updates

From your local machine, run:
```bash
./release.sh
```

This will:
1. Pull the latest code
2. Build the release binary
3. Restart the systemd service
4. Show service status

### View logs
```bash
ssh deploy@172.104.215.113
journalctl -u paxos -f
```

### Manual restart
```bash
ssh deploy@172.104.215.113
sudo systemctl restart paxos
```

### Manual stop
```bash
ssh deploy@172.104.215.113
sudo systemctl stop paxos
```
