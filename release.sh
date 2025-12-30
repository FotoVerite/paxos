#!/bin/bash
set -e

SERVER_IP="172.104.215.113"
DEPLOY_USER="deploy"
APP_DIR="/var/www/paxos"

echo "🚀 Deploying Paxos to $SERVER_IP..."

ssh -A ${DEPLOY_USER}@${SERVER_IP} << 'DEPLOY_SCRIPT'
  set -e
  echo "📦 Pulling latest code..."
  cd /var/www/paxos
  git pull origin master
  
  echo "🔨 Building release binary..."
  cargo build --release 2>&1
  
  echo "🔄 Restarting service..."
  sudo systemctl restart paxos
  
  echo "✓ Waiting for service to start..."
  sleep 2
  
  echo "📋 Service status:"
  sudo systemctl status paxos
DEPLOY_SCRIPT

echo "✅ Deployment complete!"
echo "🌐 Access at: https://paxos.matthewbergman.com"
