#!/bin/bash
# =============================================================================
# Oracle Cloud VM Setup Script for Airflow
# Run this after SSH-ing into your new VM
# Usage: bash setup_vm.sh <GITHUB_REPO_URL>
# Example: bash setup_vm.sh https://github.com/HaykelSriha/Anime-Recommender.git
# =============================================================================

set -e

REPO_URL="${1:-https://github.com/HaykelSriha/Anime-Recommender.git}"
PROJECT_DIR="$HOME/anime-recommender"

echo "============================================"
echo "  Airflow VM Setup - Oracle Cloud"
echo "============================================"

# Step 1: Update system
echo "[1/5] Updating system packages..."
sudo apt-get update -y
sudo apt-get upgrade -y

# Step 2: Install Docker
echo "[2/5] Installing Docker..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release git

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add current user to docker group (no sudo needed for docker commands)
sudo usermod -aG docker $USER

# Step 3: Clone the repo
echo "[3/5] Cloning repository..."
if [ -d "$PROJECT_DIR" ]; then
    echo "  Project directory exists, pulling latest..."
    cd "$PROJECT_DIR" && git pull
else
    git clone "$REPO_URL" "$PROJECT_DIR"
fi

cd "$PROJECT_DIR"

# Step 4: Create necessary directories
echo "[4/5] Creating directories..."
mkdir -p airflow/logs airflow/dags airflow/plugins data/staging

# Step 5: Start Airflow
echo "[5/5] Starting Airflow..."
echo ""
echo "  Run these commands (you need to log out and back in first for docker group):"
echo ""
echo "    exit"
echo "    ssh <your-vm>"
echo "    cd ~/anime-recommender"
echo "    docker compose up airflow-init"
echo "    docker compose up -d"
echo ""
echo "  Then open http://<your-vm-public-ip>:8080"
echo "  Login: admin / admin"
echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
