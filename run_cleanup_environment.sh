#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
PROJECT_DIR="/mnt/c/Projects/Bachelorarbeit"
export ANSIBLE_CONFIG="${PROJECT_DIR}/ansible.cfg"
PLAYBOOK_ENV="${PROJECT_DIR}/plays/hetzer_environment.yml"
export SERVER_STATE="absent"

# Colors for logging
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')] INFO:${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] SUCCESS:${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $1"
}

# Trap errors
trap 'error "An error occurred. Exiting..."; exit 1' ERR

log "Starting cleanup script..."
log "Destroying Hetzner Environment..."
ansible-playbook -v "${PLAYBOOK_ENV}"
success "Environment destruction completed."