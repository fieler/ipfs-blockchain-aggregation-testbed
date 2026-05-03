#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e
set -o pipefail

# Configuration
PROJECT_DIR="/mnt/c/Projects/Bachelorarbeit"
export ANSIBLE_CONFIG="${PROJECT_DIR}/ansible.cfg"
PLAYBOOK_ENV="${PROJECT_DIR}/plays/hetzer_environment.yml"
PLAYBOOK_CONFIG_BASIC="${PROJECT_DIR}/plays/basic_config.yml"
PLAYBOOK_CONFIG_NODE="${PROJECT_DIR}/plays/node_setup.yml"
PLAYBOOK_SW="${PROJECT_DIR}/plays/middleware_setup.yml"
WAIT_TIME=180
export SERVER_STATE="present"

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

log "Starting deployment script..."

log "Step 1: Deploying Hetzner Environment..."
OUTPUT_FILE=$(mktemp)
ansible-playbook -v "${PLAYBOOK_ENV}" 2>&1 | tee "$OUTPUT_FILE"

if grep -qE "changed=[1-9][0-9]*" "$OUTPUT_FILE"; then
    success "Environment deployment completed with changes."
    log "Step 2: Waiting ${WAIT_TIME} seconds for servers to initialize..."
    for (( i=WAIT_TIME; i>0; i-- )); do
        printf "\rWaiting... %3d seconds remaining" "$i"
        sleep 1
    done
    echo "" # Newline
else
    success "Environment deployment completed. No changes made."
    log "Step 2: Skipping wait time as no changes were detected."
fi
rm -f "$OUTPUT_FILE"

log "Step 3: Running Basic Configuration..."
ansible-playbook "${PLAYBOOK_CONFIG_BASIC}"

log "Step 4: Running Node Setup..."
ansible-playbook "${PLAYBOOK_CONFIG_NODE}"

log "Step 4: Running Software Setup..."
ansible-playbook "${PLAYBOOK_SW}"

success "All deployment tasks finished successfully!"