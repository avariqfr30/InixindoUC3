#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /path/to/ssh_key.pem"
  exit 1
fi

KEY_PATH="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_HOST="${REMOTE_HOST:-ubuntu@18.136.190.197}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/ai-adoption/payment-app}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-/opt/ai-adoption/payment-app}"
REMOTE_VENV_DIR="${REMOTE_VENV_DIR:-/opt/ai-adoption/payment-app/.venv}"
SERVICE_NAME="${SERVICE_NAME:-payment-app}"
PUBLIC_URL="${PUBLIC_URL:-https://payment.inworx.id}"
LOCAL_HEALTH_URL="${LOCAL_HEALTH_URL:-http://127.0.0.1:8001/health}"
SSH_OPTS=(-i "$KEY_PATH" -o StrictHostKeyChecking=accept-new)

ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" "mkdir -p '$REMOTE_APP_DIR' '$REMOTE_BASE_DIR'"

rsync -avz --delete \
  --exclude '.DS_Store' \
  --exclude '.git/' \
  --exclude '.env' \
  --exclude '.env.example' \
  --exclude '.venv/' \
  --exclude '.pytest_cache/' \
  --exclude '__pycache__/' \
  --exclude 'tests/' \
  --exclude 'test/' \
  --exclude '*_test.py' \
  --exclude 'test_*.py' \
  --exclude '*.example.*' \
  --exclude 'deployment/*.example.json' \
  --exclude 'data/db.csv' \
  --exclude 'data/*.db' \
  --exclude 'data/*.db-shm' \
  --exclude 'data/*.db-wal' \
  --exclude 'data/generated_reports/' \
  --exclude '.osint_cache/' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$ROOT_DIR/" \
  "$REMOTE_HOST:$REMOTE_APP_DIR/"

ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" "
  set -e
  python3 -m venv '$REMOTE_VENV_DIR'
  source '$REMOTE_VENV_DIR/bin/activate'
  cd '$REMOTE_APP_DIR'
  rm -f data/db.csv deployment/*.example.json
  rm -rf tests test data/generated_reports
  find . -name '.DS_Store' -type f -delete
  find . -name '__pycache__' -type d -prune -exec rm -rf {} +
  python3 -m compileall . >/tmp/${SERVICE_NAME}_compile.log 2>&1 || { cat /tmp/${SERVICE_NAME}_compile.log; exit 1; }
  pip install -r requirements.txt >/tmp/${SERVICE_NAME}_pip.log 2>&1 || { cat /tmp/${SERVICE_NAME}_pip.log; exit 1; }
  command -v hunspell >/dev/null || { echo 'Missing prerequisite: hunspell'; exit 1; }
  test -r /usr/share/hunspell/id_ID.dic || { echo 'Missing prerequisite: Indonesian Hunspell dictionary'; exit 1; }
  sudo systemctl restart '$SERVICE_NAME'
  sudo systemctl status '$SERVICE_NAME' --no-pager -l | sed -n '1,20p'
  for _ in \$(seq 1 60); do
    if curl -fsS '$LOCAL_HEALTH_URL' >/tmp/${SERVICE_NAME}_health.json 2>/dev/null; then
      cat /tmp/${SERVICE_NAME}_health.json
      echo
      break
    fi
    sleep 1
  done
  test -f /tmp/${SERVICE_NAME}_health.json
"

curl -fsSI "$PUBLIC_URL" | sed -n '1,12p'

echo
echo "Deployment complete for $PUBLIC_URL"
