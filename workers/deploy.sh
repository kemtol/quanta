#!/usr/bin/env bash
# Deploy semua worker Cloudflare (paralel) dengan log + progress bar
# Support:
#   - wrangler.toml
#   - wrangler.json
#   - wrangler.jsonc
# CLI:
#   ./deploy.sh                  -> deploy semua worker
#   ./deploy.sh --worker NAME    -> cuma deploy worker dengan basename folder NAME
#   ./deploy.sh --worker a --worker b -> deploy beberapa worker

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT_DIR/.deploy_logs"

mkdir -p "$LOG_DIR"

# ==========================
# PARSE ARGUMENT
# ==========================
TARGET_WORKERS=()

usage() {
  cat <<EOF
Usage:
  $(basename "$0")                # deploy semua worker
  $(basename "$0") --worker NAME  # deploy hanya worker tertentu (basename folder)
  $(basename "$0") --worker a --worker b  # deploy beberapa worker

Contoh:
  ./deploy.sh
  ./deploy.sh --worker asset-router
  ./deploy.sh --worker livetrade-taping --worker livetrade-state-engine
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker|-w)
      shift
      if [[ $# -eq 0 ]]; then
        echo "❌ --worker butuh nama worker."
        usage
        exit 1
      fi
      TARGET_WORKERS+=("$1")
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "❌ Argumen tidak dikenal: $1"
      usage
      exit 1
      ;;
  esac
  shift
done

echo "Root      : $ROOT_DIR"
echo "Log dir   : $LOG_DIR"
echo "Wrangler  : $(command -v wrangler || echo 'TIDAK ditemukan di PATH')"
echo

# ==========================
# CARI CONFIG WRANGLER
# ==========================

# Format: "<dir>|<config_file>"
mapfile -t CONFIG_FILES < <(find "$ROOT_DIR" \
  -maxdepth 2 \
  -type f \
  \( -name "wrangler.toml" -o -name "wrangler.json" -o -name "wrangler.jsonc" \) \
  -printf '%h|%f\n' \
)

if [ "${#CONFIG_FILES[@]}" -eq 0 ]; then
  echo "❌ Tidak ada file wrangler (toml/json/jsonc) yang ditemukan."
  exit 1
fi

# Kalau user pakai --worker, filter list sesuai nama folder
if [ "${#TARGET_WORKERS[@]}" -gt 0 ]; then
  FILTERED=()
  for entry in "${CONFIG_FILES[@]}"; do
    dir="${entry%%|*}"
    base="$(basename "$dir")"
    for target in "${TARGET_WORKERS[@]}"; do
      if [[ "$base" == "$target" ]]; then
        FILTERED+=("$entry")
        break
      fi
    done
  done

  if [ "${#FILTERED[@]}" -eq 0 ]; then
    echo "❌ Tidak ada worker yang cocok dengan filter:"
    printf '   - %s\n' "${TARGET_WORKERS[@]}"
    exit 1
  fi

  CONFIG_FILES=("${FILTERED[@]}")
fi

echo "📦 Ditemukan ${#CONFIG_FILES[@]} worker untuk di-deploy:"
for entry in "${CONFIG_FILES[@]}"; do
  dir="${entry%%|*}"
  cfg="${entry##*|}"
  echo "  - $(realpath --relative-to="$ROOT_DIR" "$dir")  (config: $cfg)"
done
echo

# ==========================
# MULAI DEPLOY PARALEL
# ==========================

PIDS=()
NAMES=()

for entry in "${CONFIG_FILES[@]}"; do
  dir="${entry%%|*}"
  cfg="${entry##*|}"
  name="$(basename "$dir")"
  log_file="$LOG_DIR/${name}.log"

  : > "$log_file"  # bersihkan log lama

  (
    cd "$dir"
    echo "=== Deploy $name ===" >> "$log_file"
    echo "Dir   : $dir" >> "$log_file"
    echo "Config: $cfg" >> "$log_file"
    echo "Time  : $(date '+%Y-%m-%d %H:%M:%S')" >> "$log_file"
    echo "---------------------------" >> "$log_file"

    # Tentukan perintah wrangler berdasarkan jenis config
    if [[ "$cfg" == "wrangler.toml" ]]; then
      CMD=(wrangler deploy)
    else
      # wrangler.json / wrangler.jsonc -> pakai -c
      CMD=(wrangler deploy -c "$cfg")
    fi

    if "${CMD[@]}" >> "$log_file" 2>&1; then
      echo "" >> "$log_file"
      echo "✔ SUCCESS: $name" >> "$log_file"
      exit 0
    else
      echo "" >> "$log_file"
      echo "✖ FAILED: $name" >> "$log_file"
      exit 1
    fi
  ) &

  pid=$!
  PIDS+=("$pid")
  NAMES+=("$name")
  echo "▶ Start deploy: $name (pid=$pid), config: $cfg, log: $(basename "$log_file")"
done

echo
echo "⏳ Menunggu semua deploy selesai..."
echo

total=${#PIDS[@]}
completed=0
success=0
failed=0
bar_width=40

print_progress() {
  local done=$1
  local total=$2
  local ok=$3
  local fail=$4

  local percent=$(( done * 100 / total ))
  local filled=$(( done * bar_width / total ))
  local empty=$(( bar_width - filled ))

  local bar_done
  bar_done=$(printf '%*s' "$filled" '' | tr ' ' '#')
  local bar_empty
  bar_empty=$(printf '%*s' "$empty" '' | tr ' ' '-')

  printf "\rProgress: [%s%s] %3d%% (%d/%d)  OK:%d  FAIL:%d" \
    "$bar_done" "$bar_empty" "$percent" "$done" "$total" "$ok" "$fail"
}

for i in "${!PIDS[@]}"; do
  pid=${PIDS[$i]}
  name=${NAMES[$i]}

  if wait "$pid"; then
    ((success++))
  else
    ((failed++))
  fi

  ((completed++))
  print_progress "$completed" "$total" "$success" "$failed"
done

echo
echo

echo "============================="
echo "✅ Deploy selesai."
echo "Total worker : $total"
echo "Sukses       : $success"
echo "Gagal        : $failed"
echo "Log detail   : $LOG_DIR"
echo "============================="
echo

if (( failed > 0 )); then
  echo "❗ Worker yang GAGAL:"
  for i in "${!PIDS[@]}"; do
    name=${NAMES[$i]}
    log_file="$LOG_DIR/${name}.log"

    if grep -q "✖ FAILED" "$log_file"; then
      echo "  - $name  (log: $(basename "$log_file"))"
    fi
  done
  echo
  echo "Cek log di folder: $LOG_DIR"
fi
