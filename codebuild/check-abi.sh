#!/usr/bin/env bash
#
# check-abi.sh - ABI dump and compatibility gate for a single C library.
#
# Expects two pre-built install trees (BASE_INSTALL, HEAD_INSTALL) produced
# by the "Build base/head ref" workflow steps. Dumps the public ABI
# (excluding private/ headers) and fails if the library is binary-incompatible
# without a SOVERSION bump.
#
# Tools required: abi-dumper, abi-compliance-checker, universal-ctags, readelf
#
# Env:
#   LIB           library name (default: basename of repo root)
#   BASE_INSTALL  install prefix for base ref  (required)
#   BASE_SRC      source checkout for base ref (required, for headers)
#   HEAD_INSTALL  install prefix for head ref  (required)
#   HEAD_SRC      source checkout for head ref (required, for headers)
#   OUT_DIR       report output dir            (default: ./abi-report)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
LIB="${LIB:-$(basename "$REPO_ROOT")}"
BASE_INSTALL="${BASE_INSTALL:?BASE_INSTALL must be set}"
BASE_SRC="${BASE_SRC:?BASE_SRC must be set}"
HEAD_INSTALL="${HEAD_INSTALL:?HEAD_INSTALL must be set}"
HEAD_SRC="${HEAD_SRC:?HEAD_SRC must be set}"
OUT_DIR="${OUT_DIR:-./abi-report}"

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

log() { echo "$*" >&2; }

for tool in abi-dumper abi-compliance-checker readelf; do
  command -v "$tool" >/dev/null || { log "ERROR: $tool not on PATH"; exit 1; }
done

find_so() {
  find "$1" -maxdepth 5 -name "lib${LIB}.so" 2>/dev/null | head -n1 || true
}

soversion() {
  readelf -d "$1" 2>/dev/null | grep -oP 'SONAME.*\.so\.\K[^\]]+' | head -n1 || true
}

base_so="$(find_so "$BASE_INSTALL")"
head_so="$(find_so "$HEAD_INSTALL")"

[[ -n "$base_so" ]] || { log "ERROR: lib${LIB}.so not found under $BASE_INSTALL"; exit 1; }
[[ -n "$head_so" ]] || { log "ERROR: lib${LIB}.so not found under $HEAD_INSTALL"; exit 1; }

# Public headers only - exclude private/ dirs
find "$BASE_SRC/include" -name "*.h" ! -path "*/private/*" > "$OUT_DIR/base-headers.txt" 2>/dev/null || true
find "$HEAD_SRC/include" -name "*.h" ! -path "*/private/*" > "$OUT_DIR/head-headers.txt" 2>/dev/null || true

log "=== ABI check: $LIB ==="
log "base: $base_so"
log "head: $head_so"

log "dumping ABI (base)"
abi-dumper "$base_so" -o "$OUT_DIR/base.dump" -lver base \
  -public-headers "$OUT_DIR/base-headers.txt" >/dev/null 2>&1 \
  || { log "ERROR: abi-dumper failed (base)"; exit 1; }

log "dumping ABI (head)"
abi-dumper "$head_so" -o "$OUT_DIR/head.dump" -lver head \
  -public-headers "$OUT_DIR/head-headers.txt" >/dev/null 2>&1 \
  || { log "ERROR: abi-dumper failed (head)"; exit 1; }

log "comparing ABI"
rc=0
abi-compliance-checker -l "$LIB" \
  -old "$OUT_DIR/base.dump" -new "$OUT_DIR/head.dump" \
  -report-path "$OUT_DIR/compat_report.html" \
  -binary >"$OUT_DIR/acc.log" 2>&1 || rc=$?

pct="$(grep -ioP 'binary compatibility: \K[0-9.]+' "$OUT_DIR/acc.log" 2>/dev/null | head -n1 || echo '?')"
base_sover="$(soversion "$base_so")"
head_sover="$(soversion "$head_so")"

{
  echo "## Check ABI compliance: \`$LIB\`"
  echo ""
  echo "- Binary compatibility: **${pct}%**"
  echo "- SOVERSION: base \`${base_sover:-none}\` ->head \`${head_sover:-none}\`"
  echo ""
  if [[ "$rc" -eq 0 ]]; then
    echo "ABI is backward-compatible."
  elif [[ -n "$base_sover" && "$base_sover" != "$head_sover" ]]; then
    echo "ABI changed, but SOVERSION was bumped (\`$base_sover\` ->\`$head_sover\`)."
  else
    echo "**ABI is incompatible and SOVERSION was not bumped.**"
    echo ""
    echo "Either revert the breaking change or bump SOVERSION in CMakeLists.txt."
    echo ""
    echo '```'
    grep -v '^Report:' "$OUT_DIR/acc.log" 2>/dev/null | tail -20
    echo '```'
  fi
} > "$OUT_DIR/summary.md"

if [[ "$rc" -eq 0 ]]; then
  log "PASS: ABI backward-compatible (${pct}%)"
  exit 0
fi
if [[ -n "$base_sover" && "$base_sover" != "$head_sover" ]]; then
  log "PASS: ABI changed (${pct}%) but SOVERSION bumped ($base_sover -> $head_sover)"
  exit 0
fi
log "FAIL: ABI incompatible (${pct}%) and SOVERSION unchanged (${base_sover:-none})"
exit 1
