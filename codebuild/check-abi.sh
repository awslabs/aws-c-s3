#!/usr/bin/env bash
#
# check-abi.sh - ABI dump and compatibility gate for a single C library.
#
# Expects two pre-built install trees (BASE_INSTALL, HEAD_INSTALL) produced
# by the "Build base/head ref" workflow steps. cmake installs only public
# headers to the install tree, so we use those directly for abi-dumper.
#
# Tools required: abi-dumper, abi-compliance-checker, universal-ctags, readelf
#
# Env:
#   LIB           library name (default: basename of repo root)
#   BASE_INSTALL  install prefix for base ref  (required)
#   HEAD_INSTALL  install prefix for head ref  (required)
#   OUT_DIR       report output dir            (default: ./abi-report)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
LIB="${LIB:-$(basename "$REPO_ROOT")}"
BASE_INSTALL="${BASE_INSTALL:?BASE_INSTALL must be set}"
HEAD_INSTALL="${HEAD_INSTALL:?HEAD_INSTALL must be set}"
OUT_DIR="${OUT_DIR:-./abi-report}"

log() { echo "$*" >&2; }

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

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

log "=== ABI check: $LIB ==="
log "base: $base_so"
log "head: $head_so"

log "dumping ABI (base)"
abi-dumper "$base_so" -o "$OUT_DIR/base.dump" -lver base \
  -public-headers "$BASE_INSTALL/include" >/dev/null 2>&1 \
  || { log "ERROR: abi-dumper failed (base)"; exit 1; }

log "dumping ABI (head)"
abi-dumper "$head_so" -o "$OUT_DIR/head.dump" -lver head \
  -public-headers "$HEAD_INSTALL/include" >/dev/null 2>&1 \
  || { log "ERROR: abi-dumper failed (head)"; exit 1; }

log "comparing ABI"
# -binary: we gate on binary compatibility (old apps running against the new
#   .so). Every break we care about - added/removed struct member, changed
#   param/return type, removed symbol - is a binary break, so a single binary
#   check is sufficient; a separate source check would only add recompilation
#   noise that is not the goal.
# -ext: check ALL public data types, even those not referenced by any function
#   in this library's own symbols. Without it, abicc's reachability filter skips
#   public structs reached only through a callback fn-ptr (e.g.
#   aws_s3_meta_request_progress via progress_callback), so adding a member to
#   such a struct is silently reported as compatible. -ext closes that gap.
rc=0
abi-compliance-checker -l "$LIB" \
  -old "$OUT_DIR/base.dump" -new "$OUT_DIR/head.dump" \
  -report-path "$OUT_DIR/compat_report.html" \
  -strict -ext -binary >"$OUT_DIR/acc.log" 2>&1 || rc=$?

pct="$(grep -ioP 'binary compatibility: \K[0-9.]+' "$OUT_DIR/acc.log" 2>/dev/null | head -n1 || echo '?')"
base_sover="$(soversion "$base_so")"
head_sover="$(soversion "$head_so")"

# abi-compliance-checker exit codes (see tool docs):
#   0      compatible, ran clean
#   1      incompatible, ran clean   <- the only code a SOVERSION bump may clear
#   2-11   tool error (bad input, can't compile, empty symbol set, etc.)
# A tool error means we have NO trustworthy verdict, so it must fail loudly and
# can never be masked by a SOVERSION bump.
acc_error_meaning() {
  case "$1" in
    2)  echo "common error (undifferentiated)" ;;
    3)  echo "a system command is not found" ;;
    4)  echo "cannot access input files" ;;
    5)  echo "cannot compile header files" ;;
    6)  echo "headers compiled with minor errors" ;;
    7)  echo "invalid input ABI dump" ;;
    8)  echo "unsupported version of input ABI dump" ;;
    9)  echo "cannot find a module" ;;
    10) echo "empty intersection between headers and shared objects" ;;
    11) echo "empty set of symbols in headers" ;;
    *)  echo "unknown error (code $1)" ;;
  esac
}

bumped=false
[[ -n "$base_sover" && "$base_sover" != "$head_sover" ]] && bumped=true

# summary.md — the brief status table appended first to $GITHUB_STEP_SUMMARY
{
  echo "## Check ABI compliance: \`$LIB\`"
  echo ""
  echo "- Binary compatibility: **${pct}%**"
  echo "- SOVERSION: base \`${base_sover:-none}\` -> head \`${head_sover:-none}\`"
  echo "- abi-compliance-checker exit code: \`$rc\`"
  if [[ "$rc" -ge 2 ]]; then
    echo ""
    echo "**ABI check ERRORED ($(acc_error_meaning "$rc")). No verdict was produced — this is not a pass and cannot be cleared by bumping SOVERSION. See \`acc.log\`.**"
  elif [[ "$rc" -eq 1 && "$bumped" != true ]]; then
    echo ""
    echo "**ABI is incompatible and SOVERSION was not bumped. Either revert the breaking change or bump SOVERSION in CMakeLists.txt.**"
  fi
} > "$OUT_DIR/summary.md"

# Tool error (2-11): fail loudly with the captured log; never treat as a verdict.
if [[ "$rc" -ge 2 ]]; then
  log "ERROR: abi-compliance-checker failed (exit $rc: $(acc_error_meaning "$rc"))"
  log "----- acc.log -----"
  cat "$OUT_DIR/acc.log" >&2 || true
  log "-------------------"
  exit "$rc"
fi

if [[ "$rc" -eq 0 ]]; then
  log "PASS: ABI backward-compatible (${pct}%)"
  exit 0
fi

# rc == 1: genuine incompatibility. A SOVERSION bump is the only valid escape.
if [[ "$bumped" == true ]]; then
  log "PASS: ABI changed (${pct}%) but SOVERSION bumped ($base_sover -> $head_sover)"
  exit 0
fi
log "FAIL: ABI incompatible (${pct}%) and SOVERSION unchanged (${base_sover:-none})"
exit 1
