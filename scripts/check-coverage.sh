#!/usr/bin/env sh
set -eu

profile="${1:-coverage.out}"
minimum="${COVERAGE_MINIMUM:-70.0}"
total="$(go tool cover -func="$profile" | awk '/^total:/ { gsub(/%/, "", $3); print $3 }')"
if [ -z "$total" ]; then
  echo "coverage total is missing from $profile" >&2
  exit 1
fi
awk -v total="$total" -v minimum="$minimum" 'BEGIN {
  if (total + 0 < minimum + 0) {
    printf "coverage %.1f%% is below required %.1f%%\n", total, minimum > "/dev/stderr"
    exit 1
  }
  printf "coverage %.1f%% meets required %.1f%%\n", total, minimum
}'
