#!/usr/bin/env bash
set -euo pipefail

json=false
if [[ "${1:-}" == "--json" ]]; then
  json=true
  shift
fi
if (($# > 0)); then
  printf 'Usage: %s [--json]\n' "${0##*/}" >&2
  exit 1
fi

# Each non-empty output row describes one under-replicated partition.
output=$(kafka-topics --describe --under-replicated-partitions)
count=$(printf '%s\n' "$output" | awk 'NF { n++ } END { print n+0 }')

if [[ "$json" == true ]]; then
  printf '{"under_replicated_partitions":%s}\n' "$count"
else
  printf 'Under-replicated partitions: %s\n' "$count"
fi

if ((count > 0)); then
  exit 2
fi
exit 0