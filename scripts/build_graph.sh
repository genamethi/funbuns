#!/usr/bin/env bash
# Build the compressed WebGraph from parquet blocks.
#
# Usage:
#   scripts/build_graph.sh [--skip-ids] [--skip-forward] [--skip-transpose]
#
# After this completes, run EF index construction:
#   webgraph build ef /media/extssd/research/dioph.pp/data/graph/forward
#   webgraph build ef /media/extssd/research/dioph.pp/data/graph/transpose

set -euo pipefail

BLOCKS_DIR="/media/extssd/research/dioph.pp/data/blocks"
OUT_DIR="/media/extssd/research/dioph.pp/data/graph"
SORT_MEMORY=4294967296  # 4 GiB

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_ROOT/funbuns_graph/target/release/build-graph"

# Build the binary if needed
if [ ! -f "$BINARY" ]; then
    echo "Building build-graph binary..."
    cd "$PROJECT_ROOT/funbuns_graph" && cargo build --release
    cd "$PROJECT_ROOT"
fi

echo "Blocks: $BLOCKS_DIR"
echo "Output: $OUT_DIR"
echo "Sort memory: $SORT_MEMORY bytes"
echo ""

exec "$BINARY" \
    --blocks-dir "$BLOCKS_DIR" \
    --out-dir "$OUT_DIR" \
    --sort-memory "$SORT_MEMORY" \
    "$@"
