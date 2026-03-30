#!/usr/bin/env bash
# Profile any funbuns command with py-spy, viztracer, or pyinstrument.
#
# Usage:
#   scripts/profile.sh pyspy   funbuns --ladic --ladic-limit 100000
#   scripts/profile.sh trace   funbuns --remainder --remainder-limit 50000
#   scripts/profile.sh instr   funbuns --spectral
#
# Outputs land in logs/ with timestamps.  py-spy produces a flamegraph SVG,
# viztracer produces an interactive HTML timeline, pyinstrument produces an
# HTML call tree.
#
# Compose with pixi:
#   pixi run -e dev profile pyspy funbuns --ladic
#   pixi run -e dev profile trace funbuns --remainder

set -euo pipefail

MODE="${1:?Usage: profile.sh <pyspy|trace|instr> <command...>}"
shift

if [ $# -eq 0 ]; then
    echo "Error: no command specified"
    echo "Usage: profile.sh <pyspy|trace|instr> <command...>"
    exit 1
fi

STAMP=$(date +%Y%m%d_%H%M%S)
LOGDIR="logs"
mkdir -p "$LOGDIR"

case "$MODE" in
    pyspy)
        OUT="$LOGDIR/pyspy_${STAMP}.svg"
        echo "py-spy -> $OUT"
        echo "Command: $*"
        echo "---"
        py-spy record --subprocesses --native -o "$OUT" -- "$@"
        echo "---"
        echo "Flamegraph: $OUT"
        ;;
    trace)
        OUT="$LOGDIR/trace_${STAMP}.json"
        echo "viztracer -> $OUT"
        echo "Command: $*"
        echo "---"
        # Convert "funbuns" entry point to "python -m funbuns" for viztracer -m
        CMD="$1"; shift
        if [ "$CMD" = "funbuns" ]; then
            viztracer --include_files src/funbuns/ -o "$OUT" -m funbuns -- "$@"
        elif [ "$CMD" = "funbuns-admin" ]; then
            viztracer --include_files src/funbuns/ -o "$OUT" -m funbuns.admin -- "$@"
        else
            viztracer --include_files src/funbuns/ -o "$OUT" -- "$CMD" "$@"
        fi
        echo "---"
        echo "Trace: $OUT"
        echo "View:  viztracer --open $OUT"
        ;;
    instr)
        OUT="$LOGDIR/instr_${STAMP}.html"
        echo "pyinstrument -> $OUT"
        echo "Command: $*"
        echo "---"
        CMD="$1"; shift
        if [ "$CMD" = "funbuns" ]; then
            pyinstrument --renderer html --outfile "$OUT" -m funbuns -- "$@"
        elif [ "$CMD" = "funbuns-admin" ]; then
            pyinstrument --renderer html --outfile "$OUT" -m funbuns.admin -- "$@"
        else
            pyinstrument --outfile "$OUT" "$CMD" "$@"
        fi
        echo "---"
        echo "Report: $OUT"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: profile.sh <pyspy|trace|instr> <command...>"
        exit 1
        ;;
esac
