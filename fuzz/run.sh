#!/bin/sh
# Runs fuzz targets one after another: `fuzz/run.sh [seconds [target...]]`,
# 30 seconds each and every target unless told otherwise. A target starts
# from the seeds kept in git and from whatever earlier runs left in
# fuzz/corpus/, and adds what it finds there. Stops at the first target that
# finds a failing input and leaves that input under fuzz/artifacts/.
set -eu
cd "$(dirname "$0")"
seconds=${1:-30}
[ $# -gt 0 ] && shift
[ $# -gt 0 ] || set -- $(cargo +nightly fuzz list)
for target in "$@"; do
    mkdir -p "corpus/$target"
    echo "== $target, $seconds s"
    # A build directory named in the environment is used as it is.
    cargo +nightly fuzz run ${CARGO_TARGET_DIR:+--target-dir "$CARGO_TARGET_DIR"} \
        "$target" "corpus/$target" "seeds/$target" -- \
        -max_total_time="$seconds" -timeout=10 -verbosity=0 -print_final_stats=1
done
