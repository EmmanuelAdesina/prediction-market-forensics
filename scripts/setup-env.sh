#!/bin/sh
# tokor-engine environment setup (Linux/WSL).
# Source on /mnt/c (persistent, Windows-visible). Build artifacts on ext4 (fast).
# Usage: . ./scripts/setup-env.sh

CARGO_ROOT="${CARGO_ROOT:-$HOME/.cargo}"
if [ -d "$CARGO_ROOT/bin" ]; then
    PATH="$CARGO_ROOT/bin:$PATH"
fi

# Keep target artifacts off the Windows drive.
DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$DATA_DIR/tokor-engine-target}"
mkdir -p "$CARGO_TARGET_DIR"

export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
