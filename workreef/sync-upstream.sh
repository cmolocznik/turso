#!/bin/bash
# Merge upstream Turso into workreef. Fetches upstream, fast-forwards main (a mirror of upstream
# main), merges main into workreef, and checks the merged tree before committing it: it compiles
# with its tests, the core and FTS suites pass, clippy is clean, and ivf_crash's kill rounds pass
# under WAL, MVCC and MVCC with four writers. Only then is the merge committed and pushed. A
# conflict or a failed check aborts the merge, leaving workreef as it was, and alerts on the desktop
# and the agent bus. A checkout that is busy (another branch, uncommitted changes, a merge under
# way, cargo running in it) or a hold file skips the run. workreef/turso-sync.timer runs it four
# times a day; each run writes its log under $LOGS and its verdict to $LOGS/status.
set -u

REPO=${REPO:-$(cd "$(dirname "$0")/.." && pwd)}
LOGS=${LOGS:-/home/me/cognition-data/turso-sync}
CARGO=${CARGO:-/home/me/cognition-data/bin/cog-cargo}
BUS=${BUS:-/home/me/cognition-data/agent-bus/bus}
CORES=${CORES:-6-13}
ROUNDS=${ROUNDS:-30}
export TMPDIR=${TMPDIR:-/home/me/cognition-data/tmp}
export RUST_MIN_STACK=8388608

mkdir -p "$LOGS"
find "$LOGS" -maxdepth 1 -name '20*' -mtime +30 -exec rm -rf {} +
stamp=$(date -u +%Y-%m-%dT%H%MZ)
log="$LOGS/$stamp.log"
exec >>"$log" 2>&1
echo "== $(date -u +%FT%TZ) sync of $REPO"

# verdict STATE MESSAGE: record the run's outcome; a stop that leaves work for a person alerts.
verdict() {
    local line
    line="$(date -u +%FT%TZ) $1 $2"
    echo "$line" > "$LOGS/status"
    echo "$line" >> "$LOGS/history"
    echo "VERDICT $1: $2"
    case "$1" in
        CONFLICT | FAILED | ERROR | STUCK)
            notify-send -u critical "Turso upstream merge: $1" "$2" 2>/dev/null
            "$BUS" send --from turso-sync --to claude "Turso upstream merge $1: $2 (log $log)" >/dev/null 2>&1
            ;;
    esac
}

if [ -e "$LOGS/hold" ]; then
    verdict SKIPPED "hold file $LOGS/hold"
    exit 0
fi
exec 9>"$LOGS/lock"
if ! flock -n 9; then
    verdict SKIPPED "another run holds $LOGS/lock"
    exit 0
fi
cd "$REPO" || { verdict ERROR "no checkout at $REPO"; exit 1; }
for pid in $(pgrep -x cargo); do
    case "$(readlink "/proc/$pid/cwd" 2>/dev/null)" in
        "$REPO"*) verdict SKIPPED "cargo is running in the checkout (pid $pid)"; exit 0 ;;
    esac
done
if [ -e "$(git rev-parse --git-path MERGE_HEAD)" ]; then
    verdict STUCK "a merge is under way in the checkout; finish or abort it"
    exit 1
fi
branch=$(git branch --show-current)
if [ "$branch" != workreef ]; then
    verdict SKIPPED "the checkout is on ${branch:-a detached HEAD}, not workreef"
    exit 0
fi
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    verdict SKIPPED "the checkout has uncommitted changes"
    exit 0
fi

git fetch -q upstream main || { verdict ERROR "fetching upstream failed"; exit 1; }
git fetch -q origin || { verdict ERROR "fetching origin failed"; exit 1; }
if ! git merge-base --is-ancestor main upstream/main; then
    verdict ERROR "main is not an ancestor of upstream/main: upstream rewrote its history"
    exit 1
fi
if ! git merge-base --is-ancestor origin/workreef workreef; then
    verdict ERROR "origin/workreef has commits this checkout lacks"
    exit 1
fi
old=$(git rev-parse --short main)
git branch -f main upstream/main
new=$(git rev-parse --short main)
git push -q origin main || { verdict ERROR "pushing main failed"; exit 1; }
behind=$(git rev-list --count workreef..main)
if [ "$behind" -eq 0 ]; then
    verdict CURRENT "workreef holds upstream $new"
    exit 0
fi
took=$(git rev-list --count --no-merges "$(git merge-base workreef main)..main")

git merge --no-ff --no-commit main >/dev/null 2>&1
conflicts=$(git diff --name-only --diff-filter=U | tr '\n' ' ')
if [ -n "$conflicts" ]; then
    git merge --abort
    verdict CONFLICT "merging upstream $new ($took commits) conflicts in $conflicts"
    exit 1
fi
if [ ! -e "$(git rev-parse --git-path MERGE_HEAD)" ]; then
    verdict ERROR "the merge of upstream $new did not start"
    exit 1
fi
trap 'git merge --abort 2>/dev/null' EXIT

# fail WHY: abort the merge and stop.
fail() {
    git merge --abort
    trap - EXIT
    verdict FAILED "upstream $new ($took commits) merged cleanly but $1"
    exit 1
}

run() { taskset -c "$CORES" "$@"; }

echo "== compile"
run "$CARGO" check -p turso_core --tests --features fts >"$LOGS/$stamp.check.log" 2>&1 ||
    fail "does not compile ($stamp.check.log)"

# suite NAME [ARGS]: the core library's tests; every result line must report no failure, and the
# ivf_rq tests must be among those that ran.
suite() {
    local name=$1 out="$LOGS/$stamp.$1.log" rc passed failed lines ivf
    shift
    echo "== suite $name"
    run "$CARGO" test -p turso_core --lib --no-fail-fast "$@" >"$out" 2>&1
    rc=$?
    read -r passed failed lines < <(grep -E '^test result' "$out" | awk '{p += $4; f += $6} END {print p + 0, f + 0, NR}')
    ivf=$(grep -cE '^test index_method::ivf_rq::.* \.\.\. ok$' "$out")
    if [ "$rc" -ne 0 ] || [ "$failed" -ne 0 ] || [ "$lines" -eq 0 ] || [ "$ivf" -eq 0 ]; then
        fail "the $name suite did not pass: rc $rc, $passed passed, $failed failed, $ivf ivf_rq tests ran ($stamp.$name.log)"
    fi
    eval "${name}_passed=$passed"
}
suite core
suite fts --features fts

echo "== clippy"
run "$CARGO" clippy -p turso_core --all-targets --all-features -- -D warnings >"$LOGS/$stamp.clippy.log" 2>&1 ||
    fail "clippy reports problems ($stamp.clippy.log)"

echo "== ivf_crash"
(cd "$REPO/workreef/repro" && run "$CARGO" build --release --bin ivf_crash) >"$LOGS/$stamp.repro.log" 2>&1 ||
    fail "ivf_crash does not build ($stamp.repro.log)"
seed=$(( $(date +%s) / 3600 ))
# crash NAME MODE THREADS: ivf_crash's kill rounds; a failed run's database is kept beside its log.
crash() {
    local name=$1 mode=$2 threads=$3 out="$LOGS/$stamp.crash-$1.log" dir="$TMPDIR/turso-sync-$1"
    run env THREADS="$threads" "$REPO/workreef/repro/target/release/ivf_crash" run "$dir" "$mode" "$ROUNDS" "$seed" >"$out" 2>&1
    if ! grep -q "^$ROUNDS rounds passed" "$out"; then
        mv "$dir" "$LOGS/$stamp.crash-$name.db"
        fail "ivf_crash failed under $name: $(grep -m1 FAILED "$out" | cut -c1-200) ($stamp.crash-$name.log)"
    fi
    rm -rf "$dir"
}
crash wal wal 1
crash mvcc mvcc 1
crash mvcc-4-writers mvcc 4

git commit -q -F - <<EOF || fail "the merge commit failed"
Merge upstream main $new into workreef

$took upstream commits ($old..$new), merged without conflicts. The merged tree compiled with its
tests; the core suite passed $core_passed tests and the FTS suite $fts_passed; clippy was clean;
ivf_crash passed $ROUNDS kill rounds each under WAL, MVCC and MVCC with four writers (seed $seed).
EOF
trap - EXIT
git push -q origin workreef || { verdict ERROR "pushing workreef failed; the merge is committed locally"; exit 1; }
verdict MERGED "upstream $new ($took commits): core $core_passed, FTS $fts_passed, clippy clean, ivf_crash 3 x $ROUNDS rounds"
