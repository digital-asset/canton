#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Compares how fast two commits of the engine replay the same corpora: ours (HEAD) and theirs. Each
# side is built in its own git worktree, kept between runs, and runs its own CorpusBenchmark on each
# corpus in turn. The runs alternate between the sides, and AbReport compares them round by round.
# Run it inside the canton nix-shell (for sbt, java, dpm and jq).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
SNAPSHOT=community/daml-lf/snapshot

print_help() {
  echo "Usage: $(basename "$0") [--theirs REF] [--corpus DIR...] [--work-dir DIR] [--rounds N] [--jmh-args ARGS] [--margin PCT] [--build-only] [-h]"
  echo "  --theirs REF      commit to compare with (default: where HEAD split off origin/main)"
  echo "  --corpus DIR...   corpora to replay, one after the other in each run (default: canton-tests, recorded and cached)"
  echo "  --work-dir DIR    where the results and the report go (default: \$WORK_DIR, else a temporary directory)"
  echo "  --rounds N        rounds of one run per side (default: 5)"
  echo "  --jmh-args ARGS   JMH options for each run (default: \"-f 1 -wi 5 -i 5\")"
  echo "  --margin PCT      list results whose whole 95% CI lies within +-PCT% as ignored (default: 0, none)"
  echo "  --build-only      build both sides and stop, without recording or running anything"
  echo "Environment: AB_CACHE, where the worktrees and recorded corpora are kept (default: ~/.cache/canton-ab)"
}

usage() {
  print_help >&2
  exit 1
}

THEIRS_REF=""
CORPORA=()
WORK_DIR=${WORK_DIR:-}
ROUNDS=5
JMH_ARGS="-f 1 -wi 5 -i 5"
MARGIN=0
BUILD_ONLY=false
while [ "$#" -gt 0 ]; do
  case $1 in
    --theirs) THEIRS_REF=$2; shift 2 ;;
    --corpus)
      shift
      [ "$#" -gt 0 ] && [[ $1 != -* ]] || usage
      while [ "$#" -gt 0 ] && [[ $1 != -* ]]; do CORPORA+=("$1"); shift; done ;;
    --work-dir) WORK_DIR=$2; shift 2 ;;
    --rounds) ROUNDS=$2; shift 2 ;;
    --jmh-args) JMH_ARGS=$2; shift 2 ;;
    --margin) MARGIN=$2; shift 2 ;;
    --build-only) BUILD_ONLY=true; shift ;;
    -h | --help) print_help; exit 0 ;;
    *) usage ;;
  esac
done
# the guard keeps bash 3 (macOS) from failing on an empty array under set -u
for I in ${CORPORA[@]+"${!CORPORA[@]}"}; do
  [ -f "${CORPORA[$I]}/manifest.json" ] || { echo "${CORPORA[$I]} is not a corpus (no manifest.json)" >&2; exit 1; }
  CORPORA[I]=$(realpath "${CORPORA[$I]}")
done

# ours is HEAD, so the working tree must be exactly HEAD
if [ -n "$(git -C "$REPO" status --porcelain)" ]; then
  echo "Commit or stash your changes first; ours is HEAD:" >&2
  git -C "$REPO" status --short >&2
  exit 1
fi
OURS=$(git -C "$REPO" rev-parse HEAD)
THEIRS=$(git -C "$REPO" rev-parse "${THEIRS_REF:-$(git -C "$REPO" merge-base HEAD origin/main)}^{commit}")

CACHE=${AB_CACHE:-$HOME/.cache/canton-ab}/$(echo "$REPO" | sha1sum | cut -c1-12)
WORK_DIR=${WORK_DIR:-$(mktemp -d)}
mkdir -p "$WORK_DIR/runs" "$CACHE/worktrees" "$CACHE/corpora"
WORK_DIR=$(realpath "$WORK_DIR")
echo "Working in $WORK_DIR"
echo "Ours:   $(git -C "$REPO" log -1 --format='%h %s' "$OURS")"
echo "Theirs: $(git -C "$REPO" log -1 --format='%h %s' "$THEIRS")"

# checks out a commit in the side's worktree, creating it on first use
checkout() {
  local side=$1 commit=$2 worktree=$CACHE/worktrees/$1
  git -C "$REPO" worktree prune
  if [ -d "$worktree" ]; then
    git -C "$worktree" checkout -q --detach --force "$commit"
  else
    git -C "$REPO" worktree add -q --detach "$worktree" "$commit"
  fi
}

# builds the side's benchmark and writes its classpath, so the runs don't need sbt
build() {
  local side=$1 log=$WORK_DIR/build-$1.log
  echo "Building $side (see $log)"
  if ! (cd "$CACHE/worktrees/$side" &&
    sbt -batch "daml-lf-snapshot/Jmh/compile" "export daml-lf-snapshot/Jmh/fullClasspath") > "$log" 2>&1; then
    echo "Building $side failed; see $log" >&2
    exit 1
  fi
  grep -E '^/.*jmh-core' "$log" | tail -1 > "$WORK_DIR/$side.classpath"
}

checkout ours "$OURS"
checkout theirs "$THEIRS"
if [ ! -f "$CACHE/worktrees/theirs/$SNAPSHOT/src/main/scala/com/digitalasset/daml/lf/testing/snapshot/CorpusBenchmark.scala" ]; then
  echo "Theirs has no CorpusBenchmark (it predates #36671), so it can't replay a corpus" >&2
  exit 1
fi
build ours
build theirs
if $BUILD_ONLY; then
  echo "Built both sides"
  exit 0
fi

if [ "${#CORPORA[@]}" -eq 0 ]; then
  # a recording depends only on what's recorded, how it's recorded and assembled, and the daml-script
  # version (recording is deterministic), so it's cached under those; its manifest names the commit
  # it was first recorded from
  OURS_WORKTREE=$CACHE/worktrees/ours
  SCALA=$SNAPSHOT/src/main/scala/com/digitalasset/daml/lf/testing/snapshot
  KEY=$( (git -C "$OURS_WORKTREE" rev-parse "HEAD:$SNAPSHOT/corpus-tests" "HEAD:$SNAPSHOT/scripts/record-corpus.sh" \
    "HEAD:$SNAPSHOT/scripts/sources/canton-tests.sh" "HEAD:$SCALA/Corpus.scala" "HEAD:$SCALA/AddToCorpus.scala" \
    "HEAD:$SNAPSHOT-proto/src/main/protobuf/com/digitalasset/daml/lf/snapshot.proto" &&
    grep 'val version' "$OURS_WORKTREE/project/project/DamlVersions.scala") | sha1sum | cut -c1-16)
  CORPUS=$CACHE/corpora/canton-tests-$KEY
  if [ ! -f "$CORPUS/manifest.json" ]; then
    echo "Recording canton-tests into $CORPUS (see $WORK_DIR/record.log)"
    rm -rf "$CORPUS.tmp"
    WORK_DIR=$WORK_DIR/record "$OURS_WORKTREE/$SNAPSHOT/scripts/record-corpus.sh" canton-tests "$OURS_WORKTREE" "$CORPUS.tmp" \
      > "$WORK_DIR/record.log" 2>&1 || { echo "Recording failed; see $WORK_DIR/record.log" >&2; exit 1; }
    mv "$CORPUS.tmp" "$CORPUS"
  fi
  CORPORA=("$CORPUS")
fi

{
  echo "Ours: $(git -C "$REPO" log -1 --format='%H %s' "$OURS")"
  echo "Theirs: $(git -C "$REPO" log -1 --format='%H %s' "$THEIRS")"
  for CORPUS in "${CORPORA[@]}"; do
    echo "Corpus: $CORPUS, $(jq '.suites | length' "$CORPUS/manifest.json") suites, $(jq '[.suites[].tests[].units // 0] | add' "$CORPUS/manifest.json") units"
    jq -r '.suites[].source | "Corpus source: \(.name) \(.repo) \(.commit), recorded with \(.recorder)"' "$CORPUS/manifest.json" | sort -u
  done
  echo "Rounds: $ROUNDS, each with JMH options $JMH_ARGS"
  echo "JVM: $(java -version 2>&1 | grep -v '^Picked up' | paste -sd ' ')"
  if command -v lscpu > /dev/null; then CPU=$(lscpu | sed -n 's/^Model name: *//p'); else CPU=$(sysctl -n machdep.cpu.brand_string); fi
  echo "Machine: $(uname -srm), $(getconf _NPROCESSORS_ONLN) CPUs, $CPU"
} > "$WORK_DIR/meta.txt"

# alternating which side goes first spreads any effect of the order over both
for ROUND in $(seq 1 "$ROUNDS"); do
  if [ $((ROUND % 2)) -eq 1 ]; then SIDES="ours theirs"; else SIDES="theirs ours"; fi
  for SIDE in $SIDES; do
    echo "Round $ROUND of $ROUNDS: $SIDE"
    # a run replays every corpus once, as one JMH invocation each
    for I in "${!CORPORA[@]}"; do
      RUN=$WORK_DIR/runs/$SIDE-$ROUND-$((I + 1))
      # shellcheck disable=SC2086
      java -cp "$(cat "$WORK_DIR/$SIDE.classpath")" org.openjdk.jmh.Main CorpusBenchmark.bench \
        -p corpus="${CORPORA[$I]}" -foe true $JMH_ARGS -rf json -rff "$RUN.json" > "$RUN.log" 2>&1 ||
        { echo "The $SIDE run failed; see $RUN.log" >&2; exit 1; }
    done
  done
done

java -cp "$(cat "$WORK_DIR/ours.classpath")" com.digitalasset.daml.lf.testing.snapshot.AbReport "$WORK_DIR" "$MARGIN"
