#!/usr/bin/env bash
set -o pipefail
# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

TMPDIR="${TMPDIR:-/tmp}"
declare -a cache_dirs=("$HOME/.ivy2/cache" "$HOME/.cache/coursier/v1" "$HOME/.sbt" "${HOME}/.m2/repository" )

function usage() {
  echo "Usage: $0 [[rm|size|chmod|mkdir]...]"
  echo "  rm|rmcache                   - remove cache directories"
  echo "  size|sizecache               - calculate cache size"
  echo "  chmod|chmodcache             - make cache readable"
  echo "  mkdir|mkdircache             - create cache directories if they do not exist"
}

function action_apply() {
  local CMD="$1"
  for dir in "${cache_dirs[@]}"; do
        ${CMD} "${dir}"
    printf '.'
  done
}

function rmcache() {
  progress_start "delete cache directories"
    action_apply "rm -rf"
    [[ -f "${TMPDIR}/sbtVersion" ]] && rm -f "${TMPDIR}/sbtVersion"
  progress_done
}

function sizecache() {
  progress_start "calculate cache size"
  for dir in "${cache_dirs[@]}"; do
    if [[ -d "$dir" ]]; then
      du --bytes --summarize ${dir} | while read size folder; do
        printf "\r\n"
        if [[ "${folder}" ==  "/tmp/remote-cache" ]]; then
          print_folder="${folder}"
        else
          print_folder="~/.${folder##*.}"
        fi
        if [[ "$size" == "0" ]]; then
            info_n "    -    \t${c_white}:${c_reset} ${print_folder}"
          else
            size_mb=$((size / 1048576))
            info_n "${size_mb}M\t${c_white}:${c_reset} ${print_folder}"
        fi
      done
    fi
    done
    printf "\r\n"
}

function chmodcache() {
  local user="${USER:-$(id -un)}"
  local group="${GROUP:-$(id -gn)}"
  mkdir -p "${cache_dirs[@]}"
  progress_start "make cache readable     "
    action_apply "chmod -R a+r"
  progress_done
  progress_start "change ownership        "
    action_apply "chown -R ${user}:${group}"
  progress_done
}

function mkdircache() {
  progress_start "create cache directories"
    action_apply "mkdir -p "
  progress_done
}

# Check if at least one parameter is provided
if [ $# -eq 0 ]; then
  usage
  exit
fi

for action in $@; do
  case "$action" in
    rm|rmcache)
      rmcache
      ;;
    size|sizecache)
      sizecache
      ;;
    chmod|chmodcache)
      chmodcache
      ;;
    mkdir|mkdircache)
      mkdircache
      ;;
    *)
      usage
      exit 1
      ;;
  esac

done

