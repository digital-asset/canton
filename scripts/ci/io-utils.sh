#!/usr/bin/env bash

# Copy stdin to stdout, while removing all lines that start with '#' or consist of blanks
remove_comment_and_blank_lines() {
  while IFS= read -r # Read a file line by line; IFS= ensures that no separators other than newline are used
  do
    if [[ -n "${REPLY// }" ]] && [[ "$REPLY" != "#"* ]] # Filter out comment lines and blank lines
    then
      cat <<< "$REPLY"
    fi
  done
}

# Read lines from stdin and invoke "$@" with input lines as extra arguments
with_input_as_params() {
  local -a PARAMS
  while IFS= read -r; do
    PARAMS+=("$REPLY")
  done
  "$@" "${PARAMS[@]}"
}

# Output a problem report.
# Problems are read from stdin.
output_problems() {
  local PROBLEMS_NAME="${1:?PROBLEMS_NAME undefined}"
  local SOURCE="${2:?SOURCE undefined}"
  local RETURN_CODE_ON_PROBLEM="${3:-1}"
  local PROBLEMS
  PROBLEMS=$(cat)
  if [[ -z "$PROBLEMS" ]]
  then
    echo "No $PROBLEMS_NAME found in $SOURCE."
    echo
  else
    echo "Found $PROBLEMS_NAME in $SOURCE:"
    cat <<< "$PROBLEMS"
    if [[ $RETURN_CODE_ON_PROBLEM != "0" ]]
    then cat <<< "$PROBLEMS" >> found_problems.txt
    fi
    echo "Total: $(wc -l <<< "$PROBLEMS") lines with $PROBLEMS_NAME."
    echo
    return "$RETURN_CODE_ON_PROBLEM"
  fi
}

# catch STDOUT STDERR cmd args..
# https://stackoverflow.com/questions/11027679/capture-stdout-and-stderr-into-different-variables/41069638#41069638
catch()
{
eval "$({
__2="$(
  { __1="$("${@:3}")"; } 2>&1;
  ret=$?;
  printf '%q=%q\n' "$1" "$__1" >&2;
  exit $ret
  )";
ret="$?";
printf '%s=%q\n' "$2" "$__2" >&2;
printf '( exit %q )' "$ret" >&2;
} 2>&1 )";
}
