#!/usr/bin/env bash

# Internal function to identify the GNU version of a tool.
# Takes candidates as arguments.
find_gnu_tool() {
  for tool in "$@"
  do
    if ${tool} --version 2>&1 | grep GNU &> /dev/null
    then
      cat <<< "$tool"
      return 0
    fi
  done
  >&2 echo "Unable to find the GNU version of $1. Tried $*. Aborting..."
  exit 1
}

# shellcheck disable=SC2034
GNU_GREP=$(find_gnu_tool "grep" "ggrep")
# shellcheck disable=SC2034
GNU_SED=$(find_gnu_tool "sed" "gsed")


# If an error happens in daml code then com.daml.lf.InternalError.log may be called. This will often
# Output a line that only contains 'root cause' with the exception text on the line below. This
# function joins joins these lines together to allow more specific pattern matching. 
join_daml_error_lines() {
  # In addition to joining the lines if an ascii colour code is present remove it
  "$GNU_SED" '/ERROR c.daml.lf - root cause$/{N;s/\n/:/;s/\x1b[^m]*m//g;}'
}
