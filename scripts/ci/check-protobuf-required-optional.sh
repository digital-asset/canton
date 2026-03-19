#!/usr/bin/env bash
set -eo pipefail

# This linting is targeting specifically Ledger API protobuf definitions, where it is important
# to have conformance with the laid out rules of Required / Optional annotations, as automatic
# translation of proto definitions to JSON-API OpenAPI definitions parses these annotations for
# correct rendering.
# For more information please read community/ledger-api-proto/README.md
LAPI_PROTO_DIR="community/ledger-api-proto/src/main/protobuf/com/daml/ledger/api/v2"

fail="no"
protofilesfound="no"
protoFile=""
line=""
previousLine=""
previousPreviousLine=""
oneofskip=""
lineNum=0

failWithError () {
  fail="yes"
  previousLineNum=$(( $lineNum - 1 ))
  previousPreviousLineNum=$(( $lineNum - 2 ))
  echo "[error] $protoFile:$lineNum: $1"
  echo "    Line:$previousPreviousLineNum $previousPreviousLine"
  echo "    Line:$previousLineNum $previousLine"
  echo "    Line:$lineNum $line"
  echo ""
}

verifyEmptyCommentLineBeforeAnnotation () {
  if [[ "$previousPreviousLine" =~ ^// && ! "$previousPreviousLine" = "//" ]]; then
    failWithError "Optional/Required annotation must be preceded by an empty comment line"
  fi
}



for protoFile in $(find "$LAPI_PROTO_DIR" -name "*.proto"); do
  echo "Checking $protoFile"
  protofilesfound="yes"
  lines=$(cat $protoFile)
  previousLine=""
  previousPreviousLine=""
  oneofskip=""
  lineNum=0
  while read line; do
    lineNum=$(( $lineNum + 1 ))
    if [[ "$oneofskip" = "skip" ]]; then
      if [[ "$line" =~ ^\} ]]; then
        # end of oneof skipping
        oneofskip=""
      fi
    else
      if [[ "$line" =~ ^oneof[[:space:]]+[[:alnum:]_]+[[:space:]]+\{ ]]; then
        # start of oneof skipping
        if [[ ! "$previousLine" =~ ^//[[:space:]]Required$|^//[[:space:]]Optional$ ]] then
          failWithError "oneof field definition is not preceded with '// Required' / '// Optional' annotation"
        else
          verifyEmptyCommentLineBeforeAnnotation
        fi
        oneofskip="skip"
      elif [[ "$line" =~ ^repeated[[:space:]]+[[:alnum:]]+[[:space:]]+[[:alnum:]_]+[[:space:]]+=[[:space:]]+[[:digit:]]+\; ]]; then
        if [[ ! "$previousLine" =~ ^//[[:space:]]Required:[[:space:]]must[[:space:]]be[[:space:]]non-empty$|^//[[:space:]]Optional:[[:space:]]can[[:space:]]be[[:space:]]empty$ ]] then
          failWithError "repeated field definition is not preceded with '// Required: must be non-empty' / '// Optional: can be empty' annotation"
        else
          verifyEmptyCommentLineBeforeAnnotation
        fi
      elif [[ "$line" =~ ^map[[:space:]]?\<[[:space:]]?[[:alnum:]]+[[:space:]]?,[[:space:]]?[[:alnum:]]+[[:space:]]?\>[[:space:]]+[[:alnum:]_]+[[:space:]]+=[[:space:]]+[[:digit:]]+\; ]]; then
        if [[ ! "$previousLine" =~ ^//[[:space:]]Required:[[:space:]]must[[:space:]]be[[:space:]]non-empty$|^//[[:space:]]Optional:[[:space:]]can[[:space:]]be[[:space:]]empty$ ]] then
          failWithError "map field definition is not preceded with '// Required: must be non-empty' / '// Optional: can be empty' annotation"
        else
          verifyEmptyCommentLineBeforeAnnotation
        fi
      elif [[ "$line" =~ ^(optional[[:space:]]+)?bytes[[:space:]]+[[:alnum:]_]+[[:space:]]+=[[:space:]]+[[:digit:]]+\; ]]; then
        if [[ ! "$previousLine" =~ ^//[[:space:]]Required:[[:space:]]must[[:space:]]be[[:space:]]non-empty$|^//[[:space:]]Optional:[[:space:]]can[[:space:]]be[[:space:]]empty$ ]] then
          failWithError "bytes field definition is not preceded with '// Required: must be non-empty' / '// Optional: can be empty' annotation"
        else
          verifyEmptyCommentLineBeforeAnnotation
        fi
      elif [[ "$line" =~ ^(optional[[:space:]]+)?[[:alnum:]]+[[:space:]]+[[:alnum:]_]+[[:space:]]+=[[:space:]]+[[:digit:]]+\; ]]; then
        if [[ ! "$previousLine" =~ ^//[[:space:]]Required$|^//[[:space:]]Optional$ ]] then
          failWithError "field definition is not preceded with '// Required' / '// Optional' annotation"
        else
          verifyEmptyCommentLineBeforeAnnotation
        fi
      fi
    fi
    previousPreviousLine="$previousLine"
    previousLine="$line"
  done <<< "$lines"
done

if [ "$fail" = "yes" ]; then
  echo ""
  echo "Problems found! Please fix required/optional issues above."
  echo "  How to fix: Please read community/ledger-api-proto/README.md and look at examples in the Ledger API proto files."
  echo "  How to check locally: Run 'scripts/ci/check-protobuf-required-optional.sh' from the project root directory."
  exit 1
elif [ "$protofilesfound" = "no" ]; then
  echo "No proto files found!"
  exit 1
else
  echo "No problems found."
  exit 0
fi
