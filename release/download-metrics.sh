#!/bin/bash

# Queries the GitHub api for download counts of the artifacts released via the
# public canton repository.
# Produces a payload suitable for the datadog series API.

set -eu
set -o pipefail

if ! jq --version 2&> /dev/null
then
  echo "jq is required to run this script."
  echo "Install jq from https://stedolan.github.io/jq/"
  exit 1
fi

OWNER=digital-asset
REPO=canton

# page through available results starting from the beginning
# if there are no results for a page index the response will be empty
page=1
# accumulated release metrics
metrics=""

while true; do
  # jq filter produces a metric per line in the format "tag,filename,download_count"
  page_metrics=$(curl --silent "https://api.github.com/repos/$OWNER/$REPO/releases?page=$page" \
    | jq -r '.[] | .tag_name as $tag | .assets[] | "\($tag),\(.name),\(.download_count)"')

  # move to next page
  page=$((page + 1))

  # but don't bother fetching it if the results for this page were empty
  if [[ -z "$page_metrics" ]]; then
    break
  else
    # append this page of metrics to previous pages
    if [[ -z "$metrics" ]]; then
      # we're first page
      metrics="$page_metrics"
    else
      metrics="$metrics"$'\n'"$page_metrics"
    fi
  fi
done

# convert to datadog series payload
timestamp=$(date +%s)
series=""
sep=""

function generate_metric {
  local metric="$1"
  local tag="$2"
  local filename="$3"
  local download_count="$4"

  local tags="[\"tag:$tag\",\"filename:$filename\"]"
  local serie="{\"metric\":\"$metric\", \"points\":[[$timestamp, $download_count]], \"tags\":$tags}"

  echo "$serie"
}

# assume the first release is the latest release as they're ordered by release date
# if we ever use prereleases this will likely have to be reevaluated
latest_release=$(head -1 <<< "$metrics" | cut -d, -f1)

while read -r metric; do
  tag=$(cut -d, -f1 <<< "$metric")
  filename=$(cut -d, -f2 <<< "$metric")
  download_count=$(cut -d, -f3 <<< "$metric")

  metric=$(generate_metric "canton.release-downloads" "$tag" "$filename" "$download_count")
  series="$series$sep$metric"
  sep=", "

  if [[ "$latest_release" == "$tag" ]]; then
    metric=$(generate_metric "canton.latest-release-downloads" "$tag" "$filename" "$download_count")
    series="$series$sep$metric"
  fi
done <<< "$metrics"

# wrap in datadog series request body
echo "{ \"series\" : [ $series ] }"

