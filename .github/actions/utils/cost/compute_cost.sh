#! /usr/bin/env bash
# Writes the OpenCost cost, CPU usage and RAM usage of every job in the current workflow run to
# the job summary, with a total row.
#
# CPU and RAM are the usage over the pod's lifetime, so that they add up across jobs like cost
# does: average cores used x hours = core-hours, and average bytes used x hours = GiB-hours.
# They are usage, not the request: cost is billed on max(request, usage) and can be higher.
#
# A job whose pod OpenCost has no data for, or incomplete data (cost, CPU or RAM missing), shows
# NA in all three columns and is left out of the totals, rather than counting as 0.
#
# If OpenCost cannot be queried or the jobs cannot be listed, the script fails without writing a
# table. The job running it is continue-on-error, so that failure alone would go unnoticed: it is
# also raised as an annotation (a warning for OpenCost, an error for the job listing) and as a note
# in the summary.

set -euo pipefail

OPENCOST_URL="${OPENCOST_URL:-http://opencost.opencost:9003}"

if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "GH_TOKEN is not set: it is needed to list the jobs of run ${GITHUB_RUN_ID}." >&2
  exit 1
fi

# Workflow commands (annotations) are only read from the real stdout, so keep it on fd 3 before
# stdout is pointed at the job summary.
exec 3>&1
exec >>"${GITHUB_STEP_SUMMARY}"

echo "## Workflow Cost"

# Annotation of the given level (on the real stdout, fd 3: stdout is the summary) plus a note in
# the summary, then fail. Usage: fail <warning|error> <title> <message>
fail() {
  echo "::$1 title=$2::$3" >&3
  echo "> :$([[ "$1" == warning ]] && echo warning || echo x): $2: $3"
  exit 1
}

opencost="$(mktemp)"
trap 'rm -f "${opencost}"' EXIT
if ! curl -sSf --max-time 60 -o "${opencost}" "${OPENCOST_URL}/allocation/compute?window=2d&aggregate=pod"; then
  fail warning "OpenCost query failed" "the request to ${OPENCOST_URL} failed, so there is no cost data."
fi
if ! jq -e '.data | type == "array"' "${opencost}" >/dev/null 2>&1; then
  fail warning "OpenCost query failed" "${OPENCOST_URL} returned a response without allocation data."
fi

# Small pages: every job in the response carries its full steps list, so a page of 100 jobs of
# a big in-progress run is a large response that GitHub can answer with a 502.
# Retried a few times, with no wait in between.
jobs_endpoint="/repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs?per_page=30&filter=all"
jobs="$(mktemp)"
trap 'rm -f "${opencost}" "${jobs}"' EXIT
for attempt in 1 2 3 4; do
  if gh api --paginate "${jobs_endpoint}" --jq '.jobs[]' >"${jobs}"; then
    break
  fi
  if [[ "${attempt}" == 4 ]]; then
    fail error "Listing jobs failed" "the jobs of run ${GITHUB_RUN_ID} could not be listed after ${attempt} attempts."
  fi
  echo "Listing jobs failed (attempt ${attempt}), retrying." >&2
done

echo "| Name | Conclusion | Cost | CPU | RAM |"
echo "| ---- | ---------- | ---- | --- | --- |"

jq -s -r \
    --slurpfile opencost "${opencost}" \
    'def round3: . * 1000 | round / 1000;
     # null means OpenCost has no value: shown as NA and left out of the totals.
     def show(unit): if . == null then "NA" else "\(round3) \(unit)" end;
     def total(f): [.[] | f | select(. != null)] | if length == 0 then null else add end;
     map(
        # Only jobs that ran on a pod and finished (success, failure or cancelled): skipped jobs
        # have no runner_name, and this job itself is still in progress (null conclusion).
        select(.runner_group_name == "Default" and .runner_name != null and
               .conclusion != null and .conclusion != "skipped") |
        (($opencost[0].data[0] // {})[.runner_name]) as $pod |
        # Pod lifetime in hours: OpenCost reports it as minutes, else take it from start/end.
        ($pod.minutes // (try ((($pod.end | fromdateiso8601) - ($pod.start | fromdateiso8601)) / 60) catch null)) as $minutes |
        (if $minutes == null then null else $minutes / 60 end) as $hours |
        {
          "name": .name,
          "conclusion": .conclusion,
          "cost": (if $pod.totalCost == null then null else [0, $pod.totalCost] | max end),
          "cpu": (if $pod.cpuCoreUsageAverage == null or $hours == null then null
                  else $pod.cpuCoreUsageAverage * $hours end),
          "ram": (if $pod.ramByteUsageAverage == null or $hours == null then null
                  else $pod.ramByteUsageAverage * $hours / 1073741824 end)
        } |
        # One missing value makes the whole row NA, so every total sums the same set of jobs.
        if .cost == null or .cpu == null or .ram == null then .cost = null | .cpu = null | .ram = null else . end
      ) |
      sort_by(.cost) |
      reverse |
      . + [{
        "name": "**TOTAL**",
        "cost": total(.cost),
        "cpu": total(.cpu),
        "ram": total(.ram)
      }] |
      .[] |
      "| \(.name) | \(.conclusion // "") | \(.cost | show("USD")) | \(.cpu | show("core-h")) | \(.ram | show("GiB-h")) |"' "${jobs}"

echo
echo "NA: OpenCost has no data, or incomplete data, for the job's pod. It is left out of the totals."
