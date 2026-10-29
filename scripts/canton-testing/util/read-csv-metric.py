#!/usr/bin/env python3
#
# Copyright (c) 2023-2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.

###############################################################################
# Extracts key metrics from a CSV file and prints them to stdout.
###############################################################################

import csv
import sys
import re

from collections import defaultdict
from datetime import datetime, UTC

filename = sys.argv[1]
prefix = sys.argv[2]
early_event_percentile = int(sys.argv[3])
late_event_percentile = int(sys.argv[4])

with open(filename, 'r') as file:
    rows = list(csv.DictReader(file))

# This is the filtering logic
# Note: 'pattern' is evaluated as a Python regular expression (re.search).
# If filtering with literal special regex characters like '.', '?', or '*', keep in mind they are evaluated as regex patterns.
if len(sys.argv) >= 6 and sys.argv[5]:
    pattern = sys.argv[5]
    rows = [x for x in rows if re.search(pattern, str(x["attributes"]))]

if not rows:
    sys.exit("No metrics reported!")

# Global test timing
test_start_ts = int(rows[0]['timestamp'])
test_end_ts = int(rows[-1]['timestamp'])

if len(sys.argv) == 7 and sys.argv[6].lower() == "calc_test_time":
    test_start_time = datetime.fromtimestamp(test_start_ts, UTC).strftime("%Y-%m-%d %H:%M:%S")
    test_duration_in_secs = test_end_ts - test_start_ts
    print(f'{prefix}_START_TIME="{test_start_time}Z"')
    print(f'{prefix}_DURATION_IN_SECS={test_duration_in_secs}')

total_rows = len(rows)

# Determine early and late row indices based on percentiles
early_idx = int((total_rows - 1) * early_event_percentile / 100)
late_idx = int((total_rows - 1) * late_event_percentile / 100)

early_ts = int(rows[early_idx]['timestamp'])
late_ts = int(rows[late_idx]['timestamp'])

# Group rows by attributes so each series is evaluated independently and then combined together
# This is important because if the filtering logic contains independent attributes
# their count value might be different so we cannot handle them uniformly.
# If we handle them uniformly, and attribute=A has count value 1, but attribute=B has count 100
# Then based on the early and late percentile we get ad-hoc values based on whether at that row
# attribute=A or attribute=B appears.
# Grouping them separately and counting these values ensures we have the appropriate values
series_dict = defaultdict(list)
for row in rows:
    series_dict[row['attributes']].append(row)

calculated_early_count = 0
calculated_late_count = 0

# Block-level metrics files contain discrete non-cumulative events disaggregated by attributes.
# Check the filename directly so any metric using a block events file aggregates correctly.
is_block_events_file = filename.endswith("block.events.csv") or filename.endswith(
    "block.event-bytes.csv"
)

# In sequencer block events, the nodes has their own attributes and their cumulative counter is separately counted
# Therefore, we need to aggregate these separate results and sum them to get the final block event/byte count
if "SEQUENCER_BLOCK" in prefix or is_block_events_file:
    for attr, attr_rows in series_dict.items():
        # Find the last recorded count for this series at or before early_ts
        early_reports = [r for r in attr_rows if int(r['timestamp']) <= early_ts]
        early_count = (
            int(early_reports[-1]['count']) if early_reports else int(attr_rows[0]['count'])
        )

        # Find the last recorded count for this series at or before late_ts
        late_reports = [r for r in attr_rows if int(r['timestamp']) <= late_ts]
        late_count = int(late_reports[-1]['count']) if late_reports else int(attr_rows[-1]['count'])

        calculated_early_count += early_count
        calculated_late_count += late_count
else:
    # If we have more than one attribute group after filtering, we need to raise an error
    # because then the filter doesn't seem good enough for collecting the final count
    if len(series_dict) > 1 and len(sys.argv) >= 6 and sys.argv[5]:
        pattern = sys.argv[5]
        raise ValueError(
            f"Ambiguous filter '{pattern}' matched {len(series_dict)} distinct metric series "
            f"for non-aggregated prefix '{prefix}'. Please provide a more specific filter."
        )

    # For single series, pick the count closest to global row indices
    calculated_early_count = int(rows[early_idx]['count'])
    calculated_late_count = int(rows[late_idx]['count'])

# Output unified shell metrics
print(f'{prefix}_EARLY_TS={early_ts}')
print(f'{prefix}_EARLY_COUNT={calculated_early_count}')
print(f'{prefix}_LATE_TS={late_ts}')
print(f'{prefix}_LATE_COUNT={calculated_late_count}')

early_to_late_count = calculated_late_count - calculated_early_count
print(f'{prefix}_EARLY_TO_LATE_COUNT={early_to_late_count}')

early_to_late_time = max(late_ts - early_ts, 1)
print(f'{prefix}_EARLY_TO_LATE_TIME={early_to_late_time}')

rate = early_to_late_count / early_to_late_time
print(f'{prefix}_RATE={rate:.2f}')
