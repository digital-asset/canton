#!/usr/bin/env python3
#
# Copyright (c) 2023-2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.

###############################################################################
# Extracts key metrics from a CSV file and prints them to stdout.
###############################################################################

import csv
import sys

from datetime import datetime, UTC

filename = sys.argv[1]
prefix = sys.argv[2]
early_event_percentile = int(sys.argv[3])
late_event_percentile = int(sys.argv[4])


with open(filename, 'r') as file:
    rows = list(csv.DictReader(file))

# this is the filtering logic
if len(sys.argv) >= 6 and sys.argv[5]:
    rows = [x for x in rows if sys.argv[5] in x["attributes"]]

if not rows:
    sys.exit("No metrics reported!")

last_report = rows[-1]
total_count = int(last_report['count'])

if len(sys.argv) == 7 and sys.argv[6].lower() == "calc_test_time":
    test_end_timestamp = int(last_report['timestamp'])
    test_start_timestamp = int(rows[0]['timestamp'])
    test_start_time = datetime.fromtimestamp(test_start_timestamp, UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    test_duration_in_secs = test_end_timestamp - test_start_timestamp
    print(f'{prefix}_START_TIME="{test_start_time}Z"')
    print(f'{prefix}_DURATION_IN_SECS={test_duration_in_secs}')

early_threshold = total_count * early_event_percentile / 100
early_report = next(row for row in rows if int(row['count']) >= early_threshold)

early_ts = int(early_report['timestamp'])
print(f'{prefix}_EARLY_TS={early_ts}')

early_count = int(early_report['count'])
print(f'{prefix}_EARLY_COUNT={early_count}')

late_threshold = total_count * late_event_percentile / 100
late_report = next(row for row in rows if int(row['count']) >= late_threshold)

late_ts = int(late_report['timestamp'])
print(f'{prefix}_LATE_TS={late_ts}')

late_count = int(late_report['count'])
print(f'{prefix}_LATE_COUNT={late_count}')

early_to_late_count = late_count - early_count
print(f'{prefix}_EARLY_TO_LATE_COUNT={early_to_late_count}')

early_to_late_time = late_ts - early_ts
print(f'{prefix}_EARLY_TO_LATE_TIME={early_to_late_time}')
