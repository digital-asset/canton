#!/usr/bin/env python3
#
#  Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates
#
#  Proprietary code. All rights reserved.
#

###############################################################################
# Extracts key metrics from a CSV file and prints them to stdout.
###############################################################################

import csv
import sys

filename = sys.argv[1]
prefix = sys.argv[2]
early_event_percentile = int(sys.argv[3])
late_event_percentile = int(sys.argv[4])


with open(filename, 'r') as file:
    rows = list(csv.DictReader(file))

if len(sys.argv) == 6:
    rows = [x for x in rows if sys.argv[5] in x["attributes"]]

if not rows:
    sys.exit("No metrics reported!")

last_report = rows[-1]
total_count = int(last_report['count'])

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
