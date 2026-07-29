# Scripts that pytest cannot import for collection and which carry no tests:
#  - hyphenated filenames are not valid module names
#  - parse_compile_times.py runs at import time (no __main__ guard)
# Skip them during collection.
collect_ignore = [
    "ci/check-datadog.py",
    "ci/parse_compile_times.py",
    "canton-testing/util/read-csv-metric.py",
]
