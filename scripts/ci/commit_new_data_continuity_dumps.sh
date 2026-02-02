#!/bin/bash

cp -r /tmp/canton/data-continuity-dumps/rv* community/app/src/test/data-continuity-dumps
git add -v community/app/src/test/data-continuity-dumps
# Only commit if data-continuity-dumps have been changed
git diff --staged --quiet || git commit -m "Updated dumps for data continuity tests"
