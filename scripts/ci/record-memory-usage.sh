#!/usr/bin/env bash

mkdir -p track
FILE=track/memory-usage-actual.txt
# delete previous file if it was stored as part of the workspace
if [ -e $FILE ]; then
   rm $FILE
fi
  let a=1
while true; do
    echo "---------- $(date): after $a iterations:" | tee -a "$FILE"
    ps -e -o pid,user,state,start_time,cputime,rss,args --sort=-rss --cols 300 | head -n7 | tee -a "$FILE"
    # [TEST ONLY] PV37 profiling: node-wide load (pods have no CPU limit), file only to keep the job log unchanged.
    echo "loadavg $(cat /proc/loadavg 2>/dev/null)" >> "$FILE"
    echo | tee -a "$FILE"
    sleep 1
 let a=$a+1
done
