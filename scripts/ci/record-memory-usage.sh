#!/usr/bin/env bash

mkdir -p track
FILE=track/memory-usage-actual.txt
# delete previous file if it was stored as part of the workspace
if [ -e $FILE ]; then
   rm $FILE
fi
# [TEST ONLY] PV37 profiling: the shard's Postgres container runs outside this process tree, so ps above
# does not see it. Every 5 s, record its cgroup counters: cumulative CPU (cpu.stat usage_usec), disk I/O
# (io.stat rbytes/wbytes), memory (memory.current and the anon/file/shmem/dirty breakdown). Every 30 s,
# also record the size of its data directory. Goes to a separate file, not the job log.
PG_FILE=track/postgres-usage.txt
rm -f "$PG_FILE"
(
  n=0
  while true; do
    c=$(docker ps -q --filter "name=^canton-postgres-" 2>/dev/null | head -n1)
    if [[ -n "$c" ]]; then
      {
        echo "---------- $(date +%s)"
        docker exec "$c" sh -c 'echo "cpu $(grep -E "^usage_usec" /sys/fs/cgroup/cpu.stat)"; echo "io $(cat /sys/fs/cgroup/io.stat | tr "\n" ";")"; echo "mem_current $(cat /sys/fs/cgroup/memory.current)"; echo "mem $(grep -E "^(anon|file|shmem|file_dirty|file_writeback) " /sys/fs/cgroup/memory.stat | tr "\n" " ")"' 2>&1
        if (( n % 6 == 0 )); then
          echo "du_mb $(docker exec "$c" du -sm /var/lib/postgresql 2>/dev/null | cut -f1)"
        fi
      } >> "$PG_FILE"
      n=$((n + 1))
    fi
    sleep 5
  done
) &
PG_PID=$!
trap 'kill "$PG_PID" 2>/dev/null' EXIT
trap 'exit 0' TERM INT

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
