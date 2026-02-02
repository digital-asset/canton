#!/usr/bin/python3
#
# simple script to parse and display compile time statistics.
# this script also serves as a reference on how to do it
#
# First, turn on the following sbt settings:
#          Global / concurrentRestrictions += Tags.limitAll(1),
#          scalacOptions += "-Ystatistics", // re-enable if you need to debug compile times
#          scalacOptions in Test += "-Ystatistics", // re-enable if you need to debug compile times
# This will turn on statistics gathering and it will sequence the compilation
#
# Then run something like sbt test:compile | tee output2.txt and then python3 <this script> output2.txt
#


import sys
import re

with open(sys.argv[1], "r") as f:
    data = {}
    scope = "bad"
    gather = False
    for rawline in f.readlines():
        if len(rawline) > 7:
            line = rawline[7:]
            newScope = re.search('^compiling \d+ Scala sources? to (.*)', line)
            if newScope:
                scope = newScope.group(1)
                data[scope] = {}
                gather = False
            elif line.startswith("#total compile time") or gather:
                gather = True
                tmp = re.match('(.*):(.*)', line)
                if tmp:
                    data[scope][tmp.group(1).strip()] = tmp.group(2).strip()
                else:
                    gather = False

for k,v in data.items():
    print(k,v["#total compile time"])





