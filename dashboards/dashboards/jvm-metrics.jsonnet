// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local panels = import '../lib-shared/panels.libsonnet';
local queries = import '../lib-shared/queries.libsonnet';
local sharedVars = import '../lib-shared/variables.libsonnet';

local db = g.dashboard;
local pts = panels.timeSeries;

db.new('JVM Metrics')
+ db.withUid('digital-asset-jvm-metrics')
+ db.withTags(['jvm', 'gc'])
+ d.settings

+ db.withVariables([
  sharedVars.datasource,
  sharedVars.namespace('jvm_cpu_time_seconds_total'),
  sharedVars.pod('jvm_cpu_time_seconds_total'),
  sharedVars.container('jvm_cpu_time_seconds_total'),
  sharedVars.jvm,
  sharedVars.jvm_mempool,
])

+ db.withPanels(
  g.util.grid.wrapPanels([
    pts.short('CPUs Utilisation', [queries.jvm.cpus.available, queries.jvm.cpus.used], width=24),
    pts.garbageCollection('Garbage Collection', queries.jvm.gc().all),
    pts.shortStacked('Threads', queries.jvm.threads),
    pts.memoryUsage('Heap Memory', queries.jvm.memory.heap.all),
    pts.memoryUsage('Non-heap Memory', queries.jvm.memory.non_heap.all),
    pts.memoryPoolsUsage('Memory pool - $jvm_mempool', queries.jvm.memory.pools.all, maxPerRow=3),
  ], panelWidth=12),
)
