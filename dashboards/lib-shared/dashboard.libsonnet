// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

/// TEMPLATED MANAGED FILE, DO NOT EDIT
// If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

local g = import 'g.libsonnet';
local db = g.dashboard;

{
  settings:
    db.withTimezone('browser')
    + db.withRefresh('10s')
    + db.time.withFrom('now-15m')
    + db.timepicker.withRefreshIntervals(['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'])
    + db.graphTooltip.withSharedCrosshair(),
}
