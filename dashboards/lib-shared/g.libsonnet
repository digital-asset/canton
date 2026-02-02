// Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

/// TEMPLATED MANAGED FILE, DO NOT EDIT
// If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

local version = std.extVar('grafonnet_version');

// unfortunately, interpolation in import statements is unsupported :(
if version == 'v9.4.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v9.4.0/main.libsonnet'
else if version == 'v9.5.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v9.5.0/main.libsonnet'
else if version == 'v10.0.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v10.0.0/main.libsonnet'
else if version == 'v10.1.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v10.1.0/main.libsonnet'
else if version == 'v10.2.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v10.2.0/main.libsonnet'
else if version == 'v10.3.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v10.3.0/main.libsonnet'
else if version == 'v10.4.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v10.4.0/main.libsonnet'
else if version == 'v11.0.0' then
  import 'github.com/grafana/grafonnet/gen/grafonnet-v11.0.0/main.libsonnet'
else
  import 'github.com/grafana/grafonnet/gen/grafonnet-latest/main.libsonnet'
