[//]: # (TEMPLATED MANAGED FILE, DO NOT EDIT)
[//]: # (If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools)

# Introduction

This directory forms the template for the dashboards authoring by Digital Asset's products through the means of
`Grafonnet` [library](https://grafana.github.io/grafonnet/). It provides the tooling and the shared libraries targeted
at the simplification of authoring process and the unification of the look-and-feel across the dashboards.

# How to author dashboards

In order to make use of the re-usable components of the shared libraries, one needs to simply import them with correct
non-clashing alias, for example:

```jsonnet
// Company-wide baseline
local p = import 'lib-shared/panels.libsonnet';
local q = import 'lib-shared/queries.libsonnet';

// Project-local overrides
local panels = import 'lib/panels.libsonnet';
local queries = import 'lib/queries.libsonnet';

db.new('The dashboard for Project X')

+ db.withPanels([
  row.new('Foo')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    p.table.settings('Settings', q.jvm.info),
    p.table.launchArgs('Launch arguments', q.jvm.info),
  ], panelWidth=12)),

  row.new('Bar')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    panels.projectx.timeSeries.contractsChurn('Churn', queries.contracts.churn.all),
    panels.projectx.timeSeries.activeContracts('Active', queries.contracts.active),
  ], panelWidth=24)),

])
```

## Development of dashboards

Normally, one would be focused on developing a single dashboard at a given time. The following commands may help:
```shell
$ cd dashboards

# check for errors
$ make check DASHBOARD_SOURCE=my-dashboard.jsonnet

# turn dashboard definition from DSL into JSON
$ make generate DASHBOARD_SOURCE=my-dashboard.jsonnet DASHBOARD_GENERATED=your-dashboard.json

# reformat all .jsonnet/.libsonnet files for consistent code style
$ make reformat
```

Alternatively, it's possible to edit the dashboard and get it uploaded automatically to a running Grafana instance on
save for fast iterative feedback:
```shell
$ export GRAFANA_URL="http://<grafana-host>:<grafana-port>/"

# (through Grafana UI) create a user with Editor permissions at: Administration > Users and access > Service accounts
$ export GRAFANA_API_KEY="<your-token-here>"

$ ./watch.sh my-dashboard.jsonnet
```

Once all dashboards are authored to one's satisfaction, it's possible to generate them all for multiple versions of
Grafana for best compatibility purposes:
```shell
$ tree dashboards
dashboards
├── grpc-endpoints.jsonnet
└── http-endpoints.jsonnet

$ ./generate-all.sh

$ tree generated
generated
├── v10.4.0
│   ├── grpc-endpoints.json
│   └── http-endpoints.json
├── v11.0.0
│   ├── grpc-endpoints.json
│   └── http-endpoints.json
└── v9.5.0
    ├── grpc-endpoints.json
    └── http-endpoints.json
```
