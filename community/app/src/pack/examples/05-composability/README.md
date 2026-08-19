# Composability Example

The composability example features a workflow that spans across two synchronizers.
It starts two synchronizers and five participants in a single process.
The details of multi-synchronizer workflows are described in
- [Multi-Synchronizer Architecture](
https://docs.canton.network/overview/learn/multi-synchronizer#multi-synchronizer-architecture)
- [Reassignment Protocol](https://docs.canton.network/overview/reference/reassignment-protocol)
- [Cross-Synchronizer DvP Example](https://docs.canton.network/overview/reference/cross-sync-dvp-example)

## Preparation
- build canton: `sbt compile`
- build the release bundle: `sbt bundle`
- navigate to the example dir: `cd <code_repo_root>/community/app/target/release/canton`

## Running the Examples
The composability examples can be invoked from the root directory of the Canton release using

```
./bin/canton -c examples/05-composability/composability.conf \
--bootstrap examples/05-composability/composability1.canton
./bin/canton -c examples/05-composability/composability.conf \
--bootstrap examples/05-composability/composability2.canton
```

It can be run from other directories if the path to the CantonExamples.dar file in the examples folder
is set as the system property canton-examples.dar-path:

```
./bin/canton -Dcanton-examples.dar-path=<path-to-dar-file> -c examples/05-composability/composability.conf \
--bootstrap examples/05-composability/composability1.canton
```
