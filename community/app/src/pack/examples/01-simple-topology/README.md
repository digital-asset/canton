# Simple Topology Example

The simple topology example features a simple setup, starting two participants named `participant1`
and `participant2`, and a synchronizer named `da` in a single process.

## Preparation
- build canton: `sbt compile`
- build the release bundle: `sbt bundle`
- navigate to the example dir: `cd <code_repo_root>/community/app/target/release/canton/examples/01-simple-topology`

How to run the example is featured in the [getting started tutorial](
https://docs.canton.network/global-synchronizer/canton-console/getting-started-tutorial).

The second file contains a set of Canton console commands that are run in order to connect the participants together
and test the connection.

The simple topology example can be invoked using

```
       ../../bin/canton -c simple-topology.conf --bootstrap simple-ping.canton
```
