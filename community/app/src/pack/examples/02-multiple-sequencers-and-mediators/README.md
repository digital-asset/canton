# Topology example with multiple Sequencers and Mediators

The topology example features two Sequencers and two Mediators; it also starts two Participants named `participant1`
and `participant2` and the Synchronizer named `da` in a single process. The synchronizer runs in the BFT mode.

How to run the example is featured in the [getting started tutorial](
https://docs.canton.network/global-synchronizer/canton-console/getting-started-tutorial).

The `simple-ping.canton` file contains a set of Canton console commands used to connect the Participants and
test the connection.

This topology example can be bootstrapped using

```
       ../../bin/canton -c multiple-sequencers-and-mediators.conf --bootstrap simple-ping.canton
```
