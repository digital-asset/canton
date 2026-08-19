# BFT sequencer example

## Preparation
- build canton: `sbt compile`
- build the release bundle: `sbt bundle`
- navigate to the example dir: `cd <code_repo_root>/community/app/target/release/canton/examples/11-bft-sequencer`

## Examples

### One sequencer example
- topology: one sequencer, one mediator, two participants
- `minimal-ping.canton` bootstraps the BFT synchronizer, connects two participants and runs a ping
- run `../../bin/canton -c minimal.conf --bootstrap minimal-ping.canton`

### Two sequencers example
- topology: two sequencers, two mediators, two participants
- `two-peers-ping.canton` bootstraps the BFT synchronizer, connects two participants and runs a ping between them
- run `../../bin/canton -c two-peers.conf --bootstrap two-peers-ping.canton`
