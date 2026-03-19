# Canton XFER Test

The normal usage of the performance runner is to create "mean" workloads for the
purpose of testing, using an intentionally poorly written DvP workflow. In order to
test the horizontal scalability, we also support a second workflow, called the
"transfer" model.

The transfer mode performs `batched asset transfers`. For a large scale test we need
the following types pod instances:

- master pod (singleton):
  - runs one participant (with master party)
  - has remote console access to ALL NODES for the purpose initialisation:
  - bootstrap the synchronizers (as many as there are sequencer & mediator pairs)
  - connect the participants to all synchronizers
  - uploads the dars and vet the packages on all synchonizers
  - after node init, runs the “Master” role
  - reference config is in [xfer-master.conf](src/main/console/topology/xfer-master.conf)
  - bootstrap script is in [xfer-master.canton](src/main/console/xfer-master.canton)

- synchronizer pods (multiple)
  - runs one sequencer and one mediator
  - does not require any init script, as it will be initialised by the master pod
  - reference config is in [xfer-synchronizer.conf](src/main/console/topology/xfer-synchronizer.conf)
  - it doesn't need a bootstrap as it will be initialized via the master pod.

- participant pods (multiple)
  - runs N participant nodes in a single JVM (for efficiency reasons)
  - waits for master pod to connect the nodes and then starts the performance runner on the local nodes
  - the performance runner auto-registers itself to all locally present participant nodes
  - reference config is in [xfer-participants.conf](src/main/console/topology/xfer-participants.conf)
  - bootstrap script is in [xfer-participants.canton](src/main/console/xfer-participants.canton)


