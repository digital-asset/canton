# Canton XFER Test

## Building and pushing images

To build images locally and push to the da-images/playground test registry, run the following command:

```shell
 (cd performance/docker && CIRCLECI=true OCI_REGISTRY=europe-docker.pkg.dev ./build_perf_image.sh)
```

## Overview

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

## Testing on Vcluster

The test harness is on [vcluster](https://github.com/DACH-NY/vclusters/tree/main/deployment/infra/perf).
Make sure to check the readme.

You need to be on the VPN to access the vcluster and you can test your connectivity by checking if you see the cluster running.

```
kubectl get pods -n perf-test
```

### Building the Image

You need to build the performance runner image and push it to the test registry.
If you run against a stable release, then you can use the normal images (or nightly release image).
Adjust the `FROM` in the [docker/images/canton-perf-base/Dockerfile](canton-perf-base Dockerfile) and
then run
```
CIRCLECI=true OCI_REGISTRY=europe-docker.pkg.dev ./build_perf_image.sh
```
If you need to build it against a custom Docker snapshot, run the [build_canton_image_playground.sh](../docker/canton/build_canton_image_playground.sh),
and then update the `FROM` in the perf base image.

###
helmfile destroy -n perf-test
## Testing Locally

Build the performance tar, unpack it and then run first to setup the db:
```
cd performance/postgres/
../../config/utils/postgres/db.sh reset
```
and then start the process:
```
./bin/canton -c performance/postgres/persistence.conf -c performance/topology/xfer-all.conf --bootstrap performance/xfer-master.canton
```
Note that you need to copy also the line from `xfer-participants.canton` to the `xfer-master.canton` file, as that
script usually runs separately.
