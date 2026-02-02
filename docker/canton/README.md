# Canton base image

This builds the canton base image from the `splice` repo. It is not by itself useful.

# Pre-reqs

This script depends on the canton packaging step:
```
sbt community-app/bundle
```

# To use
To use, run
```
cd ~/canton
sbt community-app/bundle
cd docker/canton
./build_canton_image.sh false
```

This will build `canton-base:local`, `canton-mediator:local`, `canton-participant:local`, and `canton-sequencer:local`.

We're suppling "false" to denote that it's not a nightly build.



