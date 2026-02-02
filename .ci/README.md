This directory contains the main Docker image used to run Canton's continuous integration tests.


## Pushing updates to the Dockerfile

After any changes are made to the [Dockerfile](./Dockerfile), a new version will
need to be published to Digital Asset's `canton-build` repository on docker hub.
This is done through running the `manual_update_canton_build_docker_image` on CI.

`manual_update_canton_build_docker_image` runs the `update-build-container.sh` script which will:
 - Hash the content of the `Dockerfile` 
 - Use these values to create a label for the new `canton-build` docker container
   - This allows us to push this image to Dockerhub without disrupting builds of other branches
   - Otherwise if we pushed `latest` and all branches were using this, suddenly all builds would use this container despite our changes not being reviewed or merged
 - Build the new `canton-build` container supplying the Daml SDK version as a build arg so the image and `sbt` project will have a matching version
 - Push the new image to [Dockerhub](https://cloud.docker.com/u/digitalasset/repository/docker/digitalasset/canton-build)
 - Update the [CircleCI config](../.circleci/config.yml) on this branch to use the newly created `canton-build` image by updating the tag
