
# Note, this script is used to build the canton images to be used
# for the performance tests.

export OCI_SNAPSHOT_DIR="/da-images/playground/docker/"
export OCI_REGISTRY="europe-docker.pkg.dev"
export PUBLISH_IMAGES="true"
export RELEASE_SUFFIX="3.7.0-snapshot-$(date -u +%F).$(git rev-parse --short HEAD)"
./build_canton_image.sh "${RELEASE_SUFFIX}"

