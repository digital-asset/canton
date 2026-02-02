# How to run this test

This test is run automatically as part of the CI pipeline. However, to run it locally, run from root:

```
cd docker/canton
./build_canton_image.sh false
```

This script builds the image the test requires, and then runs the test. After that has been run, you can run `run_test.sh enterprise` from `docker/canton/tests/ping`.
