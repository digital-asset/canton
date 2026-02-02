Common use cases:
=================

Run nightly performance test: `./nightly-performance-test.sh`
Run (quick) regression test: `./regression-test.sh`

To run tests on a non-main branch: `./nightly-performance-test.sh my-branch`

To setup environment so that the test can be run from cron:
`/home/canton/automated-performance-tests/from-cron.sh nightly-tests-output ./nightly-performance-test.sh`
Output will be written to `nightly-tests-output/`.

Important Remarks:
==================

The deployment scripts reside in two locations:
1) $REPOSITORY_ROOT/scripts/canton-testing/deployment - so they are backed up. If you want to change them, change this version.

2) $REPOSITORY_ROOT/.. - as symlinks, so they "live" outside of the repository and the generated state/output will survive a `git clean`.
   If you want to run them, run this version.

The tests will abort if a java process is running. Make sure to terminate all java processes when you are done.

Don't change files in the `canton/` subfolder, as the test scripts will reset all your changes.

The `lock/` subfolder (if it exists) indicates that a test is running and holding a write lock on the `canton/` subfolder.
Don't mess with this file, unless you know what you are doing.

The `last_rev` and `failures` files store state information used by `./regression-test.sh`.
Don't mess with these files, unless you know what you are doing.

