Building Canton
===============

This guide explains how to build and run Canton.

# Common sbt Commands

Compilation:
- `compile`: compile production code (excluding test code)
- `Test/compile`: compile production and test code
- `community-common/compile`: compile production code of the `community-common` subproject
- `scalafixAll`: invoke scalafix across all configurations where scalafix is enabled. It's a linting and rewrite tool that we use to organize imports
- `format`: formats, fixes imports (remove unused and order them) and creates license headers.

  Note that you need to `compile` before the first call to `format`.

Test:
- `testOnly myWildcard`: runs all tests matching wildcard, e.g.,
  `testOnly com.digitalasset.myPackage.*` runs all tests in package `com.digitalasset.myPackage`.
- `test`: runs all tests. Do not call this, as it will run for a very long time and consume huge amounts of memory.

Packaging:
- `bundle`: creates the community and enterprise release bundles
- `packRelease`: like `bundle` but delivers a working *Canton Demo*

Documentations:
- `unidoc`: creates ScalaDocs. Output is in `target/scala-2.13/unidoc/index.html`.
- `docs/makeSite`: build Sphinx documentation from `docs`. Output is in `docs/target/sphinx/html/index.html`.
- `makeOpenDocs`: build Sphinx documentation from `docs-open` ensuring that the minimal requirements are fulfilled.\
  &rarr; See also [docs-open/README.md](/docs-open/README.md) for further helpful commands.
- `packageDocs`: builds the release bundles and all documentation (ScalaDoc, docs, docs-open) from scratch.
  Runs consistency checks on docs-open.
  Slow, but will create all hyperlinks in docs-open.
- `packageDocsWithExistingRelease`: build Sphinx documentation from docs and docs-open.
  Runs consistency checks on docs-open.
  Faster than `packageDocs`, but assumes that `bundle` and `unidoc` have been called previously and their output is still up-to-date.

Further commands can be found in `build.sbt` and files referenced therein (such as `BuildCommon`).

# Running Canton

## From sbt

For development, you can simply run Canton from sbt:
```
community-app/run --no-tty --config community/app/src/pack/examples/01-simple-topology/simple-topology.conf
```
This approach allows for a short development cycle, but does sometime not work, because it is not tested by our CI.

## From the Release Bundle

To run the released Canton version from a terminal, first build it:
```
sbt bundle
```

Then:
```
cd community/app/target/release/canton
./bin/canton --config examples/01-simple-topology/simple-topology.conf
```
