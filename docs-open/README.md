Overview
========

This README explains how to write and build the public Canton documentation (locally). Ultimately, this Canton
documentation gets replicated to the [docs-website repository][docs_website_repo], where it becomes part of
the [Digital Asset documentation website][da_docs]. Please also familiarize yourself with that integration and its
peculiarities, as well as how to do local replication; see the [docs-website README][docs_website_readme].

Building documentation means running the [Sphinx documentation generator][sphinx_link] to
convert [reStructuredText][rst_link] (RST) files and other resources into a desired output like HTML.

RST itself is a [plaintext markup language][rst_primer_link] for creating technical documentation, and like Sphinx, it
originates from
the [Python][python_link] ecosystem; consequently, **whitespace carries** structural **meaning** within **your RST files
**.

### Communication

Prefer these Slack channels

- `#team-canton`
- `#product-docs` – for anything docs

### Structure

The rest of this README consists of three parts:

1. **[How can I do X?](#how-can-i-do-x)** – Steps to follow without much explanation
2. **[How does Y work (and why)?](#how-does-y-work-and-why)** – Explains the why and how
3. **[Troubleshooting](#troubleshooting)**

---

How can I do X?
===============

_Steps to follow without much explanation._

## Write

### How can I write RST files in general?

[reStructuredText Primer][rst_primer_link]

### How can I write a `literalinclude` directive?

```
.. literalinclude:: CANTON/community/common/src/main/scala/com/digitalasset/canton/config/ConfigDefaults.scala
   :language: scala
   :start-after: user-manual-entry-begin: ConfigDefaults
   :end-before: user-manual-entry-end: ConfigDefaults
   :dedent:
```

Where notably

- `CANTON` is going to be replaced with Canton repository root directory.
  (You can still use a relative path from the RST file that contains the `literalinclude` directive; but such a path may
  break when the RST file is moved to a different directory).
- `:start-after: <your-marker>-begin` and `:end-before: <your-marker>-end` extract a snippet from the file defined by
  these markers found within that file.
- `:dedent:` removes any left indented whitespace.

⚠️ We preprocess the Canton RST files and replace any `literalinclude` directive with a `code-block` directive.
And so, we only support a subset of `literalinclude` directive options, that are: `:language:`, `:start-after:`,
`:end-before:`, `:dedent:`, `:pyobject:`, `:append:`, and `:caption:`.

### How can I write a `snippet` directive?

1. Read (!) the Scaladoc for the `SphinxDocumentationGenerator.scala` abstract class.
2. And then implement an integration test which extends `SnippetGenerator` (an abstract class found in
   `SphinxDocumentationGenerator.scala` file)

### How can I add a TODO?

Use the `.. todo::` directive. For example:

```rst
.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
```

### How can I mark a document as "work in progress"?

Use the `.. wip::` directive.

This is ideal for page-level to-dos or to add a general WIP disclaimer.

For example:

```rst
.. wip::
    Find a better title.
```

## Build

### Where is the build implemented?

- Find `docs-open` SBT project in the [build.sbt](/../build.sbt).

### How can I build the canton documentation locally?

- `sbt makeOpenDocs` – builds the docs from scratch; use it either for a freshly cloned or `sbt clean`ed canton
  repository
- `sbt makeSiteFull` - assuming that the canton code has been compiled and the canton enterprise version has been
  packaged

### How can I build the canton documentation locally for a fast feedback cycle?

- Run the `sbt` console
- `makeOpenDocs` (ensure you've built the documentation once)
- Run one of the below **_rebuild_** SBT tasks after you've changed the RST source file(s)

To quickly update the Sphinx HTML output, use:

1. `rebuild`: Rebuilds for textual edits to RST source files.
2. `rebuildSnippets`: Rebuilds for modified snippet directives within RST files
3. `rebuildGeneratedIncludes`: Rebuilds for changed documentation content within the
   source code (like command references, error codes, metrics, etc.)

Refresh your browser pointing to the locally built Canton documentation HTML.

Note: Above rebuild tasks are ordered by increasing processing time (seconds to minutes).

Note: You can run `~rebuild` to trigger a rebuild whenever an RST file is changed in `docs-open/src/sphinx`.

### How can I check for Sphinx build issues?

Run the `checkDocErrors` task after some task which produces the `log/docs-open.sphinx.log`.

For example:

```
sbt rebuild checkDocErrors
```

Note: The Sphinx build task `makeSiteFull` runs the task `checkDocErrors`.

### How can I fix a failing `build_docs` CI job?

Additionally, to actual errors, any (*) Sphinx build warnings are considered errors and break the build. So a fix
depends on the logged issue.

1. First, reproduce the problem locally
   [Build the Canton Documentation locally](#how-can-i-build-the-canton-documentation-locally) because the `build_docs`
   job executes that build on CircleCI.
2. Check and fix any error or warnings

Note: See the Sphinx build configuration file [`conf.py`](src/sphinx/conf.py) for intentionally suppressed warnings (see
also [suppress_warnings](https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-suppress_warnings)).

### How to verify working links after replication Canton docs?

Before merging your Canton docs PR, it's crucial to confirm that your changes integrate seamlessly with
the [docs-website repository][docs_website_repo]. Specifically, you'll want to ensure all links are functional.

1. Replicate Canton to the docs-website locally.\
   Follow the instructions in the [docs-website README][docs_website_readme].
2. Do a clean, full build of the docs-website locally. Use the following command:

```
./scripts/build.sh --full-build --fresh-env
```

## Python

### How can I develop Python code?

Use [PyCharm](https://www.jetbrains.com/pycharm/).

> &#9432;&nbsp; Note that it is currently impossible to add a Python interpreter to IntelliJ IDEA due to a circle module
> dependency within the canton repository.\
> However, that will not stop you from opening PyCharm, and creating a "Python project" just over the directory
> containing the Python scripts.

### How can I debug the Sphinx build?

See [this tutorial](https://akrabat.com/step-debugging-sphinx-build-in-pycharm/).

You will likely want to add a _run configuration_ for a command similar to this:

```
sphinx-build -a -E -N -b html -d /PATH-TO-CANTON/canton/docs-open/target/sphinx/doctrees/html -Dembed_reference_json=/PATH-TO-CANTON/canton/docs-open/target/scala-2.12/resource_managed/main/json/embed_reference.json -Ddocs_test_outputs=/PATH-TO-CANTON/canton/docs-open/target/scala-2.12/resource_managed/main/console_scripts -Dscalatest_version=3.2.2 /PATH-TO-CANTON/canton/docs-open/src/sphinx /PATH-TO-CANTON/canton/docs-open/target/sphinx/html
```

Replace `/PATH-TO-CANTON` with the absolute path to your local Canton repository.

## Vale

### How to run Vale locally

[Vale][vale_official] is a customizable linter for grammar, style, and spelling, helping to enforce consistent writing
standards. Run Vale to address warnings, suggestions, and errors before creating a PR to ensure adherence to
our [official style guide][da_style_guide] and [terminology guidelines][terminology_guide] and to **expedite the PR
approval process.**

Vale is made available via Nix. To check that Vale is active, run:

```bash
vale --version
```

To see all the suggestions, warnings, and errors in the terminal, run `vale path/to/your/file.rst` at the root of the
project directory. For instance, run:

```bash
vale docs-open/src/sphinx/participant/howtos/optimize/performance.rst
```

To see only errors when running Vale, use the `--minAlertLevel` flag. For instance, run:

```bash
vale --minAlertLevel=error docs-open/src/sphinx/participant/howtos/optimize/performance.rst
```

For more information, refer to the official [Vale doc][vale_doc].

---

How does Y work (and why)?
==========================

_Explains the why and how._

### Why do we replace the `literalinclude` directive with code blocks?

The Canton RST files get replicated to the [docs-website repository][docs_website_repo] and its content needs to be
resolvable by docs-website Sphinx build.

Thus, our RST files cannot contain `literalinclude` directives which point to some files within the
[Canton repository][canton_repo_link] as they cannot be resolved later in the
[docs-website repository][docs_website_repo].

### How does generated content like Console Commands and the `generatedinclude` directive work?

A Python script generates content using data from the repository and RST markup:

- the Canton Console Command reference &rarr; `console.rst.inc`
- the Canton Error Codes reference &rarr; `error_codes.rst.inc`
- the Sequencer Metrics &rarr; `sequencer_metrics.rst.inc`
- the Mediator Metrics &rarr; `mediator_metrics.rst.inc`
- the Participant Metrics &rarr; `participant_metrics.rst.inc`
- the Canton Versioning Table &rarr; `versioning.rst.inc`
- ...

> &#9432;&nbsp; These files are _intended_ to be included through our custom `generatedinclude` directive like
> ```
> ..
>    Dynamically generated content:
> .. generatedinclude:: console.rst.inc
> ```
> Note that, as opposed to the `literalinclude` directive, RST markup in a file included with the `generatedinclude`
> directive will be interpreted.

The content is generated with `sbt generateIncludes`, and then processed by `sbt resolve`.
The include file generation is implemented in the Python script `include_generator.py`. Running
`sbt makeSiteFull` or `sbt rebuildGeneratedIncludes` invokes above tasks and the Python script.

If the generated content inclusion fails during the Sphinx build, the following error will be logged:

```
ERROR: Unknown directive type "generatedinclude"
```

This means the aforementioned SBT task implementation was unable to replace the `.. generatedinclude::` directive with
the generated content.

There are several possible causes:

1. Failed include file generation &rarr; ensure the include `.inc` file exists in `docs-open/target/generated`
2. Missing `.inc` file &rarr; check its generation in the `include_generator.py` Python script
3. Outdated/wrong static mapping from the include file to RST file &rarr; check
   `DocsOpenBuild#incToRstRelativePathMapping`
4. Bugged generated include resolution &rarr; check `DocsOpenBuild#resolveIncludes`
5. ...

> &#9432;&nbsp; The generation of the Canton Console Command and Error Codes depends on scaladoc because it
> includes links to it. Note however, that these scaladoc links are only created when
> - the scaladoc exists, i.e. `sbt unidoc` has been run successfully
> - the Python script is on run on CircleCI (checks for `CI` environment variable being set)

### How does the `snippet` directive work?

The `snippet`s cannot be processed by the [Sphinx documentation generator][sphinx_link] directly because they are our
custom directive. Consequently, any `snippet` directive needs to be replaced by an RST [code block][code_block_link]
directive before running the [Sphinx documentation generator][sphinx_link].

A `snippet` directive contains executable code. Running `sbt generateSphinxSnippets` invokes the integration test
`SphinxDocumentationGenerator.scala` (more precisely its subclasses) which executes the `snippet` code as a Scalatest,
and then collects the test results into a JSON file.

> &#9432;&nbsp; There is a subclass and corresponding JSON file per RST file which contains `snippet` directives.

The Python script

```
snippet_directive.py
```

generates the RST code block directives from the snippet output data, and then replaces the `snippet` directives with
them in the RST files. This script runs as a preprocessing step.

> &#9432;&nbsp; When building the [Canton Documentation locally](#how-can-i-build-the-canton-documentation-locally),
> `sbt generateRstResolvesSnippets` also invokes above script!

### Snippet "Macros"

The `community/app/src/test/scala/com/digitalasset/canton/integration/tests/docs/macros` folder contains RST files that can be re-used in a "macro" style in any other RST file.
To include a macro file, use the following syntax (example):

```
.. snippet(allocateExternalParty):: offline_party_replication
```

where `allocateExternalParty` is the name of the file in the macro folder to be included, minus the `.rst.macro` suffix. In this case there must be a file called `allocateExternalParty.rst.macro` in the macro folder.
`offline_party_replication` is the usual "scenario name" attached to snippets to group them together in a logical unit.

The content of the macro file will be run as if it was directly declared in the RST file.

### How are the scaladoc links built for the generated Canton Console Commands and Error Codes?

Links are created in the Python script `rst-preprocessor.py` when generating the Canton Console Commands and Error
Codes.

This however, the creation and addition of the scaladoc links, only happens when the documentation is built on CircleCI.

The reason for it is that building the scaladoc takes several minutes (around 7 minutes) which would slow down a local
canton documentation build considerably, thus it is deferred to the CircleCI build (`build_scaladoc` and `build_docs`
jobs).

---

Troubleshooting
===============

## Canton documentation build

To investigate a failure of the docs build, it is useful to change the log level of sphinx:
```bash
sbt
project docs-open
set logLevel := Level.Debug
```

### Problem: `sbt docs-open / makeSiteFull`  or `sbt docs-open / makeSite` is stuck when running the build locally

There's a chance that the build gets stuck at this step (running the `sbt generateSphinxSnippets` task)

```
[info] [generateSphinxSnippets] Running custom `.. snippet::` directives through tests to collect their output as JSON ...
[info] RUNNING com.digitalasset.canton.integration.tests.docs.TutorialStepTest
[info] RUNNING com.digitalasset.canton.integration.tests.docs.GettingStartedDocumentationIntegrationTest
[info] RUNNING com.digitalasset.canton.integration.tests.docs.ManagePermissionedSynchronizersDocumentationManual
[info] RUNNING com.digitalasset.canton.integration.tests.docs.DistributedSynchronizerInstallationManual
[info] RUNNING com.digitalasset.canton.integration.tests.docs.SequencerConnectivityDocumentationIntegrationTest
[info] RUNNING com.digitalasset.canton.integration.tests.docs.PackageDarManagementDocumentationIntegrationTest
[info] RUNNING com.digitalasset.canton.integration.tests.docs.UpgradingDocumentationIntegrationTest
[info] RUNNING com.digitalasset.canton.integration.tests.docs.IdentityManagementCookbook
[info] RUNNING com.digitalasset.canton.integration.tests.docs.PocDocumentationIntegrationTest
[info] Running 9 tests in project docs-open...


  | => com.digitalasset.canton.integration.tests.docs.SequencerConnectivityDocumentationIntegrationTest 76s
  | => com.digitalasset.canton.integration.tests.docs.IdentityManagementCookbook 76s
  | => com.digitalasset.canton.integration.tests.docs.UpgradingDocumentationIntegrationTest 76s
```

_Stuck_ means that for example `IdentityManagementCookbook` just keeps counting up seconds (beyond 110 seconds on my
machine).

It is also possible to get stuck at this step (running the `sbt generateReferenceJson` task):

```
[info] [generateReferenceJson] Generating the JSON used to populate the documentation for console commands, metrics, etc. ...
[info] Sphinx documentation generated: /Users/*****/Code/canton/docs/target/sphinx/docs

  | => docs-open / generateReferenceJson 10s
```

_Stuck_ means that `generateReferenceJson` just keeps counting up seconds (beyond 12 seconds on my machine).

If that happens

* Quit the sbt console (CTRL-C)
* Delete all docker containers: `docker ps | xargs docker rm -f`
* Start the sbt console again
* If you ran `sbt docs-open / makeSite`, try running `sbt docs-open / makeSiteFull` instead

### Problem: sbt crashes with "Out of metaspace" error message

Running the sbt console and executing `docs-open / makeSiteFull` repeatedly, crashes sbt with the "out of metaspace"
error eventually. (Sbt leaks classloaders when running tests, tests are being run to generate the snippet directive
output).

If that happens

* Start the sbt console again

Or configure the sbt console to fork the test execution

```
set `community-app` / Test / fork := true ;
set `community-app` / testForkedParallel := true ;
set `community-app` / Test / baseDirectory := (ThisBuild / Test / run / baseDirectory).value
```

[canton_repo_link]: https://github.com/DACH-NY/canton

[code_block_link]: https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-code-block

[da_docs]: https://docs.digitalasset.com/

[da_style_guide]: https://docs.google.com/document/d/153D6-gI0blsAvlrzReHVyz8RCHUQzzdKnxnpcADlt6c/edit?pli=1&tab=t.0#heading=h.im66lq8ybndj

[docs_website_readme]: https://github.com/DACH-NY/docs-website/blob/main/README.md

[docs_website_repo]: https://github.com/DACH-NY/docs-website

[literal_include_link]: https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-literalinclude

[python_link]: https://www.python.org/

[rst_link]: https://docutils.sourceforge.io/rst.html

[rst_primer_link]: https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html

[sphinx_link]: https://www.sphinx-doc.org/en/master/

[terminology_guide]: https://docs.google.com/document/d/1r1MogDIHN68TosxHYufnSzqEIL8lGYnFuwWhXVEgTAQ/edit?tab=t.0#heading=h.f3wg43q4j3as

[vale_doc]: https://vale.sh/docs

[vale_official]: https://vale.sh/
