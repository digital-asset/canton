import BuildCommon.*
import Dependencies.*
import coursierapi.shaded.commonscompress.archivers.ArchiveOutputStream
import sbt.Def
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.{MergeStrategy, PathList}
import better.files.{File => BetterFile, _}

import scala.language.postfixOps
import scala.util.Try
import org.apache.commons.compress.archivers.tar.*

BuildCommon.sbtSettings
Release.releaseChecksSettings

addCommandAlias(
  "packageDocsWithExistingRelease",
  "; licenseFileMappings; docs-open/makeSiteFull; docs/makeSite",
)
addCommandAlias("packageDocs", "; package; packageDocsWithExistingRelease")
addCommandAlias("packRelease", "; bundle")
addCommandAlias("package", "; packRelease; unidoc")
addCommandAlias("makeOpenDocs", "; compile; community-app/bundle; docs-open/makeSiteFull")

addCommandAlias(
  "pingTest",
  "community-app/testOnly com.digitalasset.canton.integration.tests.SimplestPingIntegrationTestH2",
)

/*
 Data continuity tests
 */
addCommandAlias(
  "generateDataContinuityDumps",
  "; community-app/testOnly com.digitalasset.canton.integration.tests.manual.CreateBasicDataContinuityDumps*" +
    "; community-app/testOnly com.digitalasset.canton.integration.tests.manual.CreateSynchronizerChangeDataContinuityDumps*" +
    "; checkErrors ",
)

addCommandAlias(
  "testDataContinuityDumps",
  "; community-app/testOnly com.digitalasset.canton.integration.tests.manual.BasicDataContinuityTest*" +
    "; community-app/testOnly com.digitalasset.canton.integration.tests.manual.SynchronizerChangeDataContinuityTest*" +
    "; checkErrors ",
)

lazy val testManyTimesCommand =
  Command.args(
    "testManyTimes",
    "<runCount> <testQualifier>",
    Help.briefDetail(
      Seq(
        "testManyTimes" -> "Runs tests N times unless they fail based on a qualifier (useful for flakes)."
      )
    ),
  ) { (state, args) =>
    if (args.size < 1)
      throw new IllegalArgumentException(
        s"Wrong number of arguments (${args.size}), should be at least 1"
      )
    val runCount = Try(args.head.toInt).getOrElse(
      throw new IllegalArgumentException(
        s"Invalid argument for `runCount`: '${args.head}' should be an integer"
      )
    )

    // Should follow whatever `testOnly` can take
    val testQualifier = args.drop(1).mkString(" ")

    // Remove the log directory first so that we only have the last logs
    List.fill(runCount)(s"truncateLogs; testOnly $testQualifier; checkErrors") ::: state
  }

lazy val licenseFileMappings =
  taskKey[Seq[(File, String)]](
    "files containing license information to be used in mappings settings"
  )

inThisBuild(
  List(
    licenseFileMappings := {
      (`community-app` / dumpLicenseReport).value
      val thirdPartyReport = s"${(`community-app` / licenseReportTitle).value}.html"
      Seq(
        // primary license for canton
        (file("LICENSE.txt"), "LICENSE.txt"),
        // aggregated license details for our dependencies
        ((`community-app` / target).value / "license-reports" / thirdPartyReport, thirdPartyReport),
      )
    },
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
  )
)

// Deps for protobuf files that are explicitly extracted into target directories
// See the `target/protobuf_external` dirs in buf.work.yaml
lazy val protobufExternalDeps = Seq(
  DamlProjects.`google-common-protos-scala` / Compile / compile
)

val generateNewProtobufSnapshot =
  taskKey[Unit](
    "Generates a snapshot of our protobuf definitions used for Protobuf continuity checks"
  )
generateNewProtobufSnapshot := {
  val log = streams.value.log
  runCommand("buf build -o .proto_snapshot_image.bin.gz", log)
  // Also generate the root namespace buf image
  generateNewRootNamespaceBufImage.value
}

val generateNewRootNamespaceBufImage = taskKey[Unit](
  "Generates a buf image to be used in scripts for initialization of a node with an offline root namespace key"
)
generateNewRootNamespaceBufImage := {
  val log = streams.value.log
  // Create a lighter buf image for offline root key scripts
  val requiredTypes = List(
    "com.digitalasset.canton.protocol.v30.TopologyTransaction",
    "com.digitalasset.canton.version.v1.UntypedVersionedMessage",
    "com.digitalasset.canton.protocol.v30.SignedTopologyTransaction",
    "com.digitalasset.canton.crypto.v30.SigningPublicKey",
  )
  val imagePath =
    "community" / "app" / "src" / "pack" / "scripts" / "offline-root-key" / "root_namespace_buf_image.json.gz"
  runCommand(
    s"buf build ${requiredTypes.mkString("--type=", " --type=", "")} -o ${imagePath.pathAsString}",
    log,
  )
}
generateNewProtobufSnapshot := generateNewProtobufSnapshot.dependsOn(protobufExternalDeps*).value

val protobufContinuityCheck =
  taskKey[Unit](
    "Checks that any Protobuf file changes don't break Protobuf continuity. Uses CLI tool Buf."
  )
protobufContinuityCheck := {
  val log = streams.value.log
  val error =
    "A breaking change in the protobuf definitions was detected. If this is expected, run 'sbt generateNewProtobufSnapshot'. Further info can be found in 'Protobuf continuity tests' in contributing/README.md."
  runCommand("scripts/ci/buf-checks.sh .proto_snapshot_image.bin.gz", log, Some(error))
  log.info("Protobuf continuity check passed!")
}
protobufContinuityCheck := protobufContinuityCheck.dependsOn(protobufExternalDeps*).value

def writeClasspathToFile(raw: Seq[Classpath], filename: String): Unit = {
  val cp = raw.flatten.distinct
  val fw = new java.io.FileWriter(filename)
  fw.write(cp.map(_.data.toString).mkString("-cp ", ":", ""))
  fw.close()
}

lazy val appScopeFilter =
  ScopeFilter(inProjects(`community-app`), inConfigurations(Runtime))
lazy val dumpClassPath = taskKey[Unit]("Dump the app classpath parameter to classpath.txt")
dumpClassPath := {
  val log = streams.value.log
  val cp = fullClasspath.all(appScopeFilter).value
  val filename = "classpath.txt"
  log.info(s"Writing app classpath to `$filename`")
  writeClasspathToFile(cp, filename)
}

lazy val testScopeFilter =
  ScopeFilter(inAnyProject, inConfigurations(Test))
lazy val dumpTestClassPath =
  taskKey[Unit]("Dump the test classpath parameter to test.classpath.txt")
dumpTestClassPath := {
  val log = streams.value.log
  val cp = fullClasspath.all(testScopeFilter).value
  val filename = "test.classpath.txt"
  log.info(s"Writing test classpath to `$filename`")
  writeClasspathToFile(cp, filename)
}

// See https://stackoverflow.com/a/21151000
lazy val showVersion = taskKey[Unit]("Show the current version of the project")
showVersion := {
  println(version.value)
}

lazy val showJavaVersion = taskKey[Unit]("Show the java version")
showJavaVersion := {
  println(System.getProperty("java.version"))
}

lazy val testLibraries = Seq(
  `community-testing`,
  `community-integration-testing`,
)

lazy val ledgerTestToolLibaries = Seq(
  `ledger-common-dars-lf-v2-dev`,
  `ledger-test-tool-suites-2-dev`,
  `ledger-test-tool-2-dev`,
)

lazy val transcodeLibraries = Seq(
  `transcode-schema`,
  `transcode-daml-lf`,
  `transcode-codec-json`,
  `transcode-codec-proto-java`,
  `transcode-codec-proto-scala`,
  `transcode-daml-examples`,
  `transcode-test-conformance`,
  `transcode-test-utils`,
)

/*
 * Root project
 */
lazy val root = (project in file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(BufPlugin, GcsPlugin, WartRemover)
  .aggregate(
    (Seq[sbt.Project](
      docs,
      `docs-open`,
    ) ++ CommunityProjects.allProjects ++ DamlProjects.allProjects).map(_.project): _*
  )
  .settings(
    commands += testManyTimesCommand,
    scalacOptions ++= Seq("-Wconf:src=src_managed/.*:silent", "-Ytasty-reader"),
    scalacOptions --= HouseRules.scalacOptionsToDisableForTests, // To build test libraries in `compile` scope
    ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject -- inProjects(
      (
        Seq(CommunityProjects.`performance-driver`)
          ++ testLibraries
          ++ DamlProjects.allProjects
          ++ ledgerTestToolLibaries
          ++ transcodeLibraries // Cannot run scaladoc 2.13 on transcode because written in Scala 3
      ).map(_.project): _*
    ),
    // guava-android.jar does not have the same API surface as guava-jre.jar (eg ValueGraph.edgeValue is missing).
    // Alas, guava-android.jar seems to be listed first on the classpath causing compile errors, and I haven't
    // figured out how to completely remove -android.jar from the declared dependencies, because of the fact that
    // guava uses -android and -jre as classifiers, which one cannot easily exclude.
    ScalaUnidoc / unidoc / fullClasspath := (ScalaUnidoc / unidoc / fullClasspath).value
      .filterNot(_.data.name.endsWith("-android.jar")),
    addArtifact(jsonApiDocsArtifact, packageJsonApiDocsArtifacts),
  )

val sphinxSettings: Seq[Def.Setting[_]] = Seq(
  Sphinx / sphinxEnv := sys.env.filterKeys(_ == "PYTHONPATH")
)

val docsBuild = DocsOpenBuild // alias
lazy val `docs-open` = project
  .settings(
    sharedCantonCommunitySettings,
    sphinxSettings,
    // Run the Sphinx build on the preprocessed directory where snippet data has been processed
    Sphinx / sourceDirectory := target.value / "preprocessed-sphinx",
    logManager := docsBuild.sphinxLogManager(false),
    addFilesToHeaderCheck("*.rst", "../sphinx", Compile),
    addFilesToHeaderCheck("*.py", "../sphinx", Compile),
    Test / test := (Test / test).dependsOn(docsBuild.runPythonTests()).value,
    Sphinx / sphinxIncremental := true,
    docsBuild.pythonCommand := docsBuild.findPythonCommand().value,
    docsBuild.allRstFilesTask := docsBuild.findAllRstInPreprocessed().value,
    docsBuild.rstToTestMapTask := docsBuild.buildRstToTestMap(`community-app`).value,
    docsBuild.syncSphinxSources := docsBuild.syncSources().value,
    docsBuild.changedRstFiles := docsBuild.filterRstFiles(docsBuild.syncSphinxSources).value,
    docsBuild.checkDocErrors := docsBuild.findSphinxLogIssues().value,
    docsBuild.makeSiteFull := docsBuild.checkDocErrors
      .dependsOn(makeSite)
      .dependsOn(
        Def.sequential(
          docsBuild.reset,
          docsBuild.generate,
          docsBuild.resolve,
        )
      )
      .value,
    docsBuild.reset := {
      docsBuild.resetGeneratedSnippets().value
      docsBuild.resetGeneratedIncludes().value
      docsBuild.resetPreprocessed().value
    },
    docsBuild.generate := {
      docsBuild.generateSphinxSnippets.value
      docsBuild.generateIncludes.value
    },
    docsBuild.generateSphinxSnippets := docsBuild
      .generateSphinxSnippets(`community-app`, docsBuild.rstToTestMapTask)
      .value,
    docsBuild.generateIncludes := docsBuild.generateIncludes(docsBuild.generateReferenceJson).value,
    docsBuild.generateReferenceJson := {
      docsBuild.generateReferenceJson(
        docsBuild.embed_reference_json,
        `community-app` / sourceDirectory,
        `community-app` / target,
        `community-integration-testing` / sourceDirectory,
      )
    }.value,
    docsBuild.embed_reference_json := (Compile / resourceManaged).value / "json" / "embed_reference.json",
    // resolve expects that preprocessed-sphinx has been reset (resetPreprocessed)
    docsBuild.resolve := Def
      .sequential(
        docsBuild.resolveSnippets(),
        docsBuild.resolveLiteralIncludes(),
        docsBuild.resolveIncludes(),
        docsBuild.resolveLedgerApiProtoDocs(),
      )
      .value,
    watchTriggers += sourceDirectory.value.toGlob / "sphinx" / ** / "*.rst",
    docsBuild.rebuild := Def
      .sequential(
        docsBuild.resolveIncludes(docsBuild.changedRstFiles),
        docsBuild.resolveLiteralIncludes(docsBuild.changedRstFiles),
        docsBuild.resolveSnippets(docsBuild.changedRstFiles),
        makeSite,
      )
      .value,
    docsBuild.rebuildSnippets := docsBuild.rebuild
      .dependsOn(
        docsBuild.generateSphinxSnippets(
          `community-app`,
          docsBuild.rstToTestMapTask,
          docsBuild.changedRstFiles,
        )
      )
      .value,
    docsBuild.rebuildGeneratedIncludes := makeSite
      .dependsOn(
        Def
          .sequential(
            `community-app` / bundle,
            docsBuild.resetGeneratedIncludes(),
            docsBuild.generateIncludes,
            docsBuild.touchGeneratedIncludeRstFiles(),
            docsBuild.rebuild,
          )
      )
      .value,
  )
  .enablePlugins(SphinxPlugin)

lazy val docs = project
  .enablePlugins(SphinxPlugin)
  .settings(
    sharedCantonSettings,
    sphinxSettings,
  )

lazy val truncateTestLogs = taskKey[Unit]("Truncate testing log before build")
truncateTestLogs := {
  IO.delete(file("log/canton_test.log"))
}

lazy val truncateLogs = taskKey[Unit]("Truncate ./log folder")
truncateLogs := {
  IO.delete(file("log"))
}

lazy val checkErrors = taskKey[Unit]("Check test log for errors and fail if there is one")
checkErrors := {
  import scala.sys.process._
  val res =
    Seq("scripts/ci/check-logs.sh", "log/canton_test.log", "project/errors-in-log-to-ignore.txt").!
  if (res != 0) {
    sys.error("canton_test.log contains problems.")
  }
}

// Generation/packaging of openapi yaml documentation
val packageJsonApiDocsArtifacts =
  taskKey[File]("Generate openapi yamls and into a tgz file artifact")

packageJsonApiDocsArtifacts := {
  (`ledger-json-api` / Test / runMain)
    .toTask(" com.digitalasset.canton.http.json.GenerateJSONApiDocs")
    .value
  val resultsDir =
    (`ledger-json-api` / Test / resourceDirectory).value / "json-api-docs"
  val tgzFile =
    target.value / s"canton-json-apidocs-${version.value}.tar.gz"

  if (!resultsDir.exists()) {
    sys.error(s"$resultsDir does not exist!")
  }

  IO.gzipFileOut(tgzFile) { gzipOut =>
    val tar = new TarArchiveOutputStream(gzipOut)
    Path.allSubpaths(resultsDir).foreach { case (file, name) =>
      val nameWithoutSuffix = name.replace(".yaml", "")
      val entry = tar
        .createArchiveEntry(file, s"$nameWithoutSuffix-${version.value}.yaml")

      tar.putArchiveEntry(entry)
      java.nio.file.Files.copy(file.toPath, tar)
      tar.closeArchiveEntry()
    }
    tar.finish()
  }

  streams.value.log.info(s"Packaged $resultsDir into $tgzFile")

  tgzFile
}

lazy val jsonApiDocsArtifact = Artifact("canton-json-apidocs", "tar.gz", "tar.gz")

lazy val `community-app` = CommunityProjects.`community-app`
lazy val `community-app-base` = CommunityProjects.`community-app-base`
lazy val `community-base` = CommunityProjects.`community-base`
lazy val `community-common` = CommunityProjects.`community-common`
lazy val `community-synchronizer` = CommunityProjects.`community-synchronizer`
lazy val `community-participant` = CommunityProjects.`community-participant`
lazy val `community-admin-api` = CommunityProjects.`community-admin-api`
lazy val `community-testing` = CommunityProjects.`community-testing`
lazy val `community-integration-testing` = CommunityProjects.`community-integration-testing`
lazy val `community-integration-testing-lib` = CommunityProjects.`community-integration-testing-lib`
lazy val `daml-script-tests` = CommunityProjects.`daml-script-tests`
lazy val microbench = CommunityProjects.microbench
lazy val `performance-driver` = CommunityProjects.`performance-driver`
lazy val performance = CommunityProjects.performance
lazy val blake2b = CommunityProjects.blake2b
lazy val `slick-fork` = CommunityProjects.`slick-fork`
lazy val `magnolify-addon` = CommunityProjects.`magnolify-addon`
lazy val `scalatest-addon` = CommunityProjects.`scalatest-addon`
lazy val `util-external` = CommunityProjects.`util-external`
lazy val `util-observability` = CommunityProjects.`util-observability`
lazy val `sequencer-driver-api` = CommunityProjects.`sequencer-driver-api`
lazy val `sequencer-driver-api-conformance-tests` =
  CommunityProjects.`sequencer-driver-api-conformance-tests`
lazy val `sequencer-driver-lib` = CommunityProjects.`sequencer-driver-lib`
lazy val `reference-sequencer-driver` = CommunityProjects.`reference-sequencer-driver`
lazy val `wartremover-extension` = CommunityProjects.`wartremover-extension`
lazy val `wartremover-annotations` = CommunityProjects.`wartremover-annotations`
lazy val `google-common-protos-scala` = DamlProjects.`google-common-protos-scala`
lazy val `ledger-api-value` = DamlProjects.`ledger-api-value`
lazy val `ledger-api-proto` = DamlProjects.`ledger-api-proto`
lazy val `ledger-api-scala` = DamlProjects.`ledger-api-scala`
lazy val `bindings-java` = DamlProjects.`bindings-java`
lazy val `ledger-common` = CommunityProjects.`ledger-common`
lazy val `ledger-common-dars` = CommunityProjects.`ledger-common-dars`
lazy val `ledger-common-dars-lf-v2-dev` = CommunityProjects.`ledger-common-dars-lf-v2-dev`
lazy val `ledger-common-dars-lf-v2-1` = CommunityProjects.`ledger-common-dars-lf-v2-1`
lazy val `base-errors` = CommunityProjects.`base-errors`
lazy val `daml-jwt` = DamlProjects.`daml-jwt`
lazy val `daml-tls` = CommunityProjects.`daml-tls`
lazy val `dam-grpc-utils` = CommunityProjects.`daml-grpc-utils`
lazy val `daml-adjustable-clock` = CommunityProjects.`daml-adjustable-clock`
lazy val `kms-driver-api` = CommunityProjects.`kms-driver-api`
lazy val `kms-driver-testing` = CommunityProjects.`kms-driver-testing`
lazy val `kms-driver-testing-lib` = CommunityProjects.`kms-driver-testing-lib`
lazy val `aws-kms-driver` = CommunityProjects.`aws-kms-driver`
lazy val `mock-kms-driver` = CommunityProjects.`mock-kms-driver`
lazy val `transcode-schema` = CommunityProjects.`transcode-schema`
lazy val `transcode-daml-lf` = CommunityProjects.`transcode-daml-lf`
lazy val `transcode-codec-json` = CommunityProjects.`transcode-codec-json`
lazy val `transcode-codec-proto-java` = CommunityProjects.`transcode-codec-proto-java`
lazy val `transcode-codec-proto-scala` = CommunityProjects.`transcode-codec-proto-scala`
lazy val `transcode-daml-examples` = CommunityProjects.`transcode-daml-examples`
lazy val `transcode-test-conformance` = CommunityProjects.`transcode-test-conformance`
lazy val `transcode-test-utils` = CommunityProjects.`transcode-test-utils`
lazy val `ledger-api-core` = CommunityProjects.`ledger-api-core`
lazy val `ledger-json-api` = CommunityProjects.`ledger-json-api`
lazy val `ledger-json-client` = CommunityProjects.`ledger-json-client`
lazy val `ledger-api-tools` = CommunityProjects.`ledger-api-tools`
lazy val `ledger-api-string-interning-benchmark` =
  CommunityProjects.`ledger-api-string-interning-benchmark`
lazy val `ledger-api-bench-tool` = CommunityProjects.`ledger-api-bench-tool`
lazy val `ledger-test-tool-suites-2-1` = CommunityProjects.`ledger-test-tool-suites-2-1`
lazy val `ledger-test-tool-suites-2-dev` = CommunityProjects.`ledger-test-tool-suites-2-dev`
lazy val `ledger-test-tool-2-1` = CommunityProjects.`ledger-test-tool-2-1`
lazy val `ledger-test-tool-2-dev` = CommunityProjects.`ledger-test-tool-2-dev`
lazy val `conformance-testing` = CommunityProjects.`conformance-testing`
lazy val `upgrading-integration-tests` =
  CommunityProjects.`upgrading-integration-tests`

lazy val `scalatest-utils` = DamlProjects.`scalatest-utils`
lazy val `scala-utils` = DamlProjects.`scala-utils`
lazy val `nonempty` = DamlProjects.`nonempty`
lazy val `nonempty-cats` = DamlProjects.`nonempty-cats`
lazy val `rs-grpc-bridge` = DamlProjects.`rs-grpc-bridge`
lazy val `rs-grpc-pekko` = DamlProjects.`rs-grpc-pekko`
lazy val `logging-entries` = DamlProjects.`logging-entries`
lazy val `contextualized-logging` = DamlProjects.`contextualized-logging`
lazy val `daml-resources` = DamlProjects.`daml-resources`
lazy val `resources-pekko` = DamlProjects.`resources-pekko`
lazy val `resources-grpc` = DamlProjects.`resources-grpc`
lazy val `ledger-resources` = DamlProjects.`ledger-resources`
lazy val `ledger-resources-test-lib` = DamlProjects.`ledger-resources-test-lib`
lazy val `timer-utils` = DamlProjects.`timer-utils`
lazy val crypto = DamlProjects.crypto
lazy val nameof = DamlProjects.nameof
lazy val `testing-utils` = DamlProjects.`testing-utils`
lazy val `grpc-test-utils` = DamlProjects.`grpc-test-utils`
lazy val `rs-grpc-testing-utils` = DamlProjects.`rs-grpc-testing-utils`
lazy val `test-evidence-tag` = DamlProjects.`test-evidence-tag`
lazy val `test-evidence-generator` = DamlProjects.`test-evidence-generator`
lazy val `test-evidence-scalatest` = DamlProjects.`test-evidence-scalatest`
lazy val `sample-service` = DamlProjects.`sample-service`
lazy val `ports` = DamlProjects.`ports`
lazy val `http-test-utils` = DamlProjects.`http-test-utils`
lazy val `executors` = DamlProjects.`executors`
lazy val `observability-metrics` = DamlProjects.`observability-metrics`
lazy val `observability-tracing` = DamlProjects.`observability-tracing`
lazy val `daml-lf-data` = DamlProjects.`daml-lf-data`
lazy val `daml-lf-data-scalacheck` = DamlProjects.`daml-lf-data-scalacheck`
lazy val `daml-lf-data-tests` = DamlProjects.`daml-lf-data-tests`
lazy val `daml-lf-repl` = DamlProjects.`daml-lf-repl`
lazy val `daml-lf-ide-ledger` = DamlProjects.`daml-lf-ide-ledger`
lazy val `daml-lf-language` = DamlProjects.`daml-lf-language`
lazy val `daml-lf-transaction` = DamlProjects.`daml-lf-transaction`
lazy val `daml-lf-transaction-tests` = DamlProjects.`daml-lf-transaction-tests`
lazy val `daml-lf-transaction-test-lib` = DamlProjects.`daml-lf-transaction-test-lib`
lazy val `daml-lf-archive` = DamlProjects.`daml-lf-archive`
lazy val `daml-lf-stable-packages` = DamlProjects.`daml-lf-stable-packages`
lazy val `daml-lf-validation` = DamlProjects.`daml-lf-validation`
lazy val `daml-lf-snapshot-proto` = DamlProjects.`daml-lf-snapshot-proto`
lazy val `daml-lf-snapshot` = DamlProjects.`daml-lf-snapshot`
lazy val `daml-lf-interpreter` = DamlProjects.`daml-lf-interpreter`
lazy val `daml-lf-interpreter-bench` = DamlProjects.`daml-lf-interpreter-bench`
lazy val `daml-lf-engine` = DamlProjects.`daml-lf-engine`
lazy val `daml-lf-upgrades-matrix` = DamlProjects.`daml-lf-upgrades-matrix`
lazy val `daml-lf-parser` = DamlProjects.`daml-lf-parser`
lazy val `daml-lf-encoder` = DamlProjects.`daml-lf-encoder`
lazy val `daml-lf-archive-encoder` = DamlProjects.`daml-lf-archive-encoder`
lazy val `daml-lf-api-type-signature` = DamlProjects.`daml-lf-api-type-signature`
lazy val `daml-lf-tests` = DamlProjects.`daml-lf-tests`

Global / excludeLintKeys += `docs-open` / logManager
Global / excludeLintKeys += `root` / wartremoverErrors
Global / excludeLintKeys += `docs` / autoAPIMappings
Global / excludeLintKeys += `docs-open` / autoAPIMappings
