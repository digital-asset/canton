import java.io.File
import BufPlugin.autoImport.bufLintCheck
import DamlPlugin.autoImport.*
import Dependencies.{daml_lf_language, *}
import better.files.{File as BetterFile, *}
import com.lightbend.sbt.JavaFormatterPlugin
import sbtlicensereport.SbtLicenseReport.autoImportImpl.*
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{headerResources, headerSources}
import org.scalafmt.sbt.ScalafmtPlugin
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys.*
import sbt.Tests.{Group, SubProcess}
import sbt.{File, *}
import sbt.internal.util.ManagedLogger
import sbt.nio.Keys.*
import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.{CustomMergeStrategy, MergeStrategy, PathList}
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin.autoImport.*
import sbtide.Keys.ideExcludedDirectories
import sbtprotoc.ProtocPlugin.autoImport.{AsProtocPlugin, PB}
import scalafix.sbt.ScalafixPlugin
import scoverage.ScoverageKeys.*
import wartremover.WartRemover
import wartremover.WartRemover.autoImport.*

import java.nio.file.StandardOpenOption
import scala.collection.compat.toOptionCompanionExtension
import scala.language.postfixOps

object BuildCommon {

  lazy val sbtSettings: Seq[Setting[_]] = {

    def alsoTest(taskName: String) = s";$taskName; Test / $taskName"

    val commandAliases =
      addCommandAlias("checkDamlProjectVersions", alsoTest("damlCheckProjectVersions")) ++
        addCommandAlias("updateDamlProjectVersions", alsoTest("damlUpdateProjectVersions")) ++
        addCommandAlias("checkLicenseHeaders", alsoTest("headerCheck")) ++
        addCommandAlias("createLicenseHeaders", alsoTest("headerCreate")) ++
        addCommandAlias(
          "lint",
          "; bufFormatCheck ; bufLintCheck ; bufWrapperValueCheck ; scalafmtCheck ; Test / scalafmtCheck ; scalafmtSbtCheck; checkLicenseHeaders; javafmtCheck; damlCheckProjectVersions",
        ) ++
        addCommandAlias(
          "scalafixCheck",
          s"${alsoTest("scalafix --check")}",
        ) ++
        addCommandAlias(
          "format",
          // `bufLintCheck` and `bufWrapperValueCheck` violations cannot be fixed automatically -- they're here to make sure violations are caught before pushing to CI
          "; bufFormat ; bufLintCheck ; bufWrapperValueCheck ; scalafixAll ; scalafmtAll ; scalafmtSbt; createLicenseHeaders ; javafmtAll",
        ) ++
        // To be used by CI:
        // enable coverage and compile
        addCommandAlias("compileWithCoverage", "; clean; coverage; Test/compile") ++
        // collect coverage information (once the tests have terminated)
        addCommandAlias("collectCoverage", "; coverageReport; coverageAggregate") ++
        // To be used locally
        // test coverage from just unit tests
        addCommandAlias("unitTestCoverage", "; compileWithCoverage; unitTest; collectCoverage")

    val buildSettings = inThisBuild(
      Seq(
        organization := "com.digitalasset.canton",
        scalaVersion := scala_version,
        resolvers := resolvers.value ++ Option.when(Dependencies.use_custom_daml_version)(
          sbt.librarymanagement.Resolver.mavenLocal // conditionally enable local maven repo for custom Daml jars
        ),
        ideExcludedDirectories := Seq(
          baseDirectory.value / "target"
        ),
        // scalacOptions += "-Ystatistics", // re-enable if you need to debug compile times
        // scalacOptions in Test += "-Ystatistics",
        // TODO (i20606) We should find versions of libraries that do not need this workaround
        libraryDependencySchemes += "io.circe" %% "circe-parser" % VersionScheme.Always,
        libraryDependencySchemes += "io.circe" %% "circe-yaml" % VersionScheme.Always,
        /*
        The default JDK is the latest stable (21 in August 2025) but we target the previous LTS
        for backwards compatibility (see `contributing/runtime-versions.md`).
         */
        javacOptions ++= Seq("--release", "17"),
        javacOptions ++= Seq("-proc:full"),
      )
    )

    import CommunityProjects._

    val globalSettings = Seq(
      name := "canton",
      // Reload on build changes
      Global / onChangedBuildSource := ReloadOnSourceChanges,
      // allow setting number of tasks via environment
      Global / concurrentRestrictions ++=
        // run assembly tasks in total isolation, because they have high memory usage
        Seq(Tags.exclusive(Assembly.assemblyTag)) ++
          // allow setting number of concurrent test tasks via environment
          maxConcurrentSbtTestTasks,
      //  Global / concurrentRestrictions += Tags.limitAll(1), // re-enable if you want to serialize compilation (to not mess up the Ystatistics output)
      Global / excludeLintKeys += Compile / damlBuildOrder,
      Global / excludeLintKeys += `community-app` / Compile / damlCompileDirectory,
      Global / excludeLintKeys += `community-app` / Compile / damlDarLfVersion,
      Global / excludeLintKeys += `community-app` / Compile / useVersionedDarName,
      Global / excludeLintKeys ++= (CommunityProjects.allProjects ++ DamlProjects.allProjects)
        .map(
          _ / autoAPIMappings
        ),
      Global / excludeLintKeys += Global / damlCodeGeneration,
      Global / excludeLintKeys ++= DamlProjects.allProjects.map(_ / wartremoverErrors),
      Global / excludeLintKeys += Compile / ideExcludedDirectories,
      Global / excludeLintKeys += Test / ideExcludedDirectories,
    )

    buildSettings ++ globalSettings ++ commandAliases
  }

  def mkTestJob(
      filter: String => Boolean,
      candidates: TaskKey[Seq[TestDefinition]] = (Test / definedTests),
      scope: InputKey[Unit] = (Test / testOnly),
      verbose: Boolean = false,
  ): Def.Initialize[Task[Unit]] = Def.taskDyn {
    val log = streams.value.log
    val selectedTestNames = candidates.value
      .map(_.name)
      .filter { x =>
        val res = filter(x)
        if (verbose) {
          if (res) {
            log.info(s"RUNNING $x")
          } else {
            //          log.info(s"IGNORING ${x}")
          }
        }
        res
      }
    if (selectedTestNames.isEmpty) {
      log.info(s"No tests to run for project ${name.value}.")
      Def.task(())
    } else {
      log.info(s"Running ${selectedTestNames.size} tests in project ${name.value}...")
      scope.toTask(
        selectedTestNames.mkString(
          " ",
          " ",
          " -- -l UnstableTest",
        )
      )
    }
  }

  lazy val unitTestTask = {
    val unitTest = taskKey[Unit]("Run all unit tests.")
    unitTest := mkTestJob(n => !n.startsWith("com.digitalasset.canton.integration.tests")).value
  }

  def runProcess(
      process: scala.sys.process.ProcessBuilder,
      commandAsString: String,
      log: ManagedLogger,
      optError: Option[String] = None,
  ): String = {
    import scala.sys.process.Process
    val processLogger = new DamlPlugin.BufferedLogger
    log.debug(s"Running $commandAsString")
    val exitCode = process ! processLogger
    val output = processLogger.output()
    if (exitCode != 0) {
      val errorMsg =
        s"A problem occurred when executing command `$commandAsString` in `build.sbt`: ${System
            .lineSeparator()} $output"
      log.error(errorMsg)
      if (optError.isDefined) log.error(optError.getOrElse(""))
      throw new IllegalStateException(errorMsg)
    }
    if (output != "") log.info(processLogger.output())
    output
  }

  def runCommand(command: String, log: ManagedLogger, optError: Option[String] = None): String =
    runProcess(scala.sys.process.Process(command), command, log, optError)

  def runSeqCommand(
      command: Seq[String],
      log: ManagedLogger,
      optError: Option[String] = None,
  ): String =
    runProcess(scala.sys.process.Process(command), command.mkString(" "), log, optError)

  private def maxConcurrentSbtTestTasks: Option[Tags.Rule] =
    sys.env.get("MAX_CONCURRENT_SBT_TEST_TASKS").map(v => Tags.limit(Tags.Test, v.toInt))

  private def formatCoverageExcludes(excludes: String): String =
    excludes.stripMargin.trim.split(System.lineSeparator).mkString(";")

  private def packDamlSources(damlSource: File, target: String): Seq[(File, String)] =
    // take all files except hidden and items under the `.daml` build directory
    // support nested folder structures in case that is ever used in our samples
    ((damlSource ** "*") --- (damlSource * ".daml" ** "*"))
      .filter { f =>
        f.isFile && !f.isHidden
      }
      .get
      .map { f =>
        (f, s"$target${Path.sep}${IO.relativize(damlSource, f).get}")
      }

  private def packDars(darOutput: File, target: String): Seq[(File, String)] =
    (darOutput * "*.dar").get
      .map { f =>
        (f, s"$target${Path.sep}${f.getName}")
      }

  private def packProtobufSourceFiles(BaseFile: BetterFile, target: String): Seq[(File, String)] = {
    val path = BaseFile / "src" / "main" / "protobuf"
    val pathJ = path.toJava
    (pathJ ** "*").get
      .filter { f =>
        f.isFile && !f.isHidden
      }
      .map { f =>
        (f, Seq("protobuf", target, IO.relativize(pathJ, f).get).mkString(s"${Path.sep}"))
      }
  }

  private def packProtobufDependencyFiles(path: BetterFile, target: String): Seq[(File, String)] = {
    val pathJ = path.toJava
    val protoFiles = if (pathJ.isDirectory) (pathJ ** "*").get else Seq(pathJ)
    protoFiles
      .filter { f =>
        f.isFile && !f.isHidden
      }
      .map { f =>
        (f, Seq("protobuf", target, IO.relativize(pathJ, f).get).mkString(s"${Path.sep}"))
      }
  }

  private def packOpenapiFiles(BaseFile: BetterFile, target: String): Seq[(File, String)] = {
    val path = BaseFile / "src" / "test" / "resources" / "json-api-docs"
    val pathJ = path.toJava
    (pathJ ** "*").get
      .filter { f =>
        f.isFile && !f.isHidden
      }
      .map { f =>
        (f, Seq("openapi", target, IO.relativize(pathJ, f).get).mkString(s"${Path.sep}"))
      }
  }

  // Originally https://tanin.nanakorn.com/technical/2018/09/10/parallelise-tests-in-sbt-on-circle-ci.html
  lazy val printTestTask = {
    val destination = "test-full-class-names.log"
    val printTests =
      taskKey[Unit](s"Print full class names of tests to the file `$destination`.")
    printTests := {
      val testNames = (Test / definedTestNames).value.toVector
      val projectName = thisProjectRef.value.project
      IO.writeLines(file(destination), testNames, append = true)
      println(s"Printed ${testNames.size} tests for `$projectName`")
    }
  }

  lazy val bundlePack = settingKey[Seq[String]]("Which pack directories / files to include")

  lazy val enterpriseGeneratedPack = "release/tmp/pack"

  lazy val additionalBundleSources =
    taskKey[Seq[(File, String)]]("Bundle these additional sources")

  lazy val bundle = taskKey[Unit]("create a release bundle")

  lazy val bundleTask: Def.Initialize[Task[String]] = Def
    .task {
      import CommunityProjects.`community-common`

      val log = streams.value.log
      dumpLicenseReport.value
      val thirdPartyReport = s"${licenseReportTitle.value}.html"
      val licenseFiles = Seq(
        // aggregated license details for our dependencies
        (target.value / "license-reports" / thirdPartyReport, thirdPartyReport)
      )
      log.info("Copying over compiled files")
      // include daml source files (as we as project file) for users to build our daml samples themselves
      val damlSampleSource = {
        val damlSource = (`community-common` / Compile / damlSourceDirectory).value
        packDamlSources(damlSource, "daml")
      }
      // include packaged sample DARs
      val damlSampleDars = {
        // depend on the daml samples being built
        (`community-common` / Compile / damlBuild).value
        val darOutput = (`community-common` / Compile / damlDarOutput).value
        packDars(darOutput, "dars")
      }
      // here, we copy the demo artefacts manually over into the packaged artefact
      // this way we can avoid to add the dependency to javaFX which is only required
      // for the demo
      // also, we hard code paths and can't use sbt references, as this would create a circular dependency
      // between app and demo.
      val demoArtefacts = {
        val path = new File(Seq("community", "demo", "src", "pack").mkString(s"${Path.sep}"))
        (path ** "*").get
          .filter { f =>
            f.isFile && !f.isHidden
          }
          .get
          .map { f =>
            (f, IO.relativize(path, f).get)
          }
      }
      // manually copy the demo jars into the release artefact
      val demoJars = {
        val path =
          Seq("community", "demo", "target", s"scala-$scala_version_short").mkString(s"${Path.sep}")
        (new File(path) * "*.jar").get.map { f =>
          (f, s"demo${Path.sep}lib${Path.sep}${f.getName}")
        }
      }
      // manually copy the demo dars into the release artefact
      val demoDars = {
        val path = Seq(
          "community",
          "demo",
          "target",
          s"scala-$scala_version_short",
          "resource_managed",
          "main",
        )
          .mkString(s"${Path.sep}")
        packDars(new File(path), s"demo${Path.sep}dars")
      }
      // manually copy the demo source daml code into the release artefact
      val demoSource = {
        val path = Seq("community", "demo", "src", "main", "daml").mkString(s"${Path.sep}")
        packDamlSources(new File(path), s"demo${Path.sep}daml")
      }
      if (bundlePack.value.contains(enterpriseGeneratedPack)) {
        log.info("Adding version info to demo files")
        runCommand(f"bash ./release/add-release-version.sh ${version.value}", log)
      }

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

      val releaseNotes: Seq[(File, String)] = {
        val sourceFile: File =
          file(s"release-notes/${version.value}.md")
        if (sourceFile.exists())
          Seq((sourceFile, "RELEASE-NOTES.md"))
        else
          Seq()
      }
      //  here, we copy the protobuf files of community manually
      val ledgerApiProto: Seq[(File, String)] = packProtobufSourceFiles(
        "community" / "ledger-api",
        "ledger-api",
      )
      val communityBaseProto: Seq[(File, String)] = packProtobufSourceFiles(
        "community" / "base",
        "community",
      )
      val communityParticipantProto: Seq[(File, String)] = packProtobufSourceFiles(
        "community" / "participant",
        "participant",
      )
      val communityAdminProto: Seq[(File, String)] = packProtobufSourceFiles(
        "community" / "admin-api",
        "admin-api",
      )
      val communitySynchronizerProto: Seq[(File, String)] = packProtobufSourceFiles(
        "community" / "synchronizer",
        "synchronizer",
      )

      val communityJsonApiOpenapi: Seq[(File, String)] = packOpenapiFiles(
        "community" / "ledger" / "ledger-json-api",
        "json-ledger-api",
      )

      val commonGoogleProtosRoot =
        (DamlProjects.`google-common-protos-scala` / target).value / "protobuf_external"
      val scalapbProto: Seq[(File, String)] = packProtobufDependencyFiles(
        commonGoogleProtosRoot.toString / "scalapb",
        "lib/scalapb",
      )
      val googleRpcProtos: Seq[(File, String)] = packProtobufDependencyFiles(
        commonGoogleProtosRoot.toString / "google" / "rpc",
        "lib/google/rpc",
      )

      val ledgerApiValueProtosRoot =
        (DamlProjects.`ledger-api-value` / target).value / "protobuf_external"
      val ledgerApiValueProto: Seq[(File, String)] = packProtobufDependencyFiles(
        BetterFile(ledgerApiValueProtosRoot.toString),
        "ledger-api",
      )

      val apiFiles =
        ledgerApiProto ++ communityBaseProto ++ communityParticipantProto ++ communityAdminProto ++ communitySynchronizerProto ++
          scalapbProto ++ googleRpcProtos ++ ledgerApiValueProto ++ communityJsonApiOpenapi

      log.info("Invoking bundle generator")
      // add license to package
      val renames =
        releaseNotes ++ licenseFiles ++ demoSource ++ demoDars ++ demoJars ++ demoArtefacts ++ damlSampleSource ++ damlSampleDars ++ apiFiles
      val args =
        bundlePack.value ++ renames.flatMap(x => Seq("-r", x._1.toString, x._2))
      // build the canton fat-jar
      val assembleJar = assembly.value
      runCommand(
        f"bash ./scripts/ci/create-bundle.sh ${assembleJar.toString} ${(assembly / mainClass).value.get} ${args
            .mkString(" ")}",
        log,
      )
    }

  def mergeStrategy(oldStrategy: String => MergeStrategy): String => MergeStrategy = {
    case PathList("LICENSE") => MergeStrategy.last
    case PathList("buf.yaml") => MergeStrategy.discard
    case PathList("scala", "tools", "nsc", "doc", "html", _*) => MergeStrategy.discard
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case "reflect.properties" => MergeStrategy.first
    case PathList("org", "checkerframework", _ @_*) => MergeStrategy.first
    case PathList("google", _*) => MergeStrategy.first
    case PathList("com", "google", _*) => MergeStrategy.first
    case PathList("io", "grpc", _*) => MergeStrategy.first
    case PathList("org", "apache", "logging", _*) => MergeStrategy.first
    case PathList("ch", "qos", "logback", _*) => MergeStrategy.first
    case PathList("com", "daml", "ledger", "api", "v1", "package.proto") => MergeStrategy.first
    case PathList(
          "META-INF",
          "org",
          "apache",
          "logging",
          "log4j",
          "core",
          "config",
          "plugins",
          "Log4j2Plugins.dat",
        ) =>
      MergeStrategy.first
    // TODO(#10617) remove when no longer needed
    case (PathList("org", "apache", "pekko", "stream", "scaladsl", broadcasthub, _*))
        if broadcasthub.startsWith("BroadcastHub") =>
      MergeStrategy.first
    case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
    case path if path.contains("module-info.class") => MergeStrategy.discard
    case PathList("org", "jline", _ @_*) => MergeStrategy.first
    case PathList("META-INF", "FastDoubleParser-LICENSE") => MergeStrategy.first
    case PathList("META-INF", "FastDoubleParser-NOTICE") => MergeStrategy.first
    // complains about okio.kotlin_module clash
    case PathList("META-INF", "okio.kotlin_module") => MergeStrategy.last
    case path if path.endsWith("/OSGI-INF/MANIFEST.MF") => MergeStrategy.first
    case x => oldStrategy(x)
  }

  // applies to all sub-projects
  lazy val sharedSettings = Seq(
    printTestTask,
    unitTestTask,
    ignoreScalacOptionsWithPathsInIncrementalCompilation,
    ideExcludedDirectories += target.value,
    scalacOptions += "-Wconf:src=src_managed/.*:silent", // Ignore warnings in generated code
  )

  lazy val sharedCommunitySettings = sharedSettings ++ JvmRulesPlugin.damlRepoHeaderSettings

  lazy val cantonWarts = {
    val prefix = "com.digitalasset.canton."
    Seq(
      wartremover.WartRemover.dependsOnLocalProjectWarts(CommunityProjects.`wartremover-extension`),
      // wartremover-extension needs to load the mirrors of those deps at compile-time.
      // They are flagged as provided because the target project may not need them at all.
      libraryDependencies ++= Seq(
        cats % Provided,
        grpc_stub % Provided,
        scalapb_runtime_grpc % Provided,
      ),
      // DirectGrpcServiceInvocation prevents direct invocation of gRPC services through a stub, but this is often useful in tests
      Compile / compile / wartremoverErrors += Wart.custom(
        s"${prefix}DirectGrpcServiceInvocation"
      ),
      Compile / compile / wartremoverErrors += Wart.custom(s"${prefix}EnforceVisibleForTesting"),
      wartremoverErrors += Wart.custom(s"${prefix}DiscardedFuture"),
      wartremoverErrors += Wart.custom(s"${prefix}FutureAndThen"),
      wartremoverErrors += Wart.custom(s"${prefix}FutureTraverse"),
      wartremoverErrors += Wart.custom(s"${prefix}GlobalExecutionContext"),
      // NonUnitForEach is too aggressive for integration tests where we often ignore the result of console commands
      Compile / compile / wartremoverErrors += Wart.custom(s"${prefix}NonUnitForEach"),
      Compile / compile / wartremoverErrors += Wart.custom(s"${prefix}RequireBlocking"),
      // In tests, we often serialize protos directly
      Compile / compile / wartremoverErrors += Wart.custom(s"${prefix}ProtobufToByteString"),
      wartremoverErrors += Wart.custom(s"${prefix}SynchronizedFuture"),
      wartremoverErrors += Wart.custom(s"${prefix}TryFailed"),
    ).flatMap(_.settings)
  }

  // applies to all Canton-based sub-projects (descendants of util-external, excluding util-external itself)
  lazy val sharedCantonSettingsExternal: Seq[Def.Setting[_]] = sharedSettings ++ cantonWarts ++ Seq(
    // Ignore daml codegen generated files from code coverage
    coverageExcludedFiles := formatCoverageExcludes(
      """
        |<empty>
        |.*sbt-buildinfo.BuildInfo
        |.*daml-codegen.*
      """
    )
  )

  // applies to all Canton-based sub-projects (descendants of util-external)
  // this is split from sharedCantonSettingsExternal because util-external does not depend on community-testing
  // which contains the LogReporter
  lazy val sharedCantonSettings: Seq[Def.Setting[_]] = sharedCantonSettingsExternal ++ Seq(
    // Enable logging of begin and end of test cases, test suites, and test runs.
    Test / testOptions += Tests.Argument("-C", "com.digitalasset.canton.LogReporter")
  )

  lazy val sharedCantonCommunitySettings = Def.settings(
    sharedCantonSettings,
    JvmRulesPlugin.damlRepoHeaderSettings,
    Compile / bufLintCheck := (Compile / bufLintCheck)
      .dependsOn(DamlProjects.`google-common-protos-scala` / PB.unpackDependencies)
      .value,
  )

  // On circle-ci, between machine executors and dockers, some plugins have different paths
  // ex: -Xplugin:/root/.cache vs -Xplugin:/home/********/.cache/
  // which makes the cache invalid. To fix this, we ignore the scalacOptions that starts with -Xplugin:.* when
  // comparing scalacOptions between the cache and the current compilation.
  lazy val ignoreScalacOptionsWithPathsInIncrementalCompilation =
    incOptions := incOptions.value.withIgnoredScalacOptions(
      incOptions.value.ignoredScalacOptions() :+ "-Xplugin:.*"
    )

  // applies to all app sub-projects
  lazy val sharedAppSettings = sharedCantonSettings ++ Seq(
    bundle := bundleTask.value,
    licenseReportTitle := "third-party-licenses",
    licenseReportTypes := Seq(Html),
    assembly / test := {}, // don't run tests during assembly
    // when building the fat jar, we need to properly merge our artefacts
    assembly / assemblyMergeStrategy := mergeStrategy((assembly / assemblyMergeStrategy).value),
  )

  // which files to include into the release package
  lazy val sharedAppPack = Seq(
    "-l",
    "community/app/src/pack",
    "-c",
    "community/demo/src/pack",
    "-r",
    "README-release.md",
    "README.md",
    "-c",
    "LICENSE.txt",
    "-c",
    "canton.lnav.json",
    "-c",
    "canton-json.lnav.json",
  )

  /** By default, sbt-header (github.com/sbt/sbt-header) will not check the /protobuf directories,
    * so we manually need to add them here. Fix is similar to
    * https://github.com/sbt/sbt-header#sbt-boilerplate
    */
  def addProtobufFilesToHeaderCheck(conf: Configuration) =
    conf / headerSources ++= (conf / PB.protoSources).value.flatMap(pdir => (pdir ** "*.proto").get)

  def addFilesToHeaderCheck(filePattern: String, relativeDir: String, conf: Configuration) =
    conf / headerSources ++= (((conf / sourceDirectory).value / relativeDir) ** filePattern).get

  object CommunityProjects {

    lazy val allProjects = Set(
      `daml-grpc-utils`,
      `util-external`,
      `util-observability`,
      `community-admin-api`,
      `community-app`,
      `community-app-base`,
      `community-base`,
      `community-common`,
      `community-synchronizer`,
      `community-participant`,
      `community-testing`,
      `community-integration-testing`,
      `sequencer-driver-api`,
      `sequencer-driver-api-conformance-tests`,
      `sequencer-driver-lib`,
      `reference-sequencer-driver`,
      blake2b,
      `slick-fork`,
      `wartremover-extension`,
      `wartremover-annotations`,
      `pekko-fork`,
      `magnolify-addon`,
      `scalatest-addon`,
      `demo`,
      `base-errors`,
      `daml-adjustable-clock`,
      `daml-tls`,
      `kms-driver-api`,
      `kms-driver-testing`,
      `aws-kms-driver`,
      `mock-kms-driver`,
      `ledger-common`,
      `ledger-common-dars`,
      `ledger-common-dars-lf-v2-2`,
      `ledger-common-dars-lf-v2-dev`,
      `ledger-api-core`,
      `ledger-json-api`,
      `ledger-json-client`,
      `ledger-api-tools`,
      `ledger-api-string-interning-benchmark`,
      `transcode`,
      `conformance-testing`,
      `ledger-api-bench-tool`,
      `ledger-test-tool-suites-2-2`,
      `ledger-test-tool-suites-2-dev`,
      `ledger-test-tool-2-2`,
      `ledger-test-tool-2-dev`,
      `enterprise-upgrading-integration-tests`,
    )

    // Project for utilities that are also used outside of the Canton repo
    lazy val `util-external` = project
      .in(file("base/util-external"))
      .dependsOn(
        `base-errors`,
        `wartremover-annotations`,
      )
      .settings(
        sharedCantonSettingsExternal,
        libraryDependencies ++= Seq(
          cats,
          daml_non_empty,
          jul_to_slf4j % Test,
          mockito_scala % Test,
          monocle_macro, // Include it here, even if unused, so that it can be used everywhere
          pekko_actor,
          pekko_stream,
          pureconfig_core,
          pureconfig_generic,
          scala_collection_contrib,
          scalatest % Test,
          scalatestMockito % Test,
          shapeless,
          slick,
        ),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `daml-grpc-utils` = project
      .in(file("base/grpc-utils"))
      .dependsOn(
        DamlProjects.`google-common-protos-scala`
      )
      .settings(
        sharedCommunitySettings,
        libraryDependencies ++= Seq(
          grpc_api,
          scalapb_runtime_grpc,
          scalatest % Test,
        ),
      )

    lazy val `util-observability` = project
      .in(file("community/util-observability"))
      .dependsOn(
        `base-errors` % "compile->compile;test->test",
        `daml-grpc-utils`,
      )
      .settings(
        sharedCommunitySettings ++ cantonWarts,
        libraryDependencies ++= Seq(
          better_files,
          daml_lf_data,
          daml_libs_scala_contextualized_logging,
          daml_metrics,
          daml_non_empty,
          daml_tracing,
          logback_classic,
          logback_core,
          scala_logging,
          log4j_core,
          log4j_api,
          opentelemetry_api,
          opentelemetry_exporter_common,
          opentelemetry_exporter_otlp,
          opentelemetry_exporter_prometheus,
          opentelemetry_exporter_zipkin,
          opentelemetry_sdk,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        coverageEnabled := false,
      )

    lazy val `community-app` = project
      .in(file("community/app"))
      .dependsOn(
        `community-app-base` % "compile->compile;test->test",
        `community-common` % "compile->compile;test->test",
        `community-synchronizer` % "compile->compile;test->test",
        `community-integration-testing` % Test,
        `sequencer-driver-api-conformance-tests` % Test,
        `mock-kms-driver` % Test,
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedAppSettings,
        libraryDependencies ++= Seq(
          scala_logging,
          janino, // not used at compile time, but required for conditionals in logback configuration
          logstash, // not used at compile time, but required for the logback json encoder
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
          scopt,
          logback_classic,
          logback_core,
          pekko_stream_testkit % Test,
          pekko_http_testkit % Test,
          cats,
          better_files,
          monocle_macro,
          scala_logging,
        ),
        // core packaging commands
        bundlePack := sharedAppPack ++ Seq(
          "-r",
          "community/LICENSE-open-source-bundle.txt",
          "LICENSE.txt",
        ),
        additionalBundleSources := Seq.empty,
        assemblyMergeStrategy := {
          case "LICENSE-open-source-bundle.txt" => CustomMergeStrategy.rename(_ => "LICENSE-DA.txt")
          // this file comes in multiple flavors, from io.get-coursier:interface and from org.scala-lang.modules:scala-collection-compat. Since the content differs it is resolve this explicitly with this MergeStrategy.
          case path if path.endsWith("scala-collection-compat.properties") => MergeStrategy.first
          case x =>
            val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
            oldStrategy(x)
        },
        // See #23185: Prevent large string allocation during JMH fat-jar generation (prevent potential OOM errors)
        // by ensuring this task never runs in assembly plugin in debug mode.
        assembly / logLevel := Level.Info,
        assembly / mainClass := Some("com.digitalasset.canton.CantonCommunityApp"),
        assembly / assemblyJarName := s"canton-open-source-${version.value}.jar",
        // Explicit set the Daml project dependency to common
        Test / damlDependencies := (`community-common` / Compile / damlBuild).value :+ (`ledger-common` / Test / resourceDirectory).value / "test-models" / "model-tests-1.15.dar",
        Test / damlEnableJavaCodegen := true,
        Test / damlBuildOrder := Seq(
          "daml/JsonApiTest/Upgrades/Iface",
          "daml/JsonApiTest/Upgrades/V1",
          "daml/JsonApiTest/Upgrades/V2",
        ),
        Test / damlCodeGeneration := Seq(
          (
            (Test / sourceDirectory).value / "daml" / "CantonTest",
            (Test / damlDarOutput).value / "CantonTests-3.4.0.dar",
            "com.digitalasset.canton.damltests",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "CantonTestDev",
            (Test / damlDarOutput).value / "CantonTestsDev-3.4.0.dar",
            "com.digitalasset.canton.damltestsdev",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "CantonLfDev",
            (Test / damlDarOutput).value / "CantonLfDev-3.4.0.dar",
            "com.digitalasset.canton.lfdev",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "CantonLfV21",
            (Test / damlDarOutput).value / "CantonLfV21-3.4.0.dar",
            "com.digitalasset.canton.lfv21",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "Account",
            (Test / damlDarOutput).value / "Account-3.4.0.dar",
            "com.digitalasset.canton.http.json.tests.account",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "CIou",
            (Test / damlDarOutput).value / "CIou-3.4.0.dar",
            "com.digitalasset.canton.http.json.tests.ciou",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "User",
            (Test / damlDarOutput).value / "User-3.4.0.dar",
            "com.digitalasset.canton.http.json.tests.user",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "Upgrades" / "Iface",
            (Test / damlDarOutput).value / "ifoo-0.0.1.dar",
            "com.digitalasset.canton.http.json.tests.upgrades.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "Upgrades" / "V1",
            (Test / damlDarOutput).value / "foo-0.0.1.dar",
            "com.digitalasset.canton.http.json.tests.upgrades.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "Upgrades" / "V2",
            (Test / damlDarOutput).value / "foo-0.0.2.dar",
            "com.digitalasset.canton.http.json.tests.upgrades.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "JsonApiTest" / "Upgrades" / "IncompatibleV3",
            (Test / damlDarOutput).value / "foo-0.0.3.dar",
            "com.digitalasset.canton.http.json.tests.upgrades.v3",
          ),
        ),
        Test / useVersionedDarName := true,
        Test / damlEnableProjectVersionOverride := false,
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.sh", "../pack", Compile),
        addFilesToHeaderCheck("*.daml", "../test/daml", Compile),
        addFilesToHeaderCheck("*.sh", ".", Test),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-app-base` = project
      .in(file("community/app-base"))
      .dependsOn(
        `community-synchronizer`,
        `community-participant` % "compile->compile;test->test",
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          ammonite,
          circe_parser,
          jul_to_slf4j,
          pureconfig_cats,
        ),
      )

    // The purpose of this module is to collect `compile`-scoped classes shared by `community-testing` (which
    // is in turn meant to be imported at `test` scope) as well as other projects which need it in `compile`
    // scope. This is to avoid cyclic dependencies. As such, this module should _not_ depend on anything internal,
    // possibly with the exception of `util-external`.
    // In principle this might be merged into `util-external` at a later time, but this is separate for the time
    // being to ensure a clean separation of modules.
    lazy val `community-base` = project
      .in(file("community/base"))
      .enablePlugins(BuildInfoPlugin)
      .dependsOn(
        DamlProjects.`daml-jwt`,
        DamlProjects.`ledger-api`,
        `daml-tls`,
        `util-observability`,
        `community-admin-api`,
        `magnolify-addon` % "compile->compile",
        // No strictly internal dependencies on purpose so that this can be a foundational module and avoid circular dependencies
        `slick-fork`,
        `scalatest-addon` % "compile->test",
        `kms-driver-api`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          aws_kms,
          aws_sts,
          gcp_kms,
          grpc_netty_shaded,
          daml_executors,
          daml_lf_transaction,
          daml_nonempty_cats,
          daml_rs_grpc_bridge,
          daml_rs_grpc_pekko,
          better_files,
          bouncycastle_bcpkix,
          bouncycastle_bcprov,
          cats,
          chimney,
          chimneyJavaConversion,
          circe_core,
          circe_generic,
          commons_compress,
          flyway.excludeAll(ExclusionRule("org.apache.logging.log4j")),
          flyway_postgres,
          opentelemetry_instrumentation_hikari,
          postgres,
          pprint,
          scaffeine,
          slick_hikaricp,
          scalatest % "test",
          tink,
        ),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // Ensure the package scoped options will be picked up by sbt-protoc if used downstream
        // See https://scalapb.github.io/docs/customizations/#publishing-package-scoped-options
        Compile / packageBin / packageOptions +=
          Package.ManifestAttributes(
            "ScalaPB-Options-Proto" -> "com/digitalasset/canton/scalapb/package.proto"
          ),
        buildInfoKeys := Seq[BuildInfoKey](
          version,
          scalaVersion,
          sbtVersion,
          BuildInfoKey("damlLibrariesVersion" -> Dependencies.daml_libraries_version),
          BuildInfoKey("stableProtocolVersions" -> List("34")),
          BuildInfoKey("betaProtocolVersions" -> List()),
        ),
        buildInfoPackage := "com.digitalasset.canton.buildinfo",
        buildInfoObject := "BuildInfo",
        // excluded generated protobuf classes from code coverage
        coverageExcludedPackages := formatCoverageExcludes(
          """
            |<empty>
            |com\.digitalasset\.canton\.protocol\.v30\..*
      """
        ),
        addProtobufFilesToHeaderCheck(Compile),
        // Remove custom LogReporter, as it is missing from classpath
        // LogReport is defined in `community-testing` which depends on `community-base`
        Test / testOptions -= Tests.Argument("-C", "com.digitalasset.canton.LogReporter"),
      )

    lazy val `community-common` = project
      .in(file("community/common"))
      .enablePlugins(DamlPlugin)
      .dependsOn(
        blake2b,
        `pekko-fork` % "compile->compile;test->test",
        `community-base`,
        `wartremover-annotations`,
        `community-testing` % "test->test",
        `wartremover-extension` % "test->test",
        DamlProjects.`bindings-java`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          awaitility % Test,
          cats_scalacheck % Test,
          daml_lf_transaction_test_lib % Test,
          daml_lf_engine,
          daml_testing_utils % Test,
          grpc_inprocess % Test,
          h2,
          opentelemetry_instrumentation_grpc,
          opentelemetry_instrumentation_runtime_metrics,
          pekko_slf4j, // not used at compile time, but required by com.digitalasset.canton.util.PekkoUtil.createActorSystem
          pekko_http, // used for http health service
          slick,
          tink,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        Test / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Test / sourceManaged).value / "protobuf"
        ),
        Test / bufLintCheck := {}, // disable linting for protobuf files in tests
        Compile / damlEnableJavaCodegen := true,
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml" / "CantonExamples",
            (Compile / damlDarOutput).value / "CantonExamples.dar",
            "com.digitalasset.canton.examples",
          )
        ),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
      )

    lazy val `community-synchronizer` = project
      .in(file("community/synchronizer"))
      .dependsOn(
        `community-common` % "compile->compile;test->test",
        `reference-sequencer-driver`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          pekko_actor_typed,
          scala_logging,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
          dropwizard_metrics_core % Test,
          logback_classic % Runtime,
          logback_core % Runtime,
          scalapb_runtime, // not sufficient to include only through the `common` dependency - race conditions ensue
          scaffeine,
        ),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // excluded generated protobuf classes from code coverage
        coverageExcludedPackages := formatCoverageExcludes(
          """
            |<empty>
            |com\.digitalasset\.canton\.admin\.mediator\.v30\..*
            |com\.digitalasset\.canton\.admin\.sequencer\.v30\..*
            |com\.digitalasset\.canton\.admin\.synchronizer\.v30\..*
      """
        ),
        addProtobufFilesToHeaderCheck(Compile),
      )

    lazy val `community-participant` = project
      .in(file("community/participant"))
      .dependsOn(
        `community-common` % "test->test",
        `ledger-json-api` % "compile->compile;test->test",
        DamlProjects.`daml-jwt`,
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          cats,
          chimney,
          grpc_inprocess,
          daml_lf_encoder % Test,
          daml_lf_parser % Test,
          daml_lf_archive_encoder % Test,
          daml_test_evidence_generator_scalatest % Test,
          daml_test_evidence_tag % Test,
          logback_classic % Runtime,
          logback_core % Runtime,
          pekko_stream,
          pekko_stream_testkit % Test,
          scala_logging,
          scalacheck % Test,
          scalapb_runtime, // not sufficient to include only through the `common` dependency - race conditions ensue
          scalatest % Test,
          scalatestScalacheck % Test,
        ),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // Ensure the package scoped options will be picked up by sbt-protoc if used downstream
        // See https://scalapb.github.io/docs/customizations/#publishing-package-scoped-options
        Compile / packageBin / packageOptions += (
          Package.ManifestAttributes(
            "ScalaPB-Options-Proto" -> "com/digitalasset/canton/participant/scalapb/package.proto"
          )
        ),
        coverageExcludedPackages := formatCoverageExcludes(
          """
            |<empty>
            |com\.digitalasset\.canton\.participant\.admin\.v0\..*
            |com\.digitalasset\.canton\.participant\.protocol\.v0\..*
      """
        ),
        Compile / damlEnableJavaCodegen := true,
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml" / "canton-builtin-admin-workflow-ping",
            (Compile / resourceDirectory).value / "dar" / "canton-builtin-admin-workflow-ping.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
          (
            (Compile / sourceDirectory).value / "daml" / "canton-builtin-admin-workflow-party-replication-alpha",
            (Compile / resourceDirectory).value / "dar" / "canton-builtin-admin-workflow-party-replication-alpha.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
        ),
        Compile / damlDarOutput := (Compile / target).value / "dar-output",
        damlFixedDars := Seq(
          "canton-builtin-admin-workflow-ping.dar",
          "canton-builtin-admin-workflow-party-replication-alpha.dar",
        ),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
      )

    lazy val `community-admin-api` = project
      .in(file("community/admin-api"))
      .dependsOn(`util-external`, `base-errors` % "compile->compile;test->test")
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          scalapb_runtime // not sufficient to include only through the `common` dependency - race conditions ensue
        ),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // Ensure the package scoped options will be picked up by sbt-protoc if used downstream
        // See https://scalapb.github.io/docs/customizations/#publishing-package-scoped-options
        Compile / packageBin / packageOptions +=
          Package.ManifestAttributes(
            "ScalaPB-Options-Proto" -> "com/digitalasset/canton/admin/scalapb/package.proto"
          ),
        addProtobufFilesToHeaderCheck(Compile),
      )

    lazy val `community-testing` = project
      .in(file("community/testing"))
      .dependsOn(
        `community-base`,
        `magnolify-addon` % "compile->test",
      )
      .settings(
        sharedCommunitySettings,
        libraryDependencies ++= Seq(
          cats,
          cats_law,
          daml_metrics_test_lib,
          jul_to_slf4j,
          mockito_scala,
          scalatest,
          scalacheck,
          scalatestScalacheck,
          testcontainers,
          testcontainers_postgresql,
        ),

        // This library contains a lot of testing helpers that previously existing in testing scope
        // As such, in order to minimize the diff when creating this library, the same rules that
        // applied to `test` scope are used here. This can be reviewed in the future.
        scalacOptions --= JvmRulesPlugin.scalacOptionsToDisableForTests,
        Compile / compile / wartremoverErrors := JvmRulesPlugin.wartremoverErrorsForTestScope,
      )

    lazy val `community-integration-testing` = project
      .in(file("community/integration-testing"))
      .dependsOn(
        `community-app-base`,
        `community-testing`,
      )
      .settings(
        sharedCantonCommunitySettings,

        // The dependency override is needed because `community-testing` depends transitively on
        // `scalatest` and `community-app-base` depends transitively on `ammonite`, which in turn
        // depend on incompatible versions of `scala-xml` -- not ideal but only causes possible
        // runtime errors while testing and none have been found so far, so this should be fine for now
        dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
        libraryDependencies ++= Seq(
          testcontainers,
          testcontainers_postgresql,
          toxiproxy_java,
          opentelemetry_proto,
          circe_yaml,
          daml_http_test_utils,
        ),

        // This library contains a lot of testing helpers that previously existing in testing scope
        // As such, in order to minimize the diff when creating this library, the same rules that
        // applied to `test` scope are used here. This can be reviewed in the future.
        scalacOptions --= JvmRulesPlugin.scalacOptionsToDisableForTests,
        Compile / compile / wartremoverErrors := JvmRulesPlugin.wartremoverErrorsForTestScope,

        // TODO(i12761): package individual libraries instead of uber JARs for external consumption
        UberLibrary.assemblySettings("community-integration-testing-lib"),
        // when building the fat jar, we need to properly merge our artefacts
        assembly / assemblyMergeStrategy := mergeStrategy((assembly / assemblyMergeStrategy).value),
      )

    // TODO(i12761): package individual libraries instead of uber JARs for external consumption
    lazy val `community-integration-testing-lib` = project
      .settings(sharedCantonCommunitySettings)
      .settings(UberLibrary.of(`community-integration-testing`))
      .settings(
        Compile / packageDoc := {
          // TODO(i12766): producing an empty file because there are errors in running the `doc` task
          val destination = (Compile / packageDoc / artifactPath).value
          IO.touch(destination)
          destination
        },

        // The dependency override is needed because `community-testing` depends transitively on
        // `scalatest` and `community-app-base` depends transitively on `ammonite`, which in turn
        // depend on incompatible versions of `scala-xml` -- not ideal but only causes possible
        // runtime errors while testing and none have been found so far, so this should be fine for now
        dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
      )

    lazy val `kms-driver-api` = project
      .in(file("community/kms-driver-api"))
      // Disable wart-remover to not pull it in as a dependency. This project only provides API specs, no implementations.
      .disablePlugins(WartRemover)
      .settings(
        sharedCommunitySettings,
        libraryDependencies ++= Seq(
          pureconfig_core,
          slf4j_api,
          opentelemetry_api,
        ),
      )

    lazy val `kms-driver-testing` = project
      .in(file("community/kms-driver-testing"))
      .dependsOn(
        `kms-driver-api`,
        `community-testing`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          scalatest
        ),
        // TODO(i19491): Move to non-uber JAR
        UberLibrary.assemblySettings("kms-driver-testing-lib"),
        // when building the fat jar, we need to properly merge our artefacts
        assembly / assemblyMergeStrategy := mergeStrategy((assembly / assemblyMergeStrategy).value),
      )

    // TODO(i12761): package individual libraries instead of uber JARs for external consumption
    lazy val `kms-driver-testing-lib` = project
      .settings(sharedCantonCommunitySettings)
      .settings(UberLibrary.of(`kms-driver-testing`))
      .settings(
        // The dependency override is needed because `community-testing` depends transitively on
        // `scalatest` and `community-app-base` depends transitively on `ammonite`, which in turn
        // depend on incompatible versions of `scala-xml` -- not ideal but only causes possible
        // runtime errors while testing and none have been found so far, so this should be fine for now
        dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1"
      )

    lazy val `aws-kms-driver` = project
      .in(file("community/aws-kms-driver"))
      .dependsOn(
        `kms-driver-api`,
        `kms-driver-testing` % Test,
        `community-common` % "compile->compile;test->test",
        `wartremover-annotations`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          aws_kms,
          aws_sts,
        ),
        UberLibrary.assemblySettings("aws-kms-driver", includeDeps = true),
        // when building the fat jar, we need to properly merge our artefacts
        assembly / assemblyMergeStrategy := mergeStrategy((assembly / assemblyMergeStrategy).value),
      )

    lazy val `mock-kms-driver` = project
      .in(file("community/mock-kms-driver"))
      .dependsOn(
        `kms-driver-api`,
        `community-base`, // Required for JCE crypto
        `kms-driver-testing` % Test,
        `community-testing` % "test->test", // Required for logback-test.xml
      )
      .settings(
        sharedCantonCommunitySettings
      )

    // Project for specifying the sequencer driver API
    lazy val `sequencer-driver-api` = project
      .in(file("community/sequencer-driver"))
      .dependsOn(
        `util-external`,
        `util-observability`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          logback_classic,
          logback_core,
          scala_logging,
          scala_collection_contrib,
          scalatest % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
          better_files,
          cats,
          jul_to_slf4j % Test,
          log4j_core,
          log4j_api,
          monocle_macro, // Include it here, even if unused, so that it can be used everywhere
          pureconfig_core,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        UberLibrary.assemblySettings("sequencer-driver-lib"),
      )

    lazy val `sequencer-driver-api-conformance-tests` = project
      .in(file("community/sequencer-driver-api-conformance-tests"))
      .dependsOn(
        `community-testing`,
        `sequencer-driver-api`,
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          scalatest
        ),
      )

    // TODO(i12761): package individual libraries instead of fat JARs for external consumption
    lazy val `sequencer-driver-lib`: Project =
      project
        .settings(
          sharedCantonCommunitySettings,
          libraryDependencies ++= Seq(
            circe_core,
            circe_generic,
            circe_parser,
            better_files,
          ),
        )
        .settings(UberLibrary.of(`sequencer-driver-api`))

    lazy val `reference-sequencer-driver` = project
      .in(file("community/reference-sequencer-driver"))
      .dependsOn(
        `util-external`,
        `community-common` % "compile->compile;test->test",
        `sequencer-driver-api` % "compile->compile;test->test",
      )
      .settings(
        sharedCantonCommunitySettings,
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
      )

    lazy val blake2b = project
      .in(file("community/lib/Blake2b"))
      .disablePlugins(BufPlugin, ScalafmtPlugin, JavaFormatterPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          bouncycastle_bcprov,
          bouncycastle_bcpkix,
        ),
        // Exclude to apply our license header to any Java files
        headerSources / excludeFilter := "*.java",
        coverageEnabled := false,
      )

    lazy val `slick-fork` = project
      .in(file("community/lib/slick"))
      .disablePlugins(BufPlugin, ScalafmtPlugin, JavaFormatterPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies += slick,
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
        coverageEnabled := false,
      )

    lazy val `wartremover-extension` = project
      .in(file("community/lib/wartremover"))
      .dependsOn(`wartremover-annotations`)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          cats,
          grpc_stub,
          mockito_scala % Test,
          scalapb_runtime_grpc,
          scalatestMockito % Test,
          scalatest % Test,
          wartremover_dep,
        ),
      )

    lazy val `wartremover-annotations` = project
      .in(file("community/lib/wartremover-annotations"))
      .settings(sharedSettings)

    // TODO(#10617) remove when no longer needed
    lazy val `pekko-fork` = project
      .in(file("community/lib/pekko"))
      .disablePlugins(BufPlugin, ScalafixPlugin, ScalafmtPlugin, JavaFormatterPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          pekko_stream,
          pekko_stream_testkit % Test,
          pekko_slf4j,
          scalatest % Test,
        ),
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
        coverageEnabled := false,
      )

    lazy val `magnolify-addon` = project
      .in(file("community/lib/magnolify"))
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          cats,
          daml_non_empty,
          magnolia,
          magnolifyScalacheck,
          magnolifyShared % Test,
          scala_reflect,
          scalacheck,
          scalatest % Test,
        ),
      )

    lazy val `scalatest-addon` = project
      .in(file("community/lib/scalatest"))
      .settings(
        sharedSettings,
        libraryDependencies += scalatest,
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
      )

    lazy val `demo` = project
      .in(file("community/demo"))
      .enablePlugins(DamlPlugin)
      .dependsOn(
        `community-app` % "compile->compile;test->test",
        `community-admin-api` % "test->test",
      )
      .settings(
        sharedCantonCommunitySettings,
        libraryDependencies ++= Seq(
          scalafx,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
        ) ++ javafx_all,
        Compile / damlEnableJavaCodegen := true,
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml" / "doctor",
            (Compile / damlDarOutput).value / "doctor.dar",
            "com.digitalasset.canton.demo.model.doctor",
          ),
          (
            (Compile / sourceDirectory).value / "daml" / "ai-analysis",
            (Compile / damlDarOutput).value / "ai-analysis.dar",
            "com.digitalasset.canton.demo.model.ai",
          ),
        ),
        Compile / damlBuildOrder := Seq(
          "bank",
          "medical-records",
          "health-insurance",
          "doctor",
          "ai-analysis",
        ),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.sh", "../pack", Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
      )

    lazy val `base-errors` = project
      .in(file("base/errors"))
      .dependsOn(
        DamlProjects.`google-common-protos-scala`,
        `wartremover-annotations`,
      )
      .settings(
        sharedCommunitySettings ++ cantonWarts,
        libraryDependencies ++= Seq(
          cats,
          slf4j_api,
          grpc_api,
          reflections,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
        ),
        coverageEnabled := false,
      )

    lazy val `daml-tls` = project
      .in(file("base/daml-tls"))
      .dependsOn(
        `util-external` % "test->compile",
        `wartremover-annotations`,
      )
      .settings(
        sharedCommunitySettings ++ cantonWarts,
        libraryDependencies ++= Seq(
          commons_io,
          grpc_netty_shaded,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          scopt,
          slf4j_api,
        ),
        coverageEnabled := false,
      )

    lazy val `daml-adjustable-clock` = project
      .in(file("base/adjustable-clock"))
      .settings(
        sharedCommunitySettings,
        coverageEnabled := false,
      )

    lazy val `ledger-common-dars` =
      project
        .in(file("community/ledger/ledger-common-dars"))
        .settings(sharedCommunitySettings, addFilesToHeaderCheck("*.daml", "daml", Compile))

    lazy val `ledger-common` = project
      .in(file("community/ledger/ledger-common"))
      .dependsOn(
        DamlProjects.`ledger-api`,
        DamlProjects.`daml-jwt`,
        DamlProjects.`bindings-java` % "test->test",
        `util-observability` % "compile->compile;test->test",
        `ledger-common-dars-lf-v2-2` % "test",
        `util-external`,
      )
      .settings(
        sharedCommunitySettings, // Upgrade to sharedCantonSettings when com.digitalasset.canton.concurrent.Threading moved out of community-base
        Compile / PB.targets := Seq(
          PB.gens.java -> (Compile / sourceManaged).value / "protobuf",
          scalapb.gen(flatPackage = false) -> (Compile / sourceManaged).value / "protobuf",
        ),
        Test / unmanagedResourceDirectories += (`ledger-common-dars-lf-v2-2` / Compile / resourceManaged).value,
        addProtobufFilesToHeaderCheck(Compile),
        libraryDependencies ++= Seq(
          daml_libs_scala_ledger_resources,
          daml_lf_data,
          daml_lf_engine,
          daml_lf_transaction,
          daml_rs_grpc_bridge,
          daml_rs_grpc_pekko,
          daml_ledger_api_value_java,
          slf4j_api,
          grpc_api,
          grpc_netty_shaded,
          scalapb_runtime,
          daml_libs_scala_ports % Test,
          scalatest % Test,
          scalacheck % Test,
        ),
        Test / parallelExecution := true,
        coverageEnabled := false,
      )

    def createLedgerCommonDarsProject(lfVersion: String) =
      Project(
        s"ledger-common-dars-lf-v$lfVersion".replace('.', '-'),
        file(s"community/ledger/ledger-common-dars/lf-v$lfVersion"),
      )
        .dependsOn(
          DamlProjects.`bindings-java`
        )
        .enablePlugins(DamlPlugin)
        .settings(
          sharedCommunitySettings,
          Compile / damlDarLfVersion := lfVersion,
          ledgerCommonDarsSharedSettings(lfVersion),
        )

    def ledgerCommonDarsSharedSettings(lfVersion: String) = Seq(
      Compile / damlEnableJavaCodegen := true,
      Compile / damlSourceDirectory := baseDirectory.value / ".." / "src",
      Compile / useVersionedDarName := true,
      Compile / damlEnableProjectVersionOverride := false,
      Compile / damlCodeGeneration := (for (
        name <- Seq(
          "model",
          "model_iface",
          "semantic",
          "ongoing_stream_package_upload",
          "package_management",
          "carbonv1",
          "carbonv2",
          "upgrade_iface",
        ) ++ (if (lfVersion == "2.dev") Seq("experimental") else Seq.empty)
      )
        yield (
          (Compile / damlSourceDirectory).value / "main" / "daml" / s"$name",
          (Compile / damlDarOutput).value / s"${name.replace("_", "-")}-tests-3.1.0.dar",
          s"com.daml.ledger.test.java.$name",
        )) ++ Seq(
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "upgrade" / "1.0.0",
          (Compile / damlDarOutput).value / "upgrade-tests-1.0.0.dar",
          s"com.daml.ledger.test.java.upgrade_1_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "upgrade" / "2.0.0",
          (Compile / damlDarOutput).value / "upgrade-tests-2.0.0.dar",
          s"com.daml.ledger.test.java.upgrade_2_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "upgrade" / "3.0.0",
          (Compile / damlDarOutput).value / "upgrade-tests-3.0.0.dar",
          s"com.daml.ledger.test.java.upgrade_3_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "upgrade_fetch" / "1.0.0",
          (Compile / damlDarOutput).value / "upgrade-fetch-tests-1.0.0.dar",
          s"com.daml.ledger.test.java.upgrade_fetch_1_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "upgrade_fetch" / "2.0.0",
          (Compile / damlDarOutput).value / "upgrade-fetch-tests-2.0.0.dar",
          s"com.daml.ledger.test.java.upgrade_fetch_2_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_dep",
          (Compile / damlDarOutput).value / "vetting-dep-1.0.0.dar",
          s"com.daml.ledger.test.java.vetting_dep",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_main" / "1.0.0",
          (Compile / damlDarOutput).value / "vetting-main-1.0.0.dar",
          s"com.daml.ledger.test.java.vetting_main_1_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_main" / "2.0.0",
          (Compile / damlDarOutput).value / "vetting-main-2.0.0.dar",
          s"com.daml.ledger.test.java.vetting_main_2_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_main" / "split-lineage-2.0.0",
          (Compile / damlDarOutput).value / "vetting-main-split-lineage-2.0.0.dar",
          s"com.daml.ledger.test.java.vetting_main_split_lineage_2_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_main" / "upgrade-incompatible-3.0.0",
          (Compile / damlDarOutput).value / "vetting-main-3.0.0.dar",
          s"com.daml.ledger.test.java.vetting_main_3_0_0",
        ),
        (
          (Compile / damlSourceDirectory).value / "main" / "daml" / "vetting_alt",
          (Compile / damlDarOutput).value / "vetting-alt-1.0.0.dar",
          s"com.daml.ledger.test.java.vetting_alt",
        ),
      ),
      Compile / damlBuildOrder := Seq(
        // define the packages that have a dependency in the right order
        // packages that are omitted will be compiled after those listed below
        "model_iface",
        "model",
        "carbonv1",
        "carbonv2",
        "upgrade_iface",
        "vetting_dep",
      ),
    )

    lazy val `ledger-common-dars-lf-v2-2` = createLedgerCommonDarsProject(lfVersion = "2.2")
    lazy val `ledger-common-dars-lf-v2-dev` = createLedgerCommonDarsProject(lfVersion = "2.dev")

    // The TlsCertificateRevocationCheckingSpec relies on the "com.sun.net.ssl.checkRevocation" system variable to
    // function properly. However, if another test (e.g. TlsSpec) modifies this variable, the change will not be
    // realized. This happens since java.sun.security.validator.PKIXValidator uses a static variable that is initialized
    // by the system variable "com.sun.net.ssl.checkRevocation". To ensure that the test runs correctly it needs to be
    // executed in its own JVM instance that is isolated from other tests.
    def separateRevocationTest(
        tests: Seq[TestDefinition]
    ): Seq[Group] = {
      val options = ForkOptions().withRunJVMOptions(
        Vector(
          // "-Djava.security.debug=certpath ocsp" // enable when debugging ocsp tests
        )
      )
      tests groupBy (_.name.contains("TlsCertificateRevocationCheckingSpec")) map {
        case (true, tests) =>
          new Group("TlsCertificateRevocationCheckingSpec", tests, SubProcess(options))
        case (false, tests) =>
          new Group("rest", tests, SubProcess(options))
      } toSeq
    }

    lazy val `ledger-api-core` = project
      .in(file("community/ledger/ledger-api-core"))
      .dependsOn(
        `base-errors` % "test->test",
        `daml-tls` % "test->test",
        `ledger-common` % "compile->compile;test->test",
        `community-common` % "compile->compile;test->test",
        `daml-adjustable-clock` % "test->test",
        DamlProjects.`daml-jwt`,
      )
      .settings(
        sharedCantonCommunitySettings,
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = false) -> (Compile / sourceManaged).value / "protobuf"
        ),
        libraryDependencies ++= Seq(
          daml_libs_scala_ports,
          daml_timer_utils,
          auth0_java,
          auth0_jwks,
          postgres,
          h2,
          flyway,
          flyway_postgres,
          grpc_inprocess,
          anorm,
          daml_http_test_utils % Test,
          daml_lf_archive_encoder % Test,
          daml_lf_encoder % Test,
          daml_lf_parser % Test,
          daml_libs_scala_grpc_test_utils % Test,
          daml_observability_tracing_test_lib % Test,
          daml_rs_grpc_testing_utils % Test,
          daml_test_evidence_generator_scalatest % Test,
          scalapb_json4s % Test,
        ),
        Test / parallelExecution := true,
        Test / fork := false,
        Test / testGrouping := separateRevocationTest((Test / definedTests).value),
        coverageEnabled := false,
      )

    lazy val `ledger-json-api` =
      project
        .in(file("community/ledger/ledger-json-api"))
        .dependsOn(
          `ledger-api-core` % "compile->compile;test->test",
          `transcode`,
          `ledger-common` % "test->test",
          `community-testing` % "test->test",
        )
        .enablePlugins(DamlPlugin)
        .settings(
          sharedCommunitySettings,
          Test / PB.targets := Seq(
            // build java codegen too
            PB.gens.java -> (Test / sourceManaged).value / "protobuf",
            // build scala codegen with java conversions
            scalapb.gen(
              javaConversions = true,
              flatPackage = false,
            ) -> (Test / sourceManaged).value / "protobuf",
          ),
          libraryDependencies ++= Seq(
            circe_generic_extras,
            circe_parser,
            circe_yaml % Test,
            daml_lf_api_type_signature,
            daml_lf_transaction_test_lib,
            daml_observability_pekko_http_metrics,
            daml_timer_utils,
            pekko_http,
            icu4j_version,
            sttp_apiscpec_openapi_circe_yaml,
            sttp_apiscpec_asyncapi_circe_yaml,
            scalapb_json4s,
            semver,
            daml_libs_scala_scalatest_utils % Test,
            pekko_stream_testkit % Test,
            protostuff_parser % Test,
            scalapb_runtime,
            scalapb_runtime_grpc,
            tapir_asyncapi_docs,
            tapir_json_circe,
            tapir_openapi_docs,
            tapir_pekko_http_server,
            ujson_circe,
            upickle,
          ),
          coverageEnabled := false,
          Test / damlCodeGeneration := Seq(
            (
              (Test / sourceDirectory).value / "daml" / "v2_2",
              (Test / damlDarOutput).value / "JsonEncodingTest.dar",
              "com.digitalasset.canton.http.json.encoding",
            ),
            (
              (Test / sourceDirectory).value / "daml" / "v2_dev",
              (Test / damlDarOutput).value / "JsonEncodingTestDev.dar",
              "com.digitalasset.canton.http.json.encoding.dev",
            ),
            (
              (Test / sourceDirectory).value / "daml" / "damldefinitionsservice" / "dep",
              (Test / damlDarOutput).value / "DamlDefinitionsServiceDep.dar",
              "com.digitalasset.canton.http.json.damldefinitionsservicedep",
            ),
            (
              (Test / sourceDirectory).value / "daml" / "damldefinitionsservice" / "main",
              (Test / damlDarOutput).value / "DamlDefinitionsServiceMain.dar",
              "com.digitalasset.canton.http.json.damldefinitionsservicemain",
            ),
          ),
        )

    import org.openapitools.generator.sbt.plugin.OpenApiGeneratorPlugin.autoImport.{
      openApiInputSpec,
      openApiConfigFile,
      openApiOutputDir,
      openApiGenerate,
      openApiGenerateApiTests,
      openApiGenerateModelTests,
    }

    // This ensures that we generate java classes for openapi.yaml only when it is  changed
    lazy val cachedOpenApiGenerate = Def.taskDyn {
      import sbt.util.Tracked
      import sjsonnew.BasicJsonProtocol.*

      val openApiYamlFile =
        baseDirectory.value.getParentFile / "ledger-json-api" / "src/test/resources/json-api-docs/openapi.yaml"

      val cacheDir = streams.value.cacheDirectory / "openapi"
      val inputFile = openApiYamlFile.getCanonicalFile

      val generateOrGet = Tracked.inputChanged(cacheDir / "input") { (hasChanged, _: String) =>
        if (hasChanged) {
          Def.task {
            streams.value.log.info(s"Detected change in ${inputFile.getName}, regenerating...")
            openApiGenerate.value
          }
        } else {
          Def.task {
            val log = streams.value.log
            log.info(s"No change in ${inputFile.getName}, skipping generation")
            val managedDir = (Test / sourceManaged).value
            (managedDir ** "*").get.filter(_.isFile)
          }
        }
      }

      generateOrGet(Hash.toHex(Hash(inputFile)))
    }

    lazy val `ledger-json-client` = project
      .in(file("community/ledger/ledger-json-client"))
      .disablePlugins(WartRemover)
      .dependsOn(`ledger-json-api` % "test->test")
      .settings(
        sharedCommunitySettings,
        name := "ledger-json-client",
        libraryDependencies := Seq(
          gson % Test,
          jackson_databind_nullable % Test,
          gson_fire % Test,
          jakarta_annotation_api % Test,
          scalatest % Test,
          scalacheck % Test,
          magnolifyScalacheck % Test,
          swagger_parser % Test,
        ),
        openApiInputSpec := (baseDirectory.value.getParentFile / "ledger-json-api" / "src/test/resources" / "json-api-docs" / "openapi.yaml").toString,
        openApiConfigFile := (baseDirectory.value.getParentFile / "ledger-json-client" / "config.yaml").toString,
        openApiOutputDir := (Test / sourceManaged).value.getPath,
        openApiGenerateApiTests := Some(false),
        openApiGenerateModelTests := Some(false),
        Test / sourceGenerators += Def.task {
          val files = cachedOpenApiGenerate.value
          files.filter(f =>
            f.getName.endsWith(".java") &&
              // Compile only model and necessary classes, to avoid compiling full client with okhttp libs
              (f.getParentFile.getName == "model" || f.getName == "JSON.java" || f.getName == "ApiException.java")
          )
        }.taskValue,
      )

    lazy val `ledger-api-tools` = project
      .in(file("community/ledger/ledger-api-tools"))
      .dependsOn(
        `community-testing`,
        `ledger-api-core`,
        DamlProjects.`daml-jwt`,
      )
      .settings(
        sharedCantonCommunitySettings,
        coverageEnabled := false,
      )

    lazy val `ledger-api-string-interning-benchmark` = project
      .in(file("community/ledger/ledger-api-string-interning-benchmark"))
      .enablePlugins(JmhPlugin)
      .dependsOn(`ledger-api-core`)
      .settings(
        sharedCantonCommunitySettings,
        Test / parallelExecution := true,
        Test / fork := false,
      )

    lazy val `transcode` =
      project
        .in(file("community/ledger/transcode/"))
        .settings(
          sharedCommunitySettings,
          scalacOptions --= DamlProjects.removeCompileFlagsForDaml
            // needed for foo.bar.{this as that} imports
            .filterNot(_ == "-Xsource:3"),
          libraryDependencies ++= Seq(
            daml_lf_language,
            "com.lihaoyi" %% "ujson" % "4.0.2",
            scalatest % Test,
            daml_lf_archive_reader % Test,
          ),
        )
        .dependsOn(
          DamlProjects.`ledger-api`,
          `community-testing` % Test,
          `community-common` % Test,
        )

    lazy val `ledger-api-bench-tool` = project
      .in(file("community/ledger-api-bench-tool"))
      .dependsOn(
        `ledger-api-core`,
        `ledger-common` % "compile->compile;compile->test",
        `community-base`,
        `community-app` % "test->test",
        `daml-adjustable-clock`,
      )
      .disablePlugins(WartRemover) // TODO(i12064): enable WartRemover
      .enablePlugins(DamlPlugin)
      .settings(
        libraryDependencies ++= Seq(
          pekko_actor_typed,
          pekko_actor_testkit_typed % Test,
          circe_core,
          circe_yaml,
        ),
        sharedSettings,
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
        Compile / damlDarLfVersion := "2.dev",
        Compile / damlEnableJavaCodegen := true,
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml" / "benchtool",
            (Compile / damlDarOutput).value / "benchtool-tests.dar",
            s"com.daml.ledger.test.java.benchtool",
          )
        ),
      )

    def ledgerTestToolSuitesProject(
        lfVersion: String,
        darsProject: Project,
        additionalSetting: Def.SettingsDefinition*
    ): Project =
      Project(
        s"ledger-test-tool-suites-$lfVersion".replace('.', '-'),
        file(s"community/ledger-test-tool/suites/lf-v$lfVersion"),
      )
        .dependsOn(
          DamlProjects.`bindings-java`,
          `community-participant`,
          `community-testing`,
          `community-base`,
          `base-errors`,
          `ledger-api-core`,
          `ledger-common`,
          `ledger-json-api`,
          darsProject,
        )
        .disablePlugins(WartRemover)
        .settings(
          libraryDependencies ++= Seq(
            daml_test_evidence_tag,
            daml_libs_scala_grpc_test_utils,
            munit,
            sttp_pekko_backend,
            pekko_stream,
            tapir_sttp_client,
          ),
          compileOrder := CompileOrder.JavaThenScala,
          sharedSettings,
          Def.settings(additionalSetting.toSeq*),
          Compile / unmanagedSourceDirectories += baseDirectory.value / ".." / "src",
          Test / unmanagedSourceDirectories += baseDirectory.value / ".." / "src",
          scalacOptions --= JvmRulesPlugin.scalacOptionsToDisableForTests,
          // 2.2 tests will fail to compile with a 2.2 dar, so we exclude them from the test suite
          if (lfVersion != "2.dev")
            Seq(
              Compile / unmanagedSources / excludeFilter := "*NamesSpec.scala" || ((_: File).getAbsolutePath
                .contains("v2_dev"))
            )
          else Seq.empty,
        )

    lazy val `ledger-test-tool-suites-2-2` =
      ledgerTestToolSuitesProject("2.2", `ledger-common-dars-lf-v2-2`)
    lazy val `ledger-test-tool-suites-2-dev` =
      ledgerTestToolSuitesProject(
        "2.dev",
        `ledger-common-dars-lf-v2-dev`,
        // Suites sources are identical between test tool versions
        // Hence, keep ledger-test-tool-suites-2-2 as primary sbt module holding the sources
        // and all other sbt suites modules add them as unmanagedSourceDirectories for compilation
        Compile / unmanagedSourceDirectories += baseDirectory.value / ".." / "lf-v2.2" / "src",
        Test / unmanagedSourceDirectories += baseDirectory.value / ".." / "lf-v2.2" / "src",
      )

    def ledgerTestToolProject(lfVersion: String, ledgerTestToolSuites: Project): Project =
      Project(
        s"ledger-test-tool-$lfVersion".replace('.', '-'),
        file(s"community/ledger-test-tool/tool/lf-v$lfVersion"),
      ).dependsOn(
        ledgerTestToolSuites
      ).disablePlugins(WartRemover)
        .enablePlugins(DamlPlugin)
        .settings(
          compileOrder := CompileOrder.JavaThenScala,
          sharedSettings,
          Compile / unmanagedSourceDirectories += baseDirectory.value / ".." / "src",
          Compile / unmanagedResourceDirectories += baseDirectory.value / ".." / "src" / "main" / "resources",
          // See #23185: Prevent potential OOM by setting info log level when conformance tests trigger assembly
          assembly / logLevel := Level.Info,
          assembly / mainClass := Some("com.daml.ledger.api.testtool.Main"),
          assembly / assemblyJarName := s"ledger-api-test-tool-$lfVersion-${version.value}.jar",
          assembly / assemblyMergeStrategy := {
            case PathList("org", "hamcrest", _ @_*) => MergeStrategy.last
            // complains about okio.kotlin_module clash
            case PathList("META-INF", "okio.kotlin_module") => MergeStrategy.last
            case x =>
              val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
              mergeStrategy(oldStrategy)(x)
          },
        )

    lazy val `ledger-test-tool-2-2` = ledgerTestToolProject("2.2", `ledger-test-tool-suites-2-2`)
    lazy val `ledger-test-tool-2-dev` =
      ledgerTestToolProject("2.dev", `ledger-test-tool-suites-2-dev`)

    lazy val `conformance-testing` = project
      .in(file("community/conformance-testing"))
      .dependsOn(
        `community-app` % "compile->compile;test->test",
        `ledger-test-tool-2-2` % Test,
        `ledger-test-tool-2-dev` % Test,
      )
      .settings(
        sharedCantonCommunitySettings,
        // Allow to exit the systematic testing generator app
        (Test / run / trapExit) := false,
        Test / run := (Test / run)
          .dependsOn(`ledger-test-tool-2-2` / assembly, `ledger-test-tool-2-dev` / assembly)
          .evaluated,
        Test / test := (Test / test)
          .dependsOn(`ledger-test-tool-2-2` / assembly, `ledger-test-tool-2-dev` / assembly)
          .value,
        Test / testOnly := (Test / testOnly)
          .dependsOn(`ledger-test-tool-2-2` / assembly, `ledger-test-tool-2-dev` / assembly)
          .evaluated,
        Test / unmanagedResourceDirectories += (`ledger-common-dars-lf-v2-2` / Compile / resourceManaged).value,
      )

    // TODO(#25385): Consider extracting this integration test setup into its own sbt file due to its size
    lazy val `enterprise-upgrading-integration-tests` = project
      .in(file("community/upgrading-integration-tests"))
      .dependsOn(
        `community-app` % "test->test"
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedCantonCommunitySettings,
        Test / damlEnableJavaCodegen := true,
        Test / damlExcludeFromCodegen := Seq("com.digitalasset.canton.damltests.nonconforming.x"),
        Test / useVersionedDarName := true,
        Test / damlEnableProjectVersionOverride := false,
        Test / damlBuildOrder := Seq(
          "daml/DvP/Assets",
          "daml/DvP/Offer",
          "daml/DvP/AssetFactory",
          "daml/Systematic/Util/V1",
          "daml/Systematic/Util/V2",
          "daml/Systematic/IBar",
          "daml/Systematic/IBaz",
          "daml/Systematic/Bar/V1",
          "daml/Systematic/Bar/V2",
          "daml/Systematic/Baz/V1",
          "daml/Systematic/Baz/V2",
          "daml/Systematic/Foo",
        ),
        Test / damlCodeGeneration := Seq(
          (
            (Test / sourceDirectory).value / "daml" / "CantonUpgrade" / "If",
            (Test / damlDarOutput).value / "UpgradeIf-1.0.0.dar",
            "com.digitalasset.canton.damltests.upgrade.upgradeif",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "CantonUpgrade" / "V1",
            (Test / damlDarOutput).value / "Upgrade-1.0.0.dar",
            "com.digitalasset.canton.damltests.upgrade.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "CantonUpgrade" / "V2",
            (Test / damlDarOutput).value / "Upgrade-2.0.0.dar",
            "com.digitalasset.canton.damltests.upgrade.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "NonConforming" / "V1",
            (Test / damlDarOutput).value / "NonConforming-1.0.0.dar",
            "com.digitalasset.canton.damltests.nonconforming.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "NonConforming" / "V2",
            (Test / damlDarOutput).value / "NonConforming-2.0.0.dar",
            "com.digitalasset.canton.damltests.nonconforming.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "NonConforming" / "X",
            (Test / damlDarOutput).value / "NonConformingX-1.0.0.dar",
            "com.digitalasset.canton.damltests.nonconforming.x",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "AppUpgrade" / "V1",
            (Test / damlDarOutput).value / "AppUpgrade-1.0.0.dar",
            "com.digitalasset.canton.damltests.appupgrade.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "AppUpgrade" / "V2",
            (Test / damlDarOutput).value / "AppUpgrade-2.0.0.dar",
            "com.digitalasset.canton.damltests.appupgrade.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "TopologyAwarePackageSelection" / "ScenarioAppInstall" / "V1",
            (Test / damlDarOutput).value / "tests-app-install-1.0.0.dar",
            "com.digitalasset.canton.damltests.appinstall.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "TopologyAwarePackageSelection" / "ScenarioAppInstall" / "V2",
            (Test / damlDarOutput).value / "tests-app-install-2.0.0.dar",
            "com.digitalasset.canton.damltests.appinstall.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "TopologyAwarePackageSelection" / "FeaturedAppRight" / "V1",
            (Test / damlDarOutput).value / "tests-featured-app-right-impl-1.0.0.dar",
            "com.digitalasset.canton.damltests.featuredapprightimpl.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "TopologyAwarePackageSelection" / "FeaturedAppRight" / "V2",
            (Test / damlDarOutput).value / "tests-featured-app-right-impl-2.0.0.dar",
            "com.digitalasset.canton.damltests.featuredapprightimpl.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "TopologyAwarePackageSelection" / "FeaturedAppRight" / "If",
            (Test / damlDarOutput).value / "tests-featured-app-right-iface-1.0.0.dar",
            "com.digitalasset.canton.damltests.featuredappright.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "HoldingV1",
            (Test / damlDarOutput).value / "tests-Holding-v1-1.0.0.dar",
            "com.digitalasset.canton.damltests.holding.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "HoldingV2",
            (Test / damlDarOutput).value / "tests-Holding-v2-1.0.0.dar",
            "com.digitalasset.canton.damltests.holding.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "TokenV1",
            (Test / damlDarOutput).value / "tests-Token-1.0.0.dar",
            "com.digitalasset.canton.damltests.token.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "TokenV2",
            (Test / damlDarOutput).value / "tests-Token-2.0.0.dar",
            "com.digitalasset.canton.damltests.token.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "TokenV3",
            (Test / damlDarOutput).value / "tests-Token-3.0.0.dar",
            "com.digitalasset.canton.damltests.token.v3",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "UpgradesWithInterfaces" / "TokenV4",
            (Test / damlDarOutput).value / "tests-Token-4.0.0.dar",
            "com.digitalasset.canton.damltests.token.v4",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "DvP" / "Assets" / "V1",
            (Test / damlDarOutput).value / "dvp-assets-1.0.0.dar",
            "com.digitalasset.canton.damltests.dvpassets.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "DvP" / "Assets" / "V2",
            (Test / damlDarOutput).value / "dvp-assets-2.0.0.dar",
            "com.digitalasset.canton.damltests.dvpassets.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "DvP" / "Offer" / "V1",
            (Test / damlDarOutput).value / "dvp-offer-1.0.0.dar",
            "com.digitalasset.canton.damltests.dvpoffer.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "DvP" / "Offer" / "V2",
            (Test / damlDarOutput).value / "dvp-offer-2.0.0.dar",
            "com.digitalasset.canton.damltests.dvpoffer.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "IBaz",
            (Test / damlDarOutput).value / "ibaz-1.0.0.dar",
            "com.digitalasset.canton.damltests.ibaz.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "IBar",
            (Test / damlDarOutput).value / "ibar-1.0.0.dar",
            "com.digitalasset.canton.damltests.ibar.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Baz" / "V1",
            (Test / damlDarOutput).value / "baz-1.0.0.dar",
            "com.digitalasset.canton.damltests.baz.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Baz" / "V2",
            (Test / damlDarOutput).value / "baz-2.0.0.dar",
            "com.digitalasset.canton.damltests.baz.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Bar" / "V1",
            (Test / damlDarOutput).value / "bar-1.0.0.dar",
            "com.digitalasset.canton.damltests.bar.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Bar" / "V2",
            (Test / damlDarOutput).value / "bar-2.0.0.dar",
            "com.digitalasset.canton.damltests.bar.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Foo" / "V1",
            (Test / damlDarOutput).value / "foo-1.0.0.dar",
            "com.digitalasset.canton.damltests.foo.v1",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Foo" / "V2",
            (Test / damlDarOutput).value / "foo-2.0.0.dar",
            "com.digitalasset.canton.damltests.foo.v2",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Foo" / "V3",
            (Test / damlDarOutput).value / "foo-3.0.0.dar",
            "com.digitalasset.canton.damltests.foo.v3",
          ),
          (
            (Test / sourceDirectory).value / "daml" / "Systematic" / "Foo" / "V4",
            (Test / damlDarOutput).value / "foo-4.0.0.dar",
            "com.digitalasset.canton.damltests.foo.v4",
          ),
        ),
      )
  }

  object DamlProjects {

    lazy val allProjects = Set(
      `daml-jwt`,
      `google-common-protos-scala`,
      `ledger-api-value`,
      `ledger-api`,
      `bindings-java`,
    )

    lazy val removeCompileFlagsForDaml =
      Seq("-Xsource:3", "-deprecation", "-Xfatal-warnings", "-Ywarn-unused", "-Ywarn-value-discard")

    lazy val `daml-jwt` = project
      .in(file("base/daml-jwt"))
      .disablePlugins(WartRemover)
      .settings(
        sharedCommunitySettings,
        scalacOptions += "-Wconf:src=src_managed/.*:silent",
        libraryDependencies ++= Seq(
          auth0_java,
          auth0_jwks,
          daml_libs_struct_spray_json,
          scalatest % Test,
          scalaz_core,
          slf4j_api,
        ),
        coverageEnabled := false,
      )

    // this project builds scala protobuf versions that include
    // java conversions of a few google standard items
    // the google protobuf files are extracted from the provided jar files
    lazy val `google-common-protos-scala` = project
      .in(file("community/lib/google-common-protos-scala"))
      .disablePlugins(
        BufPlugin,
        ScalafixPlugin,
        ScalafmtPlugin,
        JavaFormatterPlugin,
        WartRemover,
      )
      .settings(
        sharedSettings,
        scalacOptions --= removeCompileFlagsForDaml,
        // we restrict the compilation to a few files that we actually need, skipping the large majority ...
        excludeFilter := HiddenFileFilter || "scalapb.proto",
        PB.generate / includeFilter := "status.proto" || "code.proto" || "error_details.proto" || "health.proto",
        dependencyOverrides ++= Seq(),
        // compile proto files that we've extracted here
        Compile / PB.protoSources += (target.value / "protobuf_external"),
        Compile / PB.targets := Seq(
          // with java conversions but no java classes!
          scalapb.gen(
            javaConversions = true,
            flatPackage = false, // consistent with upstream daml
          ) -> (Compile / sourceManaged).value
        ),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          scalapb_runtime,
          scalapb_runtime_grpc,
          // the grpc services is necessary so we can build the
          // scala version of the health services, without
          // building the java protoc (to avoid duplicate symbols
          // during assembly)
          grpc_services,
          // extract the protobuf to target/protobuf_external
          // however, we'll only be including the ones in the includeFilter
          grpc_services % "protobuf",
          google_common_protos % "protobuf",
          google_common_protos,
          google_protobuf_java,
          google_protobuf_java_util,
        ),
      )

    // this project exists solely for the purpose of extracting value.proto
    // from the jar file built in the daml repository
    lazy val `ledger-api-value` = project
      .in(file("community/lib/ledger-api-value"))
      .dependsOn(
        `google-common-protos-scala`
      )
      .disablePlugins(
        BufPlugin
      )
      .settings(
        sharedSettings,
        // we restrict the compilation to a few files that we actually need, skipping the large majority ...
        excludeFilter := HiddenFileFilter || "scalapb.proto",
        PB.generate / includeFilter := "value.proto",
        dependencyOverrides ++= Seq(),
        // compile proto files that we've extracted here
        Compile / PB.protoSources ++= Seq(target.value / "protobuf_external"),
        Compile / PB.targets ++= Seq(
          PB.gens.plugin("doc") -> (Compile / sourceManaged).value
        ),
        Compile / PB.protocOptions := Seq(
          // the generated file can be found in src_managed, if another location is needed this can be specified via the --doc_out flag
          "--doc_opt=" + file("community/docs/rst_lapi_value.tmpl") + "," + "proto-docs.rst"
        ),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          daml_ledger_api_value % "protobuf",
          protoc_gen_doc asProtocPlugin (),
        ),
      )

    lazy val `ledger-api` = project
      .in(file("community/ledger-api"))
      .dependsOn(
        `google-common-protos-scala`,
        `ledger-api-value`,
      )
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      )
      .settings(
        sharedCommunitySettings,
        scalacOptions --= removeCompileFlagsForDaml,
        Compile / bufLintCheck := (Compile / bufLintCheck)
          .dependsOn(
            `google-common-protos-scala` / PB.unpackDependencies,
            `ledger-api-value` / PB.unpackDependencies,
          )
          .value,
        Compile / PB.targets := Seq(
          // build java codegen too
          PB.gens.java -> (Compile / sourceManaged).value,
          // build scala codegen with java conversions
          scalapb.gen(
            javaConversions = true,
            flatPackage = false,
          ) -> (Compile / sourceManaged).value,
          PB.gens.plugin("doc") -> (Compile / sourceManaged).value,
        ),
        Compile / PB.protocOptions := Seq(
          // the generated file can be found in src_managed, if another location is needed this can be specified via the --doc_out flag
          "--doc_opt=" + file("community/docs/rst_lapi.tmpl") + "," + "proto-docs.rst"
        ),
        Compile / unmanagedResources += (ThisBuild / baseDirectory).value / "community/ledger-api/VERSION",
        coverageEnabled := false,
        libraryDependencies ++= Seq(
          daml_ledger_api_value_scala,
          scalapb_runtime,
          scalapb_runtime_grpc,
          protoc_gen_doc asProtocPlugin (),
        ),
        addProtobufFilesToHeaderCheck(Compile),
      )

    lazy val `bindings-java` = project
      .in(file("community/bindings-java"))
      .dependsOn(
        `ledger-api`
      )
      .settings(
        sharedCommunitySettings,
        compileOrder := CompileOrder.JavaThenScala,
        scalacOptions ++= removeCompileFlagsForDaml,
        crossPaths := false, // Without this, the Java tests are not executed
        libraryDependencies ++= Seq(
          fasterjackson_core,
          daml_ledger_api_value_java,
          junit_interface % Test,
          jupiter_interface % Test,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          slf4j_api,
          checkerFramework,
        ),
      )
  }
}
