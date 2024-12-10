import java.io.File
import BufPlugin.autoImport.bufLintCheck
import DamlPlugin.autoImport.*
import Dependencies.{daml_lf_language, *}
import better.files.{File as BetterFile, *}
import com.lightbend.sbt.JavaFormatterPlugin
import com.typesafe.sbt.SbtLicenseReport.autoImportImpl.*
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{headerResources, headerSources}
import org.scalafmt.sbt.ScalafmtPlugin
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys.*
import sbt.Tests.{Group, SubProcess}
import sbt.*
import sbt.internal.util.ManagedLogger
import sbt.nio.Keys.*
import sbtassembly.AssemblyKeys.*
import sbtassembly.AssemblyPlugin.autoImport.assembly
import sbtassembly.{MergeStrategy, PathList}
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin.autoImport.*
import sbtide.Keys.ideExcludedDirectories
import sbtprotoc.ProtocPlugin.autoImport.{AsProtocPlugin, PB}
import scalafix.sbt.ScalafixPlugin
import scoverage.ScoverageKeys.*
import wartremover.WartRemover
import wartremover.WartRemover.autoImport.*

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
      )
    )

    import CommunityProjects._

    val globalSettings = Seq(
      name := "canton",
      // Reload on build changes
      Global / onChangedBuildSource := ReloadOnSourceChanges,
      // allow setting number of tasks via environment
      Global / concurrentRestrictions ++= sys.env
        .get("MAX_CONCURRENT_SBT_TEST_TASKS")
        .map(_.toInt)
        .map(Tags.limit(Tags.Test, _))
        .toSeq,
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

  def runCommand(command: String, log: ManagedLogger, optError: Option[String] = None): String = {
    import scala.sys.process.Process
    val processLogger = new DamlPlugin.BufferedLogger
    log.debug(s"Running $command")
    val exitCode = Process(command) ! processLogger
    val output = processLogger.output()
    if (exitCode != 0) {
      val errorMsg = s"A problem occurred when executing command `$command` in `build.sbt`: ${System
          .lineSeparator()} $output"
      log.error(errorMsg)
      if (optError.isDefined) log.error(optError.getOrElse(""))
      throw new IllegalStateException(errorMsg)
    }
    if (output != "") log.info(processLogger.output())
    output
  }

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

  private def packProtobufFiles(BaseFile: BetterFile, target: String): Seq[(File, String)] = {
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
      val releaseNotes: Seq[(File, String)] = {
        val sourceFile: File =
          file(s"release-notes/${version.value}.md")
        if (sourceFile.exists())
          Seq((sourceFile, "RELEASE-NOTES.md"))
        else
          Seq()
      }
      //  here, we copy the protobuf files of community manually
      val ledgerApiProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "ledger-api",
        "ledger-api",
      )
      val communityBaseProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "base",
        "community",
      )
      val communityParticipantProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "participant",
        "participant",
      )
      val communityAdminProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "admin-api",
        "admin-api",
      )
      val communityDomainProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "domain",
        "domain",
      )

      val protoFiles =
        ledgerApiProto ++ communityBaseProto ++ communityParticipantProto ++ communityAdminProto ++ communityDomainProto

      log.info("Invoking bundle generator")
      // add license to package
      val renames =
        releaseNotes ++ licenseFiles ++ demoSource ++ demoDars ++ demoJars ++ demoArtefacts ++ damlSampleSource ++ damlSampleDars ++ protoFiles
      val args = bundlePack.value ++ renames.flatMap(x => Seq("-r", x._1.toString, x._2))
      // build the canton fat-jar
      val assembleJar = assembly.value
      runCommand(
        f"bash ./scripts/ci/create-bundle.sh ${assembleJar.toString} ${(assembly / mainClass).value.get} ${args
            .mkString(" ")}",
        log,
      )
    }

  class RenameMergeStrategy(target: String) extends MergeStrategy {
    override def name: String = s"Rename to $target"

    override def apply(
        tempDir: File,
        path: String,
        files: Seq[File],
    ): Either[String, Seq[(File, String)]] =
      Right(files.map(_ -> target))
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
    case "META-INF/FastDoubleParser-LICENSE" => MergeStrategy.first
    // complains about okio.kotlin_module clash
    case PathList("META-INF", "okio.kotlin_module") => MergeStrategy.last
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

  lazy val cantonWarts = Seq(
    // DirectGrpcServiceInvocation prevents direct invocation of gRPC services through a stub, but this is often useful in tests
    Compile / compile / wartremoverErrors += Wart.custom(
      "com.digitalasset.canton.DirectGrpcServiceInvocation"
    ),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.DiscardedFuture"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.FutureAndThen"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.FutureTraverse"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.GlobalExecutionContext"),
    // NonUnitForEach is too aggressive for integration tests where we often ignore the result of console commands
    Compile / compile / wartremoverErrors += Wart.custom("com.digitalasset.canton.NonUnitForEach"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.RequireBlocking"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.SynchronizedFuture"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.TryFailed"),
    wartremover.WartRemover.dependsOnLocalProjectWarts(CommunityProjects.`wartremover-extension`),
  ).flatMap(_.settings)

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
    "-c",
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
      `util-logging`,
      `community-app`,
      `community-app-base`,
      `community-base`,
      `community-common`,
      `community-domain`,
      `community-participant`,
      `community-testing`,
      `community-integration-testing`,
      `sequencer-driver-api`,
      `sequencer-driver-api-conformance-tests`,
      `sequencer-driver-lib`,
      `community-reference-driver`,
      blake2b,
      `slick-fork`,
      `wartremover-extension`,
      `pekko-fork`,
      `magnolify-addon`,
      `demo`,
      `daml-errors`,
      `daml-adjustable-clock`,
      `daml-tls`,
      `ledger-common`,
      `ledger-common-dars-lf-v2-1`,
      `ledger-common-dars-lf-v2-dev`,
      `ledger-api-core`,
      `ledger-json-api`,
      `ledger-api-tools`,
      `ledger-api-string-interning-benchmark`,
      `transcode`,
    )

    // Project for utilities that are also used outside of the Canton repo
    lazy val `util-external` = project
      .in(file("daml-common-staging/util-external"))
      .dependsOn(
        `daml-errors`,
        `wartremover-extension` % "compile->compile;test->test",
      )
      .settings(
        sharedCantonSettingsExternal,
        libraryDependencies ++= Seq(
          cats,
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
      .in(file("daml-common-staging/grpc-utils"))
      .dependsOn(
        DamlProjects.`google-common-protos-scala`
      )
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          grpc_api,
          scalapb_runtime_grpc,
          scalatest % Test,
        ),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `util-logging` = project
      .in(file("community/util-logging"))
      .dependsOn(
        `daml-errors` % "compile->compile;test->test",
        `daml-grpc-utils`,
      )
      .settings(
        sharedSettings ++ cantonWarts,
        libraryDependencies ++= Seq(
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
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-app` = project
      .in(file("community/app"))
      .dependsOn(
        `community-app-base`,
        `community-common` % "test->test",
        `community-integration-testing` % "test->test",
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
          case "LICENSE-open-source-bundle.txt" => new RenameMergeStrategy("LICENSE-DA.txt")
          // this file comes in multiple flavors, from io.get-coursier:interface and from org.scala-lang.modules:scala-collection-compat. Since the content differs it is resolve this explicitly with this MergeStrategy.
          case path if path.endsWith("scala-collection-compat.properties") => MergeStrategy.first
          case x =>
            val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
            oldStrategy(x)
        },
        assembly / mainClass := Some("com.digitalasset.canton.CantonCommunityApp"),
        assembly / assemblyJarName := s"canton-open-source-${version.value}.jar",
        // clearing the damlBuild tasks to prevent compiling which does not work due to relative file "data-dependencies";
        // "data-dependencies" daml.yaml setting relies on hardcoded "0.0.1" project version
        Compile / damlBuild := Seq(), // message-0.0.1.dar is hardcoded and contact-0.0.1.dar is built by MessagingExampleIntegrationTest
        Compile / damlProjectVersionOverride := Some("0.0.1"),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.sh", "../pack", Compile),
        addFilesToHeaderCheck("*.sh", ".", Test),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-app-base` = project
      .in(file("community/app-base"))
      .dependsOn(
        `community-domain`,
        `community-participant`,
      )
      .settings(
        sharedCantonSettings,
        JvmRulesPlugin.damlRepoHeaderSettings,
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
        DamlProjects.`ledger-api`, // trace-context
        `daml-tls`,
        `util-logging`,
        `community-admin-api`,
        // No strictly internal dependencies on purpose so that this can be a foundational module and avoid circular dependencies
        `slick-fork`,
      )
      .settings(
        sharedCantonSettings,
        JvmRulesPlugin.damlRepoHeaderSettings,
        libraryDependencies ++= Seq(
          daml_executors,
          daml_lf_transaction,
          daml_nonempty_cats,
          daml_rs_grpc_bridge,
          daml_rs_grpc_pekko,
          better_files,
          bouncycastle_bcpkix_jdk15on,
          bouncycastle_bcprov_jdk15on,
          cats,
          chimney,
          chimneyJavaConversion,
          circe_core,
          circe_generic,
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
          BuildInfoKey("stableProtocolVersions" -> List()),
          BuildInfoKey("betaProtocolVersions" -> List()),
        ),
        buildInfoPackage := "com.digitalasset.canton.buildinfo",
        buildInfoObject := "BuildInfo",
        // excluded generated protobuf classes from code coverage
        coverageExcludedPackages := formatCoverageExcludes(
          """
            |<empty>
            |com\.digitalasset\.canton\.protocol\.v0\..*
            |com\.digitalasset\.canton\.domain\.v0\..*
            |com\.digitalasset\.canton\.identity\.v0\..*
            |com\.digitalasset\.canton\.identity\.admin\.v0\..*
            |com\.digitalasset\.canton\.domain\.api\.v0\..*
            |com\.digitalasset\.canton\.v0\..*
            |com\.digitalasset\.canton\.protobuf\..*
      """
        ),
        addProtobufFilesToHeaderCheck(Compile),
      )

    lazy val `community-common` = project
      .in(file("community/common"))
      .enablePlugins(DamlPlugin)
      .dependsOn(
        blake2b,
        `pekko-fork` % "compile->compile;test->test",
        `community-base`,
        `community-testing` % "test->test",
        `wartremover-extension` % "compile->compile;test->test",
        DamlProjects.`bindings-java`,
      )
      .settings(
        sharedCantonSettings,
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
          opentelemetry_sdk_autoconfigure,
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
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-domain` = project
      .in(file("community/domain"))
      .dependsOn(
        `community-common` % "compile->compile;test->test",
        `community-reference-driver`,
      )
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          pekko_actor_typed,
          scala_logging,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
          logback_classic % Runtime,
          logback_core % Runtime,
          scalapb_runtime, // not sufficient to include only through the `common` dependency - race conditions ensue
          scaffeine,
        ),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // Ensure the package scoped options will be picked up by sbt-protoc if used downstream
        // See https://scalapb.github.io/docs/customizations/#publishing-package-scoped-options
        Compile / packageBin / packageOptions += (
          Package.ManifestAttributes(
            "ScalaPB-Options-Proto" -> "com/digitalasset/canton/domain/scalapb/package.proto"
          )
        ),
        // excluded generated protobuf classes from code coverage
        coverageExcludedPackages := formatCoverageExcludes(
          """
            |<empty>
            |com\.digitalasset\.canton\.domain\.admin\.v0\..*
      """
        ),
        addProtobufFilesToHeaderCheck(Compile),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-participant` = project
      .in(file("community/participant"))
      .dependsOn(
        `community-common` % "test->test",
        `ledger-json-api`,
        DamlProjects.`daml-jwt`,
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          cats,
          chimney,
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
            (Compile / sourceDirectory).value / "daml" / "AdminWorkflows",
            (Compile / damlDarOutput).value / "AdminWorkflows.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
          (
            (Compile / sourceDirectory).value / "daml" / "PartyReplication",
            (Compile / damlDarOutput).value / "PartyReplication.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
        ),
        Compile / damlBuildOrder := Seq(
          "daml/AdminWorkflows/daml.yaml",
          "daml/PartyReplication/daml.yaml",
        ),
        // TODO(#16168) Before creating the first stable release with backwards compatibility guarantees,
        //  make "AdminWorkflows.dar" stable again
        damlFixedDars := Seq(),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-admin-api` = project
      .in(file("community/admin-api"))
      .dependsOn(`util-external`, `daml-errors` % "compile->compile;test->test")
      .settings(
        sharedCantonSettings,
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
        sharedSettings,
        JvmRulesPlugin.damlRepoHeaderSettings,
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
        sharedCantonSettings,
        JvmRulesPlugin.damlRepoHeaderSettings,

        // The dependency override is needed because `community-testing` depends transitively on
        // `scalatest` and `community-app-base` depends transitively on `ammonite`, which in turn
        // depend on incompatible versions of `scala-xml` -- not ideal but only causes possible
        // runtime errors while testing and none have been found so far, so this should be fine for now
        dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1",

        // This library contains a lot of testing helpers that previously existing in testing scope
        // As such, in order to minimize the diff when creating this library, the same rules that
        // applied to `test` scope are used here. This can be reviewed in the future.
        scalacOptions --= JvmRulesPlugin.scalacOptionsToDisableForTests,
        Compile / compile / wartremoverErrors := JvmRulesPlugin.wartremoverErrorsForTestScope,
      )

    // Project for specifying the sequencer driver API
    lazy val `sequencer-driver-api` = project
      .in(file("community/sequencer-driver"))
      .dependsOn(
        `util-external`,
        `util-logging`,
      )
      .settings(
        sharedCantonSettings,
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
        JvmRulesPlugin.damlRepoHeaderSettings,
        UberLibrary.assemblySettings("sequencer-driver-lib"),
      )

    lazy val `sequencer-driver-api-conformance-tests` = project
      .in(file("community/drivers/api-conformance-tests"))
      .dependsOn(
        `community-testing`,
        `sequencer-driver-api`,
      )
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          scalatest
        ),
      )

    // TODO(i12761): package individual libraries instead of fat JARs for external consumption
    lazy val `sequencer-driver-lib`: Project =
      project
        .settings(
          sharedCantonSettings,
          libraryDependencies ++= Seq(
            circe_core,
            circe_generic,
            circe_parser,
            better_files,
          ),
        )
        .settings(UberLibrary.of(`sequencer-driver-api`))

    lazy val `community-reference-driver` = project
      .in(file("community/drivers/reference"))
      .dependsOn(
        `util-external`,
        `community-common` % "compile->compile;test->test",
        `sequencer-driver-api` % "compile->compile;test->test",
      )
      .settings(
        sharedCantonSettings,
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
          bouncycastle_bcprov_jdk15on,
          bouncycastle_bcpkix_jdk15on,
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
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          cats,
          grpc_stub,
          mockito_scala % Test,
          scalatestMockito % Test,
          scalatest % Test,
          wartremover_dep,
        ),
      )

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
          magnolia,
          magnolifyScalacheck,
          magnolifyShared % Test,
          scala_reflect,
          scalacheck,
          scalatest % Test,
        ),
      )

    lazy val `demo` = project
      .in(file("community/demo"))
      .enablePlugins(DamlPlugin)
      .dependsOn(
        `community-app` % "compile->compile;test->test",
        `community-admin-api` % "test->test",
      )
      .settings(
        sharedCantonSettings,
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
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `daml-errors` = project
      .in(file("daml-common-staging/daml-errors"))
      .dependsOn(
        DamlProjects.`google-common-protos-scala`,
        `wartremover-extension` % "compile->compile;test->test",
      )
      .settings(
        sharedSettings ++ cantonWarts,
        libraryDependencies ++= Seq(
          slf4j_api,
          grpc_api,
          reflections,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
        ),
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `daml-tls` = project
      .in(file("daml-common-staging/daml-tls"))
      .dependsOn(
        `util-external` % "test->compile",
        `wartremover-extension` % "compile->compile;test->test",
      )
      .settings(
        sharedSettings ++ cantonWarts,
        libraryDependencies ++= Seq(
          commons_io,
          grpc_netty,
          netty_handler,
          netty_native,
          netty_boring_ssl, // This should be a Runtime dep, but needs to be declared at Compile scope due to https://github.com/sbt/sbt/issues/5568
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          scopt,
          slf4j_api,
        ),
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `daml-adjustable-clock` = project
      .in(file("daml-common-staging/adjustable-clock"))
      .settings(
        sharedSettings,
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `ledger-common` = project
      .in(file("community/ledger/ledger-common"))
      .dependsOn(
        DamlProjects.`ledger-api`,
        `util-logging` % "compile->compile;test->test",
        `ledger-common-dars-lf-v2-1` % "test",
        `util-external`,
      )
      .settings(
        sharedSettings, // Upgrade to sharedCantonSettings when com.digitalasset.canton.concurrent.Threading moved out of community-base
        Compile / PB.targets := Seq(
          PB.gens.java -> (Compile / sourceManaged).value / "protobuf",
          scalapb.gen(flatPackage = false) -> (Compile / sourceManaged).value / "protobuf",
        ),
        Test / unmanagedResourceDirectories += (`ledger-common-dars-lf-v2-1` / Compile / resourceManaged).value,
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
          grpc_netty,
          scalapb_runtime,
          daml_libs_scala_ports % Test,
          scalatest % Test,
          scalacheck % Test,
        ),
        Test / parallelExecution := true,
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
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
          sharedSettings,
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
          "semantic",
          "ongoing_stream_package_upload",
          "package_management",
          "carbonv1",
          "carbonv2",
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
      ),
      Compile / damlBuildOrder := Seq(
        // define the packages that have a dependency in the right order, the omitted will be compiled before those listed
        "carbonv1",
        "carbonv2",
      ),
    )

    lazy val `ledger-common-dars-lf-v2-1` = createLedgerCommonDarsProject(lfVersion = "2.1")
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
        `daml-errors` % "test->test",
        `daml-tls` % "test->test",
        `ledger-common` % "compile->compile;test->test",
        `community-common` % "compile->compile;test->test",
        `daml-adjustable-clock` % "test->test",
        DamlProjects.`daml-jwt`,
      )
      .settings(
        sharedCantonSettings,
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
          anorm,
          daml_lf_encoder % Test,
          daml_libs_scala_grpc_test_utils % Test,
          daml_observability_tracing_test_lib % Test,
          daml_rs_grpc_testing_utils % Test,
          scalapb_json4s % Test,
        ),
        Test / parallelExecution := true,
        Test / fork := false,
        Test / testGrouping := separateRevocationTest((Test / definedTests).value),
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
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
          sharedSettings,
          libraryDependencies ++= Seq(
            circe_parser,
            upickle,
            ujson_circe,
            tapir_json_circe,
            tapir_pekko_http_server,
            tapir_openapi_docs,
            tapir_asyncapi_docs,
            daml_lf_api_type_signature,
            daml_lf_transaction_test_lib,
            daml_observability_pekko_http_metrics,
            daml_timer_utils,
            pekko_http,
            sttp_apiscpec_openapi_circe_yaml,
            sttp_apiscpec_asyncapi_circe_yaml,
            scalapb_json4s,
            daml_libs_scala_scalatest_utils % Test,
            pekko_stream_testkit % Test,
            circe_yaml % Test,
          ),
          coverageEnabled := false,
          JvmRulesPlugin.damlRepoHeaderSettings,
          Test / damlCodeGeneration := Seq(
            (
              (Test / sourceDirectory).value / "daml" / "v2_1",
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

    lazy val `ledger-api-tools` = project
      .in(file("community/ledger/ledger-api-tools"))
      .dependsOn(
        `community-testing`,
        `ledger-api-core`,
        DamlProjects.`daml-jwt`,
      )
      .settings(
        sharedCantonSettings,
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `ledger-api-string-interning-benchmark` = project
      .in(file("community/ledger/ledger-api-string-interning-benchmark"))
      .enablePlugins(JmhPlugin)
      .dependsOn(`ledger-api-core`)
      .settings(
        sharedCantonSettings,
        JvmRulesPlugin.damlRepoHeaderSettings,
        Test / parallelExecution := true,
        Test / fork := false,
      )

    lazy val `transcode` =
      project
        .in(file("community/ledger/transcode/"))
        .settings(
          sharedSettings,
          scalacOptions --= DamlProjects.removeCompileFlagsForDaml
            // needed for foo.bar.{this as that} imports
            .filterNot(_ == "-Xsource:3"),
          libraryDependencies ++= Seq(
            daml_lf_language,
            "com.lihaoyi" %% "ujson" % "4.0.2",
          ),
          JvmRulesPlugin.damlRepoHeaderSettings,
        )
        .dependsOn(DamlProjects.`ledger-api`)
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
      .in(file("daml-common-staging/daml-jwt"))
      .disablePlugins(WartRemover)
      .settings(
        sharedSettings,
        scalacOptions += "-Wconf:src=src_managed/.*:silent",
        libraryDependencies ++= Seq(
          auth0_java,
          auth0_jwks,
          daml_http_test_utils % Test,
          daml_libs_struct_spray_json,
          daml_test_evidence_generator_scalatest % Test,
          scalatest % Test,
          scalaz_core,
          slf4j_api,
        ),
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
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
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          daml_ledger_api_value % "protobuf"
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
        sharedSettings,
        scalacOptions --= removeCompileFlagsForDaml,
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
          "--doc_opt=" + (Compile / baseDirectory).value.getAbsolutePath + "/docs/rst_mmd.tmpl," + "proto-docs.rst"
        ),
        Compile / unmanagedResources += (ThisBuild / baseDirectory).value / "community/ledger-api/VERSION",
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          daml_ledger_api_value_scala,
          scalapb_runtime,
          scalapb_runtime_grpc,
          protoc_gen_doc asProtocPlugin (),
        ),
      )

    lazy val `bindings-java` = project
      .in(file("community/bindings-java"))
      .dependsOn(
        `ledger-api`
      )
      .settings(
        sharedSettings,
        compileOrder := CompileOrder.JavaThenScala,
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
        ),
      )
  }
}
