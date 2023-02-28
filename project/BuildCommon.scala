import DamlPlugin.autoImport._
import Dependencies._
import better.files.{File => BetterFile, _}
import com.typesafe.sbt.SbtLicenseReport.autoImportImpl._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{headerResources, headerSources}
import org.scalafmt.sbt.ScalafmtPlugin
import sbt.Keys._
import sbt._
import sbt.internal.BuildDependencies
import sbt.internal.librarymanagement.ProjectResolver
import sbt.internal.util.ManagedLogger
import sbt.nio.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assembly
import sbtassembly.{MergeStrategy, PathList}
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtprotoc.ProtocPlugin.autoImport.PB
import scalafix.sbt.ScalafixPlugin
import scoverage.ScoverageKeys._
import wartremover.WartRemover
import wartremover.WartRemover.autoImport._

import java.nio.file.{Files, StandardCopyOption}
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
          "; scalafmtCheck ; Test / scalafmtCheck ; scalafmtSbtCheck; checkLicenseHeaders",
        ) ++
        addCommandAlias(
          "scalafixCheck",
          s"${alsoTest("scalafix --check")}",
        ) ++
        addCommandAlias(
          "format",
          "; scalafmt ; Test / scalafmt ; scalafmtSbt; createLicenseHeaders",
        ) ++
        addCommandAlias(
          "formatAll",
          "; scalafixAll ; scalafmtAll ; scalafmtSbt; createLicenseHeaders",
        ) ++
        // To be used by CI:
        // enable coverage and compile
        addCommandAlias("compileWithCoverage", "; clean; coverage; test:compile") ++
        // collect coverage information (once the tests have terminated)
        addCommandAlias("collectCoverage", "; coverageReport; coverageAggregate") ++
        // To be used locally
        // test coverage from just unit tests
        addCommandAlias("unitTestCoverage", "; compileWithCoverage; unitTest; collectCoverage")

    val buildSettings = inThisBuild(
      Seq(
        organization := "com.digitalasset.canton",
        scalaVersion := scala_version,
        resolvers := Seq(Dependencies.use_custom_daml_version).collect { case true =>
          sbt.librarymanagement.Resolver.mavenLocal // conditionally enable local maven repo for custom Daml jars
        } ++ Seq(
          "Redhat GA for s390x natives" at "https://maven.repository.redhat.com/ga"
        ) ++ resolvers.value,
        // scalacOptions += "-Ystatistics", // re-enable if you need to debug compile times
        // scalacOptions in Test += "-Ystatistics",
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
      Global / excludeLintKeys += `functionmeta` / wartremoverErrors,
      Global / excludeLintKeys ++= (CommunityProjects.allProjects ++ DamlProjects.allProjects)
        .map(
          _ / autoAPIMappings
        ),
      Global / excludeLintKeys += Global / damlCodeGeneration,
      Global / excludeLintKeys ++= DamlProjects.allProjects.map(_ / wartremoverErrors),
    )

    // Inspired by https://www.viget.com/articles/two-ways-to-share-git-hooks-with-your-team/
    val initDev =
      taskKey[Unit]("initialize your local repo with common development settings")

    val initDevSettings = Seq(
      initDev := {
        import scala.sys.process._
        "git config core.hooksPath .hooks" !
      }
    )

    buildSettings ++ globalSettings ++ initDevSettings ++ commandAliases
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
            log.info(s"RUNNING ${x}")
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
    val unitTest = taskKey[Unit]("Run all unit tests (excluding Oracle based tests)")
    unitTest := mkTestJob(n =>
      !n.startsWith("com.digitalasset.canton.integration.tests") && !n.endsWith("Oracle")
    ).value
  }

  lazy val oracleUnitTestTask = {
    val oracleUnitTest = taskKey[Unit]("Run all Oracle based unit tests")
    oracleUnitTest := mkTestJob(n =>
      !n.startsWith("com.digitalasset.canton.integration.tests") && n.endsWith("Oracle")
    ).value
  }

  def runCommand(command: String, log: ManagedLogger, optError: Option[String] = None): String = {
    import scala.sys.process.Process
    val processLogger = new DamlPlugin.BufferedLogger
    log.debug(s"Running ${command}")
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

  // https://tanin.nanakorn.com/technical/2018/09/10/parallelise-tests-in-sbt-on-circle-ci.html
  lazy val printTestTask = {
    val printTests =
      taskKey[Unit]("Print full class names of tests to the file `test-full-class-names.log`.")
    printTests := {
      import java.io._
      println("Appending full class names of tests to the file `test-full-class-names.log`.")
      val pw = new PrintWriter(new FileWriter(s"test-full-class-names.log", true))
      val tmp = (Test / definedTests).value
      tmp.sortBy(_.name).foreach { t =>
        pw.println(t.name)
      }
      pw.close()
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
      val communityCommonProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "common",
        "community",
      )
      val communityParticipantProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "participant",
        "participant",
      )
      val communityDomainProto: Seq[(File, String)] = packProtobufFiles(
        "community" / "domain",
        "domain",
      )
      val researchAppProto: Seq[(File, String)] =
        if (moduleName.value == "research-app")
          packProtobufFiles(
            "research" / "app",
            "research",
          )
        else Nil

      val protoFiles =
        communityCommonProto ++ communityParticipantProto ++ communityDomainProto ++ researchAppProto

      log.info("Invoking bundle generator")
      // add license to package
      val renames =
        releaseNotes ++ licenseFiles ++ demoSource ++ demoDars ++ demoJars ++ demoArtefacts ++ damlSampleSource ++ damlSampleDars ++ additionalBundleSources.value ++ protoFiles
      val args = bundlePack.value ++ renames.flatMap(x => Seq("-r", x._1.toString, x._2))
      // build the canton fat-jar
      val assembleJar = assembly.value
      runCommand(
        f"bash ./scripts/ci/create-bundle.sh ${assembleJar.toString} ${(assembly / mainClass).value.get} ${args
            .mkString(" ")}",
        log,
      )
    }

  def mergeStrategy(oldStrategy: String => MergeStrategy): String => MergeStrategy = {
    case PathList("buf.yaml") => MergeStrategy.discard
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case "reflect.properties" => MergeStrategy.first
    case PathList("org", "checkerframework", _ @_*) => MergeStrategy.first
    case PathList("google", "protobuf", _*) => MergeStrategy.first
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
    case (PathList("akka", "stream", "scaladsl", broadcasthub, _*))
        if broadcasthub.startsWith("BroadcastHub") =>
      MergeStrategy.first
    case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
    case path if path.contains("module-info.class") => MergeStrategy.discard
    case PathList("org", "jline", _ @_*) => MergeStrategy.first
    case x => oldStrategy(x)
  }

  // applies to all sub-projects
  lazy val sharedSettings = Seq(
    printTestTask,
    unitTestTask,
    oracleUnitTestTask,
  )

  lazy val cantonWarts = Seq(
    wartremoverErrors += Wart.custom("com.digitalasset.canton.DiscardedFuture"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.FutureTraverse"),
    // NonUnitForEach is too aggressive for integration tests where we often ignore the result of console commands
    Compile / compile / wartremoverErrors += Wart.custom("com.digitalasset.canton.NonUnitForEach"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.RequireBlocking"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.SlickString"),
    wartremover.WartRemover.dependsOnLocalProjectWarts(CommunityProjects.`wartremover-extension`),
  ).flatMap(_.settings)

  // applies to all Canton-based sub-projects (descendants of util-external)
  lazy val sharedCantonSettings: Seq[Def.Setting[_]] =
    sharedCantonSettingsWithoutTestArguments ++ Seq(
      // Enable logging of begin and end of test cases, test suites, and test runs.
      Test / testOptions += Tests.Argument("-C", "com.digitalasset.canton.LogReporter")
    )

  // used by ``integration-testing-toolkit-junit`` since ``-C`` seems to interfere with ``sbt-jupiter-interface``
  lazy val sharedCantonSettingsWithoutTestArguments: Seq[Def.Setting[_]] =
    sharedSettings ++ cantonWarts ++ Seq(
      // Ignore daml codegen generated files from code coverage
      coverageExcludedFiles := formatCoverageExcludes(
        """
        |<empty>
        |.*sbt-buildinfo.BuildInfo
        |.*daml-codegen.*
      """
      ),
      scalacOptions += "-Wconf:src=src_managed/.*:silent",
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
      `integration-testing-toolkit-junit`,
      `util-external`,
      `util-internal`,
      `community-app`,
      `community-common`,
      `community-domain`,
      `community-participant`,
      `sequencer-driver-api`,
      `sequencer-driver-lib`,
      blake2b,
      functionmeta,
      `slick-fork`,
      `wartremover-extension`,
      `akka-fork`,
      `demo`,
    )

    lazy val `integration-testing-toolkit-junit` = project
      .in(file("community/integration-testing-toolkit-junit"))
      .settings(
        sharedCantonSettingsWithoutTestArguments,
        JvmRulesPlugin.damlRepoHeaderSettings,
        crossPaths := false,
        libraryDependencies ++= Seq(
          junit_api,
          junit % Test,
          junit_engine % Test,
          jupiter_interface % Test,
        ),
      )
      .dependsOn(
        `community-app` // required for: Environment, CommunityEnvironment
      )

    // Project for utilities that are also used outside of the Canton repo
    lazy val `util-external` = project
      .in(file("community/util-external"))
      .dependsOn(
        `akka-fork`,
        `wartremover-extension` % "compile->compile;test->test",
        DamlProjects.`daml-copy-common`,
        DamlProjects.`daml-copy-testing` % "test->test",
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
          cats,
          jul_to_slf4j % Test,
          log4j_core,
          log4j_api,
          monocle_macro, // Include it here, even if unused, so that it can be used everywhere
          pureconfig, // Only dependencies may be needed, but it is simplest to include it like this
          opentelemetry_api,
          opentelemetry_sdk,
          opentelemetry_sdk_autoconfigure,
          opentelemetry_instrumentation_grpc,
          opentelemetry_zipkin,
          opentelemetry_jaeger,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    // Project for general utilities used inside the Canton repo only
    lazy val `util-internal` = project
      .in(file("community/util"))
      .dependsOn(
        `util-external`
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
          cats,
          cats_law % Test,
          jul_to_slf4j % Test,
          log4j_core,
          log4j_api,
          monocle_macro, // Include it here, even if unused, so that it can be used everywhere
          pureconfig, // Only dependencies may be needed, but it is simplest to include it like this
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-app` = project
      .in(file("community/app"))
      .dependsOn(
        `community-common` % "compile->compile;test->test",
        `community-domain`,
        `community-participant`,
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedAppSettings,
        libraryDependencies ++= Seq(
          scala_logging,
          jul_to_slf4j,
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
          akka_stream_testkit % Test,
          akka_http,
          akka_http_testkit % Test,
          pureconfig_cats,
          cats,
          ammonite,
          better_files,
          toxiproxy_java % Test,
          dropwizard_metrics_jvm, // not used at compile time, but required at runtime to report jvm metrics
          dropwizard_metrics_jmx,
          dropwizard_metrics_graphite,
        ),
        // core packaging commands
        bundlePack := sharedAppPack,
        additionalBundleSources := Seq.empty,
        assembly / mainClass := Some("com.digitalasset.canton.CantonCommunityApp"),
        assembly / assemblyJarName := s"canton-open-source-${version.value}.jar",
        // specifying the damlSourceDirectory to non-default location enables checking/updating of daml version
        Compile / damlSourceDirectory := sourceDirectory.value / "pack" / "examples" / "06-messaging",
        // clearing the damlBuild tasks to prevent compiling which does not work due to relative file "data-dependencies";
        // "data-dependencies" daml.yaml setting relies on hardcoded "0.0.1" project version
        Compile / damlBuild := Seq(), // message-0.0.1.dar is hardcoded and contact-0.0.1.dar is built by MessagingExampleIntegrationTest
        Compile / damlProjectVersionOverride := Some("0.0.1"),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.sh", "../pack", Compile),
        addFilesToHeaderCheck("*.sh", ".", Test),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-common` = project
      .in(file("community/common"))
      .enablePlugins(BuildInfoPlugin, DamlPlugin)
      .dependsOn(
        blake2b,
        functionmeta,
        `slick-fork`,
        `akka-fork`,
        `wartremover-extension` % "compile->compile;test->test",
        DamlProjects.`daml-copy-common`,
        DamlProjects.`daml-copy-testing` % "test;test->test",
        `util-external` % "compile->compile;test->test",
        `util-internal` % "compile->compile;test->test",
      )
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          akka_slf4j, // not used at compile time, but required by com.digitalasset.canton.util.AkkaUtil.createActorSystem
          logback_classic,
          logback_core,
          scala_logging,
          scala_collection_contrib,
          scalatest % Test,
          scalacheck % Test,
          scalatestScalacheck % Test,
          cats_scalacheck % Test,
          mockito_scala % Test,
          scalatestMockito % Test,
          better_files,
          cats,
          cats_law % Test,
          chimney,
          circe_core,
          circe_generic,
          circe_generic_extras,
          jul_to_slf4j % Test,
          bouncycastle_bcprov_jdk15on,
          bouncycastle_bcpkix_jdk15on,
          grpc_netty,
          netty_native,
          netty_native_s390,
          grpc_services,
          scalapb_runtime_grpc,
          scalapb_runtime,
          log4j_core,
          log4j_api,
          flyway excludeAll (ExclusionRule("org.apache.logging.log4j")),
          h2,
          tink,
          slick,
          slick_hikaricp,
          testcontainers % Test,
          testcontainers_postgresql % Test,
          postgres,
          sttp,
          sttp_okhttp,
          sttp_circe,
          monocle_macro, // Include it here, even if unused, so that it can be used everywhere
          pprint,
          pureconfig, // Only dependencies may be needed, but it is simplest to include it like this
          dropwizard_metrics_core,
          prometheus_dropwizard, // Include it here to overwrite transitive dependencies by DAML libraries
          prometheus_httpserver, // Include it here to overwrite transitive dependencies by DAML libraries
          prometheus_hotspot, // Include it here to overwrite transitive dependencies by DAML libraries
          opentelemetry_api,
          opentelemetry_sdk,
          opentelemetry_sdk_autoconfigure,
          opentelemetry_instrumentation_grpc,
          opentelemetry_zipkin,
          opentelemetry_jaeger,
          scaffeine,
          aws_kms,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        // Ensure the package scoped options will be picked up by sbt-protoc if used downstream
        // See https://scalapb.github.io/docs/customizations/#publishing-package-scoped-options
        Compile / packageBin / packageOptions += (
          Package.ManifestAttributes(
            "ScalaPB-Options-Proto" -> "com/digitalasset/canton/scalapb/package.proto"
          )
        ),
        Compile / PB.protoSources ++= (Test / PB.protoSources).value,
        buildInfoKeys := Seq[BuildInfoKey](
          version,
          scalaVersion,
          sbtVersion,
          BuildInfoKey("damlLibrariesVersion" -> Dependencies.daml_libraries_version),
          BuildInfoKey("protocolVersions" -> List("3", "4")),
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
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml" / "CantonExamples",
            (Compile / damlDarOutput).value / "CantonExamples.dar",
            "com.digitalasset.canton.examples",
          )
        ),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    lazy val `community-domain` = project
      .in(file("community/domain"))
      .dependsOn(
        `community-common` % "compile->compile;test->test",
        `akka-fork`,
      )
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
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
          oracle,
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
        `community-common` % "compile->compile;test->test",
        DamlProjects.`daml-copy-common`,
        DamlProjects.`daml-copy-participant`,
        DamlProjects.`daml-copy-testing` % "test",
        DamlProjects.`daml-copy-testing` % "test->test",
      )
      .enablePlugins(DamlPlugin)
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          scala_logging,
          scalatest % Test,
          scalatestScalacheck % Test,
          scalacheck % Test,
          logback_classic % Runtime,
          logback_core % Runtime,
          akka_stream,
          akka_stream_testkit % Test,
          cats,
          chimney,
          scalapb_runtime, // not sufficient to include only through the `common` dependency - race conditions ensue
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
        Compile / damlCodeGeneration := Seq(
          (
            (Compile / sourceDirectory).value / "daml",
            (Compile / resourceDirectory).value / "dar" / "AdminWorkflows.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
          (
            (Compile / sourceDirectory).value / "daml" / "ping-pong-vacuum",
            (Compile / resourceManaged).value / "AdminWorkflowsWithVacuuming.dar",
            "com.digitalasset.canton.participant.admin.workflows",
          ),
        ),
        damlFixedDars := Seq("AdminWorkflows.dar"),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

    // Project for specifying the sequencer driver API
    lazy val `sequencer-driver-api` = project
      .in(file("community/sequencer-driver"))
      .dependsOn(`util-external`)
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
          pureconfig,
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        JvmRulesPlugin.damlRepoHeaderSettings,
        sequencerDriverAssemblySettings,
      )

    val sequencerDriverAssemblySettings = Seq(
      // Conforming to the Maven dependency naming convention.
      assembly / assemblyJarName := s"sequencer-driver-lib_$scala_version_short-${version.value}.jar",
      // Do not assembly dependencies other than local projects.
      assemblyPackageDependency / assembleArtifact := false,
      assemblyPackageScala / assembleArtifact := false,
    )

    /** The project below is a ~proxy project made for the purpose of the Drivers team.
      * It is intented to be a temporary solution once canton repo gets reorganized. This project
      * contains sequencer driver API, it's dependencies and utilities - all required by existing drivers.
      * Now, in order to split repositories we have to publish minimal dependency set to artifactory
      * (see here: https://github.com/sbt/sbt-assembly#q-despite-the-concerned-friends-i-still-want-publish-%C3%BCber-jars-what-advice-do-you-have).
      * However, the problem arises from the fact that sequencer-driver is using util-external which
      * is using daml-copy-common, akka-fork etc. This chain of internal dependencies is causing the
      * need to publish more and more jars to the artifactory which we do not want to do (at least
      * now, since there's no 'publish' infrastructure ready yet). Ad hoc solution, which we've
      * found, is to create (and publish) a 'fat jar' (which is not so fat - 15MB...) with all the
      * internal deps placed inside of it. Otherwise, we would have to republish e.g. bunch of Daml classes
      * already present elsewhere in the Artifactory (daml-copy-common module).
      * Unfortunatelly, we also need to manually create a pom file for such a jar.
      * That means we have to traverse all the internal dependencies of `sequencer-driver-api`
      * and obtain their 'libraryDependencies' which should land in the final pom
      * file. Sbt doesn't allow to do that - it throws the exception when evaluating macros
      * (https://github.com/sbt/sbt/issues/6792).
      * The final solution is using `projectDependencies` task to setup all the transitive
      * external dependencies (libraryDependencies is a settingKey which has to be evaluated
      * during load phase, therefore cannot be used here). However, `projectDescriptors` (used here)
      * still contains internal deps which we're filtering out here (by organization (e.g.
      * com.digitalasset.canton) and version (e.g. 2.6.0-SNAPSHOT)).
      */
    lazy val `sequencer-driver-lib`: Project = project
      .settings(
        sharedCantonSettings,
        projectDependencies := {
          val thisOrg = (`sequencer-driver-api` / organization).value
          val thisVer = (`sequencer-driver-api` / version).value
          for {
            moduleDescriptor <- (`sequencer-driver-api` / projectDescriptors).value.values.toList
            dependency <- moduleDescriptor.getDependencies.toList
            revisionId = dependency.getDependencyRevisionId
            org = revisionId.getOrganisation if org != thisOrg
            name = revisionId.getName
            version = revisionId.getRevision() if version != thisVer
          } yield ModuleID(org, name, version)
        },
        Compile / packageBin := Def.task {
          val src = (`sequencer-driver-api` / assembly).value.toPath
          val dest = (Compile / packageBin / artifactPath).value.toPath
          Files.createDirectories(dest.getParent())
          Files
            .copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
            .toFile()
        }.value,
        Compile / packageSrc := Def.task {
          val src = (`sequencer-driver-api` / Compile / packageSrc).value.toPath
          val dest = (Compile / packageSrc / artifactPath).value.toPath
          Files.deleteIfExists(dest)
          Files.copy(src, dest).toFile()
        }.value,
        Compile / packageDoc := Def.task {
          val src = (`sequencer-driver-api` / Compile / packageDoc).value.toPath
          val dest = (Compile / packageDoc / artifactPath).value.toPath
          Files.deleteIfExists(dest)
          Files.copy(src, dest).toFile()
        }.value,
      )

    lazy val blake2b = project
      .in(file("community/lib/Blake2b"))
      .disablePlugins(ScalafmtPlugin, WartRemover)
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

    lazy val functionmeta = project
      .in(file("community/lib/functionmeta"))
      .disablePlugins(ScalafmtPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          scala_reflect,
          scalatest % Test,
          shapeless % Test,
        ),
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
        coverageEnabled := false,
      )

    lazy val `slick-fork` = project
      .in(file("community/lib/slick"))
      .disablePlugins(ScalafmtPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          scala_reflect,
          slick,
        ),
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
        coverageEnabled := false,
      )

    lazy val `wartremover-extension` = project
      .in(file("community/lib/wartremover"))
      .dependsOn(`slick-fork`)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          cats,
          mockito_scala % Test,
          scalatestMockito % Test,
          scalatest % Test,
          slick,
          wartremover_dep,
        ),
      )

    // TODO(#10617) remove when no longer needed
    lazy val `akka-fork` = project
      .in(file("community/lib/akka"))
      .disablePlugins(ScalafixPlugin, ScalafmtPlugin, WartRemover)
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(
          akka_stream,
          akka_stream_testkit % Test,
          akka_slf4j,
          scalatest % Test,
        ),
        // Exclude to apply our license header to any Scala files
        headerSources / excludeFilter := "*.scala",
        coverageEnabled := false,
      )

    lazy val `demo` = project
      .in(file("community/demo"))
      .enablePlugins(DamlPlugin)
      .dependsOn(`community-app` % "compile->compile;test->test")
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

  }

  object DamlProjects {

    lazy val damlFolder = Def.setting((ThisBuild / baseDirectory).value / "daml")

    lazy val allProjects = Set(
      `daml-copy-macro`,
      `daml-fork`,
      `daml-copy-protobuf`,
      `daml-copy-protobuf-java`,
      `google-common-protos-scala`,
      `daml-copy-common`,
      `daml-copy-testing`,
      `daml-copy-participant`,
    )

    lazy val removeCompileFlagsForDaml =
      Seq("-Xsource:3", "-deprecation", "-Xfatal-warnings", "-Ywarn-unused", "-Ywarn-value-discard")

    lazy val `daml-copy-macro` = project
      .in(file("community/lib/daml-copy-marco"))
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      ) // to accommodate different daml repo coding style
      .settings(
        scalacOptions --= removeCompileFlagsForDaml,
        sharedSettings,
        libraryDependencies ++= Seq(
          scala_reflect,
          scala_compiler,
        ),
        Compile / unmanagedSourceDirectories ++=
          Seq( // TODO(#10852) same purpose as function meta
            "libs-scala/nameof/src/main/scala"
          ).map(f => damlFolder.value / f),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
      )

    // this project builds daml protobuf objects where
    // we only need the java classes
    lazy val `daml-copy-protobuf-java` = project
      .in(file("community/lib/daml-copy-protobuf-java"))
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      )
      .settings(
        scalacOptions --= removeCompileFlagsForDaml,
        sharedSettings,
        dependencyOverrides ++= Seq(),
        Compile / PB.protoSources ++= Seq(
          "daml-lf/archive/src/main/protobuf",
          "daml-lf/transaction/src/main/protobuf",
        ).map(f => damlFolder.value / f),
        Compile / PB.targets := Seq(
          PB.gens.java -> (Compile / sourceManaged).value
        ),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          scalapb_runtime
        ),
      )

    // this project builds scala protobuf versions that include
    // java conversions of a few google standard items
    // the google protobuf files are extracted from the provided jar files
    lazy val `google-common-protos-scala` = project
      .in(file("community/lib/google-common-protos-scala"))
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      )
      .settings(
        scalacOptions --= removeCompileFlagsForDaml,
        sharedSettings,
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
        ),
      )

    lazy val `daml-copy-protobuf` = project
      .in(file("community/lib/daml-copy-protobuf"))
      .dependsOn(`google-common-protos-scala`)
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      )
      .settings(
        scalacOptions --= removeCompileFlagsForDaml,
        sharedSettings,
        dependencyOverrides ++= Seq(),
        // Without this, we get conflicts when the protofiles from the below sources
        // are copied by sbt to the target directory
        excludeFilter := HiddenFileFilter || "*.bazel" || "scalapb.proto",
        Compile / PB.protoSources ++= Seq(
          "ledger-api/grpc-definitions",
          "ledger/participant-integration-api/src/main/protobuf",
          "ledger/ledger-configuration/protobuf",
        ).map(f => damlFolder.value / f),
        Compile / PB.targets := Seq(
          // build java codegen too
          PB.gens.java -> (Compile / sourceManaged).value,
          // build scala codegen with java conversions
          scalapb.gen(
            javaConversions = true,
            flatPackage = false, // consistent with upstream daml
          ) -> (Compile / sourceManaged).value,
        ),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
        libraryDependencies ++= Seq(
          scalapb_runtime,
          scalapb_runtime_grpc,
        ),
      )

    lazy val `daml-copy-common` = project
      .in(file("community/lib/daml-copy-common"))
      .dependsOn(`daml-copy-macro`, `daml-copy-protobuf`, `daml-copy-protobuf-java`)
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      ) // to accommodate different daml repo coding style
      .settings(
        scalacOptions --= removeCompileFlagsForDaml,
        sharedSettings,
        libraryDependencies ++= Seq(
          scopt,
          dropwizard_metrics_core,
          dropwizard_metrics_graphite,
          dropwizard_metrics_jvm,
          dropwizard_metrics_jmx,
          prometheus_dropwizard,
          prometheus_httpserver,
          opentelemetry_api,
          opentelemetry_sdk,
          opentelemetry_sdk_autoconfigure,
          opentelemetry_prometheus,
          opentelemetry_instrumentation_runtime_metrics,
          scalaz_core,
          akka_stream,
          log4j_core,
          log4j_api,
          slf4j_api,
          logstash,
          fasterjackson_core,
          grpc_api,
          grpc_netty,
          scalapb_runtime_grpc,
          spray,
          caffeine,
          cats,
          apache_commons_text,
          typelevel_paiges,
          commons_io,
          commons_codec,
          pureconfig,
        ),
        dependencyOverrides ++= Seq(),
        Compile / unmanagedSourceDirectories ++=
          Seq(
            "observability/metrics/src/main/scala",
            "observability/tracing/src/main/scala",
            "observability/telemetry/src/main/scala",
            "libs-scala/concurrent/src/main/scala",
            "libs-scala/executors/src/main/scala",
            "libs-scala/resources/src/main/2.13",
            "libs-scala/resources/src/main/scala",
            "libs-scala/resources-akka/src/main/scala",
            "libs-scala/resources-grpc/src/main/scala",
            "libs-scala/contextualized-logging/src/main/scala",
            "libs-scala/grpc-utils/src/main/scala",
            "libs-scala/logging-entries/src/main/scala",
            "libs-scala/timer-utils/src/main/scala",
            "libs-scala/nonempty/src/main/scala",
            "libs-scala/nonempty-cats/src/main/scala",
            "libs-scala/scala-utils/src/main/scala",
            "libs-scala/safe-proto/src/main/scala",
            "libs-scala/crypto/src/main/scala",
            "libs-scala/build-info/src/main/scala",
            "ledger-api/rs-grpc-bridge/src/main/java",
            "ledger-api/rs-grpc-akka/src/main/scala",
            "ledger/metrics/src/main/scala",
            "libs-scala/ledger-resources/src/main/scala",
            "ledger/error/src/main/scala",
            "ledger/ledger-offset/src/main/scala",
            "ledger/ledger-grpc/src/main/scala",
            "ledger/caching/src/main/scala",
            "ledger/ledger-api-domain/src/main/scala",
            // used by ledger-api-errors
            "ledger/participant-state/src/main/scala",
            "ledger/ledger-api-errors/src/main/scala",
            // used by participant state
            "ledger/ledger-configuration/src/main/scala",
            // used by ledger-api-common
            "ledger/ledger-api-health/src/main/scala",
            // depends on ledger-api-errors, needed for community-common/tls
            "ledger/ledger-api-common/src/main/scala",
            "language-support/scala/bindings/src/main/scala",
            "language-support/scala/bindings/src/main/2.13",
            // used by engine
            "daml-lf/archive/src/main/scala",
            "daml-lf/data/src/main/scala",
            "daml-lf/language/src/main/scala",
            "daml-lf/transaction/src/main/scala",
            // used by daml-error
            "daml-lf/engine/src/main/scala",
            "daml-lf/interpreter/src/main/scala",
            "daml-lf/validation/src/main/scala",
          ).map(f => damlFolder.value / f),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
      )

    lazy val `daml-copy-testing` = project
      .in(file("community/lib/daml-copy-testing"))
      .dependsOn(`daml-copy-common`)
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      ) // to accommodate different daml repo coding style
      .settings(
        sharedSettings,
        scalacOptions --= removeCompileFlagsForDaml,
        libraryDependencies ++= Seq(
          shapeless,
          scalacheck,
          scalaz_scalacheck,
          scala_logging,
          circe_core,
          circe_generic,
          circe_generic_extras,
          circe_parser,
          scalatest,
          better_files,
          totososhi,
          lihaoyi_sourcecode,
          logback_classic,
          logback_core,
        ),
        Compile / unmanagedSourceDirectories ++=
          Seq(
            "observability/metrics/src/test/lib/scala",
            "libs-scala/test-evidence/generator/src/main/scala",
            "libs-scala/test-evidence/scalatest/src/main/scala",
            "libs-scala/test-evidence/tag/src/main/scala",
            "daml-lf/api-type-signature/src/main/scala",
            "daml-lf/interface/src/main/scala",
            "daml-lf/data-scalacheck/src/main/scala",
            "daml-lf/transaction-test-lib/src/main/scala",
            "observability/metrics/src/test/lib/scala",
            "test-common/src/main/scala",
          ).map(f => damlFolder.value / f),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
      )
    lazy val `daml-copy-participant` = project
      .in(file("community/lib/daml-copy-participant"))
      .dependsOn(`daml-copy-common`)
      .disablePlugins(
        ScalafixPlugin,
        ScalafmtPlugin,
        WartRemover,
      ) // to accommodate different daml repo coding style
      .settings(
        sharedSettings,
        scalacOptions --= removeCompileFlagsForDaml,
        libraryDependencies ++= Seq(
          auth0_java,
          auth0_jwks,
          scala_logging,
          hikaricp,
          guava,
          dropwizard_metrics_core,
          grpc_netty,
          grpc_services,
          grpc_protobuf,
          postgres,
          h2,
          flyway,
          oracle,
          anorm,
          scalapb_json4s,
          reflections,
        ),
        Compile / unmanagedResourceDirectories ++= Seq(
          damlFolder.value / "ledger/participant-integration-api/src/main/resources"
        ),
        Compile / unmanagedSourceDirectories ++=
          Seq(
            "ledger/participant-local-store/src/main/scala",
            "ledger/ledger-api-auth/src/main/scala",
            "libs-scala/ports/src/main/scala",
            "libs-scala/build-info/src/main/scala",
            "libs-scala/jwt/src/main/scala",
            "libs-scala/struct-json/struct-spray-json/src/main/scala",
            "ledger/ledger-api-client/src/main/scala",
            "ledger/ledger-api-auth-client/src/main/java",
            "ledger/caching/src/main/scala",
            "ledger/participant-state-index/src/main/scala",
            "ledger/participant-state-metrics/src/main/scala",
            "ledger/participant-integration-api/src/main/scala",
            "ledger/error/generator/lib/src/main/scala",
          ).map(f => damlFolder.value / f),
        coverageEnabled := false,
        // skip header check
        headerSources / excludeFilter := HiddenFileFilter || "*",
        headerResources / excludeFilter := HiddenFileFilter || "*",
      )

    lazy val `daml-fork` = project
      .in(file("community/lib/daml"))
      .disablePlugins(WartRemover) // to accommodate different daml repo coding style
      .settings(
        sharedSettings,
        libraryDependencies ++= Seq(),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage =
            false // consistent with upstream daml
          ) -> (Compile / sourceManaged).value / "protobuf"
        ),
        coverageEnabled := false,
        JvmRulesPlugin.damlRepoHeaderSettings,
      )

  }

}
