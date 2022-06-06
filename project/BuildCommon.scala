import DamlPlugin.autoImport._
import Dependencies._
import com.typesafe.sbt.SbtLicenseReport.autoImportImpl._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headerSources
import org.scalafmt.sbt.ScalafmtPlugin
import sbt.Keys._
import sbt.internal.util.ManagedLogger
import sbt._
import sbt.nio.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assembly
import sbtassembly.MergeStrategy
import sbtassembly.PathList
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtprotoc.ProtocPlugin.autoImport.PB
import scoverage.ScoverageKeys._
import wartremover.WartRemover
import wartremover.WartRemover.autoImport._

import scala.language.postfixOps

object BuildCommon {

  lazy val sbtSettings: Seq[Def.Setting[_]] = {

    def alsoTest(taskName: String) = s";$taskName; Test / $taskName"

    val commandAliases =
      addCommandAlias("checkDamlProjectVersions", alsoTest("damlCheckProjectVersions")) ++
        addCommandAlias("updateDamlProjectVersions", alsoTest("damlUpdateProjectVersions")) ++
        addCommandAlias("checkLicenseHeaders", alsoTest("headerCheck")) ++
        addCommandAlias("createLicenseHeaders", alsoTest("headerCreate")) ++
        addCommandAlias(
          "lint",
          "; scalafmtCheck ; Test / scalafmtCheck ; scalafmtSbtCheck; checkLicenseHeaders; checkDamlProjectVersions",
        ) ++
        addCommandAlias(
          "format",
          "; scalafmt ; Test / scalafmt ; scalafmtSbt; createLicenseHeaders",
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
        resolvers ++= Seq(Dependencies.use_custom_daml_version).collect { case true =>
          sbt.librarymanagement.Resolver.mavenLocal // conditionally enable local maven repo for custom Daml jars
        },
        // , scalacOptions += "-Ystatistics" // re-enable if you need to debug compile times
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
      Global / excludeLintKeys += Compile / damlBuildOrder,
      Global / excludeLintKeys += `community-app` / Compile / damlCompileDirectory,
      Global / excludeLintKeys += `functionmeta` / wartremoverErrors,
      Global / excludeLintKeys += `daml-fork` / wartremoverErrors,
      Global / excludeLintKeys += `blake2b` / autoAPIMappings,
      Global / excludeLintKeys += `community-app` / autoAPIMappings,
      Global / excludeLintKeys += `community-common` / autoAPIMappings,
      Global / excludeLintKeys += `community-domain` / autoAPIMappings,
      Global / excludeLintKeys += `community-participant` / autoAPIMappings,
      Global / excludeLintKeys += `demo` / autoAPIMappings,
      Global / excludeLintKeys += `functionmeta` / autoAPIMappings,
      Global / excludeLintKeys += `slick-fork` / autoAPIMappings,
      Global / excludeLintKeys += `daml-fork` / autoAPIMappings,
      Global / excludeLintKeys += Global / damlCodeGeneration,
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

  def formatCoverageExcludes(excludes: String): String =
    excludes.stripMargin.trim.split(System.lineSeparator).mkString(";")

  def packDamlSources(damlSource: File, target: String): Seq[(File, String)] = {
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
  }

  def packDars(darOutput: File, target: String): Seq[(File, String)] = {
    (darOutput * "*.dar").get
      .map { f =>
        (f, s"$target${Path.sep}${f.getName}")
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
  lazy val bundleRef = taskKey[Unit]("create a release bundle referenceable task")

  lazy val enterpriseGeneratedPack = "release/tmp/pack"

  lazy val additionalBundleSources =
    taskKey[Seq[(File, String)]]("Bundle these additional sources")

  /** Generate a release bundle including the demo */
  lazy val bundleTask = {
    import CommunityProjects.`community-common`
    lazy val bundle = taskKey[Unit]("create a release bundle")
    bundle := {

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
      log.info("Invoking bundle generator")
      // add license to package
      val renames =
        releaseNotes ++ licenseFiles ++ demoSource ++ demoDars ++ demoJars ++ demoArtefacts ++ damlSampleSource ++ damlSampleDars ++ additionalBundleSources.value
      val args = bundlePack.value ++ renames.flatMap(x => Seq("-r", x._1.toString, x._2))
      runCommand(
        f"bash ./scripts/ci/create-bundle.sh ${assembly.value.toString} ${args.mkString(" ")}",
        log,
      )
    }
  }

  def mergeStrategy(oldStrategy: String => MergeStrategy): String => MergeStrategy = {
    {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case "reflect.properties" => MergeStrategy.first
      case PathList("org", "checkerframework", _ @_*) => MergeStrategy.first
      case PathList("google", "protobuf", _*) => MergeStrategy.first
      case PathList("org", "apache", "logging", _*) => MergeStrategy.first
      case PathList("ch", "qos", "logback", _*) => MergeStrategy.first
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
      case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
      case path if path.contains("module-info.class") => MergeStrategy.discard
      case PathList("org", "jline", _ @_*) => MergeStrategy.first
      case x => oldStrategy(x)
    }
  }

  // applies to all sub-projects
  lazy val sharedSettings = Seq(
    printTestTask,
    unitTestTask,
    oracleUnitTestTask,
  )

  lazy val cantonWarts = Seq(
    wartremoverErrors += Wart.custom("com.digitalasset.canton.DiscardedFuture"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.RequireBlocking"),
    wartremoverErrors += Wart.custom("com.digitalasset.canton.SlickString"),
    wartremover.WartRemover.dependsOnLocalProjectWarts(CommunityProjects.`wartremover-extension`),
  ).flatMap(_.settings)

  // applies to all Canton-based sub-projects (descendants of community-common)
  lazy val sharedCantonSettings = sharedSettings ++ cantonWarts ++ Seq(
    // Enable logging of begin and end of test cases, test suites, and test runs.
    Test / testOptions += Tests.Argument("-C", "com.digitalasset.canton.LogReporter"),
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
    bundleTask,
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
  )

  /** By default, sbt-header (github.com/sbt/sbt-header) will not check the /protobuf directories, so we manually need to add them here.
    *  Fix is similar to https://github.com/sbt/sbt-header#sbt-boilerplate
    */
  def addProtobufFilesToHeaderCheck(conf: Configuration) = {
    conf / headerSources ++= (conf / PB.protoSources).value.flatMap(pdir => (pdir ** "*.proto").get)
  }

  def addFilesToHeaderCheck(filePattern: String, relativeDir: String, conf: Configuration) =
    conf / headerSources ++= (((conf / sourceDirectory).value / relativeDir) ** filePattern).get

  object CommunityProjects {

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
          daml_ledger_rxjava_client % Test,
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
      .dependsOn(blake2b, functionmeta, `slick-fork`, `wartremover-extension`)
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          akka_slf4j, // not used at compile time, but required by com.digitalasset.canton.util.AkkaUtil.createActorSystem
          daml_lf_archive_reader,
          daml_lf_engine,
          daml_lf_value_java_proto % "protobuf", // needed for protobuf import
          daml_lf_transaction, //needed for importing java classes
          daml_metrics,
          daml_error,
          daml_error_generator,
          daml_participant_state, //needed for ReadService/Update classes by PrettyInstances
          daml_ledger_api_common,
          daml_ledger_api_client,
          daml_nonempty_cats,
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
          daml_lf_transaction % Test,
          daml_lf_transaction_test_lib % Test,
          daml_test_evidence_tag % Test,
          daml_test_evidence_scalatest % Test,
          daml_test_evidence_generator_scalatest % Test,
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
        ),
        dependencyOverrides ++= Seq(log4j_core, log4j_api),
        Compile / PB.targets := Seq(
          scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "protobuf"
        ),
        Compile / PB.protoSources ++= (Test / PB.protoSources).value,
        buildInfoKeys := Seq[BuildInfoKey](
          version,
          scalaVersion,
          sbtVersion,
          BuildInfoKey("damlLibrariesVersion" -> Dependencies.daml_libraries_version),
          BuildInfoKey("vmbc" -> Dependencies.daml_libraries_version),
          BuildInfoKey("protocolVersions" -> List("2.0.0", "3.0.0")),
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
      .dependsOn(`community-common` % "compile->compile;test->test")
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
      .dependsOn(`community-common` % "compile->compile;test->test", `daml-fork`)
      .enablePlugins(DamlPlugin)
      .settings(
        sharedCantonSettings,
        libraryDependencies ++= Seq(
          scala_logging,
          scalatest % Test,
          scalatestScalacheck % Test,
          scalacheck % Test,
          daml_lf_archive_reader,
          daml_lf_dev_archive_java_proto,
          daml_lf_engine,
          daml_ledger_api_auth_client,
          daml_participant_integration_api,
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
          )
        ),
        damlFixedDars := Seq("AdminWorkflows.dar"),
        addProtobufFilesToHeaderCheck(Compile),
        addFilesToHeaderCheck("*.daml", "daml", Compile),
        JvmRulesPlugin.damlRepoHeaderSettings,
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
          mockito_scala % Test,
          scalatestMockito % Test,
          scalatest % Test,
          slick,
          wartremover_dep,
        ),
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

}
