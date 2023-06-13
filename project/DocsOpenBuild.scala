// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.Keys.{
  baseDirectory,
  definedTests,
  resourceDirectory,
  sourceDirectory,
  streams,
  testOnly,
  version,
}
import sbt.*
import BuildCommon.*
import sbt.internal.LogManager
import sbt.internal.util.ManagedLogger

import scala.collection.{Seq, mutable}
import scala.util.matching.Regex

object DocsOpenBuild {

  def getCantonRoot(docsOpenDir: File): File = docsOpenDir / ".."

  def getDocsCantonDirectory(cantonRoot: File, cantonVersion: String): File =
    cantonRoot / "docs.daml.com" / "docs" / extractVersion(cantonVersion)

  def getCantonDocsSourcePath(cantonRoot: File, cantonVersion: String): File = {
    getDocsCantonDirectory(cantonRoot, cantonVersion) / "docs" / "canton"
  }

  def getSnippetDirectiveScriptPath(cantonRoot: File, cantonVersion: String): File = {
    getDocsCantonDirectory(cantonRoot, cantonVersion) / "bin" / "canton"
  }

  def updateManifest() = {
    Def
      .task {
        val log: ManagedLogger = streams.value.log

        log.info(
          "[updateDocs] Refreshing canton sources manifest ..."
        )
        val cantonRoot = getCantonRoot(baseDirectory.value)
        val source = getCantonDocsSourcePath(cantonRoot, version.value)
        val scriptPath = (Compile / resourceDirectory).value / "canton_source.py"
        val manifest = sourceDirectory.value / "assembly" / "canton_sources_manifest"
        runCommand(s"python $scriptPath $source $manifest", log)
      }
  }

  def updateDocs(sourceDirectory: SettingKey[File], targetDirectory: SettingKey[File]) = {
    Def
      .task {
        val log: ManagedLogger = streams.value.log

        log.info(
          "[updateDocs] Cleaning output directories ..."
        )
        val target = sourceDirectory.value / "preprocessed-sphinx"
        val assemblyTarget = sourceDirectory.value / "preprocessed-sphinx-assembly"
        IO.delete(target)
        IO.delete(assemblyTarget)

        log.info(
          "[updateDocs] Refreshing snippet data ..."
        )
        val snippetJsonSource = targetDirectory.value / "pre"
        val cantonRoot = getCantonRoot(baseDirectory.value)
        val snippetJsonTarget =
          getCantonDocsSourcePath(cantonRoot, version.value) / "includes" / "snippet_data"

        IO.delete(snippetJsonTarget)
        IO.createDirectory(snippetJsonTarget)
        IO.copyDirectory(snippetJsonSource, snippetJsonTarget)

        updateManifest.value
      }
  }

  def extractVersion(version: String): String = {
    val versionPattern: Regex = raw"\d+\.\d+\.\d+".r
    versionPattern
      .findFirstIn(version)
      .getOrElse(throw new IllegalArgumentException(s"No version number found in '$version'"))
  }

  def generateSphinxSnippets(`enterprise-app`: Project): Def.Initialize[Task[Unit]] = {
    Def.taskDyn {
      val log = streams.value.log
      log.info(
        "[generateSphinxSnippets] Running custom `.. snippet::` directives through tests to collect their output as JSON ..."
      )
      mkTestJob(
        n =>
          n.startsWith("com.digitalasset.canton.integration.tests.docs") && !n.endsWith("Oracle"),
        `enterprise-app` / Test / definedTests,
        `enterprise-app` / Test / testOnly,
        verbose = true,
      )
    }
  }

  def generateRstInitialize(
      sourceDirectory: SettingKey[File],
      targetDirectory: SettingKey[File],
  ) = {
    Def.task {
      val log: ManagedLogger = streams.value.log

      log.info(
        "[generateRst][test] Run RST-preprocessor tests ..."
      )

      val testPath = sourceDirectory.value / "main" / "resources"
      runCommand(s"python -m unittest discover -v -s $testPath", log)
      val docsTestPath =
        getSnippetDirectiveScriptPath(getCantonRoot(baseDirectory.value), version.value)
      runCommand(s"python -m unittest discover -v -s $docsTestPath", log)

      log.info(
        "[generateRst][clean] Clean RST-preprocessor output directories and copy RST sources ..."
      )

      val source = sourceDirectory.value / "sphinx"
      val target = sourceDirectory.value / "preprocessed-sphinx"
      val assemblyTarget = sourceDirectory.value / "preprocessed-sphinx-assembly"
      val snippetJsonSource = targetDirectory.value / "pre"
      val snippetJsonTarget = target / "includes" / "snippet_data"

      IO.delete(target)
      IO.delete(assemblyTarget)

      val cantonDocsSourcePath =
        getCantonDocsSourcePath(getCantonRoot(baseDirectory.value), version.value)
      IO.copyDirectory(cantonDocsSourcePath, target)
      IO.copyDirectory(source, target)
      IO.copyDirectory(snippetJsonSource, snippetJsonTarget)
      IO.createDirectory(assemblyTarget)
    }
  }

  def generateRstResolveSnippet(sourceDirectory: SettingKey[File]) = {
    Def.task {
      val log: ManagedLogger = streams.value.log

      val target = sourceDirectory.value / "preprocessed-sphinx"
      val snippetJsonTarget = target / "includes" / "snippet_data"

      log.info(
        "[generateRst][preprocessing:step 1] Replacing custom `.. snippet::` directives with RST code blocks ..."
      )

      val snippetScriptPath = getSnippetDirectiveScriptPath(
        getCantonRoot(baseDirectory.value),
        version.value,
      ) / "snippet_directive.py"
      runCommand(s"python $snippetScriptPath $snippetJsonTarget $target", log)
    }
  }

  def generateRst(
      sourceDirectory: SettingKey[File],
      resourceDirectory: SettingKey[File],
      generateReferenceJson: TaskKey[File],
  ): Def.Initialize[Task[Unit]] = {
    Def
      .task {
        val log: ManagedLogger = streams.value.log

        val cantonRoot = getCantonRoot(baseDirectory.value)
        val source = getCantonDocsSourcePath(cantonRoot, version.value)
        val target = sourceDirectory.value / "preprocessed-sphinx"
        val assemblyTarget = sourceDirectory.value / "preprocessed-sphinx-assembly"
        val manifest = sourceDirectory.value / "assembly" / "canton_sources_manifest"

        log.info(
          "[generateRst][preprocessing:step 2] Using the reference JSON to preprocess the RST files ..."
        )

        val scriptPath = resourceDirectory.value / "rst-preprocessor.py"
        runCommand(
          s"python $scriptPath $cantonRoot ${generateReferenceJson.value} $source $manifest $target $assemblyTarget",
          log,
        )
      }
  }

  def generateReferenceJson(
      embed_reference_json: SettingKey[File],
      communityAppSourceDirectory: SettingKey[File],
      enterpriseAppSourceDirectory: SettingKey[File],
      enterpriseAppTarget: SettingKey[File],
      enterpriseIntegrationTestingSourceDirectory: SettingKey[File],
      resourceDirectory: SettingKey[File],
      sourceDirectory: SettingKey[File],
      target: SettingKey[File],
  ): Def.Initialize[Task[File]] = {
    Def.task {
      val log = streams.value.log
      log.info(
        "[generateReferenceJson] Generating the JSON used to populate the documentation for console commands, metrics, etc. ..."
      )
      val outFile = (Compile / embed_reference_json).value
      outFile.getParentFile.mkdirs()
      val appSourceDir = communityAppSourceDirectory.value
      val enterpriseAppSourceDir = enterpriseAppSourceDirectory.value
      val enterpriseIntegrationTestingSourceDir =
        enterpriseIntegrationTestingSourceDirectory.value
      val scriptPath =
        (resourceDirectory.value / "console-reference.canton").getPath
      val targetDirectory = enterpriseAppTarget.value
      val releaseDirectory = targetDirectory / "release" / "canton"
      val generateReferenceJsonConf = target.value / "generateReferenceJsonConf"
      IO.delete(generateReferenceJsonConf)
      val simpleConfig = generateReferenceJsonConf / "simple-topology.conf"
      val distributedConfig = generateReferenceJsonConf / "distributed-single-domain-topology.conf"
      val includes = generateReferenceJsonConf / "include"
      IO.copyFile(
        appSourceDir / "pack" / "examples" / "01-simple-topology" / "simple-topology.conf",
        simpleConfig,
      )
      IO.copyFile(
        enterpriseAppSourceDir / "test" / "resources" / "distributed-single-domain-topology.conf",
        distributedConfig,
      )
      IO.copyDirectory(
        enterpriseIntegrationTestingSourceDir / "main" / "resources" / "include",
        includes,
      )
      val args = Seq(
        "run",
        scriptPath,
        "-c",
        simpleConfig.getPath,
        "-c",
        distributedConfig.getPath,
        "--log-level-stdout=off",
      )
      val out = runCanton(releaseDirectory, args, None, log)
      IO.write(outFile, out)
      // Rename the generateReferenceJson log/canton.log file to be recognizable
      val cantonLogDir = new File("log")
      for (cantonLog <- (cantonLogDir * "canton.log").get) {
        IO.move(cantonLog, cantonLogDir / "create-json.canton.log")
      }
      outFile
    }
  }

  def runCanton(packPath: File, args: Seq[String], cwd: Option[File], log: Logger): String = {
    import scala.sys.process.{Process, ProcessLogger}

    val appPath = (packPath / "bin" / "canton").getPath
    val stdOut = mutable.MutableList[String]()
    val stdErr = mutable.MutableList[String]()
    val exitCode = Process(Seq(appPath) ++ args, cwd) ! ProcessLogger(
      line => stdOut += line,
      line => stdErr += line,
    )
    if (exitCode != 0) {
      log.error(s"Canton invocation with arguments $args failed: exit code $exitCode")
      log.error(s"Canton output")
      log.error(stdOut.mkString(System.lineSeparator()))
      log.error(s"Canton errors")
      log.error(stdErr.mkString(System.lineSeparator()))
      throw new MessageOnlyException(s"Canton invocation with arguments $args failed")
    }

    stdOut.mkString("\n")
  }

  // sbt-site is very skimpy with the Sphinx output in case of Sphinx errors
  // The useful output is logged at debug level
  // Here, we change the logging for the docs-open project to log all log messages to a file,
  // so that there is a useful artifact for the CI.
  // Additionally, since sbt-site doesn't currently expose the Sphinx option to fail on warnings,
  // we can get the same effect by grepping the log file for warning-level messages
  def sphinxLogManager(append: Boolean): LogManager = {
    import sbt.internal.LogManager
    import sbt.internal.util.{ConsoleAppender, ConsoleOut}
    import sbt.io.IO

    import java.io._

    val outFile = new File("log/docs-open.sphinx.log")
    IO.touch(outFile)
    val writer =
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile, append), IO.utf8))

    val appender = ConsoleAppender(
      name = "docs-open",
      out = ConsoleOut.bufferedWriterOut(writer),
      useFormat = false,
    )

    LogManager.withLoggers(backed = _ => appender)
  }

}
