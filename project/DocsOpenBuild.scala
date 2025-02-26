// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import BuildCommon.*
import DocsOpenBuild.DocsPaths.{cantonDocsSourceDirectory, repositoryRoot, pythonScriptsDirectory}
import sbt.*
import sbt.Keys.*
import sbt.internal.LogManager
import sbt.internal.util.ManagedLogger

import scala.collection.{Seq, mutable}

object DocsOpenBuild {

  object DocsPaths {

    def repositoryRoot(docsOpenDir: File): File = docsOpenDir / ".."

    def cantonDocsSourceDirectory(docsOpenDir: File): File =
      docsOpenDir / "src" / "sphinx"

    def pythonScriptsDirectory(docsOpenDir: File): File =
      docsOpenDir / "src" / "main" / "resources"

  }

  def generateSphinxSnippets(`enterprise-app`: Project): Def.Initialize[Task[Unit]] =
    Def.taskDyn {
      val log = streams.value.log
      log.info(
        "[generateSphinxSnippets] Running custom `.. snippet::` directives through tests to collect their output as JSON ..."
      )
      mkTestJob(
        n => n.startsWith("com.digitalasset.canton.integration.tests.docs"),
        `enterprise-app` / Test / definedTests,
        `enterprise-app` / Test / testOnly,
        verbose = true,
      )
    }

  def generateRstInitialize(
      sourceDirectory: SettingKey[File],
      targetDirectory: SettingKey[File],
  ): Def.Initialize[Task[Unit]] =
    Def.task {
      val log: ManagedLogger = streams.value.log

      log.info(
        "[generateRst][test] Run RST-preprocessor tests ..."
      )

      val testPath = pythonScriptsDirectory(baseDirectory.value)
      runCommand(s"python -m unittest discover -v -s $testPath", log)

      log.info(
        "[generateRst][clean] Clean RST-preprocessor output directories and copy RST sources ..."
      )

      val source = sourceDirectory.value / "sphinx"
      val preprocessed = sourceDirectory.value / "preprocessed-sphinx"
      val snippetJsonSource = targetDirectory.value / "pre"
      val generated = targetDirectory.value / "generated"

      IO.delete(preprocessed)
      IO.delete(generated)

      val cantonDocsSourcePath = cantonDocsSourceDirectory(baseDirectory.value)
      IO.copyDirectory(cantonDocsSourcePath, preprocessed)
      IO.copyDirectory(source, preprocessed)
      IO.createDirectory(generated)
    }

  def generateRstResolveSnippet(
      sourceDirectory: SettingKey[File],
      targetDirectory: SettingKey[File],
  ): Def.Initialize[Task[String]] =
    Def.task {
      val log: ManagedLogger = streams.value.log

      val target = sourceDirectory.value / "preprocessed-sphinx"
      val snippetJsonSource = targetDirectory.value / "pre"

      log.info(
        "[generateRst][preprocessing:step 2] Replacing custom `.. snippet::` directives with RST code blocks ..."
      )

      val script = pythonScriptsDirectory(baseDirectory.value) / "snippet_directive.py"
      runCommand(s"python $script $snippetJsonSource $target", log)
    }

  def generateRstResolveLiteralInclude(
      sourceDirectory: SettingKey[File]
  ): Def.Initialize[Task[String]] =
    Def.task {
      val log: ManagedLogger = streams.value.log

      val target = sourceDirectory.value / "preprocessed-sphinx"

      log.info(
        "[generateRst][preprocessing:step 1] Replacing `.. literalinclude::` directives with RST code blocks ..."
      )

      val scriptPath =
        pythonScriptsDirectory(baseDirectory.value) / "literalinclude_directive.py"

      runCommand(s"python $scriptPath $target", log)
    }

  def generateRst(
      sourceDirectory: SettingKey[File],
      resourceDirectory: SettingKey[File],
      generateReferenceJson: TaskKey[File],
      targetDirectory: SettingKey[File],
  ): Def.Initialize[Task[Unit]] =
    Def
      .task {
        val log: ManagedLogger = streams.value.log

        val cantonRoot = repositoryRoot(baseDirectory.value)
        val source = cantonDocsSourceDirectory(baseDirectory.value)
        val preprocessed = sourceDirectory.value / "preprocessed-sphinx"

        log.info(
          "[generateRst][preprocessing:step 3] Using the reference JSON to preprocess the RST files ..."
        )

        val scriptPath = resourceDirectory.value / "rst-preprocessor.py"
        runCommand(
          s"python $scriptPath $cantonRoot ${generateReferenceJson.value} $source $preprocessed ${targetDirectory.value}",
          log,
        )
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
  ): Def.Initialize[Task[File]] =
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
      val distributedConfig =
        generateReferenceJsonConf / "distributed-single-synchronizer-topology.conf"
      val includes = generateReferenceJsonConf / "include"
      IO.copyFile(
        appSourceDir / "pack" / "examples" / "01-simple-topology" / "simple-topology.conf",
        simpleConfig,
      )
      IO.copyFile(
        enterpriseAppSourceDir / "test" / "resources" / "distributed-single-synchronizer-topology.conf",
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
      // Using the GENERATE_METRICS_FOR_DOCS environment variable as a flag (enabled when set) to explicitly register
      // metrics which usually are registered by the application on-demand only.
      // Without it, the metrics documentation generation is going to miss such on-demand registered metrics.
      val out = runCanton(releaseDirectory, args, None, log, ("GENERATE_METRICS_FOR_DOCS", ""))
      IO.write(outFile, out)
      // Rename the generateReferenceJson log/canton.log file to be recognizable
      val cantonLogDir = new File("log")
      for (cantonLog <- (cantonLogDir * "canton.log").get) {
        IO.move(cantonLog, cantonLogDir / "create-json.canton.log")
      }
      outFile
    }

  def runCanton(
      packPath: File,
      args: Seq[String],
      cwd: Option[File],
      log: Logger,
      extraEnv: (String, String)*
  ): String = {
    import scala.sys.process.{Process, ProcessLogger}

    val appPath = (packPath / "bin" / "canton").getPath
    val stdOut = mutable.MutableList[String]()
    val stdErr = mutable.MutableList[String]()
    val exitCode = Process(Seq(appPath) ++ args, cwd, extraEnv: _*) ! ProcessLogger(
      line => stdOut += line,
      line => {
        log.info(s"Error detected: $line")
        stdErr += line
      },
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

    import java.io.*

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
