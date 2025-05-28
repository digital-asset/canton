// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import BuildCommon.*
import sbt.*
import sbt.Keys.*
import sbt.internal.LogManager
import sbt.internal.util.ManagedLogger
import sbt.io.IO

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import scala.collection.{Seq, mutable}
import sbt.Tracked
import sbt.FileInfo
import sbt.util.ChangeReport

import scala.util.matching.Regex
import scala.sys.process.*

object DocsOpenBuild {

  lazy val makeSiteFull = taskKey[Unit]("Builds docs from scratch")

  lazy val reset = taskKey[Unit](
    "Removes generated content and initializes the preprocessed directory with source files"
  )

  lazy val generate = taskKey[Unit](
    "Generates snippet output and other content which needs to be included in the source RST files"
  )

  lazy val resolve = taskKey[Unit](
    "Replaces directives/placeholders in RST files with content so that a standard Sphinx build can process it"
  )

  lazy val rebuild =
    taskKey[Unit](
      "Syncs changed source RST files and replaces directives/placeholders in those files only"
    )
  lazy val rebuildSnippets =
    taskKey[Unit]("Re-generates snippet output data and replaces snippet directives with it")
  lazy val rebuildGeneratedIncludes =
    taskKey[Unit]("Re-generates include content and replaces include directives with it")

  lazy val pythonCommand: SettingKey[String] =
    settingKey[String]("The python command to use (python or python3)")

  def findPythonCommand(): Def.Initialize[String] = Def.setting {
    val log = sLog.value

    if (isCommandAvailable("python3", log)) {
      log.debug("Using 'python3' as the Python command")
      "python3"
    } else if (isCommandAvailable("python", log)) {
      log.debug("Using 'python' as the Python command")
      "python"
    } else {
      val errorMessage = "Neither 'python3' nor 'python' command was found or is executable"
      log.error(errorMessage)
      sys.error(errorMessage)
    }
  }

  def runPythonTests(): Def.Initialize[Task[String]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|test][runPythonTests]"

    log.info(s"$logPrefix Run python tests ...")

    val python = pythonCommand.value
    val pythonScriptsDirectory = (Compile / resourceDirectory).value
    runCommand(s"$python -m unittest discover -v -s $pythonScriptsDirectory", log)
  }

  def resetGeneratedSnippets(): Def.Initialize[Task[Unit]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|reset][resetGeneratedSnippets]"

    val snippetJsonSource = target.value / "snippet_json_data"
    IO.delete(snippetJsonSource)

    log.info(s"$logPrefix Cleaned snippet JSON sources output directory $snippetJsonSource")
  }

  def resetGeneratedIncludes(): Def.Initialize[Task[Unit]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|reset][resetGeneratedIncludes]"

    val generated = target.value / "generated"
    IO.delete(generated)
    IO.createDirectory(generated)

    log.info(s"$logPrefix Cleaned generated include output directory $generated")
  }

  def resetPreprocessed(): Def.Initialize[Task[Unit]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|reset][resetPreprocessed]"

    val source = sourceDirectory.value / "sphinx"
    val preprocessed: File = target.value / "preprocessed-sphinx"

    IO.delete(preprocessed)
    IO.copyDirectory(source, preprocessed)

    log.info(s"$logPrefix Replaced $preprocessed with $source")
  }

  lazy val generateSphinxSnippets =
    taskKey[Unit](
      "Render snippets, that is to run the snippets and collect their execution output"
    )

  lazy val rstToTestMapTask = taskKey[Map[String, String]](
    "Maps RST source file paths, defined in snippet generation test classes, to their corresponding test class FQDNs"
  )

  /** Regex to find class definitions extending `SnippetGenerator` and their RST source file which
    * contains snippet directives.
    *
    * Regex: Attempts to capture the test class name (Group 1), and then the RST file path (Group 2
    * or Group 3). The RST source file path may be given as a positional class argument File("...")
    * (Group 2), or as a named class argument source=File("...") (Group 3).
    *
    * Details:
    *   - `class\s+([A-Za-z0-9_]+)`: Captures test class name (Group 1)
    *   - `?:File\s*\(\s*\"([^\"]+)\"`: Captures RST source file path in Group 2
    *   - `|`: OR
    *   - `?:.*?source\s*=\s*File\s*\(\s*\"([^\"]+)\"`: Captures RST source file path in Group 3
    */
  lazy val snippetTestClassRegex: Regex =
    """(?s)class\s+([A-Za-z0-9_]+)\s*extends\s+SnippetGenerator\s*\(\s*(?:File\s*\(\s*"([^"]+)"|.*?source\s*=\s*File\s*\(\s*"([^"]+)").*?\)""".r

  def buildRstToTestMap(`enterprise-app`: Project): Def.Initialize[Task[Map[String, String]]] =
    Def.task {
      val log = streams.value.log
      val logPrefix = "[docs-open|generate][buildRstToTestMap]"

      val testsDir =
        (`enterprise-app` / baseDirectory).value / "src" / "test" / "scala" / "com" / "digitalasset" / "canton" / "integration" / "tests"
      val generatorFile = testsDir / "docs" / "SphinxDocumentationGenerator.scala"
      val packagePrefix = "com.digitalasset.canton.integration.tests.docs."
      val pathPrefix = "docs-open/src/sphinx/"

      require(
        generatorFile.exists() && generatorFile.isFile,
        s"$logPrefix Missing required file: $generatorFile",
      )

      log.info(s"$logPrefix Parsing $generatorFile to map RST source files to test classes ...")

      val mapping = snippetTestClassRegex
        .findAllMatchIn(IO.read(generatorFile))
        .flatMap { m =>
          val className = m.group(1)
          val rstPath = Option(m.group(2)).orElse(Option(m.group(3)))

          rstPath.flatMap { filePath =>
            if (filePath.startsWith(pathPrefix)) {
              val relativePath = filePath.stripPrefix(pathPrefix)
              log.debug(
                s"$logPrefix Found mapping: '$relativePath' -> '${packagePrefix + className}'"
              )
              Some(relativePath -> (packagePrefix + className))
            } else {
              log.debug(
                s"$logPrefix Skipping path '$filePath' for class '$className' (unexpected prefix)"
              )
              None
            }
          }
        }
        .toMap

      log.debug(s"$logPrefix Created RST file to test class mapping with ${mapping.size} entries")
      mapping
    }

  /** Generates Sphinx snippets by running associated Scala tests based on the `rstToTestMapping`.
    *
    * It evaluates the input task key `rstFilesTask` to get a list of (changed) RST files, uses the
    * mapping to determine corresponding tests, and runs only those specific tests.
    *
    * @param rstFilesTask
    *   A sequence of (changed) RST files to process. Can be the output
    *   [[DocsOpenBuild.syncSphinxSources]] applying [[DocsOpenBuild.changedRstFiles]].
    */
  def generateSphinxSnippets(
      enterpriseApp: Project,
      rstToTestMapTask: TaskKey[Map[String, String]],
      rstFilesTask: TaskKey[Seq[File]] = allRstFilesTask,
  ): Def.Initialize[Task[Unit]] = Def.taskDyn {
    val log = streams.value.log
    val logPrefix = "[docs-open|generate][generateSphinxSnippets]"

    val rstFiles = rstFilesTask.value
    val rstToTestMap = rstToTestMapTask.value
    val preprocessed = target.value / "preprocessed-sphinx"

    log.debug(s"$logPrefix Using RST file to test class mapping:\n${rstToTestMap.mkString("\n")}")

    val testsToRun = rstFiles.flatMap { rstFile =>
      IO.relativize(preprocessed, rstFile).flatMap(rstToTestMap.get)
    }.distinct

    if (testsToRun.nonEmpty) {
      log.info(
        s"$logPrefix Running `.. snippet::` directives through tests to collect their output as JSON ..."
      )
      mkTestJob(
        testName => testsToRun.contains(testName),
        enterpriseApp / Test / definedTests,
        enterpriseApp / Test / testOnly,
        verbose = true,
      )
    } else {
      log.debug(s"$logPrefix No tests found in mapping for the provided RST files")
      Def.task(())
    }
  }

  lazy val generateIncludes = taskKey[String](
    "Generate RST include content such as console commands, error codes, metrics, etc."
  )

  def generateIncludes(generateReferenceJson: TaskKey[File]): Def.Initialize[Task[String]] =
    Def.task {
      val log: ManagedLogger = streams.value.log
      val logPrefix = "[docs-open|generate][generateIncludes]"

      log.info(s"$logPrefix Using the reference JSON to preprocess the RST files ...")

      val python = pythonCommand.value
      val scriptPath = (Compile / resourceDirectory).value / "include_generator.py"

      runCommand(s"$python $scriptPath ${generateReferenceJson.value} ${target.value}", log)
    }

  lazy val embed_reference_json =
    settingKey[File]("The JSON file where the content data is generated")

  lazy val generateReferenceJson =
    taskKey[File](
      "Generate the JSON file for the content generation, for example the Canton Console commands"
    )

  def generateReferenceJson(
      embed_reference_json: SettingKey[File],
      communityAppSourceDirectory: SettingKey[File],
      enterpriseAppSourceDirectory: SettingKey[File],
      enterpriseAppTarget: SettingKey[File],
      communityIntegrationTestingSourceDirectory: SettingKey[File],
  ): Def.Initialize[Task[File]] = Def.task {
    val log = streams.value.log
    val logPrefix = "[docs-open|generate][generateReferenceJson]"

    log.info(
      s"$logPrefix Generating the JSON used to populate the documentation for console commands, metrics, etc. ..."
    )

    val outFile = (Compile / embed_reference_json).value
    outFile.getParentFile.mkdirs()
    val appSourceDir = communityAppSourceDirectory.value
    val enterpriseAppSourceDir = enterpriseAppSourceDirectory.value
    val communityIntegrationTestingSourceDir =
      communityIntegrationTestingSourceDirectory.value
    val scriptPath =
      ((Compile / resourceDirectory).value / "console-reference.canton").getPath
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
      communityIntegrationTestingSourceDir / "main" / "resources" / "include",
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

  // Used as default argument for resolve* and generate* methods which require a list of RST files
  val allRstFilesTask =
    taskKey[Seq[File]]("Provides a default list of all RST files if none specified")

  def findAllRstInPreprocessed(): Def.Initialize[Task[Seq[File]]] = Def.task {
    val log = streams.value.log
    val logPrefix = "[docs-open][findAllRstInPreprocessed]"

    val preprocessed = target.value / "preprocessed-sphinx"
    val allRstFinder: PathFinder = preprocessed ** "*.rst"
    val allRstFiles: Seq[File] = allRstFinder.get()

    log.debug(s"$logPrefix Found ${allRstFiles.size} RST files in $preprocessed")

    allRstFiles
  }

  lazy val changedRstFiles = taskKey[Seq[File]]("RST file filter")

  def filterRstFiles(
      changedFilesTask: TaskKey[Seq[File]] = allRstFilesTask
  ): Def.Initialize[Task[Seq[File]]] = Def.task {
    val log = streams.value.log

    val changedRstFiles = changedFilesTask.value.filter(_.getName.endsWith(".rst"))

    log.info(s"[docs-open|refresh][filterRstFiles] Found ${changedRstFiles.size} RST files")
    changedRstFiles
  }

  lazy val syncSphinxSources = taskKey[Seq[File]](
    "Syncs any changed file from the Sphinx source directory to the preprocessed directory. " +
      "Returns absolute paths to the affected (modified/added) files in the preprocessed directory."
  )

  def syncSources(): Def.Initialize[Task[Seq[File]]] = Def.task {
    val log = streams.value.log
    val logPrefix = "[docs-open|refresh][syncSources]"

    val source = sourceDirectory.value / "sphinx"
    val preprocessed = target.value / "preprocessed-sphinx"

    // Use separate caches for separate concerns: one for tracking source changes, one for sync state
    val cacheStoreFactory = streams.value.cacheStoreFactory
    // This cache is solely focused on detecting changes in the source directory
    val diffCheckCacheStore = cacheStoreFactory.make("sphinx-source-diff-check")
    // This cache manages the state of the synchronization process between the source directory and
    // the preprocessed directory
    val syncStateCacheStore = cacheStoreFactory.make("sphinx-source-sync-state")

    log.info(
      s"$logPrefix Checking for changed Sphinx sources in $source ..."
    )

    val sourceFinder = source ** "*"
    val sourceFiles = sourceFinder.filter(f => f.isFile).get
    val sourceFilesSet = sourceFiles.toSet

    // Tracked.diffInputs compares the current set of source files (and their last modified times)
    // against the information stored in the cache from the previous run. It identifies which source
    // files are new, which have been modified, and which have been removed since the last check.
    // In short: Figures out which source files have changed.
    val differenceTracker = Tracked.diffInputs(diffCheckCacheStore, FileInfo.lastModified)

    val changedSources = differenceTracker(sourceFilesSet) { report: ChangeReport[File] =>
      val addedModified = report.added ++ report.modified
      log.info(
        s"$logPrefix Detected ${addedModified.size} added/modified source files"
      )
      addedModified
    }

    val fileMappings: Seq[(File, File)] = sourceFiles.flatMap { file =>
      IO.relativize(source, file).map(relPath => (file, preprocessed / relPath))
    }

    if (fileMappings.nonEmpty) {
      log.info(s"$logPrefix Sync added/modified source files to '$preprocessed' ...")

      // Sync.sync does the file copying/deletion based on comparing source/target states.
      // Meaning, it compares the current source files with the state recorded in this cache (representing the
      // target's state after the last sync) to determine:
      // - Which new/modified source files need to be copied to the target.
      // - Which target files correspond to removed source files and need to be deleted.
      // - Which target files are already up-to-date and can be skipped.
      // In short: It uses its own cache (syncStateCacheStore) to track the state of the synchronization
      Sync.sync(syncStateCacheStore)(fileMappings)
      log.debug(s"$logPrefix Synchronization complete")
    } else {
      log.info(s"$logPrefix Unchanged source files, nothing to sync")
    }

    val changedFiles = fileMappings.collect {
      case (srcFile, targetFile) if changedSources.contains(srcFile) => targetFile
    }

    log.debug {
      val fileDetails = changedFiles.map(f => s"  - ${f.name} (${f.getPath})").mkString("\n")
      s"$logPrefix Found ${changedFiles.size} changed (preprocessed) files:\n$fileDetails"
    }

    changedFiles
  }

  /** Updates the modification timestamp of RST files that include generated content.
    *
    * This forces SBT's [[DocsOpenBuild.syncSphinxSources()]] task ([[DocsOpenBuild.syncSources()]])
    * to recognize these RST files as modified, triggering Sphinx's incremental build process for
    * only the affected files.
    *
    * This step is necessary because changes to the source of the generated content occur within the
    * Canton source code, not directly in the RST files themselves.
    */
  def touchGeneratedIncludeRstFiles(): Def.Initialize[Task[Unit]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|refresh][touchGeneratedIncludeRstFiles]"

    val source = sourceDirectory.value / "sphinx"

    val sourceRstFiles = incToRstRelativePathMapping.values.flatten.map(source / _).toSet
    log.debug(
      s"$logPrefix RST files defined by mapping: ${sourceRstFiles.mkString("\n\t", "\n\t", "")}"
    )

    IO.touch(sourceRstFiles)

    log.info(
      s"$logPrefix Touched ${sourceRstFiles.size} RST files referring to a generated include in $source"
    )
  }

  /** Replaces `.. snippet::` directives in RST files with their generated content.
    *
    * It executes an external Python script (`snippet_directive_replace.py`) to perform the
    * replacements. The script uses data from `target/snippet_json_data` and processes the specified
    * RST files.
    *
    * @param rstFilesTask
    *   A sequence of (changed) RST files to process. Can be the output
    *   [[DocsOpenBuild.syncSphinxSources]] applying [[DocsOpenBuild.changedRstFiles]].
    */
  def resolveSnippets(
      rstFilesTask: TaskKey[Seq[File]] = allRstFilesTask
  ): Def.Initialize[Task[String]] =
    Def.task {
      val log: ManagedLogger = streams.value.log
      val logPrefix = "[docs-open|resolve][resolveSnippets]"

      val rstFiles = rstFilesTask.value
      if (rstFiles.nonEmpty) {
        val python = pythonCommand.value
        val scriptPath = (Compile / resourceDirectory).value / "snippet_directive_replace.py"
        val snippetJsonSource = target.value / "snippet_json_data"
        val rstFilePaths: Seq[String] = rstFiles.map(_.getAbsolutePath)
        val command: Seq[String] =
          Seq(python, scriptPath.toString, snippetJsonSource.toString) ++ rstFilePaths

        log.info(s"$logPrefix Replacing `.. snippet::` directives with RST code blocks ...")
        runCommand(command.mkString(" "), log)
      } else {
        "No snippet resolution required"
      }
    }

  /** Replaces `.. literalinclude::` directives in RST files with RST code blocks.
    *
    * It executes an external Python script (`literalinclude_directive_replace.py`) to perform the
    * replacements.
    *
    * @param rstFilesTask
    *   A sequence of (changed) RST files to process. Can be the output
    *   [[DocsOpenBuild.syncSphinxSources]] applying [[DocsOpenBuild.changedRstFiles]].
    */
  def resolveLiteralIncludes(
      rstFilesTask: TaskKey[Seq[File]] = allRstFilesTask
  ): Def.Initialize[Task[String]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|resolve][resolveLiteralIncludes]"

    val rstFiles = rstFilesTask.value
    if (rstFiles.nonEmpty) {
      val python = pythonCommand.value
      val scriptPath = (Compile / resourceDirectory).value / "literalinclude_directive_replace.py"
      val repoRoot: File = (ThisBuild / baseDirectory).value
      val rstFilePaths: Seq[String] = rstFiles.map(_.getAbsolutePath)
      val command: Seq[String] =
        Seq(python, scriptPath.toString, repoRoot.toString) ++ rstFilePaths

      log.info(s"$logPrefix Replacing `.. literalinclude::` directives with RST code blocks ...")
      runCommand(command.mkString(" "), log)
    } else {
      "No literal include resolution required"
    }
  }

  /** Defines which .rst files need to replace the `.. generatedinclude::` directive with the
    * content of a generated include file. That content here needs to be in-sync with what the
    * `generateIncludes` task produces.
    */
  private val incToRstRelativePathMapping: Map[String, Seq[String]] = Map(
    "versioning.rst.inc" -> Seq("participant/reference/versioning.rst"),
    "topology_versioning.rst.inc" -> Seq(
      "sdk/tutorials/app-dev/external_signing_topology_transaction.rst"
    ),
    "monitoring.rst.sequencer_metrics.inc" -> Seq(
      "participant/howtos/observe/monitoring.rst"
    ),
    "monitoring.rst.mediator_metrics.inc" -> Seq(
      "participant/howtos/observe/monitoring.rst"
    ),
    "monitoring.rst.participant_metrics.inc" -> Seq(
      "participant/howtos/observe/monitoring.rst"
    ),
    "error_codes.rst.inc" -> Seq("participant/reference/error_codes.rst"),
    "console.rst.inc" -> Seq("participant/reference/console.rst"),
  )

  /** Verifies existence and type of mapped RST files in
    * [[DocsOpenBuild.incToRstRelativePathMapping]].
    *
    * Returns a set of full paths to valid RST files, throws otherwise.
    */
  private def verifyMappedRstFiles(
      log: sbt.util.Logger,
      preprocessed: File,
      actualIncToRstMap: Map[String, Seq[String]],
      logPrefix: String,
  ): Set[File] = {
    log.debug(s"$logPrefix Verifying RST files defined in `incToRstRelativePathMapping` ...")

    val mappedRstFullPaths = actualIncToRstMap.values.flatten.toSet.map(preprocessed / _)

    if (mappedRstFullPaths.isEmpty) {
      throw new sbt.MessageOnlyException(s"$logPrefix No RST files with generated include mappings")
    } else {
      val invalidFiles = mappedRstFullPaths.filterNot(f => f.exists() && f.isFile)

      if (invalidFiles.nonEmpty) {
        val errorDetails = invalidFiles
          .map { file =>
            val reason = if (!file.exists()) "does not exist" else "is not a regular file"
            val includeFile = actualIncToRstMap
              .collect {
                case (incKey, rstPathList)
                    if rstPathList
                      .exists(singleRstPath => (preprocessed / singleRstPath) == file) =>
                  s"'$incKey'"
              }
              .mkString(", ")
            s"  - Path: $file ($reason). Expected to include key(s): $includeFile"
          }
          .mkString("\n")

        val message =
          s"""$logPrefix Found ${invalidFiles.size} mapped RST path(s) that are either missing or not regular files:
             |$errorDetails
             |Ensure all RST files specified in `incToRstRelativePathMapping` exist and are regular files""".stripMargin
        throw new sbt.MessageOnlyException(message)
      } else {
        log.debug(
          s"$logPrefix All ${mappedRstFullPaths.size} mapped target RST files appear valid (exist and are files)."
        )
        mappedRstFullPaths
      }
    }
  }

  /** Replaces `.. include::` for Canton generated content in RST files.
    *
    * @param rstFilesTask
    *   A sequence of (changed) RST files to process. Can be the output
    *   [[DocsOpenBuild.syncSphinxSources]] applying [[DocsOpenBuild.changedRstFiles]].
    */
  def resolveIncludes(
      rstFilesTask: TaskKey[Seq[File]] = allRstFilesTask
  ): Def.Initialize[Task[Unit]] =
    Def.task {
      val log = streams.value.log
      val logPrefix = "[docs-open|resolve][resolveIncludes]"

      val rstFiles = rstFilesTask.value
      val generated = target.value / "generated"
      val preprocessed = target.value / "preprocessed-sphinx"

      val expectedRstFiles =
        verifyMappedRstFiles(log, preprocessed, incToRstRelativePathMapping, logPrefix)

      log.debug(
        s"$logPrefix RST files defined by mapping: ${expectedRstFiles.mkString("\n\t", "\n\t", "")}"
      )

      val rstFilesWithIncludes = rstFiles.filter(expectedRstFiles.contains)

      if (rstFilesWithIncludes.nonEmpty) {
        log.info(s"$logPrefix Replacing `.. generatedinclude::` directives with RST content ...")

        val rstToIncMapping = incToRstRelativePathMapping.toSeq
          .flatMap { case (incFile, rstFiles) =>
            rstFiles.map(rstFile => (preprocessed / rstFile, incFile))
          }
          .groupBy { case (rstFile, _) => rstFile }
          .map { case (rstFile, groupedPairs) =>
            val incs = groupedPairs.map { case (_, incFile) => incFile }.distinct
            rstFile -> incs
          }

        rstFilesWithIncludes.foreach { rstFile =>
          log.debug(s"$logPrefix Preprocessing RST file: $rstFile")

          rstToIncMapping.get(rstFile).foreach { incFileNames =>
            val originalContent = IO.read(rstFile, StandardCharsets.UTF_8)
            val updatedContent = incFileNames.foldLeft(originalContent) { (content, incFileName) =>
              val incFile = generated / incFileName
              require(incFile.exists(), s"$logPrefix Missing include file: $incFile")

              val incContent = IO.read(incFile, StandardCharsets.UTF_8)
              val incMarker =
                s"..\n    Dynamically generated content:\n.. generatedinclude:: $incFileName"

              require(
                content.contains(incMarker),
                s"$logPrefix Marker not found for $incFileName in $rstFile",
              )
              require(
                content.split(Pattern.quote(incMarker), -1).length - 1 == 1,
                s"$logPrefix Marker for $incFileName appears multiple times in $rstFile",
              )

              content.replace(incMarker, incContent)
            }

            if (updatedContent != originalContent) {
              IO.write(rstFile, updatedContent, StandardCharsets.UTF_8)
              log.debug(s"$logPrefix Updated $rstFile with include content.")
            } else {
              log.debug(s"$logPrefix No changes made to $rstFile.")
            }
          }
        }

        log.debug(s"$logPrefix Include directive replacement complete")
      }
    }

  def resolveLedgerApiProtoDocs(): Def.Initialize[Task[Unit]] = Def.task {
    val log: ManagedLogger = streams.value.log
    val logPrefix = "[docs-open|resolve][resolveLedgerApiProtoDocs]"

    val preprocessed = target.value / "preprocessed-sphinx"
    val filesToCopy = Map(
      (DamlProjects.`ledger-api` / Compile / sourceManaged).value / "proto-docs.rst" -> preprocessed / "sdk" / "reference" / "lapi-proto-docs.rst",
      (DamlProjects.`ledger-api-value` / Compile / sourceManaged).value / "proto-docs.rst" -> preprocessed / "sdk" / "reference" / "lapi-value-proto-docs.rst.inc",
    )

    val postProcessScript = file("community/docs/post-process.sh")

    log.info(s"$logPrefix Adding Ledger API reference documentation ...")
    filesToCopy.foreach { case (copyFromPath, copyToPath) =>
      log.debug(s"$logPrefix Copying the LAPI reference from $copyFromPath to $copyToPath ...")

      IO.copyFile(copyFromPath, copyToPath)
      val postProcessResult = s"$postProcessScript $copyToPath".!!

      log.debug(s"$logPrefix Postprocessing result: $postProcessResult")
    }
  }

  def runCanton(
      packPath: File,
      args: Seq[String],
      cwd: Option[File],
      log: Logger,
      extraEnv: (String, String)*
  ): String = {
    val appPath = (packPath / "bin" / "canton").getPath
    val stdOut = mutable.MutableList[String]()
    val stdErr = mutable.MutableList[String]()
    val exitCode = Process(Seq(appPath) ++ args, cwd, extraEnv*) ! ProcessLogger(
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

  private val sphinxLogPath = "log/docs-open.sphinx.log"

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

    val outFile = file(sphinxLogPath)
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

  lazy val checkDocErrors =
    taskKey[Unit]("Check docs-open.sphinx.log for errors and fail if there is one")

  def findSphinxLogIssues(): Def.Initialize[Task[Unit]] = Def.task {
    import scala.sys.process.*

    val log = streams.value.log
    val sphinxLogFile = file(sphinxLogPath)

    if (sphinxLogFile.exists() && sphinxLogFile.length() > 0) {
      log.info(s"Checking $sphinxLogPath for issues ...")

      // Matches the literal string "[warn]"
      val warnPattern = "\\[warn\\]"
      // Matches critical errors in .rst files like "file.rst:123: CRITICAL"
      val criticalRstPattern = "\\.rst:[0-9]+:\\s*CRITICAL"

      val checkCmd = List("rg", "-e", warnPattern, "-e", criticalRstPattern, sphinxLogPath)

      val checkRes = Process(checkCmd, None).!

      // rg exit codes:
      // 0: One or more lines selected (match found)
      // 1: No lines selected (no match)
      // >1: Error occurred during execution
      if (checkRes != 1) {
        if (checkRes == 0) {
          val matchingLines = Process(checkCmd).!! // `!!` captures the output as a string
          log.error(s"Found matches running '${checkCmd.mkString(" ")}':\n$matchingLines")
        }
        throw new IllegalStateException(
          s"Please check the log: $sphinxLogPath contains issues, or the rg command failed (exit code: $checkRes)"
        )
      } else {
        log.debug(s"Found no issues with: ${checkCmd.mkString(" ")}")
      }
    } else {
      log.warn(s"Sphinx log file $sphinxLogPath not found or is empty")
    }
  }

  def isCommandAvailable(command: String, log: Logger): Boolean =
    try {
      val process = Process(Seq(command, "--version"))
      process.!(ProcessLogger(_ => ())) == 0 // Discard output, check exit code
    } catch {
      case _: Throwable =>
        log.warn(s"Error checking for command: $command")
        false
    }

}
