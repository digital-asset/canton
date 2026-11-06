package com.digitalasset.canton.build

import sbt.*
import sbt.Keys.*
import org.scalafmt.interfaces.{Scalafmt, ScalafmtReporter}
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtConfig
import org.scalafmt.sbt.{ScalafmtPlugin, ScalafmtSbtDependencyDownloader}
import sbt.io.FileFilter
import sbt.librarymanagement.{DependencyResolution, UpdateConfiguration}
import sbt.nio.file.FileTreeView

import java.io.{File, IOException, OutputStreamWriter, PrintWriter}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitOption, FileVisitResult, FileVisitor, Files, Path as NioPath}
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.sys.process.Process
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CantonScalafmtPlugin extends AutoPlugin {

  override def trigger: PluginTrigger = allRequirements
  override def requires: Plugins = ScalafmtPlugin

  object autoImport {
    val scalafmtCanton = taskKey[Unit]("Format all workspace .canton script targets via scalafmt.")
    val scalafmtCantonCheck =
      taskKey[Unit]("Verify workspace .canton files against style conventions without alteration.")
  }

  import autoImport._

  // Moving these settings to globalSettings ensures they run EXACTLY ONCE per sbt invocation,
  // targeting the root folder instead of evaluating on every sub-project module.
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    scalafmtCanton := {
      val log = streams.value.log
      // In global scope, we explicitly target the workspace root directory
      val repoRoot = (ThisBuild / baseDirectory).value

      val cantonFiles = listCantonScriptFiles(repoRoot, log)

      val config = (ThisBuild / scalafmtConfig).value
      val taskStreams = streams.value
      val resolution = (LocalRootProject / dependencyResolution).value
      val updateConf = (LocalRootProject / updateConfiguration).value

      if (cantonFiles.nonEmpty) {
        log.debug(
          s"Found ${cantonFiles.size} workspace script file(s). Forwarding batch to scalafmt parsing engine..."
        )

        var formatFailed = false
        formatInMemory(config, cantonFiles, taskStreams, resolution, updateConf) { failureMessage =>
          log.error(s"Canton script formatting engine parse failure: $failureMessage")
          formatFailed = true
        }.foreach { case (originalFile, formattedContent) =>
          if (IO.read(originalFile) != formattedContent) {
            log.info(s"Reformatting ${originalFile.getPath}")
            IO.write(originalFile, formattedContent)
          }
        }

        if (formatFailed) {
          throw new MessageOnlyException("Canton script formatting failed. See the errors above.")
        }
      } else {
        log.info("No .canton scripts located inside workspace root.")
      }
    },
    scalafmtCantonCheck := {
      val log = streams.value.log
      val repoRoot = (ThisBuild / baseDirectory).value
      val config = (ThisBuild / scalafmtConfig).value
      val taskStreams = streams.value
      val resolution = (LocalRootProject / dependencyResolution).value
      val updateConf = (LocalRootProject / updateConfiguration).value

      val cantonFiles = listCantonScriptFiles(repoRoot, log)

      if (cantonFiles.nonEmpty) {
        log.info(
          s"Found ${cantonFiles.size} source script(s). Running single workspace verification pass..."
        )

        var styleCheckFailed = false

        try {
          formatInMemory(config, cantonFiles, taskStreams, resolution, updateConf) {
            failureMessage =>
              log.error(s"Canton script formatting engine parse failure: $failureMessage")
              styleCheckFailed = true
          }.foreach { case (originalFile, formattedContent) =>
            if (IO.read(originalFile) != formattedContent) {
              styleCheckFailed = true
              log.error(s"Canton style violation detected: ${originalFile.getPath}")
            }
          }
        } catch {
          case NonFatal(e) =>
            log.error(s"Canton style validation check crashed internally: ${e.getMessage}")
            log.trace(e)
            styleCheckFailed = true
        }

        if (styleCheckFailed) {
          throw new MessageOnlyException(
            "Canton script lint verification failed. Please execute 'sbt format' locally to realign scripts."
          )
        } else {
          log.info("Scalafmt successfully verified all .canton script styles!")
        }
      } else {
        log.info("No .canton source scripts found to verify.")
      }
    },
  )

  private def listCantonScriptFiles(root: File, log: Logger): Seq[File] =
    Try(Process(Seq("git", "ls-files", "-z", "--", "*.canton"), root).!!) match {
      case Success(output) =>
        output.split('\u0000').toSeq.filter(_.nonEmpty).map(new File(root, _)).filter(_.isFile)
      case Failure(e) =>
        log.warn(s"Considering all .canton scripts, git ls-files failed: ${e.getMessage}")
        findCantonScriptFiles(root)
    }

  private def findCantonScriptFiles(root: File) = {
    // copy of sbt.io.DescendantOrSelfPathFinder.native, with the difference that it allows to avoid entire
    // subtrees (e.g. the `target` folders)
    def native(
        file: File,
        filter: FileFilter,
        dirFilter: FileFilter,
        fileSet: mutable.Set[File],
        depth: Int,
    ): Unit =
      try {
        if (filter.accept(file)) fileSet += file
        if (depth > 0)
          FileTreeView.default.list(file.toPath).foreach { case (path, attributes) =>
            val file = path.toFile
            val ff = new File(path.toString) {
              override def isDirectory: Boolean = attributes.isDirectory
              override def isFile: Boolean = attributes.isRegularFile
            }
            if (attributes.isRegularFile && filter.accept(ff)) {
              fileSet += file
            } else if (
              attributes.isDirectory && dirFilter.accept(ff) && !attributes.isSymbolicLink
            ) {
              native(file, filter, dirFilter, fileSet, depth - 1)
            }
          }
        ()
      } catch {
        case _: IOException =>
      }

    val nativeWalk = mutable.Set[File]()
    val targetFilter: FileFilter = "target"
    native(root, "*.canton", -targetFilter, nativeWalk, Int.MaxValue)
    nativeWalk.toSeq
  }

  /** Formats the `.canton` scripts in memory, so that the scripts themselves only ever hold
    * unmasked content. Returns each successfully formatted script with its formatted content.
    */
  private def formatInMemory(
      config: File,
      files: Seq[File],
      taskStreams: TaskStreams,
      resolution: DependencyResolution,
      updateConf: UpdateConfiguration,
  )(onFailure: String => Unit): Seq[(File, String)] = {
    val log = taskStreams.log
    val downloadOutput = new OutputStreamWriter(taskStreams.binary())
    val reporter = new ScalafmtReporter {
      override def error(file: NioPath, message: String): Unit = onFailure(s"$message [$file]")
      override def error(file: NioPath, e: Throwable): Unit =
        onFailure(s"${e.getMessage} [$file]")
      override def excluded(file: NioPath): Unit = log.debug(s"file excluded: $file")
      override def parsedConfig(config: NioPath, scalafmtVersion: String): Unit =
        log.debug(s"parsed config (v$scalafmtVersion): $config")
      override def downloadWriter(): PrintWriter = new PrintWriter(downloadOutput)
      override def downloadOutputStreamWriter(): OutputStreamWriter = downloadOutput
    }
    val session = Scalafmt
      .create(ScalafmtPlugin.getClass.getClassLoader)
      .withRespectProjectFilters(true)
      .withRepositoryPackageDownloader(
        new ScalafmtSbtDependencyDownloader(taskStreams, resolution, updateConf)
      )
      .withReporter(reporter)
      .createSession(config.toPath)

    for {
      file <- files
      formattedContent <- Option(
        session.formatOrError(file.toPath, maskCantonScript(IO.read(file))).value
      )
    } yield file -> restoreCantonScript(formattedContent)
  }

  private def maskCantonScript(content: String): String =
    // The mask token deliberately contains no '@' so that re-masking an already-masked file is a
    // no-op and can never nest into corruption.
    content.replaceAll("(?<!\\w)@(\\w*)", "/*canton-mask:$1*/")

  private def restoreCantonScript(content: String): String =
    content.replaceAll("/\\*canton-mask:(\\w*)\\*/", "@$1")
}
