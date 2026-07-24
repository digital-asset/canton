package com.digitalasset.canton.build

import sbt._
import sbt.Keys._
import org.scalafmt.sbt.ScalafmtPlugin
import scala.util.control.NonFatal

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
      val repoRoot = new java.io.File(".")
      val cantonFiles = (repoRoot ** "*.canton").get.filterNot(
        _.getAbsolutePath.contains(s"${java.io.File.separator}target${java.io.File.separator}")
      )
      val taskState = Keys.state.value

      if (cantonFiles.nonEmpty) {
        log.info(
          s"Found ${cantonFiles.size} workspace script file(s). Forwarding batch to scalafmt parsing engine..."
        )

        maskCantonScripts(cantonFiles, log)
        try {
          val filePathsBatch = cantonFiles.map(_.getAbsolutePath).mkString(" ")
          sbt.Command.process(
            s"scalafmtOnly $filePathsBatch",
            taskState,
            (failureMessage: String) => {
              log.error(s"Canton script formatting engine parse failure: $failureMessage")
              taskState.fail
            },
          )
        } finally {
          restoreCantonScripts(cantonFiles, log)
        }
      } else {
        log.info("No .canton scripts located inside workspace root.")
      }
    },
    scalafmtCantonCheck := {
      val log = streams.value.log
      val repoRoot = new java.io.File(".")
      val taskState = Keys.state.value

      val cantonFiles = (repoRoot ** "*.canton").get.filterNot(
        _.getAbsolutePath.contains(s"${java.io.File.separator}target${java.io.File.separator}")
      )

      if (cantonFiles.nonEmpty) {
        log.info(
          s"Found ${cantonFiles.size} source script(s). Running single workspace verification pass..."
        )

        maskCantonScripts(cantonFiles, log)
        var styleCheckFailed = false

        val fileMapping = cantonFiles.map { originalFile =>
          val backupFile =
            new java.io.File(originalFile.getParentFile, "backup_" + originalFile.getName + ".tmp")
          IO.copyFile(originalFile, backupFile)
          (originalFile, backupFile)
        }

        try {
          val originalFilePathsBatch = cantonFiles.map(_.getAbsolutePath).mkString(" ")
          sbt.Command.process(
            s"scalafmtOnly $originalFilePathsBatch",
            taskState,
            (failureMessage: String) => {
              log.error(s"Canton script formatting engine parse failure: $failureMessage")
              styleCheckFailed = true
              taskState.fail
            },
          )

          fileMapping.foreach { case (originalFile, backupFile) =>
            val formattedContent = IO.read(originalFile)
            val originalContent = IO.read(backupFile)

            if (originalContent != formattedContent) {
              styleCheckFailed = true
              log.error(s"Canton style violation detected: ${originalFile.getPath}")
            }
          }
        } catch {
          case NonFatal(e) =>
            log.error(s"Canton style validation check crashed internally: ${e.getMessage}")
            styleCheckFailed = true
        } finally {
          fileMapping.foreach { case (originalFile, backupFile) =>
            if (backupFile.exists()) {
              IO.copyFile(backupFile, originalFile)
              IO.delete(backupFile)
            }
          }
          restoreCantonScripts(cantonFiles, log)
        }

        if (styleCheckFailed) {
          sys.error(
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

  // --- Encapsulated Helpers ---

  private def maskCantonScripts(files: Seq[File], log: Logger): Unit = {
    log.info(
      s"Executing Pre-Format Pass: Masking structural '@' tokens in ${files.size} scripts..."
    )
    files.foreach { file =>
      val content = IO.read(file)
      // The mask token deliberately contains no '@' so that re-masking an already-masked file is a
      // no-op and can never nest into corruption.
      val masked = content.replaceAll("(?<!\\w)@(\\w*)", "/*canton-mask:$1*/")
      IO.write(file, masked)
    }
  }

  private def restoreCantonScripts(files: Seq[File], log: Logger): Unit = {
    log.info(
      s"Executing Post-Format Pass: Restoring original '@' annotations to ${files.size} scripts..."
    )
    files.foreach { file =>
      val content = IO.read(file)
      val unmasked = content.replaceAll("/\\*canton-mask:(\\w*)\\*/", "@$1")
      IO.write(file, unmasked)
    }
  }
}
