// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.external

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{ProcessingTimeout, RequireTypes}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._
import org.slf4j.event.Level

import java.io.{IOException, InputStream, StringWriter}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

class BackgroundRunnerHandler[T](timeouts: ProcessingTimeout, val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with AutoCloseable
    with NoTracing {

  private sealed trait ProcessHandle {
    def info: T
  }
  private case class Configured(name: String, command: Seq[String], info: T) extends ProcessHandle {
    def start(): Running =
      Running(name, runner = new BackgroundRunner(name, command, timeouts, loggerFactory), info)
  }
  private case class Running(name: String, runner: BackgroundRunner, info: T)
      extends ProcessHandle {
    def kill(force: Boolean = false): Configured = {
      runner.kill(force)
      Configured(name, runner.command, info)
    }
    def restart(): Running = {
      Running(name, runner.restart(), info)
    }
  }

  private val external = new TrieMap[String, ProcessHandle]()

  def add(instanceName: String, command: Seq[String], info: T, manualStart: Boolean)(implicit
      traceContext: TraceContext
  ): Unit = {
    ErrorUtil.requireArgument(
      !external.contains(instanceName),
      s"key already exists ${instanceName}",
    )
    ErrorUtil.requireArgument(
      command.nonEmpty,
      s"you've supplied empty commands for ${instanceName}",
    )
    val configured = Configured(instanceName, command, info)
    external.put(instanceName, if (!manualStart) configured.start() else configured).discard
  }

  def start(instanceName: String): Unit = {
    perform(
      instanceName,
      {
        case a: Configured =>
          noTracingLogger.info(s"Starting external process for ${instanceName}")
          a.start()
        case Running(_, _, _) =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"can not start ${instanceName} as instance is already running"
            )
          )
      },
    )
  }

  private def perform(instanceName: String, func: ProcessHandle => ProcessHandle): Unit = {
    external.get(instanceName) match {
      case Some(item) =>
        external.update(instanceName, func(item))
      case None =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"no such instance ${instanceName} configured as remote running instance. have ${external.keys}"
          )
        )
    }
  }

  def kill(instanceName: String, force: Boolean = true): Unit = {
    perform(
      instanceName,
      {
        case x: Running =>
          noTracingLogger.info(s"Stopping external process for ${instanceName} (force=${force})")
          x.kill(force)
        case a: Configured =>
          ErrorUtil.internalError(
            new IllegalStateException(s"can not kill ${instanceName} as instance is not running")
          )
      },
    )
  }

  def restart(instanceName: String): Unit = {
    perform(
      instanceName,
      {
        case x: Running =>
          noTracingLogger.info(s"Restarting external process for ${instanceName}")
          x.restart()
        case Configured(_, _, _) =>
          ErrorUtil.internalError(
            new IllegalStateException(s"can not kill ${instanceName} as instance is not running")
          )
      },
    )
  }

  def info(instanceName: String): T = {
    external
      .getOrElse(
        instanceName,
        ErrorUtil.internalError(new IllegalArgumentException(s"no such instance ${instanceName}")),
      )
      .info
  }

  def exists(instanceName: String): Boolean = external.keySet.contains(instanceName)

  def killAndRemove(): Unit = {
    logger.info("Killing background processes due to shutdown")
    external.values.foreach {
      case Configured(_, _, _) => ()
      case Running(_, runner, _) =>
        runner.kill()
    }
    external.clear()
  }

  override def close(): Unit = killAndRemove()
}

class BackgroundRunner(
    val name: String,
    val command: Seq[String],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    waitBeforeRestartMs: Int = 250,
) extends NamedLogging
    with FlagCloseable {

  import BackgroundRunner._

  private def dumpOutputToLogger(parent: InputStream, level: Level): Unit = {
    @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
    class CopyOutput() extends NoTracing with Runnable {
      val buf = new StringWriter()

      override def run(): Unit = {
        try {
          var b = parent.read()
          while (b != -1) {
            if (b == '\n' || buf.getBuffer.length() >= MaxLineLength) {
              // strip the ansi color commands from the string
              val msg = s"Output of ${name}: ${buf.toString}"
              level match {
                case Level.ERROR => logger.error(msg)
                case Level.WARN => logger.warn(msg)
                case Level.INFO => logger.info(msg)
                case Level.DEBUG => logger.debug(msg)
                case Level.TRACE => logger.trace(msg)
              }
              buf.getBuffer.setLength(0)
            } else {
              buf.write(b)
            }
            b = parent.read()
          }
        } catch {
          case e: IOException =>
            logger.debug(s"External process was closed ${e.getMessage}")
        }
      }
    }
    val tr = new Thread(new CopyOutput(), s"output-copy-${name}-${level}")
    tr.setDaemon(true)
    tr.start()
  }

  private val pb = new ProcessBuilder(command.toList.asJava)
  pb.redirectOutput()
  pb.redirectErrorStream()

  noTracingLogger.info(s"Starting command $name ${command.map(_.limit(160))}")
  private val rt = pb.start()

  dumpOutputToLogger(rt.getInputStream, Level.DEBUG)
  dumpOutputToLogger(rt.getErrorStream, Level.INFO)

  def kill(force: Boolean = true): Unit = {
    if (rt.isAlive) {
      try {
        if (!force) {
          noTracingLogger.debug(s"Killing process $name normally")
          val _ = rt.destroy()
        }
        if (
          force || !rt.waitFor(timeouts.shutdownProcessing.unwrap.toMillis, TimeUnit.MILLISECONDS)
        ) {
          noTracingLogger.info(s"Killing process $name forcibly")
          val _ = rt.destroyForcibly()
        }
      } catch {
        case ex: Throwable => noTracingLogger.error(s"Failed to kill forcibly: ${command}", ex)
      }
    } else {
      noTracingLogger.warn(s"Process $name is already shut down")
    }
  }

  def restart(): BackgroundRunner = {
    kill()
    Threading.sleep(waitBeforeRestartMs.toLong)
    noTracingLogger.info(s"Restarting background runner with ${command}")
    new BackgroundRunner(name, command, timeouts, loggerFactory)
  }

  override protected def onClosed(): Unit = {
    if (rt.isAlive) {
      noTracingLogger.debug("Shutting down external process")
      rt.destroy()
    }
  }

}

object BackgroundRunner {
  private val MaxLineLength = 8192
}

object BackgroundRunnerHelpers {

  /** Yields the jvm params specifying the current classpath, e.g., `Seq("-cp", myClassPath)`.
    * Excludes sbt dependencies.
    * @throws java.lang.IllegalStateException if there is no file `classpath.txt` in the working directory.
    */
  def extractClassPathParams(): Seq[String] = {
    loadIntelliJClasspath() match {
      case Some(cp) =>
        Seq("-cp", cp)
      case None =>
        val cpFile = tryGetClasspathFile()
        Seq(s"@${cpFile.name}")
    }
  }

  private def loadIntelliJClasspath(): Option[String] =
    Some(System.getProperty("java.class.path")).filter(!_.matches(".*sbt-launch.*\\.jar"))

  private def tryGetClasspathFile(): File = {
    val cpFile = File(s"classpath.txt")
    if (cpFile.exists()) {
      cpFile
    } else {
      throw new IllegalStateException(
        "Process is started using sbt, however you need to run `sbt dumpClassPath` before running external processes."
      )
    }
  }

  /** Yields a sequence with the elements of the current classpath.
    * Excludes sbt dependencies.
    * @throws java.lang.IllegalStateException if there is no file `classpath.txt` in the working directory.
    */
  def extractClassPath(): Seq[String] = {
    loadIntelliJClasspath() match {
      case Some(cp) => cp.split(":").toSeq
      case None =>
        val cpFile = tryGetClasspathFile()
        cpFile.contentAsString.stripPrefix("-cp ").split(":").toSeq
    }
  }

  def createParticipantStartupFile(
      targetFilename: String,
      dar: Option[String],
      urls: Seq[String],
  ): Unit = {
    val fw = new java.io.FileWriter(targetFilename)
    val upload = dar
      .map { filename =>
        s"""
      |  participant.dars.upload("${filename}")
      |  """.stripMargin
      }
      .getOrElse("")
    fw.write(s"""
         |participants.local.foreach { participant =>
         |  participant.start()
         |  if(participant.domains.list_registered().length == 0) {
         |  """.stripMargin)
    urls.zipWithIndex.foreach { case (url, index) =>
      val dn = s"domain${index}"
      fw.write("    participant.domains.connect(\"" + dn + "\", \"" + url + "\")\n")
    }
    fw.write(s"""
         |  } else
         |    participant.domains.reconnect_all()
         |  ${upload}
         |}
        """.stripMargin)
    fw.close()
  }
  @tailrec
  def waitUntilUp(port: RequireTypes.Port, retries: Int): Unit = {
    try {
      Threading.sleep(2000)
      val socket = new java.net.Socket("localhost", port.unwrap)
      socket.getInputStream.close()
      socket.close()
      println(s"process at port ${port} is active")
    } catch {
      case _: java.io.IOException =>
        if (retries > 0) {
          waitUntilUp(port, retries - 1)
        } else {
          throw new RuntimeException(s"Unable to connect to ${port}")
        }
    }
  }

}
