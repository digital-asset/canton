// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console
import ammonite.compiler.Parsers
import ammonite.interp.Watchable
import ammonite.util.{Res, _}
import com.digitalasset.canton.CantonScript
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ResourceUtil.withResource

import java.io.InputStream
import java.lang.System.lineSeparator
import scala.io.Source

/** Will create a real REPL for interactive entry and evaluation of commands
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object InteractiveConsole extends NoTracing {
  def apply(
      consoleEnvironment: ConsoleEnvironment,
      noTty: Boolean = false,
      bootstrapScript: Option[CantonScript] = None,
      logger: TracedLogger,
  ): Boolean = {

    val (_lock, baseOptions) = AmmoniteConsoleConfig.create(
      consoleEnvironment.environment.config.parameters.console,
      // for including implicit conversions
      predefCode =
        ConsoleEnvironmentBinding.predefCode(interactive = true, noTty = noTty) + lineSeparator(),
      welcomeBanner = Some(loadBanner()),
      isRepl = true,
      logger,
    )
    // where are never going to release the lock here

    val options = baseOptions

    // instead of using Main.run() from ammonite, we "inline"
    // that code here as "startup" in order to include the
    // bootstrap script in the beginning
    // the issue is that most bootstrap scripts require the bound repl arguments
    // (such as all, help, participant1, etc.), which are made available only here
    // so we can't run Main.runScript or so as the "result" of the script are lost then
    // in the REPL.
    def startup(replArgs: Bind[_]*): (Res[Any], Seq[(Watchable, Long)]) = {
      options.instantiateRepl(replArgs.toIndexedSeq) match {
        case Left(missingPredefInfo) => missingPredefInfo
        case Right(repl) =>
          repl.initializePredef().getOrElse {
            // warm up the compilation
            val warmupThread = new Thread(() => {
              val _ = repl.warmup()
            })
            warmupThread.setDaemon(true)
            warmupThread.start()
            // load and run bootstrap script
            val initRes = bootstrapScript.map(fname => {
              // all we do is to write interp.load.module(...) into the console and let it interpret it
              // the lines here are stolen from Repl.warmup()
              logger.info(s"Running startup script $fname")
              val loadModuleCode = fname.path
                .map(p => "interp.load.module(os.Path(" + toStringLiteral(p.getAbsolutePath) + "))")
                .getOrElse(fname.read().getOrElse(""))
              val stmts = Parsers
                .split(loadModuleCode)
                .getOrElse(
                  sys.error("Expected parser to always return a success or failure")
                ) match { // `Parsers.split` returns an Option but should always be Some as we always provide code
                case Left(error) => sys.error(s"Unable to parse code: $error")
                case Right(parsed) => parsed
              }
              // if we run this with currentLine = 0, it will break the console output
              repl.interp.processLine(loadModuleCode, stmts, 10000000, silent = true, () => ())
            })

            // now run the repl or exit if the bootstrap script failed
            initRes match {
              case Some(Res.Success(_)) | None =>
                val exitValue = Res.Success(repl.run())
                (exitValue.map(repl.beforeExit), repl.interp.watchedValues.toSeq)
              case Some(a @ Res.Exception(x, y)) =>
                val additionalMessage = if (y.isEmpty) "" else s", $y"
                logger.error(
                  s"Running bootstrap script failed with an exception (${x.getMessage}$additionalMessage)!"
                )
                logger.debug("Ammonite exception thrown is", x)
                (a, repl.interp.watchedValues.toSeq)
              case Some(x) =>
                logger.error(s"Running bootstrap script failed with ${x}")
                (x, repl.interp.watchedValues.toSeq)
            }
          }
      }
    }

    val (result, _) = startup(consoleEnvironment.bindings: _*)

    result match {
      // as exceptions are caught when in the REPL this is almost certainly from code in the predef
      case Res.Exception(exception, _) =>
        System.err.println(exception.getMessage)
        logger.debug("Execution of interactive script returned exception", exception)
        false
      case Res.Failure(err) =>
        System.err.println(err)
        logger.debug(s"Execution of interactive script returned failure ${err}")
        false
      case _ => true
    }
  }

  /** Turns the given String into a string literal suitable for including in scala code.
    * Includes adding surrounding quotes.
    * e.g. `some\\path` will return `"some\\\\path"`
    */
  private def toStringLiteral(raw: String): String = {
    // uses the scala reflection primitives but doesn't actually do any reflection
    import scala.reflect.runtime.universe._

    Literal(Constant(raw)).toString()
  }

  private def loadBanner(): String = {
    val stream: InputStream = Option(getClass.getClassLoader.getResourceAsStream("repl/banner.txt"))
      .getOrElse(sys.error("banner resource not found"))

    withResource(stream) { Source.fromInputStream(_).mkString }
  }

}
