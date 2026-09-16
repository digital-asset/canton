// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.daml.lf.archive.{Dar, DarDecoder, Error as ArchiveError}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, Util}
import io.circe.{Json, yaml}

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext

final case class CouldNotReadDar(path: String, err: ArchiveError) {
  val message: String = s"Error reading DAR from $path: ${err.msg}"
}

trait DarDecodable[A] {
  def decode(a: A): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]]
}

object DarDecodable {
  implicit val darDecodableString: DarDecodable[String] = new DarDecodable[String] {
    def decode(path: String): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] =
      darDecodableFile.decode(new File(path))
  }

  implicit val darDecodableFile: DarDecodable[File] = new DarDecodable[File] {
    def decode(file: File): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] = {
      val result = DarDecoder.readArchiveFromFile(file)
      result.left.map(CouldNotReadDar(file.getAbsolutePath, _))
    }
  }

  implicit val darDecodableInputStream: DarDecodable[InputStream] = new DarDecodable[InputStream] {
    def decode(stream: InputStream): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] = {
      val result = DarDecoder.readArchive("stream", new ZipInputStream(stream))
      result.left.map(CouldNotReadDar("stream", _))
    }
  }
}

final case class UpgradeCheckMain(loggerFactory: NamedLoggerFactory) {
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  val validator =
    new PackageUpgradeValidator(CachingConfigs.defaultPackageUpgradeCache, loggerFactory)

  def check[A](paths: Array[A])(implicit darDecodable: DarDecodable[A]): Int = {
    // logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(darDecodable.decode(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      1
    } else {
      val packageMap = dars
        .flatMap(_.all)
        .map { case (pkgId, pkg) =>
          logger.debug(s"Package with ID $pkgId and metadata ${pkg.metadata}")
          pkgId -> Util.toSignature(pkg)
        }
        .toMap

      val validation = validator.validateUpgrade(packageMap.keySet, packageMap.keySet, packageMap)
      validation match {
        case Left(err: TopologyManagerError.ParticipantTopologyManagerError.Upgradeability.Error) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Left(err) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Right(()) => 0
        case _ => 1
      }
    }
  }

  def main(args: Array[String]) = sys.exit(check(args))

  // DPM component logic

  case class UpgradeCheckMode(onParticipant: Boolean, onCompiler: Boolean, dars: Seq[String])

  def dpmMain(args: Array[String]) = {
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    val argParser = new scopt.OptionParser[UpgradeCheckMode]("upgrade-check") {
      opt[Unit]("compiler")
        .action((_, cm) => cm.copy(onCompiler = true))
        .text("Run compiler upgrade checks")
      opt[Unit]("participant")
        .action((_, cm) => cm.copy(onParticipant = true))
        .text("Run participant upgrade checks")
      opt[Unit]("both")
        .action((_, cm) => cm.copy(onParticipant = true, onCompiler = true))
        .text("Run both compiler and participant upgrade checks")
      arg[Seq[String]]("DAR_FILE(S)")
        .text(
          ".dar files to check"
        )
        .unbounded()
        .required()
        .action((files, cm) => cm.copy(dars = files ++ cm.dars))
    }
    val cm = argParser.parse(args, UpgradeCheckMode(false, false, Seq.empty)).getOrElse(sys.exit(1))
    if (!cm.onParticipant && !cm.onCompiler) {
      System.err.println("Must provide one of --compiler, --participant, --both")
      System.err.println(argParser.usage)
      sys.exit(1)
    }

    // Run participant checks
    val participantExit = if (cm.onParticipant) check(cm.dars.toArray) else 0

    // Compiler checks require finding the damlc binary, so we must parse the DPM Resolution file
    val compilerExit = if (cm.onCompiler) {
      // Find the file in its env var
      val resPath = Option(System.getenv("DPM_RESOLUTION_FILE")).getOrElse(
        throw new IllegalArgumentException("No resolution file found, must be run through DPM")
      )
      // Need own running path to find resolution (in case its run from a package)
      val runPath = Paths.get(".").toAbsolutePath.normalize
      val source = scala.io.Source.fromFile(resPath)
      val content =
        try source.mkString
        finally source.close()
      val eDamlcPath =
        for {
          json <- yaml.parser.parse(content).left.map(e => e.getMessage)
          // Get the packages field to try find package specific resolution
          allPackages <-
            json.hcursor
              .downField("packages")
              .as[Map[String, Json]]
              .left
              .map(e => e.getMessage)
          // get the default-sdk field in case there is no package specific resolution
          // Default resolution is under `default-sdk.<version>`, where there will only ever be one key for `<version>`
          // We don't care what the version itself actually is, so we just select the first key in the object
          defaultResolution <-
            json.hcursor
              .downField("default-sdk")
              .as[Map[String, Json]]
              .map(m =>
                m(
                  m.keys.headOption.getOrElse(
                    throw new IllegalArgumentException(
                      "No damlc version found under default-sdk in DPM resolution file"
                    )
                  )
                )
              )
              .left
              .map(e => e.getMessage)
          // Search for package in resolution, must use `Path.equals` else windows will break
          resObj = allPackages.toList
            .map { case (p, j) => (Paths.get(p), j) }
            .find { case (p, _) => runPath.equals(p) }
            .fold(defaultResolution)(_._2)
          // Grab the actual binary path
          damlcPath <-
            resObj.hcursor
              .downField("imports")
              .downField("damlc-binary")
              .downArray
              .as[String]
              .left
              .map(e => e.getMessage)
        } yield damlcPath
      val damlcPath = eDamlcPath.fold(e => throw new IllegalArgumentException(e), p => p)
      sys.process
        .Process(
          command = damlcPath,
          arguments = "upgrade-check" +: cm.dars,
        )
        .run()
        .exitValue()
    } else 0

    // Fail if either checks fail
    sys.exit(if (participantExit != 0 || compilerExit != 0) participantExit else 0)
  }
}

object UpgradeCheckMain {
  lazy val default: UpgradeCheckMain =
    UpgradeCheckMain(NamedLoggerFactory.root)
}

object UpgradeCheckDpmMain {
  def main(args: Array[String]): Unit =
    UpgradeCheckMain.default.dpmMain(args)
}
