// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.StandardOpenOption
import scala.sys.process.*
import scala.util.matching.Regex
import scala.util.{Failure, Try}

object LAPITTResolver {
  def listAllReleases(): Seq[LAPITTRelease] = {
    val output =
      """aws s3api list-objects-v2 --bucket canton-public-releases --prefix ledger-api-test-tool/ --query 'CommonPrefixes[].{Prefix: Prefix}' --output text --no-sign-request --delimiter "/"""".stripMargin.!!
    output
      .split("\n")
      .map(dirName => LAPITTRelease(dirName.stripPrefix("ledger-api-test-tool/").stripSuffix("/")))
      .toSeq
  }

  def download(
      release: LAPITTRelease,
      lfVersion: UseLedgerApiTestTool.LfVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Try[Unit] = {
    val filename = s"ledger-api-test-tool${lfVersion.testToolSuffix}-${release.version}.jar"
    val uri =
      s"https://canton-public-releases.s3.amazonaws.com/ledger-api-test-tool/${release.version}/$filename"
    LAPITTResolver.download(uri, destination, logger, HttpClient.newHttpClient, retries = 1)
  }

  private def download(
      url: String,
      destination: File,
      logger: TracedLogger,
      httpClient: HttpClient,
      retries: Int,
  )(implicit tc: TraceContext): Try[Unit] = {
    val stdErrLogger = new BufferedProcessLogger()
    lazy val stdErrOutput = stdErrLogger.output("OUTPUT: ")
    Try {
      // not using new URL(url) #> destination.toJava !! processLogger
      // as IOExceptions during the download will be written to stdout and there is no way to override this
      // behaviour. https://github.com/scala/scala/blob/2.13.x/src/library/scala/sys/process/ProcessImpl.scala#L192

      val request = HttpRequest.newBuilder(new URI(url)).build()
      val response = httpClient.send(
        request,
        HttpResponse.BodyHandlers.ofFile(
          destination.path,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING,
        ),
      )
      if (response.statusCode != 200) {
        sys.error(s"Failed to download ledger api test tool. Response: $response")
      }
      logger.debug("Verifying downloaded archive")
      val output = s"jar -tvf ${destination.toJava.getAbsolutePath}" !! stdErrLogger
      if (output.nonEmpty) logger.info(output)
      if (stdErrOutput.nonEmpty) logger.info(stdErrOutput)
    }.recoverWith { case t =>
      if (destination.exists) destination.delete(swallowIOExceptions = true)
      if (retries > 0) {
        logger.info(s"Failed to download from $url. Exception ${t.getMessage}. Will retry")
        if (stdErrOutput.nonEmpty) logger.info(stdErrOutput)
        Threading.sleep(2000)
        download(url, destination, logger, httpClient, retries - 1)
      } else {
        logger.info(s"Failed to download from $url. Exception ${t.getMessage}. Giving up", t)
        if (stdErrOutput.nonEmpty) logger.warn(stdErrOutput)
        Failure(t)
      }
    }
  }
}

final case class LAPITTRelease(version: String) {
  private val versionPattern: Regex = """^(\d+\.\d+\.\d+)-(ad-hoc|snapshot)\.(\d{8}\.\d{4}).*""".r

  // the major.minor.patch part of the version
  def baseVersion: Option[String] = version match {
    case versionPattern(majorMinorPatch, _, _) => Some(majorMinorPatch)
    case _ => None
  }

  def releaseDate: Option[String] = version match {
    case versionPattern(_, _, date) => Some(date)
    case _ => None
  }
}
