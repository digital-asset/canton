// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseVersion
import com.digitalasset.daml.lf.language.LanguageVersion

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.StandardOpenOption
import scala.sys.process.*
import scala.util.{Failure, Success, Try}

object LAPITTResolver {
  def listAllReleases(): Seq[ReleaseVersion] = {
    val output =
      """aws s3api list-objects-v2 --bucket canton-public-releases --prefix ledger-api-test-tool/ --query 'CommonPrefixes[].{Prefix: Prefix}' --output text --no-sign-request --delimiter "/"""".stripMargin.!!
    output
      .split("\n")
      .toSeq
      .flatMap { dirName =>
        ReleaseVersion
          .create(dirName.stripPrefix("ledger-api-test-tool/").stripSuffix("/"))
          .toOption
      }
  }

  def download(
      release: ReleaseVersion,
      lfVersion: LanguageVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Option[File] = {
    val filename = s"ledger-api-test-tool-$lfVersion-${release.fullVersion}.jar"
    val uri =
      s"https://canton-public-releases.s3.amazonaws.com/ledger-api-test-tool/${release.fullVersion}/$filename"
    LAPITTResolver
      .download(uri, destination, logger, HttpClient.newHttpClient, retries = 1)
      .fold(throw _, identity)
  }

  private def download(
      url: String,
      destination: File,
      logger: TracedLogger,
      httpClient: HttpClient,
      retries: Int,
  )(implicit tc: TraceContext): Try[Option[File]] = {
    val stdErrLogger = new BufferedProcessLogger()
    lazy val stdErrOutput = stdErrLogger.output("OUTPUT: ")
    val request = HttpRequest.newBuilder(new URI(url)).build()
    val downloadedFile = for {
      // not using new URL(url) #> destination.toJava !! processLogger
      // as IOExceptions during the download will be written to stdout and there is no way to override this
      // behaviour. https://github.com/scala/scala/blob/2.13.x/src/library/scala/sys/process/ProcessImpl.scala#L192
      response <- Try(
        httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofFile(
            destination.path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
          ),
        )
      )
      downloadedFile <-
        if (response.statusCode == 200) Try {
          s"jar -tvf ${destination.toJava.getAbsolutePath}" !! stdErrLogger
          Some(destination)
        }
        else if (response.statusCode == 404) {
          destination.delete(swallowIOExceptions = true)
          Success(None)
        } else Failure(new RuntimeException(response.toString))
    } yield downloadedFile
    downloadedFile.recoverWith { case t =>
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
