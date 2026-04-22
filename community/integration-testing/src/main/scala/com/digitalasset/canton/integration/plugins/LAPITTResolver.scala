// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtil
import io.circe.*
import io.circe.generic.semiauto.*
import io.circe.parser.*

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{Authenticator, PasswordAuthentication, URI}
import java.nio.file.StandardOpenOption
import scala.io.Source
import scala.sys.process.*
import scala.util.matching.Regex
import scala.util.{Failure, Try}

sealed trait LAPITTResolver {
  type Release <: LAPITTRelease

  def listAllReleases(): Seq[Release]
  def download(
      release: Release,
      lfVersion: UseLedgerApiTestTool.LfVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Try[Unit]
}

object LAPITTArtifactoryResolver extends LAPITTResolver {
  type Release = LAPITTRelease.Artifactory

  private val httpClient = HttpClient.newBuilder().authenticator(getAuthenticator()).build()

  override def listAllReleases(): Seq[LAPITTRelease.Artifactory] = {
    val artifactoryDirectoryUrl =
      "https://digitalasset.jfrog.io/artifactory/api/storage/canton-internal/com/digitalasset/canton/ledger-api-test-tool_2.13"
    val request = HttpRequest.newBuilder(new URI(artifactoryDirectoryUrl)).build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      sys.error(
        s"Failed to fetch ledger api test tool versions from $artifactoryDirectoryUrl. Response: $response"
      )

    // the jfrog API returns a JSON object containing the folders in the children field
    val json = parse(response.body) match {
      case Left(err) => sys.error(s"Failed to parse artifactory response body: ${err.getMessage}")
      case Right(j) => j
    }

    final case class ArtifactoryItem(uri: String, folder: Boolean)
    implicit val artifactoryItemDecoder: Decoder[ArtifactoryItem] = deriveDecoder[ArtifactoryItem]

    val files = json.hcursor.downField("children").as[Seq[ArtifactoryItem]] match {
      case Left(err) => sys.error(s"Failed to decode artifactory children: ${err.getMessage}")
      case Right(seq) => seq
    }

    files
      .map(_.uri)
      .map(str => if (str.startsWith("/")) str.drop(1) else str)
      .filterNot(_.contains("100000000"))
      .map(LAPITTRelease.Artifactory(_))
  }

  override def download(
      release: LAPITTRelease.Artifactory,
      lfVersion: UseLedgerApiTestTool.LfVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Try[Unit] = {
    val filename = s"ledger-api-test-tool${lfVersion.testToolSuffix}_2.13-${release.version}.jar"
    val uri =
      s"https://digitalasset.jfrog.io/artifactory/canton-internal/com/digitalasset/canton/ledger-api-test-tool_2.13/${release.version}/$filename"
    LAPITTResolver.download(uri, destination, logger, httpClient, retries = 3)
  }

  private def getAuthenticator(): Authenticator = {
    lazy val (usernameFromNetrc, passwordFromNetrc) = loadCredentialsFromNetrcFile()
    val passwordAuthentication = new PasswordAuthentication(
      sys.env
        .get("ARTIFACTORY_USER")
        .flatMap(OptionUtil.emptyStringAsNone)
        .orElse(sys.env.get("ARTIFACTORY_USERNAME").flatMap(OptionUtil.emptyStringAsNone))
        .orElse(usernameFromNetrc.flatMap(OptionUtil.emptyStringAsNone))
        .getOrElse(
          throw new IllegalArgumentException(
            "env vars ARTIFACTORY_USER or ARTIFACTORY_USERNAME not set or empty" +
              "and no login entry found in the netrc file (if existing)"
          )
        ),
      sys.env
        .get("ARTIFACTORY_PASSWORD")
        .flatMap(OptionUtil.emptyStringAsNone)
        .orElse(sys.env.get("ARTIFACTORY_TOKEN").flatMap(OptionUtil.emptyStringAsNone))
        .orElse(passwordFromNetrc)
        .getOrElse("")
        .toCharArray,
    )
    new Authenticator {
      override def getPasswordAuthentication(): PasswordAuthentication = passwordAuthentication
    }
  }

  private def loadCredentialsFromNetrcFile(): (Option[String], Option[String]) = {
    val netrcPath = System.getProperty("user.home") + "/.netrc"
    val machine = "digitalasset.jfrog.io"

    val netrcFile = Source.fromFile(netrcPath)
    val content = netrcFile.mkString

    val pattern = s"(?m)^machine\\s+$machine\\s+login\\s+(\\S+)\\s+password\\s+(\\S+)".r

    val username = pattern.findFirstMatchIn(content).map(_.group(1))
    val password = pattern.findFirstMatchIn(content).map(_.group(2))

    netrcFile.close()

    (username, password)
  }
}

object LAPITTS3Resolver extends LAPITTResolver {
  type Release = LAPITTRelease.S3

  override def listAllReleases(): Seq[LAPITTRelease.S3] = {
    val output =
      """aws s3api list-objects-v2 --bucket canton-public-releases --prefix ledger-api-test-tool/ --query 'CommonPrefixes[].{Prefix: Prefix}' --output text --no-sign-request --delimiter "/"""".stripMargin.!!
    output
      .split("\n")
      .map(dirName =>
        LAPITTRelease.S3(dirName.stripPrefix("ledger-api-test-tool/").stripSuffix("/"))
      )
      .toSeq
  }

  override def download(
      release: LAPITTRelease.S3,
      lfVersion: UseLedgerApiTestTool.LfVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Try[Unit] = {
    val filename = s"ledger-api-test-tool${lfVersion.testToolSuffix}-${release.version}.jar"
    val uri =
      s"https://canton-public-releases.s3.amazonaws.com/ledger-api-test-tool/${release.version}/$filename"
    LAPITTResolver.download(uri, destination, logger, HttpClient.newHttpClient, retries = 0)
  }
}

object LAPITTResolver {
  private[plugins] def download(
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

// Test tool version - abstracted so that we can handle various repositories
sealed trait LAPITTRelease extends Product with Serializable {
  def repositoryName: String
  def version: String
  def download(
      lfVersion: UseLedgerApiTestTool.LfVersion,
      destination: File,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Try[Unit]

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

object LAPITTRelease {
  final case class Artifactory(version: String) extends LAPITTRelease {
    override def repositoryName: String = "Artifactory"
    override def download(
        lfVersion: UseLedgerApiTestTool.LfVersion,
        destination: File,
        logger: TracedLogger,
    )(implicit tc: TraceContext): Try[Unit] =
      LAPITTArtifactoryResolver.download(this, lfVersion, destination, logger)
  }

  final case class S3(version: String) extends LAPITTRelease {
    override def repositoryName: String = "S3"
    override def download(
        lfVersion: UseLedgerApiTestTool.LfVersion,
        destination: File,
        logger: TracedLogger,
    )(implicit tc: TraceContext): Try[Unit] =
      LAPITTS3Resolver.download(this, lfVersion, destination, logger)
  }
}
