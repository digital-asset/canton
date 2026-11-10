// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.testing.utils.{OwnedResource, PekkoBeforeAndAfterAll}
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.HasSynchronizeWithClosing
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Success
import com.digitalasset.canton.util.{ConcurrentBufferedLogger, JarResourceUtils, retry}
import org.scalatest.Suite
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

/** Runs an OCSP responder on a unique port and provides test certificates pointing to it.
  *
  * The leaf certificates in the `test-certificates` resources carry a fixed OCSP responder URL in
  * their Authority Information Access extension. To avoid sharing that port between test suites,
  * the fixture re-issues them (same CSR, CA and serial number, so the responder's `index.txt` still
  * applies) into a temporary directory, with the URL of this fixture's responder.
  */
trait OcspResponderFixture extends PekkoBeforeAndAfterAll { this: Suite =>

  private val ec: ExecutionContext = system.dispatcher

  private val ResponderHost: String = "127.0.0.1"
  private val ResponderPort: Int = UniquePortGenerator.next.unwrap
  private val ResponderUrl: String = s"http://$ResponderHost:$ResponderPort"

  private val opensslExecutable: String = "openssl"

  private val processLogger = new ConcurrentBufferedLogger

  private val logger = LoggerFactory.getLogger(getClass)

  private def resource(name: String): File =
    JarResourceUtils.resourceFile("test-certificates/" + name)

  val caCrt: File = resource("ca.crt")
  val serverKey: File = resource("server.pem")
  val clientKey: File = resource("client.pem")
  val clientRevokedKey: File = resource("client-revoked.pem")

  private lazy val certificateDir: File =
    Files.createTempDirectory("ocsp-test-certificates").toFile

  // Serial numbers must match the ones in `index.txt`
  lazy val serverCrt: File = reissue("server", serial = "0x1000")
  lazy val clientCrt: File = reissue("client", serial = "0x1001")
  lazy val clientRevokedCrt: File = reissue("client-revoked", serial = "0x1003")

  private lazy val extensionsFile: File = {
    val file = new File(certificateDir, "extensions.cnf")
    Files
      .writeString(
        file.toPath,
        s"""[ leaf ]
           |subjectAltName = DNS:localhost, IP:127.0.0.1
           |authorityInfoAccess = OCSP;URI:$ResponderUrl
           |""".stripMargin,
        StandardCharsets.UTF_8,
      )
      .discard
    file
  }

  private def reissue(name: String, serial: String): File = {
    val out = new File(certificateDir, s"$name.crt")
    Process(
      List(
        opensslExecutable,
        "x509",
        "-req",
        "-in",
        resource(s"$name.csr").getAbsolutePath,
        "-CA",
        caCrt.getAbsolutePath,
        "-CAkey",
        resource("ca.key").getAbsolutePath,
        "-set_serial",
        serial,
        "-days",
        "7305",
        "-sha256",
        "-extfile",
        extensionsFile.getAbsolutePath,
        "-extensions",
        "leaf",
        "-out",
        out.getAbsolutePath,
      )
    ).!!(processLogger).discard
    out
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try {
      Seq(serverCrt, clientCrt, clientRevokedCrt).discard
      responderResource.setup()
    } catch {
      case e: Throwable =>
        // certificate generation or at least one of the two ocsp processes failed
        logger.error(processLogger.output())
        throw e
    }
  }

  override protected def afterAll(): Unit = {
    responderResource.close()
    Option(certificateDir.listFiles()).foreach(_.foreach(_.delete().discard))
    certificateDir.delete().discard
    super.afterAll()
  }

  lazy val responderResource: OwnedResource[ResourceContext, Process] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    new OwnedResource[ResourceContext, Process](
      owner = responderResourceOwner,
      acquisitionTimeout = 20.seconds,
      releaseTimeout = 5.seconds,
    )
  }

  private def responderResourceOwner: ResourceOwner[Process] =
    new ResourceOwner[Process] {

      override def acquire()(implicit context: ResourceContext): Resource[Process] = {
        def start(): Future[Process] =
          for {
            process <- startResponderProcess()
            _ <- verifyResponderReady()
          } yield process

        def stop(responderProcess: Process): Future[Unit] =
          Future {
            responderProcess.destroy()
          }

        Resource(start())(stop)
      }
    }

  private def startResponderProcess()(implicit ec: ExecutionContext): Future[Process] =
    Future(Process(ocspServerCommand).run(processLogger))

  private def verifyResponderReady()(implicit ec: ExecutionContext): Future[String] = {
    val tracedLogger = TracedLogger(logger)
    implicit val success: Success[Any] = retry.Success.always
    implicit val traceContext: TraceContext = TraceContext.empty
    retry
      .Pause(
        logger = tracedLogger,
        operationName = "verifyResponderReady",
        maxRetries = 3,
        delay = 5.seconds,
        hasSynchronizeWithClosing = HasSynchronizeWithClosing.NeverClosing,
      )
      .applyFut(
        Future(Process(testOcspRequestCommand).!!(processLogger)),
        retry.AllExceptionRetryPolicy,
      )
  }

  private def ocspServerCommand = List(
    opensslExecutable,
    "ocsp",
    "-port",
    ResponderPort.toString,
    "-text",
    "-index",
    resource("index.txt").getAbsolutePath,
    "-CA",
    caCrt.getAbsolutePath,
    "-rkey",
    resource("ocsp.key.pem").getAbsolutePath,
    "-rsigner",
    resource("ocsp.crt").getAbsolutePath,
  )

  private def testOcspRequestCommand = List(
    opensslExecutable,
    "ocsp",
    "-CAfile",
    caCrt.getAbsolutePath,
    "-url",
    ResponderUrl,
    "-resp_text",
    "-issuer",
    caCrt.getAbsolutePath,
    "-cert",
    clientCrt.getAbsolutePath,
  )
}
