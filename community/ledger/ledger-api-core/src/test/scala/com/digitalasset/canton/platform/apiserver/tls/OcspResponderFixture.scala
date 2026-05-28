// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.testing.utils.{OwnedResource, PekkoBeforeAndAfterAll}
import com.digitalasset.canton.lifecycle.HasSynchronizeWithClosing
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Success
import com.digitalasset.canton.util.{ConcurrentBufferedLogger, retry}
import org.scalatest.Suite
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

trait OcspResponderFixture extends PekkoBeforeAndAfterAll { this: Suite =>

  private val ec: ExecutionContext = system.dispatcher

  private val ResponderHost: String = "127.0.0.1"
  private val ResponderPort: Int = 2560

  protected def indexPath: String
  protected def caCertPath: String
  protected def ocspKeyPath: String
  protected def ocspCertPath: String
  protected def ocspTestCertificate: String

  private val processLogger = new ConcurrentBufferedLogger

  private val logger = LoggerFactory.getLogger(getClass)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try {
      responderResource.setup()
    } catch {
      case e: Throwable =>
        // at least one of the two ocsp processes failed
        logger.error(processLogger.output())
        throw e
    }
  }

  override protected def afterAll(): Unit = {
    responderResource.close()
    super.afterAll()
  }

  private val opensslExecutable: String = "openssl"

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
    indexPath,
    "-CA",
    caCertPath,
    "-rkey",
    ocspKeyPath,
    "-rsigner",
    ocspCertPath,
  )

  private def testOcspRequestCommand = List(
    opensslExecutable,
    "ocsp",
    "-CAfile",
    caCertPath,
    "-url",
    s"http://$ResponderHost:$ResponderPort",
    "-resp_text",
    "-issuer",
    caCertPath,
    "-cert",
    ocspTestCertificate,
  )
}
