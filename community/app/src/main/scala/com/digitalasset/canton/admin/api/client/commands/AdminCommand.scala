// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultBoundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.{NonNegativeDuration, ProcessingTimeout}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Context, ManagedChannel, Status, StatusException, StatusRuntimeException}

import java.net.URL
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

trait AdminCommand[Req, Res, Result] {

  /** Create the request from configured options
    */
  def createRequest(): Either[String, Req]

  /** Handle the response the service has provided
    */
  def handleResponse(response: Res): Either[String, Result]

  /** Determines within which time frame the request should complete
    *
    * Some requests can run for a very long time. In this case, they should be "unbounded".
    * For other requests, you will want to set a custom timeout apart from the global default bounded timeout
    */
  def timeoutType: TimeoutType = DefaultBoundedTimeout

  /** Command's full name used to identify command in logging and span reporting
    */
  def fullName: String =
    // not using getClass.getSimpleName because it ignores the hierarchy of nested classes, and it also throws unexpected exceptions
    getClass.getName.split('.').last.replace("$", ".")
}

trait HttpAdminCommand[Req, Res, Result] extends AdminCommand[Req, Res, Result] {

  type Svc

  def createService(
      baseUrl: URL,
      httpClient: HttpClient,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): Svc

  /** Submit the created request to our service
    */
  def submitRequest(service: Svc, request: Req)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Res]

}

object HttpAdminCommand {
  class NotSupported[Result](name: String)
      extends WithResult[Result](name, Left(s"Command $name is not supported by HTTP sequencer"))
  class Stub[Result](name: String, result: Result) extends WithResult[Result](name, Right(result))

  object NoopService
  object Placeholder
  sealed abstract class WithResult[Result](name: String, resultE: Either[String, Result])
      extends HttpAdminCommand[Placeholder.type, Placeholder.type, Result] {
    override type Svc = NoopService.type

    override def createService(
        baseUrl: URL,
        httpClient: HttpClient,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): NoopService.type =
      NoopService

    override def submitRequest(service: NoopService.type, request: Placeholder.type)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Placeholder.type] =
      // doing that instead of EitherT.pure because this doesnt require an ec in scope
      EitherT(Future.successful(Placeholder.asRight[String]))

    override def createRequest(): Either[String, Placeholder.type] = Right(Placeholder)

    override def handleResponse(response: Placeholder.type): Either[String, Result] =
      resultE

    override def fullName: String = name
  }
}

/** cantonctl GRPC Command
  */
trait GrpcAdminCommand[Req, Res, Result] extends AdminCommand[Req, Res, Result] {

  type Svc <: AbstractStub[Svc]

  /** Create the GRPC service to call
    */
  def createService(channel: ManagedChannel): Svc

  /** Submit the created request to our service
    */
  def submitRequest(service: Svc, request: Req): Future[Res]

}

object GrpcAdminCommand {
  sealed trait TimeoutType extends Product with Serializable

  /** Custom timeout triggered by the client */
  case class CustomClientTimeout(timeout: NonNegativeDuration) extends TimeoutType

  /** The Server will ensure the operation is timed out so the client timeout is set to an infinite value */
  case object ServerEnforcedTimeout extends TimeoutType
  case object DefaultBoundedTimeout extends TimeoutType
  case object DefaultUnboundedTimeout extends TimeoutType

  object GrpcErrorStatus {
    def unapply(ex: Throwable): Option[Status] = ex match {
      case e: StatusException => Some(e.getStatus)
      case re: StatusRuntimeException => Some(re.getStatus)
      case _ => None
    }
  }

  private[digitalasset] def streamedResponse[Request, Response, Result](
      service: (Request, StreamObserver[Response]) => Unit,
      extract: Response => Seq[Result],
      request: Request,
      expected: Int,
      timeout: FiniteDuration,
      scheduler: ScheduledExecutorService,
  ): Future[Seq[Result]] = {
    val promise = Promise[Seq[Result]]()
    val buffer = ListBuffer[Result]()
    val context = Context.ROOT.withCancellation()

    def success(): Unit = blocking(buffer.synchronized {
      context.close()
      promise.trySuccess(buffer.toList).discard[Boolean]
    })

    context.run(() =>
      service(
        request,
        new StreamObserver[Response]() {
          override def onNext(value: Response): Unit = {
            val extracted = extract(value)
            blocking(buffer.synchronized {
              if (buffer.lengthCompare(expected) < 0) {
                buffer ++= extracted
                if (buffer.lengthCompare(expected) >= 0) {
                  success()
                }
              }
            })
          }

          override def onError(t: Throwable): Unit = {
            t match {
              case GrpcErrorStatus(status) if status.getCode == Status.CANCELLED.getCode =>
                success()
              case _ =>
                val _ = promise.tryFailure(t)
            }
          }

          override def onCompleted(): Unit = {
            success()
          }
        },
      )
    )
    scheduler.schedule(
      new Runnable() {
        override def run(): Unit = {
          val _ = context.cancel(Status.CANCELLED.asException())
        }
      },
      timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
    promise.future
  }
}
