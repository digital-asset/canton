// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.health.HttpHealthServer.{LivenessPaths, ReadinessPaths, matchOneOf}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpResponse, ResponseEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.directives.DebuggingDirectives
import org.apache.pekko.http.scaladsl.server.{Directive, Route}

import java.util.concurrent.atomic.AtomicReference

/** HTTP health server allowing setting liveness and readiness while running, useful for reporting
  * liveness during startup, while components are still starting and switching to reporting actual
  * component statuses later. Initially reports NOT_SERVING if no HealthServices are present.
  * Exposes:
  *
  * Readiness endpoints:
  *   - /health (legacy)
  *   - /health/readiness
  *   - /health/ready
  *
  * Liveness endpoints:
  *   - /health/liveness
  *   - /health/live
  */
class HttpHealthServer(
    initialLiveness: Option[HealthService],
    initialReadiness: Option[HealthService],
    address: String,
    port: Port,
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit system: ActorSystem)
    extends FlagCloseableAsync
    with NamedLogging {

  private val liveness = new AtomicReference[Option[HealthService]](initialLiveness)
  private val readiness = new AtomicReference[Option[HealthService]](initialReadiness)

  def setReadiness(service: HealthService): Unit =
    readiness.set(Some(service))

  def setLiveness(service: HealthService): Unit =
    liveness.set(Some(service))

  private val notServing: HealthService = new HealthService with NamedLogging {
    override val name: String = "not-serving"
    override protected val loggerFactory: NamedLoggerFactory = HttpHealthServer.this.loggerFactory
    override protected val timeouts: ProcessingTimeout = HttpHealthServer.this.timeouts
    override protected def initialHealthState: ServingStatus = ServingStatus.NOT_SERVING
    override protected def combineDependentStates: ServingStatus = ServingStatus.NOT_SERVING
    override protected def closingState: ServingStatus = ServingStatus.NOT_SERVING
    override def dependencies: Seq[HealthQuasiComponent] = Seq.empty
  }

  private val binding = {
    import TraceContext.Implicits.Empty.*
    timeouts.unbounded.await(s"Binding the health server")(
      Http()
        .newServerAt(address, port.unwrap)
        .bind(livenessRoute ~ readinessRoute)
    )
  }

  /** Routes for powering the health server.
    *
    * Provides:
    *   - GET /health => calls check and returns:
    *     - 200 if healthy
    *     - 503 if unhealthy
    *     - 500 if the check fails
    */
  private def livenessRoute: Route = {
    def renderStatus(status: Seq[ComponentStatus]): ResponseEntity = HttpEntity(
      status.map(_.toString).mkString("\n")
    )
    implicit val _marshaller: ToResponseMarshaller[(ServingStatus, Seq[ComponentStatus])] =
      Marshaller.opaque {
        case (ServingStatus.SERVING, status) =>
          HttpResponse(status = StatusCodes.OK, entity = renderStatus(status))
        case (ServingStatus.NOT_SERVING, status) =>
          HttpResponse(status = StatusCodes.ServiceUnavailable, entity = renderStatus(status))
        case (_, status) =>
          HttpResponse(status = StatusCodes.InternalServerError, entity = renderStatus(status))
      }

    get {
      matchOneOf(LivenessPaths) {
        val activeLiveness = liveness.get().getOrElse(notServing)
        logger.debug(
          s"Reporting liveness ${activeLiveness.getState.toString}"
        )(TraceContext.empty)
        DebuggingDirectives.logRequest("health-liveness-request") {
          DebuggingDirectives.logRequestResult("health-liveness-request-response") {
            complete(
              (activeLiveness.getState, activeLiveness.dependencies.map(_.toComponentStatus))
            )
          }
        }
      }
    }
  }

  private def readinessRoute: Route = {
    def renderStatus(status: Seq[ComponentStatus]): ResponseEntity = HttpEntity(
      status.map(_.toString).mkString("\n")
    )
    implicit val _marshaller: ToResponseMarshaller[(ServingStatus, Seq[ComponentStatus])] =
      Marshaller.opaque {
        case (ServingStatus.SERVING, status) =>
          HttpResponse(status = StatusCodes.OK, entity = renderStatus(status))
        case (ServingStatus.NOT_SERVING, status) =>
          HttpResponse(status = StatusCodes.ServiceUnavailable, entity = renderStatus(status))
        case (_, status) =>
          HttpResponse(status = StatusCodes.InternalServerError, entity = renderStatus(status))
      }

    get {
      matchOneOf(ReadinessPaths) {
        val activeReadiness = readiness.get().getOrElse(notServing)
        logger.debug(
          s"Reporting readiness ${activeReadiness.getState.toString}"
        )(TraceContext.empty)
        DebuggingDirectives.logRequest("health-readiness-request") {
          DebuggingDirectives.logRequestResult("health-readiness-request-response") {
            complete(
              (activeReadiness.getState, activeReadiness.dependencies.map(_.toComponentStatus))
            )
          }
        }
      }
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    List[AsyncOrSyncCloseable](
      AsyncCloseable("binding", binding.unbind(), timeouts.shutdownNetwork)
    )
  }
}

object HttpHealthServer {
  val ReadinessPaths: List[String] = List("health", "health/ready", "health/readiness")
  val LivenessPaths: List[String] = List("health/live", "health/liveness")

  private def matchOneOf(paths: List[String]): Directive[Unit] =
    paths.map(p => path(separateOnSlashes(p))).fold(reject.toDirective[Unit])(_ | _)
}
