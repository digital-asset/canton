// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.HealthReporting.ComponentState.{
  Degraded,
  Failed,
  Ok,
  UnhealthyState,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.protobuf.services.HealthStatusManager

import java.util.concurrent.atomic.AtomicReference

object HealthReporting extends PrettyInstances {
  import com.digitalasset.canton.logging.pretty.Pretty.*

  /** State of a component
    */
  sealed trait ComponentState
  object ComponentState {

    object Unhealthy {
      def unapply(state: ComponentState): Option[UnhealthyState] = state match {
        case Ok => None
        case Degraded(degraded) => Some(degraded)
        case Failed(failed) => Some(failed)
      }
    }

    /** Ok state
      */
    case object Ok extends ComponentState

    /** Unhealthy state data
      *
      * @param description description of the state
      * @param error       associated canton error
      */
    case class UnhealthyState(
        description: Option[String] = None,
        error: Option[CantonError] = None,
        elc: Option[ErrorLoggingContext] = None,
    )

    /** Degraded state, as in not fully but still functional. A degraded component will NOT cause a service
      * to report NOT_SERVING
      *
      * @param degraded data
      */
    case class Degraded(degraded: UnhealthyState = UnhealthyState()) extends ComponentState

    /** The component has failed, any service that depends on it will report NOT_SERVING
      *
      * @param failed data
      */
    case class Failed(failed: UnhealthyState = UnhealthyState()) extends ComponentState
  }

  implicit val implicitPrettyString: Pretty[String] = PrettyInstances.prettyString
  implicit val prettyOkState: Pretty[ComponentState.Ok.type] =
    prettyOfObject[ComponentState.Ok.type]
  implicit val prettyUnhealthyState: Pretty[ComponentState.UnhealthyState] =
    prettyOfClass[ComponentState.UnhealthyState](
      param("description", _.description, _.description.isDefined),
      param(
        "error",
        { unhealthy =>
          unhealthy.error.map { error =>
            error.code.codeStr(unhealthy.elc.flatMap(_.traceContext.traceId))
          }
        },
        _.error.isDefined,
      ),
    )
  implicit val prettyFailedState: Pretty[ComponentState.Failed] = prettyOfClass(
    param("state", _.failed)
  )
  implicit val prettyDegradedState: Pretty[ComponentState.Degraded] = prettyOfClass(
    param("state", _.degraded)
  )
  implicit val prettyServiceState: Pretty[ComponentState] = prettyOfClass {
    case ComponentState.Ok => Some(prettyOkState.treeOf(ComponentState.Ok))
    case degraded: Degraded => Some(prettyDegradedState.treeOf(degraded))
    case failed: Failed => Some(prettyFailedState.treeOf(failed))
  }

  implicit val prettyComponentHealth: Pretty[ComponentHealth] = prettyOfClass(
    param("name", _.name),
    param("state", _.getState),
  )

  /** Combines a health status manager (exposed as a gRPC health service), with the set of health services it needs to report on.
    */
  case class ServiceHealthStatusManager(
      name: String,
      manager: HealthStatusManager,
      services: Set[ServiceHealth],
  )

  implicit val prettyServingStatus: Pretty[ServingStatus] = prettyOfClass(
    param("status", _.name())
  )

  implicit val prettyServiceHealth: Pretty[ServiceHealth] = prettyOfClass(
    param("name", _.name),
    param("state", _.getState),
  )

  /** There are 2 subtypes of HealthElement:
    * - ComponentHealth: directly reports its own health
    * - ServiceHealth: Aggregates ComponentHealth under critical and soft dependencies
    * Services are queryable through their name in the gRPC Health Check service/
    */
  sealed trait HealthElement[S] {
    protected val listeners = new AtomicReference[List[HealthListener]](Nil)
    type HealthListener = (this.type, S) => Unit

    def registerOnHealthChange(listener: HealthListener): Unit = {
      listeners.getAndUpdate(listener :: _)
      listener(this, getState)
    }

    /** Name of the health element. Used for logging.
      */
    def name: String

    def getState: S
  }

  trait ServiceHealth extends HealthElement[ServingStatus] { self =>

    /** Critical dependencies directly affecting the ServingStatus of the service.
      * If any of the dependency reports a failed status, this service will report as `NOT_SERVING`.
      */
    def criticalDependencies: Set[ComponentHealth] = Set.empty

    /** Components related to the service but that won't affect its serving status even when failed.
      *
      * @return
      */
    def softDependencies: Set[ComponentHealth] = Set.empty

    criticalDependencies.foreach(
      _.registerOnHealthChange((_: ComponentHealth, _: ComponentState) => {
        listeners.get().foreach(_(self, getState))
      })
    )

    // Compute the serving status for a service
    override def getState: ServingStatus = {
      def allComponentsOkOrDegraded: Boolean = criticalDependencies
        .map(component =>
          component.getState match {
            case ComponentState.Ok | _: ComponentState.Degraded => true
            case _ => false
          }
        )
        .forall(_ == true)

      if (allComponentsOkOrDegraded) ServingStatus.SERVING
      else ServingStatus.NOT_SERVING
    }
  }

  trait ComponentHealth extends HealthElement[ComponentState] { self: NamedLogging =>

    /** Initial state of the component
      */
    protected def initialState: ComponentState

    private lazy val stateRef = new AtomicReference[ComponentState](initialState)

    def reportHealthState(state: ComponentState)(implicit tc: TraceContext): Unit = {
      val old = stateRef.getAndSet(state)
      if (old != state) logStateChange(old)
      listeners.get().foreach(_(this, state))
    }

    override def getState: ComponentState = stateRef.get()

    /** Whether the component is in a degraded state. Note: degraded here means exactly _degraded_, to know if the component
      * is just unhealthy (i.e degraded or failed), use isUnhealthy.
      */
    def isDegraded: Boolean = getState match {
      case _: Degraded => true
      case _ => false
    }

    /** Whether the component is in a failed state. Note: failed here means exactly _failed_, to know if the component
      * is just unhealthy (i.e degraded or failed), use isUnhealthy.
      */
    def isFailed: Boolean = getState match {
      case _: Failed => true
      case _ => false
    }

    /** Whether the component is in a degraded or failed state.
      */
    def isUnhealthy: Boolean = isDegraded || isFailed

    /** Whether the component has last reported being ok (and at least once)
      */
    def isOk: Boolean = getState match {
      case Ok => true
      case _ => false
    }

    /** If the component is unhealthy, return the unhealthy state with information about the issue.
      */
    def getUnhealthyState: Option[UnhealthyState] =
      getState match {
        case ComponentState.Unhealthy(unhealthy) =>
          Some(unhealthy)
        case _ =>
          None
      }

    /** Set the health state to Ok and if the previous state was unhealthy, log a message to inform about the resolution
      * of the ongoing issue.
      */
    def resolveUnhealthy(implicit traceContext: TraceContext): Unit = {
      reportHealthState(ComponentState.Ok)
    }

    private def logStateChange(
        oldState: ComponentState
    )(implicit traceContext: TraceContext): Unit = {
      logger
        .info(
          show"$name is now in state $getState. Previous state was $oldState"
        )
    }

    /** Report that the component is now degraded.
      * Note that this will override the component state, even if it is currently failed!
      */
    def degradationOccurred(error: CantonError)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState(
        ComponentState.Degraded(
          UnhealthyState(None, Some(error))
        )
      )
    }

    /** Report that the component is now failed
      */
    def failureOccurred(error: CantonError)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState(
        ComponentState.Failed(
          UnhealthyState(None, Some(error))
        )
      )
    }

    /** Report that the component is now degraded.
      * Note that this will override the component state, even if it is currently failed!
      */
    def degradationOccurred(error: String)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState(
        ComponentState.Degraded(
          UnhealthyState(Some(error))
        )
      )
    }

    /** Report that the component is now failed
      */
    def failureOccurred(error: String)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState(
        ComponentState.Failed(
          UnhealthyState(Some(error))
        )
      )
    }
  }
}
