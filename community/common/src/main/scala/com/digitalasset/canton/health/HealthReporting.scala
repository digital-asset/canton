// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.HealthReporting.ComponentState.{
  Degraded,
  Failed,
  UnhealthyState,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.protobuf.services.HealthStatusManager

import java.util.concurrent.atomic.AtomicReference

object HealthReporting extends PrettyInstances {
  import com.digitalasset.canton.logging.pretty.Pretty.*

  /** State of a component
    */
  sealed trait ComponentState
  object ComponentState {

    val NotInitialized: ComponentState =
      ComponentState.Failed(ComponentState.UnhealthyState(Some("Not Initialized")))

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

  implicit val prettyHealthElement: Pretty[HealthElement] = prettyOfClass {
    case health: ServiceHealth => Some(prettyServiceHealth.treeOf(health))
    case health: ComponentHealth => Some(prettyComponentHealth.treeOf(health))
  }

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
  sealed trait HealthElement {
    type State
    protected val listeners = new AtomicReference[List[HealthListener]](Nil)
    type HealthListener = (this.type, State, TraceContext) => Unit

    /** Returns true when the component is not in a failed state (for components, degraded is still not failed)
      */
    def isNotFailed: Boolean

    def registerOnHealthChange(listener: HealthListener): Unit = {
      listeners.getAndUpdate(listener :: _)
      listener(this, getState, TraceContext.empty)
    }

    /** Name of the health element. Used for logging.
      */
    def name: String

    /** Current state
      */
    def getState: State
  }

  case class ServiceHealth(
      name: String,
      criticalDependencies: Seq[HealthElement] = Seq.empty,
      softDependencies: Seq[HealthElement] = Seq.empty,
  ) extends HealthElement { self =>
    override type State = ServingStatus

    criticalDependencies.foreach(c =>
      c.registerOnHealthChange((_: c.type, _: c.type#State, tc: TraceContext) =>
        listeners.get().foreach(_(self, getState, tc))
      )
    )

    override def registerOnHealthChange(listener: HealthListener): Unit = {
      super.registerOnHealthChange(listener)
    }

    override def getState: ServingStatus = {
      if (criticalDependencies.forall(_.isNotFailed)) ServingStatus.SERVING
      else ServingStatus.NOT_SERVING
    }

    override def isNotFailed: Boolean = getState == ServingStatus.SERVING

    def dependencies: Seq[HealthElement] = criticalDependencies ++ softDependencies
  }

  trait ComponentHealth extends HealthElement { self: NamedLogging =>
    override type State = ComponentState

    /** Initial state of the component
      */
    protected def initialState: ComponentState

    private lazy val stateRef = new AtomicReference[ComponentState](initialState)

    def reportHealthState_(state: ComponentState)(implicit tc: TraceContext): Unit = {
      reportHealthState(state).discard
    }

    def reportHealthState(
        state: ComponentState
    )(implicit tc: TraceContext): Boolean = {
      val old = stateRef.getAndSet(state)
      val changed = old != state
      if (changed) {
        logStateChange(old)
        listeners.get().foreach(_(this, state, tc))
      }
      changed
    }

    override def getState: ComponentState = stateRef.get()
    override def isNotFailed: Boolean = !isFailed

    /** Whether the component is in a failed state. Note: failed here means exactly _failed_, to know if the component
      * is just unhealthy (i.e degraded or failed), use isUnhealthy.
      */
    def isFailed: Boolean = getState match {
      case _: Failed => true
      case _ => false
    }

    /** Set the health state to Ok and if the previous state was unhealthy, log a message to inform about the resolution
      * of the ongoing issue.
      */
    def resolveUnhealthy(implicit traceContext: TraceContext): Boolean = {
      reportHealthState(ComponentState.Ok)
    }

    def resolveUnhealthy_(implicit traceContext: TraceContext): Unit = {
      resolveUnhealthy.discard
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
      reportHealthState_(
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
      reportHealthState_(
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
      reportHealthState_(
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
      reportHealthState_(
        ComponentState.Failed(
          UnhealthyState(Some(error))
        )
      )
    }
  }

  /** Deferred Health component, use when the health component is not instantiated at bootstrap time
    * @param uninitializedName name used to identify this component while it has not been initialized yet
    * @param prefix optional string to prepend to the name of the delegate component
    */
  case class DeferredHealthComponent(
      override val loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
  ) extends ComponentHealth
      with NamedLogging { self =>
    override protected val initialState: ComponentState = ComponentState.NotInitialized
    private val delegate = new SingleUseCell[ComponentHealth]

    override def name: String =
      delegate.get
        .map(_.name)
        .getOrElse(uninitializedName)

    override def getState: State = delegate.get
      .map(_.getState)
      .getOrElse(initialState)

    def set(element: ComponentHealth): Unit = {
      if (delegate.putIfAbsent(element).isEmpty) {
        element.registerOnHealthChange(
          (_: element.type, state: element.State, tc: TraceContext) => {
            listeners.get().foreach(_(self, state, tc))
          }
        )
      } else {
        logger.debug(s"The deferred health component $name was already set")(TraceContext.empty)
      }
    }
    override def isNotFailed: Boolean = delegate.get.exists(_.isNotFailed)
  }
}
