// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.Show.Shown
import cats.implicits.catsSyntaxEitherId
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.ComponentHealthState.*
import com.digitalasset.canton.health.admin.v0 as proto
import com.digitalasset.canton.lifecycle.{FlagCloseable, RunOnShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.protobuf.services.HealthStatusManager

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap

object HealthReporting extends PrettyInstances with PrettyUtil {

  /** Combines a health status manager (exposed as a gRPC health service), with the set of health services it needs to report on.
    */
  final case class ServiceHealthStatusManager(
      name: String,
      manager: HealthStatusManager,
      services: Set[HealthService],
  )

  implicit val prettyServingStatus: Pretty[ServingStatus] = prettyOfClass(
    param("status", _.name().singleQuoted)
  )

  /** There are 2 subtypes of HealthElement:
    * - ComponentHealth: directly reports its own health
    * - ServiceHealth: Aggregates ComponentHealth under critical and soft dependencies
    * Services are queryable through their name in the gRPC Health Check service/
    */
  trait HealthElement {
    type State
    protected val listeners = new AtomicReference[List[HealthListener]](Nil)
    type HealthListener = (this.type, State, TraceContext) => Unit

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

  object ComponentStatus {
    def fromProtoV0(dependency: proto.NodeStatus.ComponentStatus): ParsingResult[ComponentStatus] =
      dependency.status match {
        case proto.NodeStatus.ComponentStatus.Status.Ok(value) =>
          ComponentStatus(
            dependency.name,
            ComponentHealthState.Ok(value.description),
          ).asRight
        case proto.NodeStatus.ComponentStatus.Status
              .Degraded(value: proto.NodeStatus.ComponentStatus.StatusData) =>
          ComponentStatus(
            dependency.name,
            Degraded(UnhealthyState(value.description)),
          ).asRight
        case proto.NodeStatus.ComponentStatus.Status.Failed(value) =>
          ComponentStatus(
            dependency.name,
            Failed(UnhealthyState(value.description)),
          ).asRight
        case _ =>
          ProtoDeserializationError.UnrecognizedField("Unknown state").asLeft
      }

    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

    implicit val componentStatusPretty: Pretty[ComponentStatus] =
      prettyInfix[ComponentStatus, Shown, ComponentHealthState](
        _.name.unquoted,
        ":",
        _.state,
      )
  }

  /** Simple representation of the health state of a component, easily (de)serializable (from)to protobuf or JSON
    */
  final case class ComponentStatus(name: String, state: ComponentHealthState)
      extends PrettyPrinting {
    def toProtoV0: proto.NodeStatus.ComponentStatus =
      proto.NodeStatus.ComponentStatus(
        name = name,
        status = state.toComponentStatusV0,
      )

    override val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
  }

  object HealthService {
    implicit val prettyServiceHealth: Pretty[HealthService] = prettyOfClass(
      param("name", _.name.unquoted),
      param("state", _.getState),
    )
  }

  final case class HealthService(
      name: String,
      criticalDependencies: Seq[BaseHealthComponent] = Seq.empty,
      softDependencies: Seq[BaseHealthComponent] = Seq.empty,
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
      if (criticalDependencies.forall(!_.isFailed)) ServingStatus.SERVING
      else ServingStatus.NOT_SERVING
    }

    def dependencies: Seq[BaseHealthComponent] = criticalDependencies ++ softDependencies
  }

  /** Abstracts common behavior of a component that reports health
    * The State is left abstract. For a generic component state, use ComponentHealth
    */
  trait BaseHealthComponent extends HealthElement { self: NamedLogging with FlagCloseable =>
    override type State <: ToComponentHealthState
    def isFailed: Boolean = getState.toComponentHealthState.isFailed
    protected def initialHealthState: State
    protected lazy val stateRef = new AtomicReference[State](initialHealthState)
    def closingState: State

    override def registerOnHealthChange(listener: this.type#HealthListener): Unit = {
      // The first time a listener is registered, also register a shutdown hook to notify them on closing and clear them
      // to avoid further updates from that component
      if (listeners.get().isEmpty) {
        runOnShutdown(new RunOnShutdown {
          override def name: String = "close-listeners"
          override def done: Boolean = listeners.get().isEmpty
          override def run(): Unit = {
            reportHealthState_(closingState)(TraceContext.empty)
            listeners.set(Nil)
          }
        })(TraceContext.empty)
      }

      super.registerOnHealthChange(listener)
    }

    def reportHealthState(
        state: State
    )(implicit tc: TraceContext): Boolean = {
      val old = stateRef.getAndSet(state)
      val changed = old != state
      if (changed) {
        logStateChange(old)
        listeners.get().foreach(_(this, state, tc))
      }
      changed
    }

    def reportHealthState_(state: State)(implicit tc: TraceContext): Unit = {
      reportHealthState(state).discard
    }

    private def logStateChange(
        oldState: State
    )(implicit traceContext: TraceContext): Unit = {
      logger
        .info(
          show"$name is now in state $getState. Previous state was $oldState"
        )
    }

    override def getState: State = stateRef.get()

    def toComponentStatus: ComponentStatus =
      ComponentStatus(name, getState.toComponentHealthState)
  }

  /** Implements health component Base using ComponentHealthState as a state.
    * Suitable for most components unless there is a need for a custom State type.
    */
  trait HealthComponent extends BaseHealthComponent { self: NamedLogging with FlagCloseable =>
    override type State = ComponentHealthState
    override val closingState: ComponentHealthState = ComponentHealthState.ShutdownState

    /** Set the health state to Ok and if the previous state was unhealthy, log a message to inform about the resolution
      * of the ongoing issue.
      */
    def resolveUnhealthy(implicit traceContext: TraceContext): Boolean = {
      reportHealthState(ComponentHealthState.Ok())
    }

    def resolveUnhealthy_(implicit traceContext: TraceContext): Unit = {
      resolveUnhealthy.discard
    }

    /** Report that the component is now degraded.
      * Note that this will override the component state, even if it is currently failed!
      */
    def degradationOccurred(error: CantonError)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState_(
        ComponentHealthState.Degraded(
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
        ComponentHealthState.Failed(
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
        ComponentHealthState.degraded(error)
      )
    }

    /** Report that the component is now failed
      */
    def failureOccurred(error: String)(implicit
        tc: TraceContext
    ): Unit = {
      reportHealthState_(ComponentHealthState.failed(error))
    }
  }

  /** Mutable Health component, use when the health component is not instantiated at bootstrap time and/or
    *  changes during the lifetime of the node.
    * This class does not enforce a specific ComponentHealth type to defer to.
    * For convenience DeferredHealthComponent can be used when dealing with HealthComponents
    * @param uninitializedName name used to identify this component while it has not been initialized yet
    * @param initialHealthState state the component will return while it has not been initialized yet
    */
  class BaseMutableHealthComponent[H <: BaseHealthComponent](
      override val loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
      override val initialHealthState: H#State,
      override val timeouts: ProcessingTimeout,
      val initialClosingState: H#State,
  ) extends BaseHealthComponent
      with NamedLogging
      with FlagCloseable { self =>
    override type State = H#State
    private val delegate = new AtomicReference[Option[H]](None)

    override def name: String =
      delegate.get
        .map(_.name)
        .getOrElse(uninitializedName)

    override def closingState: H#State =
      delegate.get().map(_.closingState).getOrElse(initialClosingState)

    override def getState: H#State = delegate.get
      .map(_.getState)
      .getOrElse(initialHealthState)

    def set(element: H): Unit = {
      // It is possible for a component to be set several times, in particular when nodes transition from
      // passive -> active new components are instantiated and need to be replaced here
      // A HealthComponent removes all its listeners when it gets closed so there's no need to take care of that here
      delegate.set(Some(element))
      element.registerOnHealthChange((_: element.type, state: element.State, tc: TraceContext) => {
        listeners.get().foreach(_(self, state, tc))
      })
    }
  }

  object BaseMutableHealthComponent {
    // Apply method creating a MutableHealthComponent for convenience
    def apply(
        loggerFactory: NamedLoggerFactory,
        uninitializedName: String,
        timeouts: ProcessingTimeout,
    ): MutableHealthComponent =
      MutableHealthComponent(loggerFactory, uninitializedName, timeouts)
  }

  /** Mutable component for ComponentHealths
    */
  final case class MutableHealthComponent(
      override val loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
      override val timeouts: ProcessingTimeout,
      shutdownState: ComponentHealthState = ComponentHealthState.ShutdownState,
  ) extends BaseMutableHealthComponent[HealthComponent](
        loggerFactory,
        uninitializedName,
        ComponentHealthState.NotInitializedState,
        timeouts,
        shutdownState,
      )
      with HealthComponent

  /** Mutable Health component, use when the multiple health components identified by `Id` are not
    * instantiated at bootstrap time and/or change during the lifetime of the node.
    * Also supports aggregation of the health of multiple components into a single health state.
    *
    * This class does not enforce a specific ComponentHealth type to defer to.
    * For convenience DelegatingMutableHealthComponent can be used when dealing with HealthComponents
    *
    * @param name  name used to identify this component
    * @param initialHealthState state the component will return while it has not been initialized yet
    * @param reduceState a function based on the health of all components finding out the health of this component
    */
  class BaseDelegatingMutableHealthComponent[Id, H <: BaseHealthComponent](
      override val loggerFactory: NamedLoggerFactory,
      override val name: String,
      override val initialHealthState: H#State,
      override val timeouts: ProcessingTimeout,
      override val closingState: H#State,
      val reduceState: Map[Id, H#State] => H#State,
  ) extends BaseHealthComponent
      with NamedLogging
      with FlagCloseable {
    self =>
    override type State = H#State
    private val delegate = new TrieMap[Id, H]()

    override def getState: H#State =
      reduceState(
        delegate
          .readOnlySnapshot()
          .map { case (id, componentHealth) =>
            id -> componentHealth.getState
          }
          .toMap
      )

    def set(id: Id, element: H): Unit = {
      // It is possible for a component to be set several times, in particular when nodes transition from
      // passive -> active new components are instantiated and need to be replaced here
      // A HealthComponent removes all its listeners when it gets closed so there's no need to take care of that here
      delegate.put(id, element).discard
      element.registerOnHealthChange((_: element.type, state: element.State, tc: TraceContext) => {
        listeners.get().foreach(_(self, state, tc))
      })
    }
  }

  final class DelegatingMutableHealthComponent[Id](
      override val loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
      override val timeouts: ProcessingTimeout,
      shutdownState: ComponentHealthState = ComponentHealthState.ShutdownState,
      reduceStateFromMany: Map[Id, ComponentHealthState] => ComponentHealthState,
  ) extends BaseDelegatingMutableHealthComponent[Id, HealthComponent](
        loggerFactory,
        uninitializedName,
        ComponentHealthState.NotInitializedState,
        timeouts,
        shutdownState,
        reduceStateFromMany,
      )
      with HealthComponent
}
