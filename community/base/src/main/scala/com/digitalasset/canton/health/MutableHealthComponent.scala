// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap

/** Mutable Health component, use when the health component is not instantiated at bootstrap time and/or
  * changes during the lifetime of the node.
  * This class does not enforce a specific ComponentHealth type to defer to.
  * For convenience DeferredHealthComponent can be used when dealing with HealthComponents
  *
  * @param uninitializedName  name used to identify this component while it has not been initialized yet
  * @param initialHealthState state the component will return while it has not been initialized yet
  */
class BaseMutableHealthComponent[H <: BaseHealthComponent](
    override protected val loggerFactory: NamedLoggerFactory,
    uninitializedName: String,
    override val initialHealthState: H#State,
    override protected val timeouts: ProcessingTimeout,
    val initialClosingState: H#State,
) extends BaseHealthComponent
    with NamedLogging
    with FlagCloseable {
  self =>
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
    new MutableHealthComponent(loggerFactory, uninitializedName, timeouts)
}

/** Mutable component for ComponentHealths
  */
final class MutableHealthComponent(
    override protected val loggerFactory: NamedLoggerFactory,
    uninitializedName: String,
    override protected val timeouts: ProcessingTimeout,
    shutdownState: ComponentHealthState = ComponentHealthState.ShutdownState,
) extends BaseMutableHealthComponent[HealthComponent](
      loggerFactory,
      uninitializedName,
      ComponentHealthState.NotInitializedState,
      timeouts,
      shutdownState,
    )
    with HealthComponent

object MutableHealthComponent {
  def apply(
      loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
      timeouts: ProcessingTimeout,
      shutdownState: ComponentHealthState = ComponentHealthState.ShutdownState,
  ): MutableHealthComponent =
    new MutableHealthComponent(loggerFactory, uninitializedName, timeouts, shutdownState)
}

/** Mutable Health component, use when the multiple health components identified by `Id` are not
  * instantiated at bootstrap time and/or change during the lifetime of the node.
  * Also supports aggregation of the health of multiple components into a single health state.
  *
  * This class does not enforce a specific ComponentHealth type to defer to.
  * For convenience DelegatingMutableHealthComponent can be used when dealing with HealthComponents
  *
  * @param name               name used to identify this component
  * @param initialHealthState state the component will return while it has not been initialized yet
  * @param reduceState        a function based on the health of all components finding out the health of this component
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
