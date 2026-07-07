// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.Applicative
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.TopologyManagerError.TopologyStoreNotInitialized
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreInitializationStatus.{
  Initialized,
  NotInitialized,
}
import com.digitalasset.canton.topology.store.{
  HasTopologyStoreId,
  TopologyStore,
  TopologyStoreId as InternalTopologyStoreId,
}

/** This type is used to hold the initialization status of subtypes of
  * [[com.digitalasset.canton.topology.store.HasTopologyStoreId]] (e.g.
  * [[com.digitalasset.canton.topology.TopologyManager]] or
  * [[com.digitalasset.canton.topology.store.TopologyStore]]).
  *
  * It also extends [[com.digitalasset.canton.topology.store.HasTopologyStoreId]] itself, so that it
  * can be passed to `TopologyStore.select`.
  *
  * @tparam StoreId
  *   The type of [[com.digitalasset.canton.topology.store.TopologyStoreId]] that is used for
  *   `Container`.
  * @tparam Container
  *   A type that extends [[com.digitalasset.canton.topology.store.HasTopologyStoreId]].
  */
sealed trait TopologyStoreInitializationStatus[
    +StoreId <: InternalTopologyStoreId,
    +Container[+x <: InternalTopologyStoreId] <: HasTopologyStoreId[x],
] extends HasTopologyStoreId[StoreId]
    with Product
    with Serializable {
  def storeId: StoreId

  /** Converts this value to an Either so that an appropriate error can be reported.
    */
  def toEither(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[TopologyManagerError, Container[StoreId]]

  /** A non-generalized version of traverse, that allows to effectfully transform a value of type
    * `Container[TopologyStoreId]` to `NewContainer[NewStoreId]`.
    */
  def traverse[NewStoreId >: StoreId <: InternalTopologyStoreId, NewContainer[
      +x <: InternalTopologyStoreId
  ] <: HasTopologyStoreId[x], G[+_]: Applicative](
      f: Container[NewStoreId] => G[NewContainer[NewStoreId]]
  ): G[TopologyStoreInitializationStatus[NewStoreId, NewContainer]] =
    this match {
      case NotInitialized(storeId) =>
        Applicative[G].pure(NotInitialized[StoreId, NewContainer](storeId))
      case Initialized(value) => Applicative[G].map(f(value))(Initialized(_))
    }
}

object TopologyStoreInitializationStatus {

  /** Helper type to use `TopologyStoreInitializationStatus` in places where a unary type
    * constructor with restrictions on the type parameter is expected (.i.e.: `F[+_ <:
    * TopologyStoreId]`).
    */
  type Aux[+StoreId <: topology.store.TopologyStoreId] =
    TopologyStoreInitializationStatus[StoreId, TopologyStore]

  /** Conveys that the topology store with [[storeId]] has not been initialized yet.
    */
  final case class NotInitialized[
      +StoreId <: InternalTopologyStoreId,
      +Container[+x <: InternalTopologyStoreId] <: HasTopologyStoreId[x],
  ](storeId: StoreId)
      extends TopologyStoreInitializationStatus[StoreId, Container] {
    override def toEither(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Either[TopologyManagerError, Container[StoreId]] =
      Left(TopologyStoreNotInitialized.Failure(storeId))
  }

  /** Represents that the topology store that the Container refers to (or uses) has been properly
    * initialized.
    */
  final case class Initialized[
      +StoreId <: InternalTopologyStoreId,
      +Container[+x <: InternalTopologyStoreId] <: HasTopologyStoreId[x],
  ](value: Container[StoreId])
      extends TopologyStoreInitializationStatus[StoreId, Container] {
    override def storeId: StoreId = value.storeId

    override def toEither(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Either[TopologyManagerError, Container[StoreId]] = Right(value)
  }
}
