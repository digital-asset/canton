// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  Identifier,
  NameTypeConRef,
  PackageId,
  ParticipantId,
  UserId,
}

import scala.concurrent.Future

/** Main sync interface for using string-interning. Client code can use this to map between interned
  * id-s and string-domain objects back and forth. All interned ids are non-negative. Provides
  * trivial overrides for the Async StringInterning API
  *
  * @tparam T
  *   is the type of the string-related domain object which is interned
  */
trait StringInterningAccessor[T] extends AsyncStringInterningAccessor[T] {

  /** Get the interned id
    *
    * @param t
    *   the value
    * @return
    *   the integer id, throws exception if id not found
    */
  def internalize(t: T): Int

  /** Get the value for an id
    *
    * @param id
    *   integer id
    * @return
    *   the value, throws exception if no value found
    */
  def externalize(id: Int): T

  /** Optionally get the interned id
    * @param t
    *   the value
    * @return
    *   some integer id, or none if not found
    */
  def tryInternalize(t: T): Option[Int]

  /** Optionally get the value for an id
    *
    * @param id
    *   integer id
    * @return
    *   some value, or none if not found
    */
  def tryExternalize(id: Int): Option[T]

  final override def internalizeAsync(t: T)(implicit
      traceContext: TraceContext
  ): Future[Option[Int]] =
    Future.successful(tryInternalize(t))

  final override def internalizeManyAsync(ts: Iterable[T])(implicit
      traceContext: TraceContext
  ): Future[Map[T, Int]] =
    Future.successful(ts.view.flatMap(t => tryInternalize(t).map(t -> _)).toMap)

  final override def externalizeAsync(id: Int)(implicit
      traceContext: TraceContext
  ): Future[Option[T]] =
    Future.successful(tryExternalize(id))

  final override def externalizeManyAsync(ids: Iterable[Int])(implicit
      traceContext: TraceContext
  ): Future[Map[Int, T]] =
    Future.successful(ids.view.flatMap(id => tryExternalize(id).map(id -> _)).toMap)
}

/** Main async interface for using string-interning. Client code can use this to map between
  * interned id-s and string-domain objects back and forth. All interned ids are non-negative.
  *
  * @tparam T
  *   is the type of the string-related domain object which is interned
  */
trait AsyncStringInterningAccessor[T] {
  def internalizeAsync(t: T)(implicit traceContext: TraceContext): Future[Option[Int]]

  def internalizeManyAsync(ts: Iterable[T])(implicit
      traceContext: TraceContext
  ): Future[Map[T, Int]]

  def externalizeAsync(id: Int)(implicit traceContext: TraceContext): Future[Option[T]]

  def externalizeManyAsync(ids: Iterable[Int])(implicit
      traceContext: TraceContext
  ): Future[Map[Int, T]]
}

trait StringInterningDomain[T]
    extends CachedStringInterningDomain[T]
    with StringInterningAccessor[T] {
  def unsafe: StringInterningAccessor[String]
}

trait CachedStringInterningDomain[T] extends AsyncStringInterningAccessor[T] {
  def unsafe: AsyncStringInterningAccessor[String]
}

trait StringInterningDomains[+T[_]] {
  def templateId: T[NameTypeConRef]
  def packageId: T[PackageId]
  def party: T[Party]
  def synchronizerId: T[SynchronizerId]
  def userId: T[UserId]
  def participantId: T[ParticipantId]
  def choiceName: T[ChoiceName]
  def interfaceId: T[Identifier]

  def allDomains: List[T[?]] = List(
    templateId,
    packageId,
    party,
    synchronizerId,
    userId,
    participantId,
    choiceName,
    interfaceId,
  )
}

trait StringInterning {
  def templateId: StringInterningDomain[NameTypeConRef]
  def packageId: StringInterningDomain[PackageId]
  def party: StringInterningDomain[Party]
  def synchronizerId: StringInterningDomain[SynchronizerId]
  def userId: StringInterningDomain[UserId]
  def participantId: StringInterningDomain[ParticipantId]
  def choiceName: StringInterningDomain[ChoiceName]
  def interfaceId: StringInterningDomain[Identifier]

  def externalize(providers: Iterable[StringExternalizingProvider])(implicit
      traceContext: TraceContext
  ): Future[ExternalStringLookup]
  def internalize(providers: Iterable[StringInternalizingProvider])(implicit
      traceContext: TraceContext
  ): Future[InternalIdLookup]
}

trait CollectorDomain[-T] {
  def add(t: T): Unit
}

// Externalizing

trait StringExternalizingProvider {
  def provideIds(builder: StringExternalizingBuilder): Unit
}

trait StringExternalizingBuilder extends StringInterningDomains[StringExternalizingBuilderDomain]

trait StringExternalizingBuilderDomain[+T] extends CollectorDomain[Int]

trait ExternalStringLookup extends StringInterningDomains[LookupExternalDomain]

trait LookupExternalDomain[+T] {
  def lookup(id: Int): T
}

// Internalizing

trait LookupInternalDomain[-T] {
  def lookup(t: T): Int
}

trait LookupInternalIfExistsDomain[-T] {
  def lookupIfExists(t: T): Option[Int]
}

trait StringInternalizingBuilderDomain[-T] extends CollectorDomain[T]

trait StringInternalizingBuilder extends StringInterningDomains[StringInternalizingBuilderDomain]

trait InternalIdLookup extends StringInterningDomains[LookupInternalIfExistsDomain]

trait StringInternalizingProvider {
  def provideStrings(builder: StringInternalizingBuilder): Unit
}

// TODO(#37258) remove
trait StringInterningBuilder {
  def addTemplateId(templateId: NameTypeConRef): Unit
  def addPackageId(packageId: PackageId): Unit
  def addParty(party: Party): Unit
  def addSynchronizerId(synchronizerId: SynchronizerId): Unit
  def addUserId(userId: UserId): Unit
  def addParticipantId(participantId: ParticipantId): Unit
  def addChoiceName(choiceName: ChoiceName): Unit
  def addInterfaceId(interfaceId: Identifier): Unit
}

// TODO(#37258) remove
trait StringInterningProvider {
  def provideInternedStrings(builder: StringInterningBuilder): Unit
}

object StringInterningDomain {
  // TODO(#37258) remove
  private[interning] def prefixing[T](
      prefix: String,
      prefixedAccessor: StringInterningAccessor[String],
      to: String => T,
      from: T => String,
  ): StringInterningDomain[T] =
    new StringInterningDomain[T] {
      override val unsafe: StringInterningAccessor[String] = new StringInterningAccessor[String] {
        override def internalize(t: String): Int = prefixedAccessor.internalize(prefix + t)

        override def tryInternalize(t: String): Option[Int] =
          prefixedAccessor.tryInternalize(prefix + t)

        override def externalize(id: Int): String =
          prefixedAccessor.externalize(id).substring(prefix.length)

        override def tryExternalize(id: Int): Option[String] =
          prefixedAccessor.tryExternalize(id).map(_.substring(prefix.length))
      }

      override def internalize(t: T): Int = unsafe.internalize(from(t))

      override def tryInternalize(t: T): Option[Int] = unsafe.tryInternalize(from(t))

      override def externalize(id: Int): T = to(unsafe.externalize(id))

      override def tryExternalize(id: Int): Option[T] = unsafe.tryExternalize(id).map(to)
    }
}
