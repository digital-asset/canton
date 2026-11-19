// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

private[interning] trait CommonStringInterningDomainInt[T] {
  def initializationRequired: Boolean
  def add(id: Int, t: T): Unit
  def resetTo(id: Int): Unit
  def to: String => T
  def from: T => String
  def prefix: String

  def addPrefixed(id: Int, prefixed: String): Unit =
    fromPrefixed(prefixed).foreach(add(id, _))

  def toPrefixed(t: T): String =
    prefix + from(t)

  def fromPrefixed(rawString: String): Option[T] =
    Option.when(rawString.startsWith(prefix))(
      to(rawString.substring(prefix.length))
    )
}

private[interning] trait StringInterningDomainInt[T]
    extends StringInterningDomain[T]
    with CommonStringInterningDomainInt[T]

private[interning] trait CachedStringInterningDomainInt[T]
    extends CachedStringInterningDomain[T]
    with CommonStringInterningDomainInt[T] {
  def internalizeIfPresent(t: T): Option[Int]
  def externalizeIfPresent(id: Int): Option[T]
}

private[interning] trait RawStringResolverDomain {
  def resolvedRawString(id: Int, rawString: String): Unit
}

private[interning] trait StringInternalizingBuilderDomainInt[-T]
    extends StringInternalizingBuilderDomainForIndexing[T]
    with LookupInternalIfExistsDomain[T]
    with RawStringResolverDomain

private[interning] trait StringInternalizingBuilderInt
    extends StringInterningDomains[StringInternalizingBuilderDomainInt]
    with StringInternalizingBuilderForIndexing
    with StringInternalizingBuilder
    with InternalIdLookup

private[interning] trait StringExternalizingBuilderDomainInt[+T]
    extends StringExternalizingBuilderDomain[T]
    with LookupExternalDomain[T]
    with RawStringResolverDomain

private[interning] trait StringExternalizingBuilderInt
    extends StringInterningDomains[StringExternalizingBuilderDomainInt]
    with StringExternalizingBuilder
    with ExternalStringLookup {
  def resolve(implicit traceContext: TraceContext): Future[ExternalStringLookup]
}
