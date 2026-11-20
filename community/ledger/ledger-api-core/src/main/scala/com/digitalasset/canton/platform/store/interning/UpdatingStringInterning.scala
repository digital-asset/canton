// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

private[platform] trait UpdatingStringInterning extends StringInterning {
  def update(lastStringInterningId: Option[Int])(implicit traceContext: TraceContext): Future[Unit]

  def internalizingBuilderForIndexing: StringInternalizingBuilderForIndexing
}

private[platform] trait StringInternalizingBuilderDomainForIndexing[-T]
    extends StringInternalizingBuilderDomain[T]
    with LookupInternalDomain[T]

private[platform] trait StringInternalizingBuilderForIndexing
    extends StringInterningDomains[StringInternalizingBuilderDomainForIndexing]
    with StringInternalizingBuilder {
  def resolve(implicit
      traceContext: TraceContext
  ): Future[InternalIdLookup]

  def internNewRawStringsForIndexing(
      recentlyInternedNewRawStrings: scala.collection.Map[String, Int]
  ): (Iterable[(Int, String)], Int)
}

private[platform] trait StringProviderForIndexing {
  def builder: StringInternalizingBuilderForIndexing
  def provideStrings(): Unit
}
