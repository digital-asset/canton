// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  ResolvedPackagesAndDependencies,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

class PackageDependencyResolverImpl(
    participantId: ParticipantId,
    val packageMetadataView: PackageMetadataView,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with PackageDependencyResolver {
  def resolvePackagesAndDependencies(packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): Either[(ParticipantId, Set[PackageId]), ResolvedPackagesAndDependencies] = {
    val snapshot = packageMetadataView.getSnapshot
    val unknownPackages = packages.filterNot(snapshot.packages.contains)

    Either.cond(
      unknownPackages.isEmpty,
      ResolvedPackagesAndDependencies(
        mainPackageIds = packages,
        mainPackageAndDependencyIds = snapshot.allDependenciesRecursively(packages),
      ),
      participantId -> unknownPackages,
    )
  }
}
