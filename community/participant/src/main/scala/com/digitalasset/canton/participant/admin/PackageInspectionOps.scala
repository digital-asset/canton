// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{toFoldableOps, _}
import cats.instances.future._
import cats.instances.list._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  PackageInUse,
  PackageVetted,
}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.{
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.{TopologyStoreFactory, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait PackageInspectionOps extends NamedLogging {

  def packageVetted(pkg: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageVetted, Unit]

  def packageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit]

}

class PackageInspectionOpsImpl(
    participantId: ParticipantId,
    storage: Storage,
    aliasManager: DomainAliasManager,
    stateManager: SyncDomainPersistentStateManager,
    syncCryptoApiProvider: SyncCryptoApiProvider,
    timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends PackageInspectionOps {

  override def packageVetted(
      pkg: PackageId
  )(implicit tc: TraceContext): EitherT[Future, PackageVetted, Unit] = {
    //TODO(i7860): Unit test this.

    import cats.implicits._

    val store = TopologyStoreFactory.apply(storage, timeouts, loggerFactory)

    def snapshotFromStore(id: TopologyStoreId) = {
      val domainStore = store.forId(id)

      new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue,
        domainStore,
        initKeys = Map(),
        useStateTxs = false,
        packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
        loggerFactory,
      )
    }

    // Use the aliasManager to query all domains, even those that are currently disconnected
    val snapshotsForDomains: List[TopologySnapshot] =
      aliasManager.aliases.toList.mapFilter({ domainAlias =>
        val maybeTopologyClient = aliasManager
          .domainIdForAlias(domainAlias)
          .map({ domainId =>
            syncCryptoApiProvider.ips
              .forDomain(domainId)
              .map(client => (client.headSnapshot))
              .getOrElse({
                snapshotFromStore(DomainStore(domainId))
              })
          })

        if (maybeTopologyClient.isEmpty) {
          logger.info(s"No domain ID for alias $domainAlias")
        }

        maybeTopologyClient
      })

    val snapshotFromAuthorizedStore = snapshotFromStore(AuthorizedStore)

    val packageIsVettedOn = (snapshotFromAuthorizedStore :: snapshotsForDomains)
      .traverse { snapshot =>
        snapshot
          .findUnvettedPackagesOrDependencies(participantId, Set(pkg))
          .map { pkgId =>
            val isVetted = pkgId.isEmpty
            isVetted
          }
      }

    val vettingF = packageIsVettedOn.value.map {
      case Left(missingPackage) =>
        // The package dependencies for `pkg` are broken as we're missing `missingPackage`.
        // So `pkg` it is not a fully vetted package. Allow it to be removed.
        logger.warn(
          s"Package $pkg is missing dependency $missingPackage. " +
            s"Allowing removal of $pkg due to pre-existing broken dependencies."
        )
        Right(())
      case Right(list) =>
        val vetted = list.contains(true)
        if (!vetted) Right(()) else Left(new PackageVetted(pkg))
    }
    EitherT(vettingF)
  }

  override def packageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit] = {
    implicit val loggingContext: ErrorLoggingContext = ErrorLoggingContext.fromTracedLogger(logger)

    stateManager.getAll.toList
      .sortBy(_._1.toProtoPrimitive) // Sort to keep tests deterministic
      .traverse_ { case (id, state) =>
        EitherT(
          state.activeContractStore
            .packageUsage(packageId, state.contractStore)
            .map(opt =>
              opt.fold[Either[PackageInUse, Unit]](Right(()))(contractId =>
                Left(new PackageInUse(packageId, contractId, state.domainId.domainId))
              )
            )
        )

      }
  }
}
