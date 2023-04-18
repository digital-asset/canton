// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  PackageInUse,
  PackageVetted,
}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyManager,
  ParticipantTopologyManagerError,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.{
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyTransaction,
  VettedPackages,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPackageId, participant}

import scala.concurrent.{ExecutionContext, Future}

trait PackageInspectionOps extends NamedLogging {

  def packageVetted(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageVetted, Unit]

  def packageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit]

  def runTx(tx: TopologyTransaction[TopologyChangeOp], force: Boolean)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def genRevokePackagesTx(packages: List[LfPackageId])(implicit
      tc: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, TopologyTransaction[TopologyChangeOp]]

}

class PackageInspectionOpsImpl(
    participantId: ParticipantId,
    storage: Storage,
    authorizedTopologyStore: TopologyStore[AuthorizedStore],
    aliasManager: DomainAliasManager,
    stateManager: SyncDomainPersistentStateManager,
    syncCryptoApiProvider: SyncCryptoApiProvider,
    timeouts: ProcessingTimeout,
    topologyManager: ParticipantTopologyManager,
    protocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends PackageInspectionOps {

  override def packageVetted(
      pkg: PackageId
  )(implicit tc: TraceContext): EitherT[Future, PackageVetted, Unit] = {
    // TODO(i9505): Consider unit testing this

    def snapshotFromStore(domainStore: TopologyStore[TopologyStoreId]) = {

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
      stateManager.getAll.map { case (domainId, state) =>
        syncCryptoApiProvider.ips
          .forDomain(domainId)
          .map(client => (client.headSnapshot))
          .getOrElse({
            snapshotFromStore(state.topologyStore)
          })
      }.toList

    val snapshotFromAuthorizedStore = snapshotFromStore(authorizedTopologyStore)

    val packageIsVettedOn = (snapshotFromAuthorizedStore :: snapshotsForDomains)
      .parTraverse { snapshot =>
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
    stateManager.getAll.toList
      .sortBy(_._1.toProtoPrimitive) // Sort to keep tests deterministic
      .parTraverse_ { case (id, state) =>
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

  override def runTx(tx: TopologyTransaction[TopologyChangeOp], force: Boolean)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    for {
      _signedTx <- topologyManager.authorize(
        tx,
        signingKey = None,
        protocolVersion,
        force,
      )
    } yield ()
  }

  override def genRevokePackagesTx(packages: List[LfPackageId])(implicit
      tc: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, TopologyTransaction[TopologyChangeOp]] = {
    val op = TopologyChangeOp.Remove
    val mapping = VettedPackages(participantId, packages)
    topologyManager
      .genTransaction(op, mapping, protocolVersion)
      .leftMap(err =>
        participant.topology.ParticipantTopologyManagerError
          .IdentityManagerParentError(err)
      )
  }
}
