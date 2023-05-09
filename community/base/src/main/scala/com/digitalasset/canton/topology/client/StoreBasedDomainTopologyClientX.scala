// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TimeQueryX,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** The domain topology client that reads data from a topology store
  *
  * @param domainId The domain-id corresponding to this store
  * @param store The store
  */
class StoreBasedDomainTopologyClientX(
    val clock: Clock,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends BaseDomainTopologyClient
    with NamedLogging {

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshotX = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, topology known until=$topologyKnownUntilTimestamp",
    )
    new StoreBasedTopologySnapshotX(
      timestamp,
      store,
      packageDependencies,
      loggerFactory,
    )
  }

}

/** Topology snapshot loader
  *
  * @param timestamp the asOf timestamp to use
  * @param store the db store to use
  * @param packageDependencies lookup function to determine the direct and indirect package dependencies
  */
class StoreBasedTopologySnapshotX(
    val timestamp: CantonTimestamp,
    store: TopologyStoreX[TopologyStoreId],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging
    with NoTracing {

  private def findTransactions(
      asOfInclusive: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive,
        isProposal = false,
        types,
        filterUid,
        filterNamespace,
      )

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] = {

    val vettedET = EitherT.right[PackageId](
      findTransactions(
        asOfInclusive = false,
        types = Seq(TopologyMappingX.Code.VettedPackagesX),
        filterUid = Some(Seq(participant.uid)),
        filterNamespace = None,
      ).map { transactions =>
        collectLatestMapping(
          TopologyMappingX.Code.VettedPackagesX,
          transactions.collectOfMapping[VettedPackagesX].result,
        ).toList.flatMap(_.packageIds).toSet
      }
    )

    val requiredPackagesET = EitherT.right[PackageId](
      findTransactions(
        asOfInclusive = false,
        types = Seq(TopologyMappingX.Code.DomainParametersStateX),
        filterUid = None,
        filterNamespace = None,
      ).map { transactions =>
        collectLatestMapping(
          TopologyMappingX.Code.DomainParametersStateX,
          transactions.collectOfMapping[DomainParametersStateX].result,
        ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
          .discard

        // TODO(#11255) Once the non-proto DynamicDomainParametersX is available, use it
        //   _.parameters.requiredPackages
        Seq.empty[PackageId]
      }
    )

    lazy val dependenciesET = packageDependencies(packageId)

    for {
      vetted <- vettedET
      requiredPackages <- requiredPackagesET
      // check that the main package is vetted
      res <-
        if (!vetted.contains(packageId))
          EitherT.rightT[Future, PackageId](Set(packageId)) // main package is not vetted
        else {
          // check which of the dependencies aren't vetted
          dependenciesET.map(deps => (deps ++ requiredPackages) -- vetted)
        }
    } yield res

  }

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] =
    findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.DomainParametersStateX),
      filterUid = None,
      filterNamespace = None,
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMappingX.Code.DomainParametersStateX,
          transactions
            .collectOfMapping[DomainParametersStateX]
            .result,
        ).toRight(s"Unable to fetch domain parameters at $timestamp")

        domainParameters = {
          val mapping = storedTx.transaction.transaction.mapping
          DynamicDomainParametersWithValidity(
            mapping.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            mapping.domain,
          )
        }
      } yield domainParameters
    }

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] = store
    .inspect(
      proposals = false,
      timeQuery = TimeQueryX.Range(None, Some(timestamp)),
      recentTimestampO = None,
      ops = Some(TopologyChangeOpX.Replace),
      typ = Some(TopologyMappingX.Code.DomainParametersStateX),
      idFilter = "",
      namespaceOnly = false,
    )
    .map {
      _.collectOfMapping[DomainParametersStateX].result
        .map { storedTx =>
          val dps = storedTx.transaction.transaction.mapping
          DynamicDomainParametersWithValidity(
            dps.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
            dps.domain,
          )
        }
    }

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    loadBatchActiveParticipantsOf(Seq(party), participantStates).map(
      _.getOrElse(party, Map.empty)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]] = {
    for {
      // get all party to participant mappings and also participant states for this uid (latter to mix in admin parties)
      partyToParticipantMap <- findTransactions(
        asOfInclusive = false,
        types = Seq(TopologyMappingX.Code.PartyToParticipantX),
        filterUid = Some(parties.map(_.uid)),
        filterNamespace = None,
      ).map(
        _.collectOfMapping[PartyToParticipantX].result
          .groupBy(_.transaction.transaction.mapping.partyId)
          .map { case (partyId, seq) =>
            val ptp =
              collectLatestMapping(
                TopologyMappingX.Code.PartyToParticipantX,
                seq.sortBy(_.validFrom),
              )
                .getOrElse(
                  throw new IllegalStateException("Group-by would not have produced empty seq")
                )
            PartyId(partyId) -> ptp.participants.map {
              case HostingParticipant(participantId, partyPermission) =>
                ParticipantId(participantId) -> partyPermission.toNonX
            }.toMap
          }
      )

      participantIds = partyToParticipantMap.values.flatMap(_.keys).toSeq

      participantToAttributesMap <- loadParticipantStates(participantIds)

      // In case the party->participant mapping contains participants missing from map returned
      // by loadParticipantStates, filter out participants with "empty" permissions and transitively
      // parties whose participants have all been filtered out this way.
      // this can only affect participants that have left the domain
      result = partyToParticipantMap.toSeq.mapFilter {
        case (partyId, participantToPermissionsMap) =>
          val participantIdToAttribs = participantToPermissionsMap.toSeq.mapFilter {
            case (participantId, partyPermission) =>
              participantToAttributesMap
                .get(participantId)
                .map { participantAttributes =>
                  // Use the lower permission between party and the permission granted to the participant by the domain
                  val reducedPermission = {
                    ParticipantPermission.lowerOf(partyPermission, participantAttributes.permission)
                  }
                  participantId -> ParticipantAttributes(
                    reducedPermission,
                    participantAttributes.trustLevel,
                  )
                }
          }.toMap
          if (participantIdToAttribs.isEmpty) None else Some(partyId -> participantIdToAttribs)
      }.toMap
    } yield result
  }

  /** returns the list of currently known mediator groups */
  override def mediatorGroups(): Future[Seq[MediatorGroup]] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.MediatorDomainStateX),
    filterUid = None,
    filterNamespace = None,
  ).map(
    _.collectOfMapping[MediatorDomainStateX].result
      .groupBy(_.transaction.transaction.mapping.group)
      .map { case (groupId, seq) =>
        val mds = collectLatestMapping(
          TopologyMappingX.Code.MediatorDomainStateX,
          seq.sortBy(_.validFrom),
        )
          .getOrElse(throw new IllegalStateException("Group-by would not have produced empty seq"))
        MediatorGroup(groupId, mds.active, mds.observers, mds.threshold)
      }
      .toSeq
  )

  /** Returns a list of all known parties on this domain */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant, limit)

  /** Returns a list of owner's keys (at most limit) */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] = {
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQueryX.Snapshot(timestamp),
        recentTimestampO = None,
        ops = Some(TopologyChangeOpX.Replace),
        typ = Some(TopologyMappingX.Code.OwnerToKeyMappingX),
        idFilter = filterOwner,
        namespaceOnly = false,
      )
      .map(
        _.collectOfMapping[OwnerToKeyMappingX]
          .collectOfType[TopologyChangeOpX.Replace]
          .result
          .groupBy(_.transaction.transaction.mapping.member)
          .collect {
            case (owner, seq)
                if owner.filterString.startsWith(filterOwner)
                  && filterOwnerType.forall(_ == owner.code) =>
              val keys = KeyCollection(Seq(), Seq())
              val okm =
                collectLatestMapping(
                  TopologyMappingX.Code.OwnerToKeyMappingX,
                  seq.sortBy(_.validFrom),
                )
              owner -> okm
                .fold(keys)(_.keys.take(limit).foldLeft(keys) { case (keys, key) =>
                  keys.addTo(key)
                })
          }
      )
  }

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] =
    loadParticipantStates(Seq(participantId)).map(_.get(participantId))

  private def loadParticipantStatesHelper(
      participantsFilter: Option[Seq[ParticipantId]] // None means fetch all participants
  ): Future[Map[ParticipantId, ParticipantDomainPermissionX]] = for {
    // Looks up domain parameters for default rate limits.
    domainParametersState <- findTransactions(
      asOfInclusive = false,
      types = Seq(
        TopologyMappingX.Code.DomainParametersStateX
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(transactions =>
      collectLatestMapping(
        TopologyMappingX.Code.DomainParametersStateX,
        transactions.collectOfMapping[DomainParametersStateX].result,
      ).getOrElse(throw new IllegalStateException("Unable to locate domain parameters state"))
    )
    // 1. Participant needs to have requested access to domain by issuing a domain trust certificate
    participantsWithCertificates <- findTransactions(
      asOfInclusive = false,
      types = Seq(
        TopologyMappingX.Code.DomainTrustCertificateX
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[DomainTrustCertificateX].result
        .groupBy(_.transaction.transaction.mapping.participantId)
        .collect {
          case (pid, seq) if participantsFilter.forall(_.contains(ParticipantId(pid))) =>
            // invoke collectLatestMapping only to warn in case a participantId's domain trust certificate is not unique
            collectLatestMapping(
              TopologyMappingX.Code.DomainTrustCertificateX,
              seq.sortBy(_.validFrom),
            ).discard
            pid
        }
        .toSeq
    )
    // 2. Participant needs to have keys registered on the domain
    participantsWithCertAndKeys <- findTransactions(
      asOfInclusive = false,
      types = Seq(TopologyMappingX.Code.OwnerToKeyMappingX),
      filterUid = Some(participantsWithCertificates),
      filterNamespace = None,
    ).map(
      _.collectOfMapping[OwnerToKeyMappingX].result
        .groupBy(_.transaction.transaction.mapping.member.uid)
        .filter { case (pid, seq) =>
          collectLatestMapping(
            TopologyMappingX.Code.OwnerToKeyMappingX,
            seq.sortBy(_.validFrom),
          ).nonEmpty
        }
        .keys
    )
    // Warn about participants with cert but no keys
    _ = (participantsWithCertificates.toSet -- participantsWithCertAndKeys.toSet).foreach { pid =>
      logger.warn(
        s"Participant ${pid} has a domain trust certificate, but no keys on domain ${domainParametersState.domain}"
      )
    }
    // 3. Attempt to look up permissions/trust from participant domain permission
    participantDomainPermissions <- findTransactions(
      asOfInclusive = false,
      types = Seq(
        TopologyMappingX.Code.ParticipantDomainPermissionX
      ),
      filterUid = None,
      filterNamespace = None,
    ).map(
      _.collectOfMapping[ParticipantDomainPermissionX].result
        .groupBy(_.transaction.transaction.mapping.participantId)
        .map { case (pid, seq) =>
          val mapping =
            collectLatestMapping(
              TopologyMappingX.Code.ParticipantDomainPermissionX,
              seq.sortBy(_.validFrom),
            )
              .getOrElse(
                throw new IllegalStateException("Group-by would not have produced empty seq")
              )
          pid -> mapping
        }
    )
    // 4. Apply default permissions/trust of submission/ordinary if missing participant domain permission and
    // grab rate limits from dynamic domain parameters if not specified
    participantIdDomainPermissionsMap = participantsWithCertAndKeys.map { pid =>
      ParticipantId(pid) -> participantDomainPermissions
        .getOrElse(
          pid,
          ParticipantDomainPermissionX.default(domainParametersState.domain.uid, pid),
        )
        .setDefaultLimitIfNotSet(domainParametersState.parameters.v2DefaultParticipantLimits)
    }.toMap
  } yield participantIdDomainPermissionsMap

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    if (participants.isEmpty)
      Future.successful(Map())
    else
      loadParticipantStatesHelper(participants.some).map(_.map { case (pid, pdp) =>
        pid -> pdp.toParticipantAttributes
      })

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    Future.failed(
      new UnsupportedOperationException(
        s"Participants lookup not supported by StoreBasedDomainTopologyClientX. This is a coding bug."
      )
    )

  override def findParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[LegalIdentityClaimEvidence.X509Cert]] =
    Future.failed(
      new UnsupportedOperationException(
        s"Legal claims not supported by StoreBasedDomainTopologyClientX. This is a coding bug."
      )
    )

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: KeyOwner): Future[KeyCollection] = findTransactions(
    asOfInclusive = false,
    types = Seq(TopologyMappingX.Code.OwnerToKeyMappingX),
    filterUid = Some(Seq(owner.uid)),
    filterNamespace = None,
  )
    .map { transactions =>
      val keys = KeyCollection(Seq(), Seq())
      collectLatestMapping[OwnerToKeyMappingX](
        TopologyMappingX.Code.OwnerToKeyMappingX,
        transactions.collectOfMapping[OwnerToKeyMappingX].result,
      ).fold(keys)(_.keys.foldLeft(keys) { case (keys, key) => keys.addTo(key) })
    }

  private def collectLatestMapping[T <: TopologyMappingX](
      typ: TopologyMappingX.Code,
      transactions: Seq[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]],
  ): Option[T] = collectLatestTransaction(typ, transactions).map(_.transaction.transaction.mapping)

  private def collectLatestTransaction[T <: TopologyMappingX](
      typ: TopologyMappingX.Code,
      transactions: Seq[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]],
  ): Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, T]] = {
    if (transactions.size > 1) {
      logger.warn(s"Expected unique \"${typ.code}\", but found multiple instances")
      transactions
        .foldLeft(CantonTimestamp.Epoch) { case (previous, tx) =>
          val validFrom = tx.validFrom.value
          if (previous >= validFrom) {
            logger.warn(
              s"Instance of \"${typ.code}\" with hash \"${tx.transaction.transaction.hash.hash.toHexString}\" with non-monotonically growing valid-from effective time."
            )
          }
          validFrom
        }
        .discard
    }
    transactions.lastOption
  }
}
