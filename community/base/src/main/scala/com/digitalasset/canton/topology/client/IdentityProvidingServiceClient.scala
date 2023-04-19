// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.Monad
import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{EncryptionPublicKey, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.protocol.{
  DynamicDomainParameters,
  DynamicDomainParametersWithValidity,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}

import scala.Ordered.orderingToOrdered
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

// architecture-handbook-entry-begin: IdentityProvidingServiceClient

/** Client side API for the Identity Providing Service. This API is used to get information about the layout of
  * the domains, such as party-participant relationships, used encryption and signing keys,
  * package information, participant states, domain parameters, and so on.
  */
class IdentityProvidingServiceClient {

  private val domains = TrieMap.empty[DomainId, DomainTopologyClient]

  def add(domainClient: DomainTopologyClient): this.type = {
    domains += (domainClient.domainId -> domainClient)
    this
  }

  def allDomains: Iterable[DomainTopologyClient] = domains.values

  def tryForDomain(domain: DomainId): DomainTopologyClient =
    domains.getOrElse(domain, sys.error("unknown domain " + domain.toString))

  def forDomain(domain: DomainId): Option[DomainTopologyClient] = domains.get(domain)

}

trait TopologyClientApi[+T] { this: HasFutureSupervision =>

  /** The domain this client applies to */
  def domainId: DomainId

  /** Our current snapshot approximation
    *
    * As topology transactions are future dated (to prevent sequential bottlenecks), we do
    * have to "guess" the current state, as time is defined by the sequencer after
    * we've sent the transaction. Therefore, this function will return the
    * best snapshot approximation known.
    */
  def currentSnapshotApproximation(implicit traceContext: TraceContext): T

  /** Possibly future dated head snapshot
    *
    * As we future date topology transactions, the head snapshot is our latest knowledge of the topology state,
    * but as it can be still future dated, we need to be careful when actually using it: the state might not
    * yet be active, as the topology transactions are future dated. Therefore, do not act towards the sequencer
    * using this snapshot, but use the currentSnapshotApproximation instead.
    */
  def headSnapshot(implicit traceContext: TraceContext): T = checked(
    trySnapshot(topologyKnownUntilTimestamp)
  )

  /** The approximate timestamp
    *
    * This is either the last observed sequencer timestamp OR the effective timestamp after we observed
    * the time difference of (effective - sequencer = epsilon) to elapse
    */
  def approximateTimestamp: CantonTimestamp

  /** The most recently observed effective timestamp
    *
    * The effective timestamp is sequencer_time + epsilon(sequencer_time), where
    * epsilon is given by the topology change delay time, defined using the domain parameters.
    *
    * This is the highest timestamp for which we can serve snapshots
    */
  def topologyKnownUntilTimestamp: CantonTimestamp

  /** Returns true if the topology information at the passed timestamp is already known */
  def snapshotAvailable(timestamp: CantonTimestamp): Boolean

  /** Returns the topology information at a certain point in time
    *
    * Use this method if you are sure to be synchronized with the topology state updates.
    * The method will block & wait for an update, but emit a warning if it is not available
    */
  def snapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[T]
  def snapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Waits until a snapshot is available */
  def awaitSnapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[T]

  /** Supervised version of [[awaitSnapshot]] */
  def awaitSnapshotSupervised(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[T] = supervised(description, warnAfter)(awaitSnapshot(timestamp))

  /** Shutdown safe version of await snapshot */
  def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Supervised version of [[awaitSnapshotUS]] */
  def awaitSnapshotUSSupervised(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T] = supervisedUS(description, warnAfter)(awaitSnapshotUS(timestamp))

  /** Returns the topology information at a certain point in time
    *
    * Fails with an exception if the state is not yet known.
    */
  def trySnapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): T

  /** Returns an optional future which will complete when the timestamp has been observed
    *
    * If the timestamp is already observed, we return None.
    *
    * Note that this function allows to wait for effective time (true) and sequenced time (false).
    * If we wait for effective time, we wait until the topology snapshot for that given
    * point in time is known. As we future date topology transactions (to avoid bottlenecks),
    * this might be before we actually observed a sequencing timestamp.
    */
  def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]]

  def awaitTimestampUS(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]]

}

/** The client that provides the topology information on a per domain basis
  */
trait DomainTopologyClient extends TopologyClientApi[TopologySnapshot] with AutoCloseable {
  this: HasFutureSupervision =>

  /** Wait for a condition to become true according to the current snapshot approximation
    *
    * @return true if the condition became true, false if it timed out
    */
  def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

}

trait BaseTopologySnapshotClient {

  protected implicit def executionContext: ExecutionContext

  /** The official timestamp corresponding to this snapshot */
  def timestamp: CantonTimestamp

  /** Internally used reference time (representing when the last change happened that affected this snapshot) */
  def referenceTime: CantonTimestamp = timestamp

}

/** The subset of the topology client providing party to participant mapping information */
trait PartyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Load the set of active participants for the given parties */
  def activeParticipantsOfParties(
      parties: Seq[LfPartyId]
  ): Future[Map[LfPartyId, Set[ParticipantId]]]

  def activeParticipantsOfPartiesWithAttributes(
      parties: Seq[LfPartyId]
  ): Future[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]]

  /** Returns the set of active participants the given party is represented by as of the snapshot timestamp
    *
    * Should never return a PartyParticipantRelationship where ParticipantPermission is DISABLED.
    */
  def activeParticipantsOf(party: LfPartyId): Future[Map[ParticipantId, ParticipantAttributes]]

  /** Returns Right if all parties have at least an active participant passing the check. Otherwise, all parties not passing are passed as Left */
  def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: (ParticipantPermission => Boolean) = _.isActive,
  ): EitherT[Future, Set[LfPartyId], Unit]

  /** Returns the consortium thresholds (how many votes from different participants that host the consortium party
    * are required for the confirmation to become valid). For normal parties returns 1.
    */
  def consortiumThresholds(parties: Set[LfPartyId]): Future[Map[LfPartyId, PositiveInt]] = {
    // TODO(i11255): this is a stub implementation for non-consortium parties
    Future.successful(parties.map(party => party -> PositiveInt.one).toMap)
  }

  /** Returns true if there is at least one participant that satisfies the predicate */
  def isHostedByAtLeastOneParticipantF(
      party: LfPartyId,
      check: ParticipantAttributes => Boolean,
  ): Future[Boolean]

  /** Returns the participant permission for that particular participant (if there is one) */
  def hostedOn(
      partyId: LfPartyId,
      participantId: ParticipantId,
  ): Future[Option[ParticipantAttributes]]

  /** Returns true of all given party ids are hosted on a certain participant */
  def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _.permission.isActive,
  ): Future[Boolean]

  /** Returns whether a participant can confirm on behalf of a party. */
  def canConfirm(
      participant: ParticipantId,
      party: LfPartyId,
      requiredTrustLevel: TrustLevel = TrustLevel.Ordinary,
  ): Future[Boolean]

  /** Returns all active participants of all the given parties. Returns a Left if some of the parties don't have active
    * participants, in which case the parties with missing active participants are returned. Note that it will return
    * an empty set as a Right when given an empty list of parties.
    */
  def activeParticipantsOfAll(
      parties: List[LfPartyId]
  ): EitherT[Future, Set[LfPartyId], Set[ParticipantId]]

  /** Returns a list of all known parties on this domain */
  def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]]

}

/** The subset of the topology client, providing signing and encryption key information */
trait KeyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** returns newest signing public key */
  def signingKey(owner: KeyOwner): Future[Option[SigningPublicKey]]

  /** returns all signing keys */
  def signingKeys(owner: KeyOwner): Future[Seq[SigningPublicKey]]

  /** returns newest encryption public key */
  def encryptionKey(owner: KeyOwner): Future[Option[EncryptionPublicKey]]

  /** returns all encryption keys */
  def encryptionKeys(owner: KeyOwner): Future[Seq[EncryptionPublicKey]]

  /** Returns a list of all known parties on this domain */
  def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]]

}

/** The subset of the topology client, providing participant state information */
trait ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  // used by domain to fetch all participants
  @Deprecated(since = "3.0")
  def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]]

  /** Checks whether the provided participant exists and is active */
  def isParticipantActive(participantId: ParticipantId): Future[Boolean]

}

/** The subset of the topology client providing mediator state information */
trait MediatorDomainStateClient {
  this: BaseTopologySnapshotClient =>

  /** returns the list of currently known mediators */
  def mediators(): Future[Seq[MediatorId]]

  def isMediatorActive(mediatorId: MediatorId): Future[Boolean] =
    mediators().map(_.contains(mediatorId))
}

// this can be removed with 3.0
@Deprecated(since = "3.0")
trait CertificateSnapshotClient {

  this: BaseTopologySnapshotClient =>

  @Deprecated(since = "3.0.0")
  def hasParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    findParticipantCertificate(participantId).map(_.isDefined)

  @Deprecated(since = "3.0.0")
  def findParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[X509Cert]]

}

trait VettedPackagesSnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Returns the set of packages that are not vetted by the given participant
    *
    * @param participantId the participant for which we want to check the package vettings
    * @param packages the set of packages that should be vetted
    * @return Right the set of unvetted packages (which is empty if all packages are vetted)
    *         Left if a package is missing locally such that we can not verify the vetting state of the package dependencies
    */
  def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]]

}

trait DomainGovernanceSnapshotClient {
  this: BaseTopologySnapshotClient with NamedLogging =>

  def findDynamicDomainParametersOrDefault(
      protocolVersion: ProtocolVersion,
      warnOnUsingDefault: Boolean = true,
  )(implicit traceContext: TraceContext): Future[DynamicDomainParameters] =
    findDynamicDomainParameters().map {
      case Right(value) => value.parameters
      case Left(_) =>
        if (warnOnUsingDefault) {
          logger.warn(s"Unexpectedly using default domain parameters at ${timestamp}")
        }

        DynamicDomainParameters.initialValues(
          // we must use zero as default change delay parameter, as otherwise static time tests will not work
          // however, once the domain has published the initial set of domain parameters, the zero time will be
          // adjusted.
          topologyChangeDelay = DynamicDomainParameters.topologyChangeDelayIfAbsent,
          protocolVersion = protocolVersion,
        )
    }

  def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]]

  /** List all the dynamic domain parameters (past and current) */
  def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]]
}

trait TopologySnapshot
    extends PartyTopologySnapshotClient
    with BaseTopologySnapshotClient
    with ParticipantTopologySnapshotClient
    with KeyTopologySnapshotClient
    with CertificateSnapshotClient
    with VettedPackagesSnapshotClient
    with MediatorDomainStateClient
    with DomainGovernanceSnapshotClient { this: BaseTopologySnapshotClient with NamedLogging => }

// architecture-handbook-entry-end: IdentityProvidingServiceClient

/** The internal domain topology client interface used for initialisation and efficient processing */
trait DomainTopologyClientWithInit
    extends DomainTopologyClient
    with TopologyTransactionProcessingSubscriber
    with HasFutureSupervision
    with NamedLogging {

  implicit override protected def executionContext: ExecutionContext

  /** Move the most known timestamp ahead in future based of newly discovered information
    *
    * We don't know the most recent timestamp directly. However, we can guess it from two sources:
    * What was the timestamp of the latest topology transaction added? And what was the last processing timestamp.
    * We need to know both such that we can always deliver the latest valid set of topology information, and don't use
    * old snapshots.
    * Therefore, we expose the updateHead function on the public interface for initialisation purposes.
    *
    * @param effectiveTimestamp sequencer timestamp + epsilon(sequencer timestamp)
    * @param approximateTimestamp our current best guess of what the "best" timestamp is to get a valid current topology snapshot
    * @param potentialTopologyChange if true, the time advancement is related to a topology change that might have occurred or become effective
    */
  def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Unit

  /** current number of changes waiting to become effective */
  def numPendingChanges: Int

  /** Overloaded recent snapshot returning derived type */
  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def trySnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader

  /** Overloaded snapshot returning derived type */
  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[TopologySnapshotLoader] = {
    snapshotInternal(timestamp)((timestamp, waitForEffectiveTime) =>
      this.awaitTimestamp(timestamp, waitForEffectiveTime)
    )
  }

  /** Overloaded snapshot returning derived type */
  override def snapshotUS(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] = {
    snapshotInternal[FutureUnlessShutdown](timestamp)(
      (timestamp, waitForEffectiveTime) => this.awaitTimestampUS(timestamp, waitForEffectiveTime),
      // Do not log a warning if we get a shutdown future
      logWarning = f => f != FutureUnlessShutdown.abortedDueToShutdown,
    )
  }

  private def snapshotInternal[F[_]](
      timestamp: CantonTimestamp
  )(
      awaitTimestampFn: (CantonTimestamp, Boolean) => Option[F[Unit]],
      logWarning: F[Unit] => Boolean = Function.const(true),
  )(implicit traceContext: TraceContext, monad: Monad[F]): F[TopologySnapshotLoader] = {
    val syncF = awaitTimestampFn(timestamp, true) match {
      case None => monad.unit
      // No need to log a warning if the future we get is due to a shutdown in progress
      case Some(fut) =>
        if (logWarning(fut)) {
          logger.warn(
            s"Unsynchronized access to topology snapshot at $timestamp, topology known until=$topologyKnownUntilTimestamp"
          )
        }
        fut
    }
    syncF.map(_ => trySnapshot(timestamp))
  }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    awaitTimestamp(timestamp, waitForEffectiveTime = true)
      .getOrElse(Future.unit)
      .map(_ => trySnapshot(timestamp))

  override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    awaitTimestampUS(timestamp, waitForEffectiveTime = true)
      .getOrElse(FutureUnlessShutdown.unit)
      .map(_ => trySnapshot(timestamp))

  /** internal await implementation used to schedule state evaluations after topology updates */
  private[topology] def scheduleAwait(
      condition: => Future[Boolean],
      timeout: Duration,
  ): FutureUnlessShutdown[Boolean]

}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait KeyTopologySnapshotClientLoader extends KeyTopologySnapshotClient {
  this: BaseTopologySnapshotClient =>

  /** abstract loading function used to obtain the full key collection for a key owner */
  def allKeys(owner: KeyOwner): Future[KeyCollection]

  override def signingKey(owner: KeyOwner): Future[Option[SigningPublicKey]] =
    allKeys(owner).map(_.signingKeys.lastOption)

  override def signingKeys(owner: KeyOwner): Future[Seq[SigningPublicKey]] =
    allKeys(owner).map(_.signingKeys)

  override def encryptionKey(owner: KeyOwner): Future[Option[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys.lastOption)

  override def encryptionKeys(owner: KeyOwner): Future[Seq[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys)

}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait ParticipantTopologySnapshotLoader extends ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  override def isParticipantActive(participantId: ParticipantId): Future[Boolean] =
    participantState(participantId).map(_.permission.isActive)

  def findParticipantState(participantId: ParticipantId): Future[Option[ParticipantAttributes]]

  def participantState(participantId: ParticipantId): Future[ParticipantAttributes] =
    findParticipantState(participantId).map(
      _.getOrElse(ParticipantAttributes(ParticipantPermission.Disabled, TrustLevel.Ordinary))
    )

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]]

}

private[client] trait PartyTopologySnapshotBaseClient {

  this: PartyTopologySnapshotClient with BaseTopologySnapshotClient =>

  override def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: (ParticipantPermission => Boolean) = _.isActive,
  ): EitherT[Future, Set[LfPartyId], Unit] = {
    val fetchedF = activeParticipantsOfPartiesWithAttributes(parties.toSeq)
    EitherT(
      fetchedF
        .map { fetched =>
          fetched.foldLeft(Set.empty[LfPartyId]) { case (acc, (party, relationships)) =>
            if (relationships.exists(x => check(x._2.permission)))
              acc
            else acc + party
          }
        }
        .map { res =>
          if (res.isEmpty) Right(())
          else Left(res)
        }
    )
  }

  override def isHostedByAtLeastOneParticipantF(
      party: LfPartyId,
      check: ParticipantAttributes => Boolean,
  ): Future[Boolean] =
    activeParticipantsOf(party).map(_.values.exists(check))

  override def hostedOn(
      partyId: LfPartyId,
      participantId: ParticipantId,
  ): Future[Option[ParticipantAttributes]] =
    // TODO(i4930) implement directly, must not return DISABLED
    activeParticipantsOf(partyId).map(_.get(participantId))

  override def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _.permission.isActive,
  ): Future[Boolean] =
    partyIds.toList
      .parTraverse(hostedOn(_, participantId).map(_.exists(permissionCheck)))
      .map(_.forall(x => x))

  override def canConfirm(
      participant: ParticipantId,
      party: LfPartyId,
      requiredTrustLevel: TrustLevel = TrustLevel.Ordinary,
  ): Future[Boolean] =
    hostedOn(party, participant)
      .map(
        _.exists(relationship =>
          relationship.permission.canConfirm && relationship.trustLevel >= requiredTrustLevel
        )
      )(executionContext)

  override def activeParticipantsOfAll(
      parties: List[LfPartyId]
  ): EitherT[Future, Set[LfPartyId], Set[ParticipantId]] =
    EitherT(for {
      withActiveParticipants <- parties.parTraverse(p =>
        activeParticipantsOf(p).map(pMap => p -> pMap)
      )
      (noActive, allActive) = withActiveParticipants.foldLeft(
        Set.empty[LfPartyId] -> Set.empty[ParticipantId]
      ) { case ((noActive, allActive), (p, active)) =>
        (if (active.isEmpty) noActive + p else noActive, allActive.union(active.keySet))
      }
    } yield Either.cond(noActive.isEmpty, allActive, noActive))

}

private[client] trait PartyTopologySnapshotLoader
    extends PartyTopologySnapshotClient
    with PartyTopologySnapshotBaseClient {

  this: BaseTopologySnapshotClient with ParticipantTopologySnapshotLoader =>

  final override def activeParticipantsOf(
      party: LfPartyId
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    PartyId
      .fromLfParty(party)
      .map(loadActiveParticipantsOf(_, loadParticipantStates))
      .getOrElse(Future.successful(Map()))

  private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[ParticipantId, ParticipantAttributes]]

  final override def activeParticipantsOfParties(
      parties: Seq[LfPartyId]
  ): Future[Map[LfPartyId, Set[ParticipantId]]] = {
    val converted = parties.mapFilter(PartyId.fromLfParty(_).toOption)
    loadBatchActiveParticipantsOf(converted, loadParticipantStates).map(_.map { case (k, v) =>
      (k.toLf, v.keySet)
    })
  }

  final override def activeParticipantsOfPartiesWithAttributes(
      parties: Seq[LfPartyId]
  ): Future[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]] = {
    val converted = parties.mapFilter(PartyId.fromLfParty(_).toOption)
    loadBatchActiveParticipantsOf(converted, loadParticipantStates).map(_.map { case (k, v) =>
      (k.toLf, v)
    })
  }

  private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]]

}

trait VettedPackagesSnapshotLoader extends VettedPackagesSnapshotClient {
  this: BaseTopologySnapshotClient with PartyTopologySnapshotLoader =>

  private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]]

  protected def findUnvettedPackagesOrDependenciesUsingLoader(
      participantId: ParticipantId,
      packages: Set[PackageId],
      loader: (ParticipantId, PackageId) => EitherT[Future, PackageId, Set[PackageId]],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    packages.toList
      .parFlatTraverse(packageId => loader(participantId, packageId).map(_.toList))
      .map(_.toSet)

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    findUnvettedPackagesOrDependenciesUsingLoader(
      participantId,
      packages,
      (pid, packId) => loadUnvettedPackagesOrDependencies(pid, packId),
    )

}

trait DomainGovernanceSnapshotLoader extends DomainGovernanceSnapshotClient {
  this: BaseTopologySnapshotClient with NamedLogging =>
}

/** Loading interface with a more optimal method to read data from a store
  *
  * The topology information is stored in a particular way. In order to optimise loading and caching
  * of the data, we use such loader interfaces, such that we can optimise caching and loading of the
  * data while still providing a good and convenient access to the topology information.
  */
trait TopologySnapshotLoader
    extends TopologySnapshot
    with PartyTopologySnapshotLoader
    with BaseTopologySnapshotClient
    with ParticipantTopologySnapshotLoader
    with KeyTopologySnapshotClientLoader
    with VettedPackagesSnapshotLoader
    with DomainGovernanceSnapshotLoader
    with NamedLogging
