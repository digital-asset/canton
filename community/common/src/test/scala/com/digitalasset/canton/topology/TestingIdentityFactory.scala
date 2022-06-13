// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.functor._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.DefaultTestIdentities._
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, TestDomainParameters}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Add
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import org.mockito.MockitoSugar.mock

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Utility functions to setup identity & crypto apis for testing purposes
  *
  * You are trying to figure out how to setup identity topologies and crypto apis to drive your unit tests?
  * Then YOU FOUND IT! The present file contains everything that you should need.
  *
  * First, let's re-call that we are abstracting the identity and crypto aspects from the transaction protocol.
  *
  * Therefore, all the key crypto operations hide behind the so-called crypto-api which splits into the
  * pure part [[CryptoPureApi]] and the more complicated part, the [[SyncCryptoApi]] such that from the transaction
  * protocol perspective, we can conveniently use methods like [[SyncCryptoApi.sign]] or [[SyncCryptoApi.encryptFor]]
  *
  * The abstraction creates the following hierarchy of classes to resolve the state for a given [[KeyOwner]]
  * on a per (domainId, timestamp)
  *
  * SyncCryptoApiProvider - root object that makes the synchronisation topology state known to a node accessible
  *   .forDomain          - method to get the specific view on a per domain basis
  * = DomainSyncCryptoApi
  *   .snapshot(timestamp) | recentState - method to get the view for a specific time
  * = DomainSnapshotSyncCryptoApi (extends SyncCryptoApi)
  *
  * All these object carry the necessary objects ([[CryptoPureApi]], [[TopologySnapshot]], [[KeyVaultApi]])
  * as arguments with them.
  *
  * Now, in order to conveniently create a static topology for testing, we provide a
  * <ul>
  *   <li>[[TestingTopology]] which allows us to define a certain static topology</li>
  *   <li>[[TestingIdentityFactory]] which consumes a static topology and delivers all necessary components and
  *       objects that a unit test might need.</li>
  *   <li>[[DefaultTestIdentities]] which provides a predefined set of identities that can be used for unit tests.</li>
  * </ul>
  *
  * Common usage patterns are:
  * <ul>
  *   <li>Get a [[DomainSyncCryptoClient]] with an empty topology: `TestingIdentityFactory().forOwnerAndDomain(participant1)`</li>
  *   <li>To get a [[DomainSnapshotSyncCryptoApi]]: same as above, just add `.recentState`.</li>
  *   <li>Define a specific topology and get the [[SyncCryptoApiProvider]]: `TestingTopology().withTopology(Map(party1 -> participant1)).build()`.</li>
  * </ul>
  *
  * @param domains Set of domains for which the topology is valid
  * @param topology Static association of parties to participants in the most complete way it can be defined in this testing class.
  * @param additionalParticipants Additional participants for which keys should be added besides the ones mentioned in the topology.
  * @param keyPurposes The purposes of the keys that will be generated.
  */
case class TestingTopology(
    domains: Set[DomainId] = Set(DefaultTestIdentities.domainId),
    topology: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]] = Map.empty,
    mediators: Set[MediatorId] = Set(DefaultTestIdentities.mediator),
    additionalParticipants: Set[ParticipantId] = Set.empty,
    keyPurposes: Set[KeyPurpose] = KeyPurpose.all,
    domainParameters: List[DynamicDomainParameters.WithValidity] = List(
      DynamicDomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameters = DefaultTestIdentities.defaultDynamicDomainParameters,
      )
    ),
) {

  /** Define for which domains the topology should apply.
    *
    * All domains will have exactly the same topology.
    */
  def withDomains(domains: DomainId*): TestingTopology = this.copy(domains = domains.toSet)

  /** Define which participants to add
    *
    * These participants will be added in addition to the participants mentioned in the topologies.
    */
  def withParticipants(additionalParticipants: ParticipantId*): TestingTopology =
    this.copy(additionalParticipants = additionalParticipants.toSet)

  def participants(): Set[ParticipantId] = {
    (topology.values
      .flatMap(x => x.keys) ++ additionalParticipants).toSet
  }

  def withKeyPurposes(keyPurposes: Set[KeyPurpose]): TestingTopology =
    this.copy(keyPurposes = keyPurposes)

  /** Define the topology as a simple map of party to participant */
  def withTopology(
      parties: Map[LfPartyId, ParticipantId],
      permission: ParticipantPermission = ParticipantPermission.Submission,
      trustLevel: TrustLevel = TrustLevel.Ordinary,
  ): TestingTopology = {
    val tmp: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]] = parties.toSeq
      .map { case (party, participant) =>
        (party, (participant, ParticipantAttributes(permission, trustLevel)))
      }
      .groupBy(_._1)
      .fmap(res => res.map(_._2).toMap)
    this.copy(topology = tmp)
  }

  /** Define the topology as a map of participant to map of parties */
  def withReversedTopology(
      parties: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      trustLevel: TrustLevel = TrustLevel.Ordinary,
  ): TestingTopology = {
    val converted = parties
      .flatMap { case (participantId, partyToPermission) =>
        partyToPermission.toSeq.map { case (party, permission) =>
          (party, participantId, permission)
        }
      }
      .groupBy(_._1)
      .fmap(_.map { case (_, pid, permission) =>
        (pid, permission)
      }.toMap)
    withDetailedTopology(converted, trustLevel)
  }

  /** Define the topology as a map of parties to a map of participant ids and permission */
  def withDetailedTopology(
      parties: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]],
      trustLevel: TrustLevel = TrustLevel.Ordinary,
  ): TestingTopology =
    this.copy(topology = parties.fmap(w => w.fmap(ParticipantAttributes(_, trustLevel))))

  def build(
      loggerFactory: NamedLoggerFactory = NamedLoggerFactory("test-area", "crypto")
  ): TestingIdentityFactory =
    new TestingIdentityFactory(this, loggerFactory, domainParameters)
}

class TestingIdentityFactory(
    topology: TestingTopology,
    override protected val loggerFactory: NamedLoggerFactory,
    dynamicDomainParameters: List[DynamicDomainParameters.WithValidity],
) extends NamedLogging {

  private implicit val directExecutionContext: ExecutionContext = DirectExecutionContext(logger)
  private val defaultProtocolVersion = TestDomainParameters.defaultStatic.protocolVersion

  def forOwner(owner: KeyOwner): SyncCryptoApiProvider = {
    new SyncCryptoApiProvider(
      owner,
      ips(),
      newCrypto(owner),
      CachingConfigs.testing,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
  }

  def forOwnerAndDomain(
      owner: KeyOwner,
      domain: DomainId = DefaultTestIdentities.domainId,
  ): DomainSyncCryptoClient =
    forOwner(owner).tryForDomain(domain)

  def ips(): IdentityProvidingServiceClient = {
    val ips = new IdentityProvidingServiceClient()
    topology.domains.foreach(dId =>
      ips.add(new DomainTopologyClient() {
        override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(
            implicit traceContext: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???
        override def domainId: DomainId = dId
        override def trySnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): TopologySnapshot =
          topologySnapshot(domainId, timestampForDomainParameters = timestamp)
        override def currentSnapshotApproximation(implicit
            traceContext: TraceContext
        ): TopologySnapshot =
          topologySnapshot(
            domainId,
            timestampForDomainParameters = CantonTimestamp.Epoch,
          )
        override def snapshotAvailable(timestamp: CantonTimestamp): Boolean = true
        override def awaitTimestamp(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[Future[Unit]] = None

        override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(
            implicit traceContext: TraceContext
        ): Option[FutureUnlessShutdown[Unit]] = None
        override def approximateTimestamp: CantonTimestamp =
          currentSnapshotApproximation(TraceContext.empty).timestamp
        override def snapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] = awaitSnapshot(timestamp)
        override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[TopologySnapshot] = Future.successful(trySnapshot(timestamp))

        override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[TopologySnapshot] =
          FutureUnlessShutdown.pure(trySnapshot(timestamp))
        override def close(): Unit = ()
        override def topologyKnownUntilTimestamp: CantonTimestamp = approximateTimestamp
      })
    )
    ips
  }

  def topologySnapshot(
      domainId: DomainId = DefaultTestIdentities.domainId,
      packages: Seq[PackageId] = Seq(),
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]] =
        StoreBasedDomainTopologyClient.NoPackageDependencies,
      timestampForDomainParameters: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshot = {

    val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)

    val permissions: Map[ParticipantId, ParticipantAttributes] = topology.topology.flatMap(_._2)

    val participantTxs = participantsTxs(permissions, packages)

    val domainMembers =
      (Seq[KeyOwner](
        DomainTopologyManagerId(domainId),
        SequencerId(domainId),
      ) ++ topology.mediators.toSeq)
        .flatMap(m => genKeyCollection(m))

    val mediatorOnboarding =
      topology.mediators.toSeq.map { mediator =>
        mkAdd(MediatorDomainState(RequestSide.Both, domainId, mediator))
      }

    val partyDataTx = partyToParticipantTxs()

    val domainGovernanceTxs = List(
      mkReplace(
        DomainParametersChange(domainId, domainParametersChangeTx(timestampForDomainParameters))
      )
    )

    val updateF = store.updateState(
      SequencedTime(CantonTimestamp.Epoch.immediatePredecessor),
      EffectiveTime(CantonTimestamp.Epoch.immediatePredecessor),
      deactivate = Seq(),
      positive =
        participantTxs ++ domainMembers ++ mediatorOnboarding ++ partyDataTx ++ domainGovernanceTxs,
    )(TraceContext.empty)
    Await.result(
      updateF,
      1.seconds,
    ) // The in-memory topology store should complete the state update immediately

    new StoreBasedTopologySnapshot(
      CantonTimestamp.Epoch,
      store,
      Map(),
      useStateTxs = true,
      packageDependencies,
      loggerFactory,
    )
  }

  private def domainParametersChangeTx(ts: CantonTimestamp): DynamicDomainParameters =
    dynamicDomainParameters.collect { case dp if dp.isValidAt(ts) => dp.parameters } match {
      case dp :: Nil => dp
      case Nil => DynamicDomainParameters.initialValues(NonNegativeFiniteDuration.Zero)
      case _ => throw new IllegalStateException(s"Multiple domain parameters are valid at $ts")
    }

  private val signedTxProtocolRepresentative: RepresentativeProtocolVersion =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(ProtocolVersion.latestForTest)

  private def mkReplace(
      mapping: DomainGovernanceMapping
  ): SignedTopologyTransaction[TopologyChangeOp.Replace] = SignedTopologyTransaction(
    DomainGovernanceTransaction(
      DomainGovernanceElement(mapping),
      defaultProtocolVersion,
    ),
    mock[SigningPublicKey],
    mock[Signature],
  )(signedTxProtocolRepresentative, None)

  private def mkAdd(
      mapping: TopologyStateUpdateMapping
  ): SignedTopologyTransaction[TopologyChangeOp.Add] = SignedTopologyTransaction(
    TopologyStateUpdate(
      TopologyChangeOp.Add,
      TopologyStateUpdateElement(TopologyElementId.generate(), mapping),
    )(defaultProtocolVersion),
    mock[SigningPublicKey],
    mock[Signature],
  )(signedTxProtocolRepresentative, None)

  private def genKeyCollection(
      owner: KeyOwner
  ): Seq[SignedTopologyTransaction[TopologyChangeOp.Add]] = {
    val keyPurposes = topology.keyPurposes

    val sigKey =
      if (keyPurposes.contains(KeyPurpose.Signing))
        Seq(SymbolicCrypto.signingPublicKey(s"sigK-${keyFingerprintForOwner(owner).unwrap}"))
      else Seq()

    val encKey =
      if (keyPurposes.contains(KeyPurpose.Encryption))
        Seq(SymbolicCrypto.encryptionPublicKey(s"encK-${keyFingerprintForOwner(owner).unwrap}"))
      else Seq()

    (Seq.empty[PublicKey] ++ sigKey ++ encKey).map { key =>
      mkAdd(OwnerToKeyMapping(owner, key))
    }
  }

  private def partyToParticipantTxs(): Iterable[SignedTopologyTransaction[Add]] = topology.topology
    .flatMap { case (lfParty, participants) =>
      val partyId = PartyId.tryFromLfParty(lfParty)
      participants.iterator.filter(_._1.uid != partyId.uid).map {
        case (participantId, relationship) =>
          mkAdd(
            PartyToParticipant(
              RequestSide.Both,
              partyId,
              participantId,
              relationship.permission,
            )
          )
      }
    }

  private def participantsTxs(
      permissions: Map[ParticipantId, ParticipantAttributes],
      packages: Seq[PackageId],
  ): Seq[SignedTopologyTransaction[Add]] = topology
    .participants()
    .toSeq
    .flatMap { participantId =>
      val attributes = permissions
        .getOrElse(
          participantId,
          ParticipantAttributes(ParticipantPermission.Submission, TrustLevel.Ordinary),
        )
      val pkgs =
        if (packages.nonEmpty) Seq(mkAdd(VettedPackages(participantId, packages))) else Seq()
      pkgs ++ genKeyCollection(participantId) :+ mkAdd(
        ParticipantState(
          RequestSide.Both,
          domainId,
          participantId,
          attributes.permission,
          attributes.trustLevel,
        )
      )
    }

  private def keyFingerprintForOwner(owner: KeyOwner): Fingerprint =
    // We are converting an Identity (limit of 185 characters) to a Fingerprint (limit of 68 characters) - this would be
    // problematic if this function wasn't only used for testing
    Fingerprint.tryCreate(owner.uid.id.toLengthLimitedString.unwrap)

  def newCrypto(
      owner: KeyOwner,
      signingFingerprints: Seq[Fingerprint] = Seq(),
      fingerprintSuffixes: Seq[String] = Seq(),
  ): Crypto = {
    val signingFingerprintsOrOwner =
      if (signingFingerprints.isEmpty)
        Seq(keyFingerprintForOwner(owner))
      else
        signingFingerprints

    val fingerprintSuffixesOrOwner =
      if (fingerprintSuffixes.isEmpty)
        Seq(keyFingerprintForOwner(owner).unwrap)
      else
        fingerprintSuffixes

    SymbolicCrypto.tryCreate(
      signingFingerprintsOrOwner,
      fingerprintSuffixesOrOwner,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
  }

  def newSigningPublicKey(owner: KeyOwner): SigningPublicKey = {
    SymbolicCrypto.signingPublicKey(keyFingerprintForOwner(owner))
  }

}

/** something used often: somebody with keys and ability to created signed transactions */
class TestingOwnerWithKeys(
    val keyOwner: KeyOwner,
    loggerFactory: NamedLoggerFactory,
    initEc: ExecutionContext,
) extends NoTracing {

  val cryptoApi = TestingIdentityFactory(loggerFactory).forOwnerAndDomain(keyOwner)
  private val defaultProtocolVersion = TestDomainParameters.defaultStatic.protocolVersion

  object SigningKeys {

    implicit val ec = initEc

    val key1 = genSignKey("key1")
    val key2 = genSignKey("key2")
    val key3 = genSignKey("key3")
    val key4 = genSignKey("key4")
    val key5 = genSignKey("key5")
    val key6 = genSignKey("key6")
    val key7 = genSignKey("key7")
    val key8 = genSignKey("key8")
    val key9 = genSignKey("key9")

  }

  object EncryptionKeys {
    private implicit val ec = initEc
    val key1 = genEncKey("enc-key1")
  }

  object TestingTransactions {
    import SigningKeys._
    val namespaceKey = key1
    val uid2 = uid.copy(id = Identifier.tryCreate("second"))
    val ts = CantonTimestamp.Epoch
    val ts1 = ts.plusSeconds(1)
    val ns1k1 = mkAdd(
      NamespaceDelegation(
        Namespace(namespaceKey.fingerprint),
        namespaceKey,
        isRootDelegation = true,
      )
    )
    val ns1k2 = mkAdd(
      NamespaceDelegation(Namespace(namespaceKey.fingerprint), key2, isRootDelegation = false)
    )
    val id1k1 = mkAdd(IdentifierDelegation(uid, key1))
    val id2k2 = mkAdd(IdentifierDelegation(uid2, key2))
    val okm1 = mkAdd(OwnerToKeyMapping(domainManager, namespaceKey))
    val rokm1 = revert(okm1)
    val okm2 = mkAdd(OwnerToKeyMapping(sequencer, key2))
    val ps1m =
      ParticipantState(
        RequestSide.Both,
        domainId,
        participant1,
        ParticipantPermission.Submission,
        TrustLevel.Ordinary,
      )
    val ps1 = mkAdd(ps1m)
    val rps1 = revert(ps1)
    val ps2 = mkAdd(ps1m.copy(permission = ParticipantPermission.Observation))
    val ps3 = mkAdd(
      ParticipantState(
        RequestSide.Both,
        domainId,
        participant2,
        ParticipantPermission.Confirmation,
        TrustLevel.Ordinary,
      )
    )
    val p2p1 = mkAdd(
      PartyToParticipant(
        RequestSide.Both,
        PartyId(UniqueIdentifier(Identifier.tryCreate("one"), Namespace(key1.id))),
        participant1,
        ParticipantPermission.Submission,
      )
    )
    val p2p2 = mkAdd(
      PartyToParticipant(
        RequestSide.Both,
        PartyId(UniqueIdentifier(Identifier.tryCreate("two"), Namespace(key1.id))),
        participant1,
        ParticipantPermission.Submission,
      )
    )

    private val defaultDomainParameters = TestDomainParameters.defaultDynamic

    val dpc1 = mkDmGov(
      DomainParametersChange(
        DomainId(uid),
        defaultDomainParameters.copy(participantResponseTimeout =
          NonNegativeFiniteDuration.ofSeconds(1)
        ),
      ),
      namespaceKey,
    )
    val dpc1Updated = mkDmGov(
      DomainParametersChange(
        DomainId(uid),
        defaultDomainParameters.copy(
          participantResponseTimeout = NonNegativeFiniteDuration.ofSeconds(2),
          topologyChangeDelay = NonNegativeFiniteDuration.ofMillis(100),
        ),
      ),
      namespaceKey,
    )

    val dpc2 =
      mkDmGov(DomainParametersChange(DomainId(uid2), defaultDomainParameters), key2)
  }

  def mkTrans[Op <: TopologyChangeOp](
      trans: TopologyTransaction[Op],
      signingKey: SigningPublicKey = SigningKeys.key1,
  )(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[Op] =
    Await
      .result(
        SignedTopologyTransaction
          .create(
            trans,
            signingKey,
            cryptoApi.crypto.pureCrypto,
            cryptoApi.crypto.privateCrypto,
            ProtocolVersion.latestForTest,
          )
          .value,
        10.seconds,
      )
      .getOrElse(sys.error("failed to create signed topology transaction"))

  def mkAdd(mapping: TopologyStateUpdateMapping, signingKey: SigningPublicKey = SigningKeys.key1)(
      implicit ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Add] =
    mkTrans(TopologyStateUpdate.createAdd(mapping, defaultProtocolVersion), signingKey)

  def mkDmGov(mapping: DomainGovernanceMapping, signingKey: SigningPublicKey)(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp.Replace] =
    mkTrans(DomainGovernanceTransaction(mapping, defaultProtocolVersion), signingKey)

  def revert(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      ec: ExecutionContext
  ): SignedTopologyTransaction[TopologyChangeOp] =
    transaction.transaction match {
      case topologyStateUpdate: TopologyStateUpdate[_] =>
        mkTrans(topologyStateUpdate.reverse, transaction.key)
      case DomainGovernanceTransaction(_) => transaction
    }

  private def genSignKey(name: String): SigningPublicKey =
    Await
      .result(
        cryptoApi.crypto
          .generateSigningKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .getOrElse(sys.error("key should be there"))

  def genEncKey(name: String): EncryptionPublicKey =
    Await
      .result(
        cryptoApi.crypto
          .generateEncryptionKey(name = Some(KeyName.tryCreate(name)))
          .value,
        30.seconds,
      )
      .getOrElse(sys.error("key should be there"))

}

object TestingIdentityFactory {

  def apply(
      loggerFactory: NamedLoggerFactory,
      topology: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]] = Map.empty,
  ): TestingIdentityFactory =
    TestingIdentityFactory(
      TestingTopology(topology = topology),
      loggerFactory,
      TestDomainParameters.defaultDynamic,
    )

  def apply(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
      dynamicDomainParameters: DynamicDomainParameters,
  ) = new TestingIdentityFactory(
    topology,
    loggerFactory,
    dynamicDomainParameters = List(
      DynamicDomainParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameters = dynamicDomainParameters,
      )
    ),
  )

  def pureCrypto(): CryptoPureApi = new SymbolicPureCrypto

}

object DefaultTestIdentities {

  private def createParticipantAndParty(counter: Int): (ParticipantId, PartyId) = {
    val namespace = Namespace(Fingerprint.tryCreate(s"participant$counter-identity"))
    val id = ParticipantId(Identifier.tryCreate(s"participant$counter"), namespace)
    val party = PartyId(UniqueIdentifier(Identifier.tryCreate(s"party$counter"), namespace))
    (id, party)
  }

  val namespace = Namespace(Fingerprint.tryCreate("default"))
  val uid = UniqueIdentifier(Identifier.tryCreate("da"), namespace)
  val domainId = DomainId(uid)
  val domainManager = DomainTopologyManagerId(uid)
  val sequencer = SequencerId(uid)
  val mediator = MediatorId(uid)

  val domainEntities: Set[KeyOwner] = Set[KeyOwner](domainManager, sequencer, mediator)
  val (participant1, party1) = createParticipantAndParty(1)
  val (participant2, party2) = createParticipantAndParty(2)
  val (participant3, party3) = createParticipantAndParty(3)

  val defaultDynamicDomainParameters =
    DynamicDomainParameters.initialValues(NonNegativeFiniteDuration.Zero)
}
