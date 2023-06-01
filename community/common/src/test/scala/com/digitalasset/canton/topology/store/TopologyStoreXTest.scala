// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.{
  DomainTrustCertificateX,
  HostingParticipant,
  IdentifierDelegationX,
  MediatorDomainStateX,
  NamespaceDelegationX,
  OwnerToKeyMappingX,
  ParticipantPermissionX,
  PartyToParticipantX,
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
  TopologyTransactionX,
  UnionspaceDefinitionX,
}
import com.digitalasset.canton.topology.{
  DomainId,
  Identifier,
  MediatorId,
  Namespace,
  ParticipantId,
  PartyId,
  SequencerId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=match may not be exhaustive")
trait TopologyStoreXTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterAll {

  private val Seq(ts1, ts2, ts3, ts4, ts5, ts6) = (1L to 6L).map(CantonTimestamp.Epoch.plusSeconds)

  // TODO(#11255): Test coverage is rudimentary - enough to convince ourselves that queries basically seem to work.
  //  Increase coverage.
  def topologyStore(mk: () => TopologyStoreX[TopologyStoreId]): Unit = {

    val factory: TestingOwnerWithKeys =
      new TestingOwnerWithKeys(
        SequencerId(
          UniqueIdentifier(
            Identifier.tryCreate("da"),
            Namespace(Fingerprint.tryCreate("sequencer")),
          )
        ),
        loggerFactory,
        executorService,
      )

    val daDomainNamespace = Namespace(Fingerprint.tryCreate("default"))
    val daDomainUid = UniqueIdentifier(
      Identifier.tryCreate("da"),
      daDomainNamespace,
    )
    val Seq(participantId1, participantId2) = Seq("participant1", "participant2").map(p =>
      ParticipantId(
        UniqueIdentifier(
          Identifier.tryCreate(p),
          Namespace(Fingerprint.tryCreate("participants")),
        )
      )
    )
    val domainId1 = DomainId(
      UniqueIdentifier(
        Identifier.tryCreate("domain1"),
        Namespace(Fingerprint.tryCreate("domains")),
      )
    )
    val mediatorId1 = MediatorId(
      Identifier.tryCreate("mediator1"),
      Namespace(Fingerprint.tryCreate("mediators")),
    )
    val signingKeys = NonEmpty(Seq, factory.SigningKeys.key1)
    val owners = NonEmpty(Set, Namespace(Fingerprint.tryCreate("owner1")))
    val fredOfCanton = PartyId(
      Identifier.tryCreate("fred"),
      Namespace(Fingerprint.tryCreate("canton")),
    )

    val tx_NSD_Proposal = makeSignedTx(
      NamespaceDelegationX
        .create(daDomainNamespace, signingKeys.head1, isRootDelegation = false)
        .getOrElse(fail()),
      isProposal = true,
    )
    val tx2_OTK = makeSignedTx(
      OwnerToKeyMappingX(participantId1, domain = None, signingKeys)
    )
    val tx3_IDD_Removal = makeSignedTx(
      IdentifierDelegationX(daDomainUid, signingKeys.head1),
      op = TopologyChangeOpX.Remove,
      serial = PositiveInt.tryCreate(1),
    )
    val tx4_USD = makeSignedTx(
      UnionspaceDefinitionX
        .create(
          Namespace(Fingerprint.tryCreate("unionspace")),
          PositiveInt.one,
          owners = owners,
        )
        .getOrElse(fail())
    )
    val tx5_PTP = makeSignedTx(
      PartyToParticipantX(
        partyId = fredOfCanton,
        domainId = None,
        threshold = PositiveInt.one,
        participants = Seq(HostingParticipant(participantId1, ParticipantPermissionX.Submission)),
        groupAddressing = true,
      )
    )
    val tx5_DTC = makeSignedTx(
      DomainTrustCertificateX(
        participantId2,
        domainId1,
        transferOnlyToGivenTargetDomains = false,
        targetDomains = Seq.empty,
      )
    )
    val tx6_MDS = makeSignedTx(
      MediatorDomainStateX
        .create(
          domain = domainId1,
          group = NonNegativeInt.one,
          threshold = PositiveInt.one,
          active = Seq(mediatorId1),
          observers = Seq.empty,
        )
        .getOrElse(fail())
    )

    val bootstrapTransactions = StoredTopologyTransactionsX(
      Seq[
        (CantonTimestamp, (GenericSignedTopologyTransactionX, Option[CantonTimestamp]))
      ](
        ts2 -> (tx2_OTK, ts3.some),
        ts3 -> (tx3_IDD_Removal, ts3.some),
        ts4 -> (tx4_USD, None),
        ts5 -> (tx5_PTP, None),
        ts5 -> (tx5_DTC, None),
      ).map { case (from, (tx, until)) =>
        StoredTopologyTransactionX(
          SequencedTime(from),
          EffectiveTime(from),
          until.map(EffectiveTime(_)),
          tx,
        )
      }
    )

    "topology store x" should {

      "deal with authorized transactions" when {

        "handle simple operations" in {
          val store = mk()

          for {
            _ <- update(store, ts1, add = Seq(tx_NSD_Proposal))
            _ <- update(store, ts2, add = Seq(tx2_OTK))
            _ <- update(store, ts5, add = Seq(tx5_DTC))
            _ <- update(store, ts6, add = Seq(tx6_MDS))

            maxTs <- store.maxTimestamp()
            retrievedTx <- store.findStored(tx_NSD_Proposal)
            txProtocolVersion <- store.findStoredForVersion(
              tx_NSD_Proposal.transaction,
              ProtocolVersion.dev,
            )
            txBadProtocolVersion <- store.findStoredForVersion(
              tx_NSD_Proposal.transaction,
              ProtocolVersion.v4,
            )

            proposalTransactions <- inspect(
              store,
              TimeQueryX.Range(ts1.some, ts4.some),
              proposals = true,
            )
            positiveProposals <- findPositiveTransactions(store, ts6, isProposal = true)

            txByTxHash <- store.findProposalsByTxHash(
              EffectiveTime(ts1.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, tx_NSD_Proposal.transaction.hash),
            )
            txByMappingHash <- store.findTransactionsForMapping(
              EffectiveTime(ts2.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, tx2_OTK.transaction.mapping.uniqueKey),
            )

            _ <- store.updateDispatchingWatermark(ts1)
            tsWatermark <- store.currentDispatchingWatermark

            _ <- update(
              store,
              ts4,
              removeMapping = Set(tx_NSD_Proposal.transaction.mapping.uniqueKey),
            )
            removedByMappingHash <- store.findStored(tx_NSD_Proposal)
            _ <- update(store, ts4, removeTxs = Set(tx2_OTK.transaction.hash))
            removedByTxHash <- store.findStored(tx2_OTK)

            mdsTx <- store.findFirstMediatorStateForMediator(
              tx6_MDS.transaction.mapping.active.headOption.getOrElse(fail())
            )

            dtsTx <- store.findFirstTrustCertificateForParticipant(
              tx5_DTC.transaction.mapping.participantId
            )

          } yield {
            store.dumpStoreContent()

            assert(maxTs.contains((SequencedTime(ts6), EffectiveTime(ts6))))
            retrievedTx.map(_.transaction) shouldBe Some(tx_NSD_Proposal)
            txProtocolVersion.map(_.transaction) shouldBe Some(tx_NSD_Proposal)
            txBadProtocolVersion shouldBe None

            expectTransactions(
              proposalTransactions,
              Seq(
                tx_NSD_Proposal
              ), // only proposal transaction, TimeQueryX.Range is inclusive on both sides
            )
            expectTransactions(positiveProposals, Seq(tx_NSD_Proposal))

            txByTxHash shouldBe Seq(tx_NSD_Proposal)
            txByMappingHash shouldBe Seq(tx2_OTK)

            tsWatermark shouldBe Some(ts1)

            removedByMappingHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))
            removedByTxHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))

            mdsTx.map(_.transaction) shouldBe Some(tx6_MDS)

            dtsTx.map(_.transaction) shouldBe Some(tx5_DTC)
          }
        }

        "able to inspect" in {
          val store = mk()

          for {
            _ <- store.bootstrap(bootstrapTransactions)
            headStateTransactions <- inspect(store, TimeQueryX.HeadState)
            rangeBetweenTs2AndTs3Transactions <- inspect(
              store,
              TimeQueryX.Range(ts2.some, ts3.some),
            )
            snapshotAtTs3Transactions <- inspect(
              store,
              TimeQueryX.Snapshot(ts3),
            )
            unionspaceTransactions <- inspect(
              store,
              TimeQueryX.Range(ts1.some, ts4.some),
              typ = UnionspaceDefinitionX.code.some,
            )
            removalTransactions <- inspect(
              store,
              timeQuery = TimeQueryX.Range(ts1.some, ts4.some),
              op = TopologyChangeOpX.Remove.some,
            )
            idDaTransactions <- inspect(
              store,
              timeQuery = TimeQueryX.Range(ts1.some, ts4.some),
              idFilter = "da",
            )
            idNamespaceTransactions <- inspect(
              store,
              timeQuery = TimeQueryX.Range(ts1.some, ts4.some),
              idFilter = "unionspace",
              namespaceOnly = true,
            )
            bothParties <- inspectKnownParties(store, ts6)
            onlyFred <- inspectKnownParties(store, ts6, filterParty = "fr::can")
            fredFullySpecified <- inspectKnownParties(
              store,
              ts6,
              filterParty = fredOfCanton.uid.toProtoPrimitive,
              filterParticipant = participantId1.uid.toProtoPrimitive,
            )
            onlyParticipant2 <- inspectKnownParties(store, ts6, filterParticipant = "participant2")
            neitherParty <- inspectKnownParties(store, ts6, "fred::canton", "participant2")
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(tx4_USD, tx5_PTP, tx5_DTC),
            )
            expectTransactions(rangeBetweenTs2AndTs3Transactions, Seq(tx2_OTK, tx3_IDD_Removal))
            expectTransactions(
              snapshotAtTs3Transactions,
              Seq(tx2_OTK), // tx2 include as until is inclusive, tx3 missing as from exclusive
            )
            expectTransactions(unionspaceTransactions, Seq(tx4_USD))
            expectTransactions(removalTransactions, Seq(tx3_IDD_Removal))
            expectTransactions(idDaTransactions, Seq(tx3_IDD_Removal))
            expectTransactions(idNamespaceTransactions, Seq(tx4_USD))

            bothParties shouldBe Set(
              tx5_PTP.transaction.mapping.partyId,
              tx5_DTC.transaction.mapping.participantId.adminParty,
            )
            onlyFred shouldBe Set(tx5_PTP.transaction.mapping.partyId)
            fredFullySpecified shouldBe Set(tx5_PTP.transaction.mapping.partyId)
            onlyParticipant2 shouldBe Set(tx5_DTC.transaction.mapping.participantId.adminParty)
            neitherParty shouldBe Set.empty
          }
        }

        "able to find positive transactions" in {
          val store = mk()

          for {
            _ <- store.bootstrap(bootstrapTransactions)
            positiveTransactions <- findPositiveTransactions(store, ts6)
            positiveTransactionsExclusive <- findPositiveTransactions(
              store,
              ts5,
            )
            positiveTransactionsInclusive <- findPositiveTransactions(
              store,
              ts5,
              asOfInclusive = true,
            )
            selectiveMappingTransactions <- findPositiveTransactions(
              store,
              ts6,
              types =
                Seq(UnionspaceDefinitionX.code, OwnerToKeyMappingX.code, PartyToParticipantX.code),
            )
            uidFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterUid = Some(
                Seq(
                  tx5_PTP.transaction.mapping.partyId.uid,
                  tx5_DTC.transaction.mapping.participantId.uid,
                )
              ),
            )
            namespaceFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterNamespace = Some(
                Seq(tx4_USD.transaction.mapping.namespace, tx5_DTC.transaction.mapping.namespace)
              ),
            )

            essentialStateTransactions <- store.findEssentialStateForMember(
              tx2_OTK.transaction.mapping.member,
              asOfInclusive = ts6,
            )

            upcomingTransactions <- store.findUpcomingEffectiveChanges(asOfInclusive = ts4)

            dispatchingTransactionsAfter <- store.findDispatchingTransactionsAfter(
              timestampExclusive = ts1,
              limit = None,
            )

            onboardingTransactionUnlessShutdown <- store
              .findParticipantOnboardingTransactions(
                tx5_DTC.transaction.mapping.participantId,
                tx5_DTC.transaction.mapping.domainId,
              )
              .unwrap // FutureUnlessShutdown[_] -> Future[UnlessShutdown[_]]
          } yield {
            expectTransactions(positiveTransactions, Seq(tx4_USD, tx5_PTP, tx5_DTC))
            expectTransactions(positiveTransactionsExclusive, Seq(tx4_USD))
            expectTransactions(
              positiveTransactionsInclusive,
              positiveTransactions.result.map(_.transaction),
            )
            expectTransactions(
              selectiveMappingTransactions,
              Seq( /* tx2_OKM only valid until ts3 */ tx4_USD, tx5_PTP),
            )
            expectTransactions(uidFilterTransactions, Seq(tx5_PTP, tx5_DTC))
            expectTransactions(namespaceFilterTransactions, Seq(tx4_USD, tx5_DTC))

            // Essential state currently encompasses all positive transactions at specified time
            expectTransactions(
              essentialStateTransactions,
              Seq(tx4_USD, tx5_PTP, tx5_DTC),
            )

            upcomingTransactions shouldBe bootstrapTransactions.result.collect {
              case tx if tx.validFrom.value >= ts4 =>
                TopologyStore.Change.Other(tx.sequenced, tx.validFrom)
            }.distinct

            expectTransactions(
              dispatchingTransactionsAfter,
              Seq(tx3_IDD_Removal, tx4_USD, tx5_PTP, tx5_DTC),
            )

            onboardingTransactionUnlessShutdown.toRight(fail()).getOrElse(fail()) shouldBe Seq(
              tx4_USD,
              tx5_DTC,
            )

          }
        }
      }
    }

    def update(
        store: TopologyStoreX[TopologyStoreId],
        ts: CantonTimestamp,
        add: Seq[GenericSignedTopologyTransactionX] = Seq.empty,
        removeMapping: Set[MappingHash] = Set.empty,
        removeTxs: Set[TxHash] = Set.empty,
    ): Future[Unit] = {
      store.update(
        SequencedTime(ts),
        EffectiveTime(ts),
        removeMapping,
        removeTxs,
        add.map(ValidatedTopologyTransactionX(_)),
      )
    }

    def inspect(
        store: TopologyStoreX[TopologyStoreId],
        timeQuery: TimeQueryX,
        proposals: Boolean = false,
        recentTimestampO: Option[CantonTimestamp] = None,
        op: Option[TopologyChangeOpX] = None,
        typ: Option[TopologyMappingX.Code] = None,
        idFilter: String = "",
        namespaceOnly: Boolean = false,
    ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] =
      store.inspect(
        proposals,
        timeQuery,
        recentTimestampO,
        op,
        typ,
        idFilter,
        namespaceOnly,
      )

    def inspectKnownParties(
        store: TopologyStoreX[TopologyStoreId],
        timestamp: CantonTimestamp,
        filterParty: String = "",
        filterParticipant: String = "",
    ): Future[Set[PartyId]] =
      store.inspectKnownParties(
        timestamp,
        filterParty,
        filterParticipant,
        limit = 1000,
      )

    def findPositiveTransactions(
        store: TopologyStoreX[TopologyStoreId],
        asOf: CantonTimestamp,
        asOfInclusive: Boolean = false,
        isProposal: Boolean = false,
        types: Seq[TopologyMappingX.Code] = TopologyMappingX.Code.all,
        filterUid: Option[Seq[UniqueIdentifier]] = None,
        filterNamespace: Option[Seq[Namespace]] = None,
    ): Future[PositiveStoredTopologyTransactionsX] =
      store.findPositiveTransactions(
        asOf,
        asOfInclusive,
        isProposal,
        types,
        filterUid,
        filterNamespace,
      )

    def expectTransactions(
        actual: GenericStoredTopologyTransactionsX,
        expected: Seq[GenericSignedTopologyTransactionX],
    ): Assertion = {
      logger.info(s"Actual: ${actual.result.map(_.transaction).mkString(",")}")
      logger.info(s"Expected: ${expected.mkString(",")}")
      // run more readable assert first since mapping codes are easier to identify than hashes ;-)
      actual.result.map(_.transaction.transaction.mapping.code.code) shouldBe expected.map(
        _.transaction.mapping.code.code
      )
      actual.result.map(_.transaction.transaction.hash) shouldBe expected.map(_.transaction.hash)
    }
  }

  def makeSignedTx[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      mapping: M,
      op: Op = TopologyChangeOpX.Replace,
      isProposal: Boolean = false,
      serial: PositiveInt = PositiveInt.one,
  ): SignedTopologyTransactionX[Op, M] =
    SignedTopologyTransactionX.apply[Op, M](
      TopologyTransactionX(
        op,
        serial,
        mapping,
        ProtocolVersion.dev,
      ),
      signatures = NonEmpty(Set, Signature.noSignature),
      isProposal = isProposal,
    )(
      SignedTopologyTransactionX.supportedProtoVersions
        .protocolVersionRepresentativeFor(
          ProtocolVersion.dev
        )
    )

}
