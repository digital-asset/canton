// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr.multiparty

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.FlagSet
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  assertCommitmentsMatch,
  awaitHostingStates,
}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.multiparty.PartyFreezeMultiPartyReplicationIntegrationTest.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasSimClockUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.version.ProtocolVersion

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

/** Test scenario: Alice is hosted on P1, Bob and Dave on P2, and Carol on P3. Each of Alice, Bob
  * and Carol proposes a contract to each of the others and to Dave, which the other party accepts,
  * so that both parties sign it and confirm every action on it. Alice, Bob and Carol nominate P4,
  * which replicates all three with HyPR (Hybrid Party Replication) at the same time: it freezes
  * them, imports their ACS snapshots, moves to Onboarding for all three, and schedules their
  * onboarding clearances.
  *
  * Test goal: show that P4 becomes an Active host of the three parties with exactly their
  * contracts, although their snapshots come from different sources and overlap; that the ACS
  * commitments among all participants match; and that P4 can then archive and create contracts for
  * the parties.
  *
  * The repair service stands in for the remote ACS read, which runs only on the dev protocol
  * version so far, and for the import; P4 moves itself from Freezing to Onboarding.
  */
final class PartyFreezeMultiPartyReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private var alice: Party = _
  private var bob: Party = _
  private var carol: Party = _
  private var dave: Party = _
  private var sources: Map[Party, LocalParticipantReference] = _
  private var target: LocalParticipantReference = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(BaseTest.CantonTestsPath)
        alice = participant1.parties.enable("Alice")
        bob = participant2.parties.enable("Bob")
        carol = participant3.parties.enable("Carol")
        dave = participant2.parties.enable("Dave")
        sources = Map(alice -> participant1, bob -> participant2, carol -> participant3)
        target = participant4
      }

  "P4 replicates Alice, Bob and Carol, frozen at the same time" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*
      val replicated = Seq(alice, bob, carol)
      val contracts = createContracts(replicated, hostOf = sources + (dave -> participant2))

      moveAll(replicated, signerOf = sources, HostingState.Nominated).discard
      // P4 loads its three freezes in one call, so that they are sequenced together
      target.topology.transactions.load(
        replicated.map(signedByTarget(_, HostingState.Freezing)),
        daId,
      )
      val frozenAt = awaitAll(replicated, HostingState.Freezing)
      frozenAt.values.toSet should have size 1

      // Each contract between two of the parties is in both of their snapshots
      val snapshots = replicated.map(party =>
        exportAcs(sources(party), party, asOf = frozenAt(party).immediateSuccessor)
      )
      target.synchronizers.disconnect_all()
      snapshots.foreach(
        target.repair
          .import_acs(daId.logical, _, contractImportMode = ContractImportMode.Validation)
      )
      target.synchronizers.reconnect_all()

      val beforeOnboarding = target.ledger_api.state.end()
      moveAll(replicated, signerOf = _ => target, HostingState.Onboarding).discard
      val safeToClear = replicated.map(party =>
        inside(target.parties.clear_party_onboarding_flag(party, daId.logical, beforeOnboarding)) {
          case FlagSet(deadline) => deadline
        }
      )
      // The scheduled clearances move P4 to Active once it observes a time past their deadlines
      HasSimClockUtils.advanceClockInStepsTo(
        environment.simClock.value,
        safeToClear.max.immediateSuccessor,
        maxStep = PositiveFiniteDuration.tryOfSeconds(10),
      )(participants.local.foreach(participant => target.health.ping(participant.id)))
      val activatedAt = replicated.map(party =>
        awaitHostingStates(
          party,
          daId,
          participants.local,
          hostingStates(party, HostingState.Active),
        )
      )

      forAll(replicated)(party =>
        target.ledger_api.state.acs.of_party(party).map(_.contractId).toSet shouldBe
          contractIdsOf(party, contracts)
      )
      assertCommitmentsMatch(participants.local, daId, since = activatedAt.max)

      contracts.foreach(contract =>
        target.ledger_api.javaapi.commands
          .submit(
            Seq(contract.proposer),
            contract.id.exerciseDelete(List.empty[String].asJava).commands.asScala.toSeq,
            synchronizerId = Some(daId.logical),
          )
          .discard
      )
      val newContracts = createContracts(
        replicated,
        hostOf = replicated.map(_ -> target).toMap + (dave -> participant2),
      )
      val hostsOf = sources.map { case (party, source) =>
        party -> Seq(source, target)
      } +
        (dave -> Seq(participant2))
      eventually() {
        forAll(hostsOf.toSeq) { case (party, hosts) =>
          forAll(hosts)(
            _.ledger_api.state.acs.of_party(party).map(_.contractId).toSet shouldBe
              contractIdsOf(party, newContracts)
          )
        }
      }
  }

  /** Creates a contract that each of `parties` proposes to each other party and to Dave, on the
    * participants given by `hostOf`.
    */
  private def createContracts(
      parties: Seq[Party],
      hostOf: Map[Party, LocalParticipantReference],
  )(implicit env: TestConsoleEnvironment): Seq[Contract] =
    for {
      proposer <- parties
      acceptor <- parties :+ dave
      if acceptor != proposer
    } yield create(proposer, acceptor, hostOf)

  /** `proposer` proposes a contract on its host, which `acceptor` accepts on its host. Both sign
    * the resulting contract, and `proposer` can delete it alone.
    */
  private def create(
      proposer: Party,
      acceptor: Party,
      hostOf: Map[Party, LocalParticipantReference],
  )(implicit env: TestConsoleEnvironment): Contract = {
    import env.*
    def javaList(parties: Party*) = parties.map(_.toProtoPrimitive).asJava
    val proposal = JavaDecodeUtil
      .decodeAllCreated(UniversalContract.COMPANION)(
        hostOf(proposer).ledger_api.javaapi.commands.submit(
          Seq(proposer),
          new UniversalContract(
            javaList(),
            javaList(proposer),
            javaList(acceptor),
            javaList(acceptor),
          ).create.commands.asScala.toSeq,
          synchronizerId = Some(daId.logical),
        )
      )
      .loneElement
      .id
    hostOf(acceptor).ledger_api.state.acs
      .await_active_contract(acceptor, LfContractId.assertFromString(proposal.contractId))
    val accepted = hostOf(acceptor).ledger_api.javaapi.commands.submit(
      Seq(acceptor),
      proposal
        .exerciseReplace(
          javaList(),
          javaList(proposer, acceptor),
          javaList(),
          javaList(proposer),
          javaList(),
        )
        .commands
        .asScala
        .toSeq,
      synchronizerId = Some(daId.logical),
    )
    Contract(
      proposer,
      acceptor,
      JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(accepted).loneElement.id,
    )
  }

  /** `signerOf(party)` proposes, for each of `parties` without waiting in between, the mapping in
    * which the party's source is Active and P4 is in `state`.
    *
    * @return
    *   The effective time of each party's mapping, as for [[awaitAll]].
    */
  private def moveAll(
      parties: Seq[Party],
      signerOf: Party => LocalParticipantReference,
      state: HostingState,
  )(implicit env: TestConsoleEnvironment): Map[Party, CantonTimestamp] = {
    import env.*
    parties.foreach { party =>
      signerOf(party).topology.party_to_participant_mappings
        .propose(
          party.partyId,
          newParticipants = Seq(sources(party).id -> Submission, target.id -> Submission),
          threshold = PositiveInt.one,
          synchronize = None,
          store = daId,
          participantStates = hostingStates(party, state),
        )
        .discard
    }
    awaitAll(parties, state)
  }

  /** Waits until every participant sees, for each of `parties`, the mapping in which the party's
    * source is Active and P4 is in `state`, and has reached its effective time.
    *
    * @return
    *   The effective time of each party's mapping.
    */
  private def awaitAll(parties: Seq[Party], state: HostingState)(implicit
      env: TestConsoleEnvironment
  ): Map[Party, CantonTimestamp] = {
    import env.*
    val effectiveTimes = parties
      .map(party =>
        party -> awaitHostingStates(party, daId, participants.local, hostingStates(party, state))
      )
      .toMap
    participants.local.foreach(_.testing.await_synchronizer_time(daId, effectiveTimes.values.max))
    effectiveTimes
  }

  private def hostingStates(party: Party, targetState: HostingState) =
    Map(sources(party).id -> HostingState.Active, target.id -> targetState)

  /** The mapping in which the party's source is Active and P4 is in `state`, with the next serial,
    * signed by P4 alone.
    */
  private def signedByTarget(party: Party, state: HostingState)(implicit
      env: TestConsoleEnvironment
  ): GenericSignedTopologyTransaction = {
    import env.*
    val current = target.topology.party_to_participant_mappings
      .list(daId, filterParty = party.filterString)
      .loneElement
    val transaction = TopologyTransaction
      .create(
        TopologyChangeOp.Replace,
        PositiveInt.tryCreate(current.context.serial.unwrap + 1),
        PartyToParticipant
          .create(
            party.partyId,
            PositiveInt.one,
            hostingStates(party, state).toSeq.map { case (participantId, hostingState) =>
              HostingParticipant(participantId, Submission, hostingState)
            },
            None,
          )
          .value,
        daId.protocolVersion,
      )
      .value
    val signature = target.crypto.privateCrypto
      .sign(transaction.hash.hash, target.id.fingerprint, SigningKeyUsage.NamespaceOnly)
      .futureValueUS
      .value
    SignedTopologyTransaction.withSignature(
      transaction,
      signature,
      isProposal = false,
      daId.protocolVersion,
    )
  }

  /** Stands in for the remote ACS read: `source` exports `party`'s ACS as of `asOf` into a file. */
  private def exportAcs(
      source: LocalParticipantReference,
      party: Party,
      asOf: CantonTimestamp,
  )(implicit env: TestConsoleEnvironment): String = {
    import env.*
    // A request sequenced after asOf, so that the source's clean index passes asOf. Then the
    // highest offset with a record time at or before asOf covers the ACS as of asOf.
    source.health.ping(source.id)
    val offset = source.parties.find_highest_offset_by_timestamp(
      daId.logical,
      asOf.toInstant,
      force = true,
    )
    val file = Files.createTempFile("multi-party-replication-acs-snapshot", ".gz")
    file.toFile.deleteOnExit()
    source.repair.export_acs(
      Set(party.partyId),
      offset,
      file.toString,
      synchronizerId = Some(daId.logical),
    )
    file.toString
  }
}

object PartyFreezeMultiPartyReplicationIntegrationTest {

  /** A contract that both `proposer` and `acceptor` sign, and that `proposer` can delete alone. */
  private final case class Contract(
      proposer: Party,
      acceptor: Party,
      id: UniversalContract.ContractId,
  )

  private def contractIdsOf(party: Party, contracts: Seq[Contract]): Set[String] =
    contracts.collect {
      case Contract(proposer, acceptor, id) if proposer == party || acceptor == party =>
        id.contractId
    }.toSet
}
