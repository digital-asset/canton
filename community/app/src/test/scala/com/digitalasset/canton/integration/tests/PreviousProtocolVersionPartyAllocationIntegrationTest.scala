// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands.Write.GenerateTransactions
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, TopologyAdminCommands}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.protocol.SynchronizerLimits
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.admin.grpc.{BaseQuery, BaseWriteRequest}
import com.digitalasset.canton.topology.admin.v30.TopologyManagerReadServiceGrpc.TopologyManagerReadServiceStub
import com.digitalasset.canton.topology.admin.v30.{
  ListPartyToParticipantRequest,
  ListPartyToParticipantResponse,
}
import com.digitalasset.canton.topology.store.TimeQuery.HeadState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
  TopologyTransactionDeserializationContext,
}
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  Namespace,
  ParticipantId,
  PartyId,
  TopologyManagerError,
  UniqueIdentifier,
}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionValidation,
  ReleaseVersion,
  v1,
}
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.Future

/** This test shows that regular and external parties of the previous protocol version 36 can be
  * created under protocol version 37 or later, and that clients before 3.7 get the `v30` item of
  * `ListPartyToParticipant` where `v30` can express the mapping.
  */
final class PreviousProtocolVersionPartyAllocationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, daName)
      participants.all.dars.upload(CantonExamplesPath)
    }

  s"able to allocate n-1 (pv36) party on $testedProtocolVersion" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val bank = participant2.parties.enable("Bank")

      val alice36 =
        clue("Allocating internal with pv=36")(allocateParty(ProtocolVersion.v36, "Alice36"))

      IouSyntax.createIou(participant2)(bank, alice36)

      val aliceLatest =
        clue(s"Allocating internal with $testedProtocolVersion")(
          allocateParty(testedProtocolVersion, "AliceNew")
        )

      IouSyntax.createIou(participant2)(bank, aliceLatest)

  }

  s"able to allocate n-1 (pv36) external party on $testedProtocolVersion" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      // generate the party's namespace
      val partyKey = env.tryGlobalCrypto
        .generateSigningKey(usage =
          NonEmpty.mk(Set, SigningKeyUsage.Namespace, SigningKeyUsage.Protocol)
        )
        .futureValueUS
        .value

      val externalPartyId = PartyId.tryCreate("Alice36External", Namespace(partyKey.fingerprint))
      // generate the self-signed external party PTP on PV36, ie V30
      val txV30 = TopologyTransaction.tryCreate(
        Replace,
        PositiveInt.one,
        PartyToParticipant.tryCreate(
          externalPartyId,
          PositiveInt.one,
          Seq(HostingParticipant(participant1.id, Confirmation)),
          Some(SigningKeysWithThreshold(NonEmpty(Set, partyKey), PositiveInt.one)),
        ),
        ProtocolVersion.v36,
      )

      // the external party signs the transaction
      val signedV30 = SignedTopologyTransaction
        .signAndCreate(
          txV30,
          NonEmpty(Set, partyKey.fingerprint),
          isProposal = true,
          env.tryGlobalCrypto.privateCrypto,
          ProtocolVersion.v37,
          multiHash = Some(NonEmpty(Set, txV30.hash) -> env.tryGlobalCrypto.pureCrypto),
        )
        .futureValueUS
        .value

      // the hosting participant signs the transaction
      val fullySignedV30 = participant1.topology.transactions.sign(Seq(signedV30), daId).loneElement

      // verify the RPV (transitively the proto version)
      fullySignedV30.transaction.representativeProtocolVersion shouldBe TopologyTransaction
        .protocolVersionRepresentativeFor(ProtocolVersion.v36)

      // load the transaction on the participant
      participant1.topology.transactions.load(Seq(fullySignedV30), daId)

      val tx = findTx("Alice36External")

      logger.info(
        s"For partyId = ${tx.transaction.mapping.partyId}, found PV ${tx.transaction.transaction.representativeProtocolVersion}"
      )

      val bank = participant2.parties.enable("Bank2")
      IouSyntax.createIou(participant2)(bank, externalPartyId)

  }

  s"allocate a n-1 (pv36) party on $testedProtocolVersion upgrade to pv37+ then downgrade to pv36 on P1" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val carol36 =
        clue("Allocating internal with pv=36")(allocateParty(ProtocolVersion.v36, "Carol36"))

      // Next apply a pv37+/v31 PTP with the new HostingState support
      // valid with party key sharing participant1's namespace.
      val txn2 = TopologyTransaction.tryCreate(
        TopologyChangeOp.Replace,
        PositiveInt.two,
        PartyToParticipant.tryCreate(
          carol36,
          PositiveInt.one,
          Seq(
            HostingParticipant(
              participant1.id,
              ParticipantPermission.Submission,
              onboarding = false,
            ),
            HostingParticipant(
              participant2.id,
              ParticipantPermission.Observation,
              HostingParticipant.HostingState.Nominated,
            ),
          ),
        ),
        testedProtocolVersion,
      )
      prepareSignedTopoTx(testedProtocolVersion, txn2)

      val foundTx2 = findTx("Carol36")
      logger.info(
        s"For partyId = ${foundTx2.transaction.mapping.partyId} serial 2, found PV ${foundTx2.transaction.transaction.representativeProtocolVersion}"
      )
      foundTx2.transaction.transaction.representativeProtocolVersion.representative shouldBe TopologyTransaction
        .protocolVersionRepresentativeFor(testedProtocolVersion)
        .representative

      // Then apply a pv36/v30 PTP, which cannot express the Nominated state of participant2.
      // participant1 rejects it unless forced; the synchronizer accepts it, as the party key shares
      // participant1's namespace.
      val txn3 = TopologyTransaction.tryCreate(
        TopologyChangeOp.Replace,
        PositiveInt.three,
        PartyToParticipant.tryCreate(
          carol36,
          PositiveInt.one,
          Seq(
            HostingParticipant(
              participant1.id,
              ParticipantPermission.Submission,
              onboarding = false,
            )
          ),
        ),
        ProtocolVersion.v36,
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        prepareSignedTopoTx(ProtocolVersion.v36, txn3),
        _.shouldBeCommandFailure(
          TopologyManagerError.HostingStatesNotExpressible,
          s"${participant2.id} -> Nominated",
        ),
      )
      prepareSignedTopoTx(
        ProtocolVersion.v36,
        txn3,
        forceFlags = ForceFlags(ForceFlag.AllowV30PartyToParticipantAfterHostingStates),
      )

      val foundTx3 = findTx("Carol36")
      logger.info(
        s"For partyId = ${foundTx3.transaction.mapping.partyId} serial 3, found PV ${foundTx3.transaction.transaction.representativeProtocolVersion}"
      )
      foundTx3.transaction.transaction.representativeProtocolVersion.representative shouldBe ProtocolVersion.v36
  }

  s"allocate a n-1 (pv36) party on $testedProtocolVersion upgrade to pv37+ then downgrade to pv36 on P2" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val david36 =
        clue("Allocating internal with pv=36")(allocateParty(ProtocolVersion.v36, "David36"))

      // Next apply a pv37+/v31 PTP with the new HostingState support
      // valid with party key sharing participant1's namespace.
      prepareSignedTopoTx(
        testedProtocolVersion,
        TopologyTransaction.tryCreate(
          TopologyChangeOp.Replace,
          PositiveInt.two,
          PartyToParticipant.tryCreate(
            david36,
            PositiveInt.one,
            Seq(
              HostingParticipant(
                participant1.id,
                ParticipantPermission.Submission,
                onboarding = false,
              ),
              HostingParticipant(
                participant2.id,
                ParticipantPermission.Submission,
                HostingParticipant.HostingState.Nominated,
              ),
            ),
          ),
          testedProtocolVersion,
        ),
      )

      val foundTx2 = findTx("David36")
      logger.info(
        s"For partyId = ${foundTx2.transaction.mapping.partyId} serial 2, found PV ${foundTx2.transaction.transaction.representativeProtocolVersion}"
      )
      foundTx2.transaction.transaction.representativeProtocolVersion.representative shouldBe TopologyTransaction
        .protocolVersionRepresentativeFor(testedProtocolVersion)
        .representative

      // Then apply a pv36/v30 PTP not using HostingState anymore
      // and missing P2's signature. The submitting node rejects a v30 PTP while participant2 is
      // Nominated unless forced; the force flag lets the case reach the authorization check.
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        prepareSignedTopoTx(
          ProtocolVersion.v36,
          TopologyTransaction.tryCreate(
            TopologyChangeOp.Replace,
            PositiveInt.three,
            PartyToParticipant.tryCreate(
              david36,
              PositiveInt.one,
              Seq(
                HostingParticipant(
                  participant2.id,
                  ParticipantPermission.Submission,
                  onboarding = false,
                )
              ),
            ),
            ProtocolVersion.v36,
          ),
          forceFlags = ForceFlags(ForceFlag.AllowV30PartyToParticipantAfterHostingStates),
        ),
        _.shouldBeCantonErrorCode(TopologyManagerError.UnauthorizedTransaction),
        _.errorMessage should include(
          "INVALID_ARGUMENT/An error occurred. Please contact the operator and inquire about the request"
        ),
      )

      // Should find the serial 2 transaction on PV37+ as serial 3 was not fully authorized.
      val foundTx2Again = findTx("David36")
      logger.info(
        s"For partyId = ${foundTx2Again.transaction.mapping.partyId}, found PV $foundTx2Again}"
      )
      foundTx2Again.transaction.serial shouldBe PositiveInt.two
      foundTx2Again.transaction.transaction.representativeProtocolVersion.representative shouldBe TopologyTransaction
        .protocolVersionRepresentativeFor(testedProtocolVersion)
        .representative
  }

  s"reject a v30 mapping while the party has hosting states that v30 cannot express on $testedProtocolVersion" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val dave = allocateParty(ProtocolVersion.v36, "Dave36")
      val participant1Active =
        HostingParticipant(participant1.id, ParticipantPermission.Submission, onboarding = false)
      def proposeWithConsole(serial: PositiveInt, participants: Seq[ParticipantId]): Unit = {
        val _ = participant1.topology.party_to_participant_mappings
          .propose(
            dave,
            participants.map(_ -> ParticipantPermission.Submission),
            serial = Some(serial),
            store = daId,
            participantStates =
              if (participants.contains(participant2.id))
                Map(participant2.id -> HostingParticipant.HostingState.Nominated)
              else Map.empty,
          )
      }
      proposeWithConsole(PositiveInt.two, Seq(participant1.id, participant2.id))

      // A client that only reads v30 cannot see the Nominated participant2, so its mapping leaves it
      // out. Without a server version, the admin client sends the mapping as v30, like clients
      // before 3.7. The node would create a v31 transaction that removes participant2.
      val v30Mapping = PartyToParticipant.tryCreate(dave, PositiveInt.one, Seq(participant1Active))
      def proposeV30(serial: PositiveInt, forceChanges: ForceFlags) =
        runCommand(
          TopologyAdminCommands.Write.Propose(
            BaseWriteRequest(None),
            v30Mapping,
            signedBy = Seq(participant1.id.fingerprint),
            store = daId,
            serial = Some(serial),
            mustFullyAuthorize = true,
            forceChanges = forceChanges,
            waitToBecomeEffective = None,
            serverVersion = None,
          )
        )
      val notExpressible = s"${participant2.id} -> Nominated"

      inside(
        runCommand(
          TopologyAdminCommands.Write.GenerateTransactions(
            BaseWriteRequest(None),
            Seq(GenerateTransactions.Proposal(v30Mapping, daId, serial = Some(PositiveInt.three))),
            serverVersion = None,
          )
        )
      ) { case Left(error) =>
        error should (include(TopologyManagerError.HostingStatesNotExpressible.id) and include(
          notExpressible
        ))
      }
      inside(proposeV30(PositiveInt.three, ForceFlags.none)) { case Left(error) =>
        error should (include(TopologyManagerError.HostingStatesNotExpressible.id) and include(
          notExpressible
        ))
      }

      // The current console client sends a v31 mapping, so it can remove participant2
      proposeWithConsole(PositiveInt.three, Seq(participant1.id))
      proposeWithConsole(PositiveInt.tryCreate(4), Seq(participant1.id, participant2.id))

      proposeV30(
        PositiveInt.tryCreate(5),
        ForceFlags(ForceFlag.AllowV30PartyToParticipantAfterHostingStates),
      ) shouldBe a[Right[?, ?]]
      eventually() {
        participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = dave.filterString)
          .loneElement
          .item
          .participants
          .map(_.participantId) shouldBe Seq(participant1.id)
      }
  }

  s"answer clients before 3.7 with the v30 item where v30 can express the mapping on $testedProtocolVersion" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      // Both parties are hosted by v31 transactions
      val erin = participant1.parties.enable("Erin")
      val frank = participant1.parties.enable("Frank")
      val _ = participant1.topology.party_to_participant_mappings.propose(
        frank,
        Seq(participant1.id, participant2.id).map(_ -> ParticipantPermission.Submission),
        serial = Some(PositiveInt.two),
        store = daId,
        participantStates = Map(participant2.id -> HostingParticipant.HostingState.Nominated),
      )

      def items(party: PartyId, clientVersion: Option[ReleaseVersion]) =
        runCommand(
          PreviousProtocolVersionPartyAllocationIntegrationTest.ListPartyToParticipantAsIs(
            ListPartyToParticipantRequest(
              baseQuery = Some(
                BaseQuery(
                  store = daId,
                  proposals = false,
                  timeQuery = HeadState,
                  ops = None,
                  filterSigningKey = "",
                  protocolVersion = None,
                  // Decides between the v30 and the v31 item
                  clientVersion = clientVersion,
                ).toProtoV1
              ),
              filterParty = party.filterString,
              filterParticipant = "",
            )
          )
        ).value

      inside(items(erin, clientVersion = None)) { case Seq(item) => item.isV30 shouldBe true }
      inside(items(erin, Some(ReleaseVersion.tryCreate("3.6.4")))) { case Seq(item) =>
        item.isV30 shouldBe true
      }
      inside(items(erin, Some(ReleaseVersion.current))) { case Seq(item) =>
        item.isV31 shouldBe true
      }
      // v30 cannot express the Nominated entry
      eventually() {
        inside(items(frank, clientVersion = None)) { case Seq(item) =>
          item.v31.value.participants.size shouldBe 2
        }
      }
  }

  "a v31 PTP can be deserialized as a v30" in { implicit env =>
    val tx = createPtpTx(ProtocolVersion.dev, "test")
    tx.protoVersion.v shouldBe 31

    val bs = tx.toByteString.value

    val untypedVersionedMessage =
      ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bs).value
    val innerBs = untypedVersionedMessage.wrapper.data.value

    val v30Mapping = ProtoConverter
      .protoParser(com.digitalasset.canton.protocol.v30.TopologyTransaction.parseFrom)(innerBs)
      .value

    val deserializedTx = TopologyTransaction
      .fromProtoV30(
        ProtocolVersionValidation.PV(ProtocolVersion.v36),
        TopologyTransactionDeserializationContext(SynchronizerLimits.default),
        v30Mapping,
      )(bs)
      .value

    deserializedTx.protoVersion.v shouldBe 30

    tx shouldBe deserializedTx
  }

  s"party with shared target participant namespace may unintentionally advance hosting state on v30 from $testedProtocolVersion" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val ellie = allocateParty(testedProtocolVersion, "Ellie", Some(participant2))

      // Next apply a pv37+/v31 PTP with the new HostingState support
      // valid with party key sharing participant1's namespace.
      prepareSignedTopoTx(
        testedProtocolVersion,
        TopologyTransaction.tryCreate(
          TopologyChangeOp.Replace,
          PositiveInt.two,
          PartyToParticipant.tryCreate(
            ellie,
            PositiveInt.one,
            Seq(
              HostingParticipant(
                participant1.id,
                ParticipantPermission.Submission,
              ),
              HostingParticipant(
                participant2.id,
                ParticipantPermission.Observation,
                HostingParticipant.HostingState.Nominated,
              ),
            ),
          ),
          testedProtocolVersion,
        ),
        Some(participant2),
      )

      val foundTx2 = findTx("Ellie")
      logger.info(
        s"For partyId = ${foundTx2.transaction.mapping.partyId} serial ${foundTx2.serial}, found PV ${foundTx2.transaction.transaction.representativeProtocolVersion}"
      )
      foundTx2.serial shouldBe PositiveInt.two

      // Testing likely unintentional behavior unless/until we choose to extend PTP validation rules:
      // When only the party signs, but because the party namespace is based on P2's namespace, participant 2 is not
      // required to sign anymore as the party signature suffices.
      // This is somewhat unexpected from the user as they would have to somehow choose to use v30 in spite of having
      // "upgraded" to hosting states and v31.
      // The submitting node therefore rejects such a v30 PTP while participant2 is Nominated, unless forced.
      val advanceOnV30 = TopologyTransaction.tryCreate(
        TopologyChangeOp.Replace,
        PositiveInt.three,
        PartyToParticipant.tryCreate(
          ellie,
          PositiveInt.one,
          Seq(
            HostingParticipant(
              participant1.id,
              ParticipantPermission.Submission,
            ),
            HostingParticipant(
              participant2.id,
              ParticipantPermission.Observation,
              // Does not specify hosting state unavailable in v30: HostingParticipant.HostingState.Nominated,
            ),
          ),
        ),
        ProtocolVersion.v36,
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        prepareSignedTopoTx(ProtocolVersion.v36, advanceOnV30, Some(participant2)),
        _.shouldBeCommandFailure(
          TopologyManagerError.HostingStatesNotExpressible,
          s"${participant2.id} -> Nominated",
        ),
      )
      prepareSignedTopoTx(
        ProtocolVersion.v36,
        advanceOnV30,
        Some(participant2),
        ForceFlags(ForceFlag.AllowV30PartyToParticipantAfterHostingStates),
      )

      Seq(participant1, participant2).foreach { p =>
        val foundTx3 = findTx("Ellie", Some(p))
        logger.info(
          s"For partyId = ${foundTx3.transaction.mapping.partyId} serial ${foundTx3.serial} and participant ${p.name}, found PV ${foundTx3.transaction.transaction.representativeProtocolVersion}"
        )
        foundTx3.serial shouldBe PositiveInt.three
        foundTx3.transaction.transaction.representativeProtocolVersion.representative shouldBe TopologyTransaction
          .protocolVersionRepresentativeFor(ProtocolVersion.v36)
          .representative
      }
  }

  private def runCommand[Result](command: GrpcAdminCommand[?, ?, Result])(implicit
      env: TestConsoleEnvironment
  ): Either[String, Result] = {
    import env.*
    participant1.consoleEnvironment.grpcAdminCommandRunner
      .runCommandAsync(
        participant1.name,
        command,
        participant1.config.clientAdminApi,
        participant1.config.adminApi.adminTokenConfig.fixedAdminToken,
      )
      ._2
      .value
      .futureValue
  }

  private def findTx(
      partyName: String,
      alternateParticipantO: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ) =
    alternateParticipantO
      .getOrElse(env.participant1)
      .underlying
      .value
      .sync
      .syncPersistentStateManager
      .get(env.daId)
      .value
      .topologyStore
      .inspect(
        proposals = false,
        timeQuery = HeadState,
        asOfExclusiveO = None,
        op = Some(TopologyChangeOp.Replace),
        types = Seq(TopologyMapping.Code.PartyToParticipant),
        idFilter = None,
        namespaceFilter = None,
      )
      .futureValueUS
      .collectOfMapping[PartyToParticipant]
      .result
      .filter(_.mapping.partyId.toProtoPrimitive.startsWith(partyName))
      .loneElement

  private def allocateParty(
      protocolVersion: ProtocolVersion,
      partyName: String,
      additionalSignerO: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): PartyId = {
    val tx = createPtpTx(
      protocolVersion,
      partyName,
      // use the additional signer's namespace instead for non-standard PTP setup
      additionalSignerO.map(_.namespace),
    )
    val partyId = tx.mapping.partyId

    prepareSignedTopoTx(protocolVersion, tx, additionalSignerO)

    eventually() {
      env.nodes.all.foreach(
        _.topology.party_to_participant_mappings
          .list(env.daId, filterParty = partyId.toProtoPrimitive)
          .loneElement
      )
    }

    val foundTx = findTx(partyName)

    foundTx.transaction.transaction.representativeProtocolVersion.representative shouldBe TopologyTransaction
      .protocolVersionRepresentativeFor(protocolVersion)
      .representative

    logger.info(
      s"For partyId = $partyId, found PV ${foundTx.transaction.transaction.representativeProtocolVersion}"
    )

    partyId
  }

  private def createPtpTx(
      protocolVersion: ProtocolVersion,
      partyName: String,
      namespaceO: Option[Namespace] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): TopologyTransaction[TopologyChangeOp.Replace.type, PartyToParticipant] = {

    val partyId = PartyId(
      UniqueIdentifier.tryCreate(partyName, namespaceO.getOrElse(env.participant1.namespace))
    )

    val mapping = PartyToParticipant.tryCreate(
      partyId,
      PositiveInt.one,
      Seq(
        HostingParticipant(
          env.participant1.id,
          ParticipantPermission.Submission,
          onboarding = false,
        )
      ),
    )

    TopologyTransaction.tryCreate(
      TopologyChangeOp.Replace,
      PositiveInt.one,
      mapping,
      protocolVersion,
    )
  }

  private def prepareSignedTopoTx[Op <: TopologyChangeOp, M <: TopologyMapping](
      protocolVersion: ProtocolVersion,
      tx: TopologyTransaction[Op, M],
      additionalSignerO: Option[LocalParticipantReference] = None,
      forceFlags: ForceFlags = ForceFlags.none,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val sig = participant1.crypto.privateCrypto
      .sign(
        tx.hash.hash,
        participant1.id.fingerprint,
        SigningKeyUsage.NamespaceOnly,
      )
      .futureValueUS
      .value

    val signedTx = additionalSignerO.fold(
      SignedTopologyTransaction.withSignature(
        tx,
        sig,
        isProposal = false,
        protocolVersion,
      )
    ) { alternateSigner =>
      val additionalSignature = alternateSigner.crypto.privateCrypto
        .sign(
          tx.hash.hash,
          alternateSigner.id.fingerprint,
          SigningKeyUsage.NamespaceOnly,
        )
        .futureValueUS
        .value
      SignedTopologyTransaction.withSignatures(
        tx,
        NonEmpty.mk(Seq, sig, additionalSignature),
        isProposal = false,
        protocolVersion,
      )
    }

    participant1.topology.transactions.load(
      Seq(signedTx),
      daId,
      forceFlags,
    )
  }
}

object PreviousProtocolVersionPartyAllocationIntegrationTest {

  /** Sends the request as is, e.g. without a client version, like clients before 3.7. */
  private final case class ListPartyToParticipantAsIs(request: ListPartyToParticipantRequest)
      extends TopologyAdminCommands.Read.BaseCommand[
        ListPartyToParticipantRequest,
        ListPartyToParticipantResponse,
        Seq[ListPartyToParticipantResponse.Result.Item],
      ] {
    override protected def createRequest(): Either[String, ListPartyToParticipantRequest] =
      Right(request)
    override protected def submitRequest(
        service: TopologyManagerReadServiceStub,
        request: ListPartyToParticipantRequest,
    ): Future[ListPartyToParticipantResponse] = service.listPartyToParticipant(request)
    override protected def handleResponse(
        response: ListPartyToParticipantResponse
    ): Either[String, Seq[ListPartyToParticipantResponse.Result.Item]] =
      ProtoValidation
        .validateLength(
          response.results,
          "results",
          ProtocolVersionValidation.AlwaysValidation,
          ProtoValidation.MaxCollectionSize,
        )
        .map(_.map(_.item))
        .left
        .map(_.message)
  }
}
