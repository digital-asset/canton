// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

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
import com.digitalasset.canton.protocol.TopologyLimits
import com.digitalasset.canton.serialization.ProtoConverter
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
import com.digitalasset.canton.topology.{Namespace, PartyId, TopologyManagerError, UniqueIdentifier}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation, v1}
import com.digitalasset.nonempty.NonEmpty

/** This test shows that regular and external parties of the previous protocol version 36 can be
  * created under protocol version 37 or later.
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

      // Then apply a (valid) pv36/v30 PTP not using HostingState anymore
      // Hosting State "downgrade" valid with party key sharing participant1's namespace.
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
      prepareSignedTopoTx(ProtocolVersion.v36, txn3)

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
      // and missing P2's signature
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
        TopologyTransactionDeserializationContext(TopologyLimits.default),
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
      prepareSignedTopoTx(
        ProtocolVersion.v36,
        TopologyTransaction.tryCreate(
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
        ),
        Some(participant2),
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
    )
  }
}
