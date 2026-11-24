// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.tls.BaseServerTlsConfig
import com.digitalasset.canton.config.PemFile
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, PositiveInt}
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseBftSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.protocol.messages.EnvelopeContent
import com.digitalasset.canton.protocol.{v30 as protocolV30, v31 as protocolV31, v32 as protocolV32}
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpenerError.EnvelopeOpenerDeserializationError
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  Batch,
  ClosedUncompressedEnvelope,
  MessageId,
  Recipients,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.transaction.{
  GrpcConnection,
  LsuSequencerConnectionSuccessor,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.validation.ProtoUnvalidatedSeq
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.digitalasset.canton.version.{ProtocolVersion, VersionedMessage}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import monocle.macros.{GenLens, GenPrism}
import org.slf4j.event.Level

import java.time.Duration

/** Check that an invalid certificate (cannot be parsed) included in the sequencer connection
  * successor is handled gracefully.
  *
  *   - Set up sequencer1 with custom certificate.
  *   - Connect participant1 to sequencer1
  *   - Announce LSU
  *   - sequencer1 announces its successor with invalid certificate
  *   - participant1 can ping itself
  *   - sequencer1 announces it successor with correct certificate
  *   - LSU is completed
  */
final class LsuInvalidCertificateSequencerConnectionSuccessorIntegrationTest extends LsuBase {

  override protected def testName: String = "Lsu-invalid-certificate-sequencer-connection-successor"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val certChainFile = PemFile(
    ExistingFile.tryCreate("community/app/src/test/resources/tls/public-api.crt")
  )
  private val privateKeyFile = PemFile(
    ExistingFile.tryCreate("community/app/src/test/resources/tls/public-api.pem")
  )

  private var fixture: Fixture = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*
        defaultEnvironmentSetup(connectParticipants = false, hasTrafficControl = false)

        participant1.synchronizers.connect(
          synchronizerAlias = daName,
          connection = s"https://localhost:${sequencer1.config.publicApi.port.unwrap}",
          // path to certificate chain file (.pem) of sequencer
          certificatesPath = certChainFile.pemFile.unwrap.getPath,
        )

        fixture = fixtureWithDefaults()
      }
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.tls).replace(
            Some(
              BaseServerTlsConfig(
                certChainFile = certChainFile,
                privateKeyFile = privateKeyFile,
              )
            )
          )
        )
      )

  private def buildLsuSequencerConnectionSuccessor(implicit
      env: TestConsoleEnvironment
  ): LsuSequencerConnectionSuccessor = {
    import env.*

    val endpoints = sequencer2.sequencerConnection.endpoints.map(_.toURI(useTls = true))

    Endpoint
      .fromUris(endpoints.toSeq)
      .map { case (validatedEndpoints, useTls) =>
        LsuSequencerConnectionSuccessor(
          sequencer1.id,
          fixture.newPsid.opaque,
          GrpcConnection(
            validatedEndpoints,
            useTls,
            sequencer1.sequencerConnection.customTrustCertificates,
          ),
        )
      }
      .value
  }

  // Transformation that changes the customTrustCertificates to be invalid on a v31.TopologyTransaction
  private lazy val setInvalidCert =
    // Select the mapping
    GenLens[protocolV31.TopologyTransaction](_.mapping).some
      .andThen(GenLens[protocolV31.TopologyMapping](_.mapping))
      // Select SequencerConnectionSuccessor
      .andThen(
        GenPrism[
          protocolV31.TopologyMapping.Mapping,
          protocolV31.TopologyMapping.Mapping.SequencerConnectionSuccessor,
        ]
      )
      // Select the customTrustCertificates
      .andThen(
        GenLens[protocolV31.TopologyMapping.Mapping.SequencerConnectionSuccessor](
          _.value.connection
        )
      )
      .some
      .andThen(
        GenLens[protocolV30.LsuSequencerConnectionSuccessor.SequencerConnection](
          _.customTrustCertificates
        )
      )
      .replace(Some(ByteString.copyFromUtf8("not a PEM certificate")))

  private def buildSubmissionRequest(valid: Boolean)(implicit env: TestConsoleEnvironment) = {
    val pv = fixture.currentPsid.protocolVersion

    val tx = TopologyTransaction
      .create(
        TopologyChangeOp.Replace,
        PositiveInt.one,
        buildLsuSequencerConnectionSuccessor,
        pv,
      )
      .value

    val topologyTransactionVersion = 31
    if (TopologyTransaction.protoVersionFor(pv).value.v != topologyTransactionVersion) {
      // If this fails, it means that the test should be adapted to handle other versions
      fail(
        s"TopologyTransaction serialization for $pv should be a v$topologyTransactionVersion"
      )
    }
    val txProto = tx.toProtoV31.value

    val updatedTxProto = if (valid) txProto else setInvalidCert(txProto)

    val txBs =
      VersionedMessage(updatedTxProto.toByteString, topologyTransactionVersion).toByteString
    val txHash = Hash.digest(
      HashPurpose.TopologyTransactionSignature,
      txBs,
      HashAlgorithm.Sha256,
    )

    val txSignature = env.sequencer1.crypto.privateCrypto
      .sign(txHash, env.sequencer1.id.fingerprint, SigningKeyUsage.NamespaceOnly)
      .futureValueUS
      .value

    val signedTopologyTransactionVersion = 30
    if (SignedTopologyTransaction.protoVersionFor(pv).value.v != signedTopologyTransactionVersion) {
      // If this fails, it means that the test should be adapted to other versions
      fail(
        s"SignedTopologyTransaction serialization for $pv should be a v$signedTopologyTransactionVersion"
      )
    }

    val signedTxProto = protocolV30.SignedTopologyTransaction(
      transaction = txBs,
      signatures = ProtoUnvalidatedSeq(Seq(txSignature.toProtoV30)),
      proposal = false,
      multiTransactionSignatures = ProtoUnvalidatedSeq(Nil),
    )
    val signedTxBs =
      VersionedMessage(signedTxProto.toByteString, signedTopologyTransactionVersion).toByteString

    val envelopeContentVersion = 32
    if (EnvelopeContent.protoVersionFor(pv).value.v != envelopeContentVersion) {
      // If this fails, it means that the test should be adapted to handle other versions
      fail(s"SignedTopologyTransaction serialization for $pv should be a v$envelopeContentVersion")
    }
    val envelopeContent = protocolV32.EnvelopeContent(
      protocolV32.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(
        protocolV30.TopologyTransactionsBroadcast(
          fixture.currentPsid.toProtoPrimitive,
          Some(
            protocolV30.SignedTopologyTransactions(
              ProtoUnvalidatedSeq(Seq(signedTxBs))
            )
          ),
        )
      )
    )

    val messageBytes = UntypedVersionedMessage(
      UntypedVersionedMessage.Wrapper.Data(envelopeContent.toByteString),
      envelopeContentVersion,
    ).toByteString

    val closedEnvelope = ClosedUncompressedEnvelope.create(
      messageBytes,
      Recipients.cc(AllMembersOfSynchronizer),
      Seq.empty,
      pv,
    )

    val rawBatch = Batch(List(closedEnvelope), pv)

    SubmissionRequest.tryCreate(
      sender = env.sequencer1.id,
      messageId = MessageId.randomMessageId(),
      batch = rawBatch,
      maxSequencingTime = CantonTimestamp.MaxValue,
      aggregationRule = None,
      submissionCost = None,
      protocolVersion = pv,
    )
  }

  private def sendLsuSequencerConnectionSuccessor(
      valid: Boolean
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val kind = if (valid) "valid" else "invalid"
    val tc = TraceContext.createNew(if (valid) "valid" else "invalid")

    logger.info(s"Submitting $kind LsuSequencerConnectionSuccessor with trace context $tc")(tc)

    val submissionRequest = buildSubmissionRequest(valid = valid)
    val signedSubmissionRequest = SequencerTestHelper
      .signSubmissionRequest(submissionRequest, sequencer1, fixture.currentPsid)
      .futureValueUS
      .value

    sequencer1.underlying.value.sequencer.sequencer
      .sendAsyncSigned(signedSubmissionRequest)(tc)
      .futureValueUS
      .value
  }

  "Logical synchronizer upgrade" should {
    /*
      This test tweaks the proto message of the topology transaction.
      To reduce complexity, we only support v31 of topology, and thus pv=37+.
     */
    "work end-to-end" onlyRunWithOrGreaterThan ProtocolVersion.v37 in { implicit env =>
      import env.*

      performSynchronizerNodesLsu(fixture, announceSequencerSuccessors = false)

      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        {
          sendLsuSequencerConnectionSuccessor(valid = false)

          participant1.health.ping(participant1)

          sendLsuSequencerConnectionSuccessor(valid = true)

          // Ensure P1 sees the transaction
          eventually() {
            participant1.topology.lsu.sequencer_successors.list(daId).loneElement
          }

          environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))

          eventually() {
            environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
            participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
            participants.all.forall(
              _.synchronizers.is_connected(fixture.currentPsid)
            ) shouldBe false
          }
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(EnvelopeOpenerDeserializationError),
              "sequencer error",
            ),
            (_.shouldBeCantonError(MediatorError.MalformedMessage), "mediator error"),
            (_.shouldBeCantonError(SyncServiceError.SyncServiceAlarm), "participant error"),
          )
        ),
      )
    }
  }
}
