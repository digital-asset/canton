// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.{
  CryptoKeyPair,
  CryptoPureApi,
  EncryptionPrivateKey,
  EncryptionPublicKey,
  Fingerprint,
  KeyPurpose,
  SecureRandomness,
}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.ActionDescription.CreateActionDescription
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.jdk.CollectionConverters.*

trait LedgerTransparencyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with SecurityTestHelpers
    with HasCycleUtils {

  private def assertMatchesNormalizedRegex(message: String, regex: String) =
    assert(
      regex.r.findFirstIn(message.replaceAll("\\s+", " ")).nonEmpty,
      s"Expected message '$message' to match regex '$regex'",
    )

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  private var maliciousP2: MaliciousParticipantNode = _

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val staticSynchronizerParameters = StaticSynchronizerParameters.fromConfig(
          SynchronizerParametersConfig(),
          CryptoConfig(provider = CryptoProvider.Jce),
          testedProtocolVersion,
        )

        val network = NetworkTopologyDescription.createWithStaticSynchronizerParameters(
          daName,
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.two,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          staticSynchronizerParameters =
            staticSynchronizerParameters.copy(enableTransparencyChecks = true),
        )

        new NetworkBootstrapper(network)
      }
      .withSetup { implicit env =>
        import env.*
        pureCryptoRef.set(participant1.crypto.pureCrypto)
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          testedTransactionLimits,
          timeouts,
          loggerFactory,
        )
      }

  "Synchronizer with transparency checks enabled" should {
    "views are deterministically encrypted" onlyRunWithOrGreaterThan ProtocolVersion.transparencyAndCiphertextId in {
      implicit env =>
        import env.*

        // download public and private key for participants
        val p1EncId =
          participant1.keys.public.list(filterPurpose = Set(KeyPurpose.Encryption)).loneElement.id
        val p1EncKeyPair = CryptoKeyPair
          .fromTrustedByteString(participant1.keys.secret.download(p1EncId))
          .valueOrFail("parsing of participant1 encryption key pair")

        val p2EncId =
          participant2.keys.public.list(filterPurpose = Set(KeyPurpose.Encryption)).loneElement.id
        val p2EncKeyPair = CryptoKeyPair
          .fromTrustedByteString(participant2.keys.secret.download(p2EncId))
          .valueOrFail("parsing of participant2 encryption key pair")

        val createIouCmd = new Iou(
          participant1.adminParty.toProtoPrimitive,
          participant2.adminParty.toProtoPrimitive,
          new Amount(100.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.asScala.toSeq

        // extract encrypted view messages
        checkingEncryptedViewMessages(pureCrypto, participant1, sequencer1) {
          participant1.ledger_api.javaapi.commands
            .submit(Seq(participant1.adminParty), createIouCmd)
        } { encryptedViewMessages =>
          // we only expect one encrypted view
          val encryptedViewMessage = encryptedViewMessages.loneElement
          val viewEncryptionKeyRandomness = encryptedViewMessage.viewEncryptionKeyRandomness

          // the session encryption key for the message must be encrypted for both `participant1`
          // and `participant2`
          viewEncryptionKeyRandomness
            .map(_.encryptedFor)
            .forgetNE should contain allOf (p1EncId, p2EncId)

          val p1Ciphertext = viewEncryptionKeyRandomness
            .find(_.encryptedFor == p1EncId)
            .valueOrFail("no encrypted randomness for participant1")

          val randomnessP1 = pureCrypto
            .decryptWith(
              p1Ciphertext,
              p1EncKeyPair.privateKey.asInstanceOf[EncryptionPrivateKey],
            )(
              SecureRandomness.fromByteString(
                encryptedViewMessage.viewEncryptionScheme.keySizeInBytes
              )(_)
            )
            .valueOrFail("decrypt encrypted randomness for participant1")

          val p2Ciphertext = encryptedViewMessage.viewEncryptionKeyRandomness
            .find(_.encryptedFor == p2EncId)
            .valueOrFail("no encrypted randomness for participant2")

          val randomnessP2 = pureCrypto
            .decryptWith(
              p2Ciphertext,
              p2EncKeyPair.privateKey.asInstanceOf[EncryptionPrivateKey],
            )(
              SecureRandomness.fromByteString(
                encryptedViewMessage.viewEncryptionScheme.keySizeInBytes
              )(_)
            )
            .valueOrFail("decrypt encrypted randomness for participant2")

          // sanity check: both ciphertexts decrypt to the same randomness,
          // even though each was encrypted with a different key
          randomnessP1 shouldBe randomnessP2

          // verify determinism: re-encrypting the same randomness with each participant’s public key
          // must reproduce the original ciphertexts
          val encryptedRandomnessP1 = pureCrypto
            .encryptDeterministicWith(
              randomnessP1,
              p1EncKeyPair.publicKey.asInstanceOf[EncryptionPublicKey],
            )
            .valueOrFail("re-encrypt randomness for participant1")

          val encryptedRandomnessP2 = pureCrypto
            .encryptDeterministicWith(
              randomnessP2,
              p2EncKeyPair.publicKey.asInstanceOf[EncryptionPublicKey],
            )
            .valueOrFail("re-encrypt randomness for participant2")

          p1Ciphertext shouldBe encryptedRandomnessP1
          p2Ciphertext shouldBe encryptedRandomnessP2

        }
    }

    // TODO(#36417): We still crash if we cannot decrypt our own randomness
    "does not crash and fails consistently if the listed randomnesses fail the transparency checks" onlyRunWithOrGreaterThan
      ProtocolVersion.transparencyAndCiphertextId in { implicit env =>
        import env.*

        def createIou(amount: Int) =
          new Iou(
            participant1.adminParty.toProtoPrimitive,
            participant2.adminParty.toProtoPrimitive,
            new Amount(amount.toBigDecimal, "USD"),
            List.empty.asJava,
          ).create.commands.asScala.toSeq

        // Submit two creates in one transaction so we get two root views.
        val createIouCmds = createIou(100) ++ createIou(200)

        val commandsWithMetadata = CommandsWithMetadata(
          actAs = Seq(participant2.adminParty),
          commands = createIouCmds.map(c =>
            com.daml.ledger.api.v2.commands.Command.fromJavaProto(c.toProtoCommand)
          ),
        )

        val bogusRecipientKey = Fingerprint.tryFromString("Nudelsuppe")
        val bogusCiphertext = ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF")
        val ciphertextMismatchReason =
          "Re-encrypted ciphertext does not match the view ciphertext"
        val missingKeyReason =
          s"No encryption key found for recipient key ID $bogusRecipientKey"
        val malformedPayloadRegex =
          raw"Rejected transaction due to malformed payload within views List\(TransparencyCheckError\(.+(?:$ciphertextMismatchReason|$missingKeyReason).+for message with hash\(es\).+"
        val decryptionErrorRegex =
          raw"Decryption error: TransparencyCheckError\(.+(?:$ciphertextMismatchReason|$missingKeyReason).+for message with hash\(es\).+"
        val rootViewIndex = new AtomicInteger(0)

        val (_, events) =
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            trackingLedgerEvents(participants.all, Seq.empty) {
              maliciousP2
                .submitCommand(
                  commandsWithMetadata,
                  envelopeInterceptor = envelope => {
                    envelope.protocolMessage match {
                      case v @ EncryptedViewMessage(_, _, rand, _, _, _) =>
                        val mutatedRandomness = rootViewIndex.getAndIncrement() match {
                          case 0 =>
                            rand ++ Seq(rand.head1.copy(ciphertext = bogusCiphertext))
                          case 1 =>
                            rand ++ Seq(
                              rand.head1.copy[SecureRandomness](encryptedFor = bogusRecipientKey)
                            )
                          case _ => rand
                        }
                        envelope.copy(
                          protocolMessage = v.copy(viewEncryptionKeyRandomness = mutatedRandomness)
                        )
                      case _ => envelope
                    }
                  },
                )
                .futureValueUS
            },
            LogEntry.assertLogSeq(
              mustContainWithClue = Seq(
                (
                  le => {
                    le.loggerName should (
                      include("participant=participant1") or include("participant=participant2")
                    )
                    le.shouldBeCantonErrorCode(MalformedRejects.Payloads)
                    assertMatchesNormalizedRegex(
                      le.warningMessage,
                      malformedPayloadRegex,
                    )
                  },
                  "malformed entry",
                ),
                (
                  le => {
                    le.loggerName should (
                      include("participant=participant1") or include("participant=participant2")
                    )
                    assertMatchesNormalizedRegex(
                      le.warningMessage,
                      decryptionErrorRegex,
                    )
                  },
                  "decryption error",
                ),
              )
            ),
          )
        events.assertNoTransactions()
      }

    // Tree shape used below:
    //
    //                 Transaction
    //                /     |     \
    //               /      |      \
    //           View(#0) View(#2) View(#3)
    //              |
    //           View(#1)
    //
    // View details:
    //   - #0 root view: UniversalContract.exerciseReplace(...)
    //   - #1 nested subview: the child view whose transparency-check data we corrupt below
    //   - #2 root view: Iou.create(300)
    //   - #3 root view: Iou.create(400)
    //
    // Invalidating the nested subview should invalidate #0. We also invalidate one of the
    // other IOU root views (#2). The transaction as a whole is rejected.
    "propagates transparency-check failures from nested subviews" onlyRunWithOrGreaterThan
      ProtocolVersion.transparencyAndCiphertextId in { implicit env =>
        import env.*

        def createIou(amount: Int) =
          new Iou(
            participant1.adminParty.toProtoPrimitive,
            participant2.adminParty.toProtoPrimitive,
            new Amount(amount.toBigDecimal, "USD"),
            List.empty.asJava,
          ).create.commands.asScala.toSeq

        // only participant2 will receive the view for the universal contract
        val createUniversalCmd = new UniversalContract(
          List(participant2.adminParty.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(participant2.adminParty.toProtoPrimitive).asJava,
        ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq

        val universalCid = JavaDecodeUtil
          .decodeAllCreated(UniversalContract.COMPANION)(
            participant2.ledger_api.javaapi.commands.submit(
              Seq(participant2.adminParty),
              createUniversalCmd,
            )
          )
          .loneElement
          .id

        val replaceCmd = universalCid
          .exerciseReplace(
            List(participant2.adminParty.toProtoPrimitive).asJava,
            List.empty.asJava,
            List(participant1.adminParty.toProtoPrimitive).asJava,
            List(participant1.adminParty.toProtoPrimitive).asJava,
            List.empty.asJava,
          )
          .commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq

        val bogusCiphertext = ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF")
        val ciphertextMismatchReason =
          "Re-encrypted ciphertext does not match the view ciphertext"
        val malformedPayloadRegex =
          raw"Rejected transaction due to malformed payload within views List\(TransparencyCheckError\(.+$ciphertextMismatchReason.+for message with hash\(es\).+"
        val decryptionErrorRegex =
          raw"Decryption error: TransparencyCheckError\(.+$ciphertextMismatchReason.+for message with hash\(es\).+"

        // Add extra root views to make the transaction larger while still containing subviews.
        val complexTxCommands = replaceCmd ++ createIou(300) ++ createIou(400)

        val commandsWithMetadata = CommandsWithMetadata(
          actAs = Seq(participant2.adminParty),
          commands = complexTxCommands.map(c =>
            com.daml.ledger.api.v2.commands.Command.fromJavaProto(c.toProtoCommand)
          ),
        )

        var childViewHashRef = Option.empty[ViewHash]
        var iouRootViewHashRef = Option.empty[ViewHash]

        val (_, events) =
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            trackingLedgerEvents(participants.all, Seq.empty) {
              maliciousP2
                .submitCommand(
                  commandsWithMetadata,
                  transactionTreeInterceptor = tree => {
                    childViewHashRef = tree.allTransactionViewTrees.collectFirst {
                      case tvt if !tvt.isTopLevel => tvt.viewHash
                    }

                    // pick one of the IOUs
                    iouRootViewHashRef = tree.allTransactionViewTrees.collectFirst {
                      case tvt
                          if tvt.isTopLevel &&
                            tvt.viewParticipantData.actionDescription
                              .isInstanceOf[CreateActionDescription] &&
                            tvt.viewParticipantData.createdCore.headOption
                              .exists(_.contract.inst.templateId.toString.contains(":Iou:Iou")) =>
                        tvt.viewHash
                    }
                    tree
                  },
                  envelopeInterceptor = envelope => {
                    envelope.protocolMessage match {
                      case v @ EncryptedViewMessage(_, _, rand, _, _, _)
                          if childViewHashRef.contains(v.viewHashes.head1) =>
                        envelope.copy(
                          protocolMessage = v.copy(viewEncryptionKeyRandomness =
                            rand ++ Seq(rand.head1.copy(ciphertext = bogusCiphertext))
                          )
                        )
                      case v @ EncryptedViewMessage(_, _, rand, _, _, _)
                          if iouRootViewHashRef.contains(v.viewHashes.head1) =>
                        envelope.copy(
                          protocolMessage = v.copy(viewEncryptionKeyRandomness =
                            rand ++ Seq(rand.head1.copy(ciphertext = bogusCiphertext))
                          )
                        )
                      case _ => envelope
                    }
                  },
                )
                .futureValueUS
            },
            LogEntry.assertLogSeq(
              mustContainWithClue = Seq(
                (
                  le => {
                    le.loggerName should (
                      include("participant=participant1") or include("participant=participant2")
                    )
                    le.shouldBeCantonErrorCode(MalformedRejects.Payloads)
                    assertMatchesNormalizedRegex(
                      le.warningMessage,
                      malformedPayloadRegex,
                    )
                  },
                  "malformed entry",
                ),
                (
                  le => {
                    le.loggerName should (
                      include("participant=participant1") or include("participant=participant2")
                    )
                    assertMatchesNormalizedRegex(
                      le.warningMessage,
                      decryptionErrorRegex,
                    )
                  },
                  "decryption error",
                ),
                (
                  le => {
                    le.loggerName should include("participant=participant2")
                    le.warningMessage should include(
                      "is invalid because a subview is invalid"
                    )
                  },
                  "invalid parent view",
                ),
              )
            ),
          )
        events.assertNoTransactions()
      }
  }
}

class LedgerTransparencyBftOrderingIntegrationTestDefault
    extends LedgerTransparencyIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
