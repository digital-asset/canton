// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.{ContractInstance, CreatedContract}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.google.protobuf.ByteString
import monocle.Traversal

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

sealed abstract class TransactionVersionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private var maliciousP1: MaliciousParticipantNode = _
  private var alice: PartyId = _
  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      pureCryptoRef.set(sequencer1.crypto.pureCrypto)

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )

      alice = participant1.parties.enable("alice")

      participant1.dars.upload(CantonTestsPath)
    }

  "An unsupported transaction version" should {

    "Fail deserialization but not prevent further processing" in { implicit env =>
      import env.*

      val cmd =
        new UniversalContract(
          Seq(alice.toProtoPrimitive).asJava,
          Seq.empty.asJava,
          Seq.empty.asJava,
          Seq.empty.asJava,
        ).create.commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .map(c => Command.fromJavaProto(c.toProtoCommand))
          .toSeq

      val command = CommandsWithMetadata(
        commands = cmd,
        actAs = Seq(alice),
      )

      val changeTxVersion: GenTransactionTree => GenTransactionTree =
        GenTransactionTree.Optics.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .andThen(Traversal.fromTraverse[Seq, CreatedContract])
          .andThen(CreatedContract.Optics.contractUnsafe)
          .modify({ c =>
            // The first char of the serialization version is stored in the stored in the
            // second byte of the serialization. To verify this look at the `Versioned`
            // message in `transaction.proto` and `SerializationVersion.toProtoValue`.
            val serialization: Array[Byte] = c.serialization.toByteArray
            val svCharPos = 2
            // Verify that the existing version starts with a 2 (e.g. 2.1)
            serialization(svCharPos) shouldBe '2'.toByte
            // Change the version to 9 (so `2.1` becomes `9.1`)
            serialization.update(svCharPos, '9'.toByte)
            ContractInstance
              .createWithSerialization(c.inst, c.metadata, ByteString.copyFrom(serialization))
          })

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val (_, events) = trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1
              .submitCommand(
                command = command,
                transactionTreeInterceptor = changeTxVersion,
              )
              .futureValueUS
          }
          events.assertNoTransactions()
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include regex raw"(?s).*FailedToDeserialize.*Unsupported serialization version '9.*",
              "TransactionProcessor warning",
            ),
            (
              _.warningMessage should include regex raw"(?s)LOCAL_VERDICT_MALFORMED_PAYLOAD.*Rejected transaction due to malformed payload.*Unsupported serialization version '9.*",
              "TransactionProcessingSteps warning",
            ),
          )
        ),
      )
      participant1.health.ping(participant1)
    }
  }
}

final class ReferenceTransactionVersionIntegrationTestPostgres
    extends TransactionVersionIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
