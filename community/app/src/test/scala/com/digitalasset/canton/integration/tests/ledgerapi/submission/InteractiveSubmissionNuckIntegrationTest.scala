// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.interactive.interactive_submission_common_data.GlobalKeyWithMaintainers
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.HASHING_SCHEME_VERSION_V3
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.value as lapiValue
import com.daml.ledger.javaapi.data.{
  Command,
  CreatedEvent as JCreatedEvent,
  ExerciseByKeyCommand,
  Value,
}
import com.digitalasset.canton.damltestslf23.java.basickeys.{BasicKey, KeyOps}
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Utility trait with methods / contracts to write tests involving contract keys
  */
trait InteractiveSubmissionNuckSetupTest extends InteractiveSubmissionIntegrationTestSetup {

  /** Contract that can be used to create / manipulate contracts with keys. Only usable in tests
    * from PV35
    */
  protected var keyOpsContract: KeyOps.Contract = _

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        // Create a key ops contract in the setup already so we can use it in the test suite
        val keyOpsCreateCommand = KeyOps
          .create(
            TestUtils.damlSet(Set(aliceE.toProtoPrimitive)),
            aliceE.toProtoPrimitive,
          )
          .commands()
          .asScala
          .headOption
          .value

        if (testedProtocolVersion >= ProtocolVersion.v35) {
          keyOpsContract = inside(
            cpn.ledger_api.javaapi.commands
              .submit(Seq(aliceE), Seq(keyOpsCreateCommand), includeCreatedEventBlob = true)
              .getEvents
              .loneElement
          ) { case created: JCreatedEvent =>
            JavaDecodeUtil.decodeCreated(KeyOps.COMPANION)(created).value
          }
        }
      }

  protected def contractWithKeyCreateCommand: Command =
    keyOpsContract.id
      .exerciseCreateKey(aliceE.toProtoPrimitive, TestUtils.damlSet(Set(aliceE.toProtoPrimitive)))
      .commands()
      .loneElement

  protected def createContractWithKey(
      hashingSchemeVersion: HashingSchemeVersion
  )(implicit env: TestConsoleEnvironment): Transaction = {
    import env.*

    val createBasicKeyCommand = keyOpsContract.id
      .exerciseCreateKey(aliceE.toProtoPrimitive, TestUtils.damlSet(Set(aliceE.toProtoPrimitive)))
      .commands()
      .loneElement

    val prepared = cpn.ledger_api.javaapi.interactive_submission
      .prepare(Seq(aliceE), Seq(createBasicKeyCommand), hashingSchemeVersion = hashingSchemeVersion)

    prepared.getPreparedTransaction.getTransaction.nodes
      .map(_.versionedNode.v1.value.nodeType)
      .collect { case NodeType.Create(value) =>
        value.argument.foreach(assertLabelsAndIdentifiersNonEmpty)
        // Assert that there is a key and that it is enriched
        assertLabelsAndIdentifiersNonEmpty(value.getKey.getKey.getKey)
      }

    // Submit the transaction
    cpn.ledger_api.interactive_submission.execute_and_wait_for_transaction(
      prepared.getPreparedTransaction,
      Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
      submissionId = UUID.randomUUID().toString,
      hashingSchemeVersion = hashingSchemeVersion,
      includeCreatedEventBlob = true,
    )
  }

  // Expect a created event in the transaction
  protected def getFirstCreatedEventFromTransaction(transaction: Transaction): CreatedEvent =
    transaction.events
      .map(_.event)
      .collectFirst { case Event.Created(value) => value }
      .value

  // Expect a created event in the transaction with a key
  protected def getContractKeFromCreatedEvent(transaction: Transaction): lapiValue.Value =
    getFirstCreatedEventFromTransaction(transaction).getContractKey

  protected def getKeyValue(key: lapiValue.Value): lapiValue.Value =
    key.getRecord.fields.loneElement.getValue.getGenMap.entries.loneElement.getKey

  protected def getCreatedContractId(transaction: Transaction): String =
    getFirstCreatedEventFromTransaction(transaction).contractId
}

class InteractiveSubmissionNuckIntegrationTest extends InteractiveSubmissionNuckSetupTest {

  // Keep references to contracts with keys created in the first test so we can re-use them in following tests
  private var contractWithKeyTx1: Transaction = _
  private var contractWithKeyTx2: Transaction = _

  private lazy val contractKey1 = getContractKeFromCreatedEvent(contractWithKeyTx1)
  private lazy val contractKey2 = getContractKeFromCreatedEvent(contractWithKeyTx2)

  private lazy val contractId1 = getCreatedContractId(contractWithKeyTx1)
  private lazy val contractId2 = getCreatedContractId(contractWithKeyTx2)

  /** Model-based testing and BasicNuckIntegrationTest already run with external parties. This suite
    * just adds a few simple smoke tests and validates the prepared transaction.
    */
  "Interactive submission for Nuck" should {

    "create contracts with keys" onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
      // Create 2 contracts with the same key (alice is the key)
      contractWithKeyTx1 = createContractWithKey(HASHING_SCHEME_VERSION_V3)
      contractWithKeyTx2 = createContractWithKey(HASHING_SCHEME_VERSION_V3)
    }

    "exercise a choice by key" onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
      import env.*

      // Make sure they have the same key (alice)
      getKeyValue(contractKey1).getParty shouldBe aliceE.toProtoPrimitive
      contractKey1 shouldBe contractKey2

      // Pick the first one just to help us get the exercise choice and arguments we'll use in the ExerciseByKeyCommand
      // below
      val created = getFirstCreatedEventFromTransaction(contractWithKeyTx1)
      val contractKey = contractKey1
      val contract = JavaDecodeUtil
        .decodeCreated(BasicKey.COMPANION)(
          JCreatedEvent.fromProto(CreatedEvent.toJavaProto(created))
        )
        .value
      val exerciseCommand = contract.id
        .exerciseConsumeSelf(aliceE.toProtoPrimitive)
        .commands()
        .loneElement
        .asExerciseCommand()
        .get()

      val prepared = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE),
        commands = Seq(
          new ExerciseByKeyCommand(
            exerciseCommand.getTemplateId,
            Value.fromProto(lapiValue.Value.toJavaProto(contractKey)),
            exerciseCommand.getChoice,
            exerciseCommand.getChoiceArgument,
          )
        ),
        hashingSchemeVersion = HASHING_SCHEME_VERSION_V3,
      )

      val exercise = prepared.getPreparedTransaction.getTransaction.nodes
        .map(_.versionedNode.v1.value.nodeType)
        .collect { case NodeType.Exercise(value) => value }
        .loneElement

      // Assert that there is a key and that it is enriched
      assertExpectedGlobalKeyWithMaintainers(exercise.getKey)
      // ByKey should be true
      exercise.byKey shouldBe true
      // The engine will choose one of those contracts, here we don't care how it does it, just that it's
      // correctly reflected in the proto
      List(exercise.contractId) should contain oneOf (contractId1, contractId2)

      cpn.ledger_api.interactive_submission.execute_and_wait_for_transaction(
        prepared.getPreparedTransaction,
        Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
        submissionId = UUID.randomUUID().toString,
        hashingSchemeVersion = HASHING_SCHEME_VERSION_V3,
        includeCreatedEventBlob = true,
      )
    }

    "query by key" onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
      val queryCommand =
        keyOpsContract.id.exerciseLookup(aliceE.toProtoPrimitive).commands().loneElement
      val prepared = cpn.ledger_api.javaapi.interactive_submission
        .prepare(
          Seq(aliceE),
          Seq(queryCommand),
          hashingSchemeVersion = HASHING_SCHEME_VERSION_V3,
        )

      val query = prepared.getPreparedTransaction.getTransaction.nodes
        .map(_.versionedNode.v1.value.nodeType)
        .collect { case NodeType.QueryByKey(value) => value }
        .loneElement

      assertExpectedGlobalKeyWithMaintainers(query.getKey)
      query.result should contain oneOf (contractId1, contractId2)
    }
  }

  private def assertExpectedGlobalKeyWithMaintainers(
      globalKeyWithMaintainers: GlobalKeyWithMaintainers
  ): Unit = {
    globalKeyWithMaintainers.maintainers should contain theSameElementsAs Seq(
      aliceE.toProtoPrimitive
    )
    val globalKey = globalKeyWithMaintainers.getKey
    val keyValue = globalKey.getKey
    assertLabelsAndIdentifiersNonEmpty(keyValue)
  }
}
