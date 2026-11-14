// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.admin.api.client.data.PendingOperationMetadata
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.synchronizer.PendingLsuOperation
import com.digitalasset.canton.store.PendingOperation
import com.digitalasset.canton.topology.{DefaultTestIdentities, PhysicalSynchronizerId}

final class PendingOperationsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair))

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private def createPendingOperation(
      name: NonEmptyString,
      key: String,
      psid: PhysicalSynchronizerId,
  ): PendingOperation[PendingLsuOperation, PhysicalSynchronizerId] =
    PendingOperation(
      name = name,
      key = key,
      operation = PendingLsuOperation(psid, testedProtocolVersion),
      synchronizer = psid,
    )

  private def createMetadata(op: PendingOperation[PendingLsuOperation, PhysicalSynchronizerId]) =
    PendingOperationMetadata(op.name.unwrap, op.key, op.synchronizer)

  "repair.pending_operations.list" should {
    "list metadata filtered by different filters" in { implicit env =>
      import env.*
      val name1Str = "name1"
      val name2Str = "name2"
      val name1 = NonEmptyString.tryCreate(name1Str)
      val name2 = NonEmptyString.tryCreate(name2Str)

      val key1 = "list-pending-operations-first"
      val key2 = "list-pending-operations-second"
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op1 = createPendingOperation(name1, key1, daId)
      val op2 = createPendingOperation(name2, key2, daId)

      store.insert(op1).futureValueUS.value shouldBe ()
      store.insert(op2).futureValueUS.value shouldBe ()

      participant1.repair.pending_operations
        .list(
          Some(name1Str)
        )
        .loneElement shouldBe createMetadata(op1)

      participant1.repair.pending_operations
        .list(
          filterOperationName = None,
          filterSynchronizer = Some(daId),
          filterOperationKey = Some(key2),
        )
        .loneElement shouldBe createMetadata(op2)

      participant1.repair.pending_operations
        .list(
          filterOperationName = None,
          filterSynchronizer = None,
          filterOperationKey = Some(key1),
        )
        .loneElement shouldBe createMetadata(op1)

      participant1.repair.pending_operations.list(
        filterOperationName = None,
        filterSynchronizer = Some(daId),
        filterOperationKey = None,
      ) should contain theSameElementsAs Seq(
        createMetadata(op1),
        createMetadata(op2),
      )

      participant1.repair.pending_operations
        .list(
          None,
          filterSynchronizer = Some(DefaultTestIdentities.physicalSynchronizerId),
          None,
        )
        .toList shouldBe empty

      store.delete(daId, key1, PendingLsuOperation.operationName).futureValueUS
      store
        .delete(
          daId,
          key2,
          PendingLsuOperation.operationName,
        )
        .futureValueUS
    }
  }

  "repair.pending_operations.delete" should {
    "correctly delete operations" in { implicit env =>
      import env.*
      val name1Str = "name1"
      val name2Str = "name2"
      val name1 = NonEmptyString.tryCreate(name1Str)
      val name2 = NonEmptyString.tryCreate(name2Str)

      val key1 = "list-pending-operations-first"
      val key2 = "list-pending-operations-second"
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op1 = createPendingOperation(name1, key1, daId)
      val op2 = createPendingOperation(name2, key2, daId)

      store.insert(op1).futureValueUS.value shouldBe ()
      store.insert(op2).futureValueUS.value shouldBe ()

      participant1.repair.pending_operations.list(
        filterOperationName = None,
        filterSynchronizer = Some(daId),
        filterOperationKey = None,
      ) should contain theSameElementsAs Seq(
        createMetadata(op1),
        createMetadata(op2),
      )

      participant1.repair.pending_operations.delete(name1Str, daId, key1)
      participant1.repair.pending_operations
        .list(
          filterOperationName = None,
          filterSynchronizer = Some(daId),
          filterOperationKey = None,
        )
        .loneElement shouldBe createMetadata(op2)

      participant1.repair.pending_operations.delete(
        name1Str,
        DefaultTestIdentities.synchronizerId,
        key1,
      )
      participant1.repair.pending_operations
        .list(
          filterOperationName = None,
          filterSynchronizer = Some(daId),
          filterOperationKey = None,
        )
        .loneElement shouldBe createMetadata(op2)

      participant1.repair.pending_operations.delete(
        name2Str,
        daId,
        key2,
      )
      participant1.repair.pending_operations
        .list(
          filterOperationName = None,
          filterSynchronizer = Some(daId),
          filterOperationKey = None,
        )
        .toList shouldBe empty
    }
  }
}
