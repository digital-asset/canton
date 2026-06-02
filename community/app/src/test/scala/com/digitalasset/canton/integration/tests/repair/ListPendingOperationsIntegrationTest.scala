// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.admin.api.client.data.PendingOperationMetadata
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.synchronizer.PendingLsuOperation
import com.digitalasset.canton.store.PendingOperation
import com.digitalasset.canton.topology.{DefaultTestIdentities, PhysicalSynchronizerId}

final class ListPendingOperationsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair))

  registerPlugin(new UseBftSequencer(loggerFactory))

  private def createPendingOperation(
      key: String,
      psid: PhysicalSynchronizerId,
  ): PendingOperation[PendingLsuOperation, PhysicalSynchronizerId] =
    PendingOperation(
      name = PendingLsuOperation.operationName,
      key = key,
      operation = PendingLsuOperation(psid, testedProtocolVersion),
      synchronizer = psid,
    )

  private def createMetadata(op: PendingOperation[PendingLsuOperation, PhysicalSynchronizerId]) =
    PendingOperationMetadata(op.name.unwrap, op.key, op.synchronizer)

  "repair.list_pending_operations" should {
    "list metadata for a stored pending LSU operation" in { implicit env =>
      import env.*

      val key = PendingLsuOperation.operationKey
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op = createPendingOperation(key, daId)

      store.insert(op).futureValueUS.value shouldBe ()

      participant1.repair.pending_operations
        .list(PendingLsuOperation.operationName.unwrap)
        .loneElement shouldBe createMetadata(op)
      store.delete(daId, key, PendingLsuOperation.operationName).futureValueUS
    }

    "return an empty list when filtering by a different synchronizer id" in { implicit env =>
      import env.*

      val key = PendingLsuOperation.operationKey
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op = createPendingOperation(key, daId)
      store.insert(op).futureValueUS.value shouldBe ()
      val pendingOperations =
        participant1.repair.pending_operations
          .list(
            PendingLsuOperation.operationName.unwrap,
            filterSynchronizer = Some(DefaultTestIdentities.physicalSynchronizerId),
          )
          .toList

      pendingOperations shouldBe empty

      participant1.repair.pending_operations
        .list(PendingLsuOperation.operationName.unwrap, filterSynchronizer = Some(daId))
        .loneElement shouldBe createMetadata(op)

      store.delete(daId, key, PendingLsuOperation.operationName).futureValueUS

    }

    "list metadata for two stored pending LSU operations" in { implicit env =>
      import env.*

      val key1 = "list-pending-operations-first"
      val key2 = "list-pending-operations-second"
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op1 = createPendingOperation(key1, daId)
      val op2 = createPendingOperation(key2, DefaultTestIdentities.physicalSynchronizerId)

      store.insert(op1).futureValueUS.value shouldBe ()
      store.insert(op2).futureValueUS.value shouldBe ()

      participant1.repair.pending_operations.list(
        PendingLsuOperation.operationName.unwrap
      ) should contain theSameElementsAs Seq(
        createMetadata(op1),
        createMetadata(op2),
      )

      store.delete(daId, key1, PendingLsuOperation.operationName).futureValueUS
      store
        .delete(
          DefaultTestIdentities.physicalSynchronizerId,
          key2,
          PendingLsuOperation.operationName,
        )
        .futureValueUS
    }

    "list metadata filtered by filterOperationKey" in { implicit env =>
      import env.*

      val key1 = "list-pending-operations-first"
      val key2 = "list-pending-operations-second"
      val store = participant1.underlying.value.sync.pendingLsuOperationsStore

      val op1 = createPendingOperation(key1, daId)
      val op2 = createPendingOperation(key2, daId)

      store.insert(op1).futureValueUS.value shouldBe ()
      store.insert(op2).futureValueUS.value shouldBe ()

      participant1.repair.pending_operations
        .list(
          PendingLsuOperation.operationName.unwrap,
          filterOperationKey = Some(key1),
        )
        .loneElement shouldBe createMetadata(op1)

      participant1.repair.pending_operations
        .list(
          PendingLsuOperation.operationName.unwrap,
          filterOperationKey = Some(key2),
        )
        .loneElement shouldBe createMetadata(op2)

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
}
