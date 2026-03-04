// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.index.ContractKeyPage
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ExampleContractFactory,
  ReassignmentId,
  TestUpdateId,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.flatspec.AnyFlatSpec

import scala.annotation.tailrec

class NonUniqueContractKeyIndexComponentTest extends AnyFlatSpec with IndexComponentTest {
  behavior of "Non unique contract lookup"

  it should "successfully look up contract keys" in {
    val party = Ref.Party.assertFromString("party1")

    val key1 = GlobalKeyWithMaintainers(
      globalKey = GlobalKey.assertBuild(
        templateId = ExampleContractFactory.templateId,
        key = Value.ValueInt64(10),
        packageName = ExampleContractFactory.packageName,
        keyHash = crypto.Hash.hashPrivateKey("1"),
      ),
      maintainers = Set(party),
    )

    val key2 = GlobalKeyWithMaintainers(
      globalKey = GlobalKey.assertBuild(
        templateId = ExampleContractFactory.templateId,
        key = Value.ValueInt64(20),
        packageName = ExampleContractFactory.packageName,
        keyHash = crypto.Hash.hashPrivateKey("2"),
      ),
      maintainers = Set(party),
    )

    def contract(
        keyWithMaintainers: Option[GlobalKeyWithMaintainers],
        index: Long,
    ): ContractInstance =
      ExampleContractFactory.build(
        stakeholders = Set(party),
        signatories = Set(party),
        templateId = Ref.Identifier.assertFromString("P:M:T"),
        argument = Value.ValueRecord(
          tycon = None,
          fields = ImmArray(None -> Value.ValueInt64(index)),
        ),
        keyOpt = keyWithMaintainers,
      )

    val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")

    def reassignmentAccepted(
        updateIdString: String,
        recordTime: CantonTimestamp,
        synchronizerId: SynchronizerId,
        sourceSynchronizerId: SynchronizerId,
        targetSynchronizerId: SynchronizerId,
        reassignmentBatch: Reassignment.Batch,
    ): Update.SequencedReassignmentAccepted =
      Update.SequencedReassignmentAccepted(
        workflowId = None,
        updateId = TestUpdateId(updateIdString),
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(sourceSynchronizerId),
          targetSynchronizer = Target(targetSynchronizerId),
          submitter = Option(party),
          reassignmentId = ReassignmentId.tryCreate("00"),
          isReassigningParticipant = true,
        ),
        reassignment = reassignmentBatch,
        recordTime = recordTime,
        synchronizerId = synchronizerId,
        acsChangeFactory = TestAcsChangeFactory(),
        optCompletionInfo = None,
      )

    def assigns(
        updateIdString: String,
        recordTime: CantonTimestamp,
        contracts: Vector[ContractInstance],
    ): (Update.SequencedReassignmentAccepted, Vector[ContractInstance]) =
      reassignmentAccepted(
        updateIdString = updateIdString,
        recordTime = recordTime,
        synchronizerId = synchronizer2,
        sourceSynchronizerId = synchronizer1,
        targetSynchronizerId = synchronizer2,
        reassignmentBatch = Reassignment.Batch(
          NonEmpty
            .from(contracts.zipWithIndex.map { case (contract, index) =>
              Reassignment.Assign(
                ledgerEffectiveTime = recordTime.toLf,
                createNode = contract.toLf,
                contractAuthenticationData = Bytes.Empty,
                reassignmentCounter = 15L,
                nodeId = index,
                internalContractId =
                  -1, // will be filled when contracts are stored in the participant contract store
              )
            }.toSeq)
            .value
        ),
      ) -> contracts

    def unassigns(
        updateIdString: String,
        recordTime: CantonTimestamp,
        contracts: Vector[ContractInstance],
    ): (Update.SequencedReassignmentAccepted, Vector[ContractInstance]) =
      reassignmentAccepted(
        updateIdString = updateIdString,
        recordTime = recordTime,
        synchronizerId = synchronizer2,
        sourceSynchronizerId = synchronizer2,
        targetSynchronizerId = synchronizer1,
        reassignmentBatch = Reassignment.Batch(
          NonEmpty
            .from(contracts.zipWithIndex.map { case (contract, index) =>
              Reassignment.Unassign(
                contractId = contract.contractId,
                templateId = contract.templateId,
                packageName = contract.inst.packageName,
                stakeholders = contract.stakeholders,
                assignmentExclusivity = None,
                reassignmentCounter = 15L,
                nodeId = index,
              )
            }.toSeq)
            .value
        ),
      ) -> Vector.empty

    val contractNoKey1 = contract(None, 1)
    val contractNoKey2 = contract(None, 2)
    val contractKey1 = contract(Some(key1), 10)
    val contractKey2 = contract(Some(key1), 20)
    val contractKey3 = contract(Some(key1), 30)
    val contractKey4 = contract(Some(key1), 40)
    val contractKey5 = contract(Some(key1), 50)
    val contractKey6 = contract(Some(key1), 60)
    val contractKey7 = contract(Some(key1), 70)
    val contractKey8 = contract(Some(key1), 80)
    val contractKey9 = contract(Some(key1), 90)
    val contractKey10 = contract(Some(key1), 100)
    val contractOtherKey1 = contract(Some(key2), 1000)
    val contractOtherKey2 = contract(Some(key2), 2000)

    val baseRecordTime = CantonTimestamp.now()

    val assign1 = assigns(
      updateIdString = "assigns1",
      recordTime = baseRecordTime,
      contracts = Vector(
        contractNoKey1,
        contractKey1,
        contractKey2,
        contractKey3,
        contractKey4,
        contractKey5,
        contractOtherKey1,
      ),
    )

    val unassign1 = unassigns(
      updateIdString = "unassigns1",
      recordTime = baseRecordTime.plusSeconds(1),
      contracts = Vector(
        contractKey1
      ),
    )

    val assign2 = assigns(
      updateIdString = "assigns2",
      recordTime = baseRecordTime.plusSeconds(2),
      contracts = Vector(
        contractNoKey2,
        contractKey6,
        contractKey7,
        contractKey8,
        contractKey9,
        contractKey10,
        contractOtherKey2,
      ),
    )

    val unassign2 = unassigns(
      updateIdString = "unassigns2",
      recordTime = baseRecordTime.plusSeconds(3),
      contracts = Vector(
        contractKey10,
        contractOtherKey2,
        contractNoKey1,
      ),
    )

    ingestUpdates(
      assign1,
      unassign1,
      assign2,
      unassign2,
    )

    def lookupNonUniqueKeys(
        key: GlobalKeyWithMaintainers,
        limit: Int,
        token: Option[Long],
    ): ContractKeyPage =
      index
        .lookupNonUniqueContractKey(
          readers = key.maintainers,
          key = key.globalKey,
          pageToken = token,
          limit = limit,
        )
        .futureValue

    @tailrec
    def nuckPages(
        key: GlobalKeyWithMaintainers,
        limit: Int,
    )(
        page: ContractKeyPage = lookupNonUniqueKeys(key, limit, None)
    )(
        acc: Vector[Vector[ContractId]] = Vector.empty
    ): Vector[Vector[ContractId]] =
      page.nextPageToken match {
        case Some(l) =>
          nuckPages(key, limit)(lookupNonUniqueKeys(key, limit, Some(l)))(
            acc appended page.contracts.map(_.contractId)
          )
        case None => acc appended page.contracts.map(_.contractId)
      }

    def coids(cs: Vector[ContractInstance]*): Vector[Vector[ContractId]] =
      cs.map(_.map(_.contractId)).toVector

    nuckPages(key1, 100)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
        contractKey7,
        contractKey6,
        contractKey5,
        contractKey4,
        contractKey3,
        contractKey2,
      )
    )
    nuckPages(key1, 8)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
        contractKey7,
        contractKey6,
        contractKey5,
        contractKey4,
        contractKey3,
        contractKey2,
      )
    )
    nuckPages(key1, 7)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
        contractKey7,
        contractKey6,
        contractKey5,
        contractKey4,
        contractKey3,
      ),
      Vector(
        contractKey2
      ),
    )
    nuckPages(key1, 4)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
        contractKey7,
        contractKey6,
      ),
      Vector(
        contractKey5,
        contractKey4,
        contractKey3,
        contractKey2,
      ),
    )
    nuckPages(key1, 3)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
        contractKey7,
      ),
      Vector(
        contractKey6,
        contractKey5,
        contractKey4,
      ),
      Vector(
        contractKey3,
        contractKey2,
      ),
    )
    nuckPages(key1, 2)()() shouldBe coids(
      Vector(
        contractKey9,
        contractKey8,
      ),
      Vector(
        contractKey7,
        contractKey6,
      ),
      Vector(
        contractKey5,
        contractKey4,
      ),
      Vector(
        contractKey3,
        contractKey2,
      ),
    )
    nuckPages(key1, 1)()() shouldBe coids(
      Vector(contractKey9),
      Vector(contractKey8),
      Vector(contractKey7),
      Vector(contractKey6),
      Vector(contractKey5),
      Vector(contractKey4),
      Vector(contractKey3),
      Vector(contractKey2),
    )
  }
}
