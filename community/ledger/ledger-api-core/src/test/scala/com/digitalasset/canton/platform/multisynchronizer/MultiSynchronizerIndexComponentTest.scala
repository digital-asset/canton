// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.multisynchronizer

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.{AcsContinuationToken, CumulativeFilter, EventFormat}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.{IndexComponentTest, Party}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ExampleContractFactory,
  ReassignmentId,
  TestUpdateId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.concurrent.Future

class MultiSynchronizerIndexComponentTest extends AnyFlatSpec with IndexComponentTest {
  behavior of "MultiSynchronizer contract lookup"

  private val sequentiallyPostProcessedUpdates = mutable.Buffer[Update]()

  override protected def sequentialPostProcessor: Update => Unit =
    sequentiallyPostProcessedUpdates.append

  val templateId = Ref.Identifier.assertFromString("P:M:T")

  it should "successfully look up contract, even if only the assigned event is visible" in {
    val party = Ref.Party.assertFromString("party1")

    val c1 = createContract(party)
    val c2 = createContract(party)
    val cn1 = c1.inst.toCreateNode
    val reassignmentAccepted1 =
      mkReassignmentAccepted(
        party,
        "UpdateId1",
        createNodes = Seq(cn1),
        withAcsChange = false,
      )
    val cn2 = c2.inst.toCreateNode
    val reassignmentAccepted2 =
      mkReassignmentAccepted(
        party,
        "UpdateId2",
        createNodes = Seq(cn2),
        withAcsChange = true,
      )
    ingestUpdates(reassignmentAccepted1 -> Vector(c1), reassignmentAccepted2 -> Vector(c2))

    (for {
      activeContractO1 <- index.lookupActiveContract(Set(party), cn1.coid)
      activeContractO2 <- index.lookupActiveContract(Set(party), cn2.coid)
    } yield {
      Seq(cn1 -> activeContractO1, cn2 -> activeContractO2).foreach { case (cn, activeContractO) =>
        activeContractO.map(_.createArg) shouldBe Some(cn.versionedCoinst.unversioned.arg)
        activeContractO.map(_.templateId) shouldBe Some(cn.versionedCoinst.unversioned.template)
      }
    }).futureValue

    // Verify that the AcsChanges have been propagated to the sequential post-processor.
    sequentiallyPostProcessedUpdates.count {
      case _: Update.OnPRReassignmentAccepted => true
      case _ => false
    } shouldBe 1
    sequentiallyPostProcessedUpdates.count {
      case _: Update.RepairReassignmentAccepted => true
      case _ => false
    } shouldBe 1
  }

  override def incompleteOffsets(
      _o: Offset,
      _p: Option[Set[Party]],
      _tc: TraceContext,
  ): FutureUnlessShutdown[Vector[Offset]] =
    FutureUnlessShutdown.pure(
      Vector(
        Offset.tryFromLong(2),
        Offset.tryFromLong(4),
        Offset.tryFromLong(6),
        Offset.tryFromLong(8),
        Offset.tryFromLong(10),
        Offset.tryFromLong(12),
      )
    )

  it should "support continuation of ACS stream with incomplete reassignments" in {
    val party = Ref.Party.assertFromString("party1")

    val (createC1, contracts1) = mkTransaction(createContract(party))
    val (createC2, contracts2) = mkTransaction(createContract(party))
    val incompleteUnassignC1 =
      mkReassignmentWithUnassign(party, "incompleteUnassignC1", contracts1.map(_.contractId))
    val (_, contracts3) = mkTransaction(createContract(party))
    val incompleteAssignC3 =
      mkReassignmentAccepted(party, "incompleteAssignC3", true, contracts3.map(_.inst.toCreateNode))
    val (createC4, contracts4) = mkTransaction(createContract(party))
    val (createC5, contracts5) = mkTransaction(createContract(party))
    val incompleteUnassignC4 =
      mkReassignmentWithUnassign(party, "incompleteUnassignC4", contracts4.map(_.contractId))
    val (_, contracts6) = mkTransaction(createContract(party))
    val incompleteAssignC6 =
      mkReassignmentAccepted(party, "incompleteAssignC3", true, contracts6.map(_.inst.toCreateNode))
    // last round with multiple events for each offset
    val (createC7, contracts7) =
      mkTransaction(createContract(party), createContract(party), createContract(party))
    val (createC8, contracts8) =
      mkTransaction(createContract(party), createContract(party), createContract(party))
    val incompleteUnassignC7 =
      mkReassignmentWithUnassign(party, "incompleteUnassignC7", contracts7.map(_.contractId))
    val (_, contracts9) =
      mkTransaction(createContract(party), createContract(party), createContract(party))
    val incompleteAssignC9 =
      mkReassignmentAccepted(party, "incompleteAssignC9", true, contracts9.map(_.inst.toCreateNode))

    ingestUpdates(
      createC1 -> contracts1, // temporary activation for unassign
      incompleteUnassignC1 -> contracts1, // incomplete unassign
      createC2 -> contracts2, // std activation
      incompleteAssignC3 -> contracts3, // incomplete assign
      createC4 -> contracts4, // temporary activation for unassign
      incompleteUnassignC4 -> contracts4, // incomplete unassign
      createC5 -> contracts5, // std activation
      incompleteAssignC6 -> contracts6, // incomplete assign
      createC7 -> contracts7, // temporary activation for unassign
      incompleteUnassignC7 -> contracts7, // incomplete unassign
      createC8 -> contracts8, // std activation
      incompleteAssignC9 -> contracts9, // incomplete assign
    )

    val eventFormat = EventFormat(
      filtersByParty = Map(party -> CumulativeFilter.templateWildcardFilter()),
      filtersForAnyParty = None,
      verbose = false,
    )

    val allContracts = getAcsF(eventFormat, None).futureValue
    allContracts should have length (4 + 4 + 3 * 4)

    val continuationTokens = allContracts.map(c =>
      AcsContinuationToken
        .decodeAndValidate(
          AcsContinuationToken.emptyChecksum,
          Some(c.streamContinuationToken),
        )
        .getOrElse(sys.error("Cannot decode continuation token"))
        .getOrElse(sys.error("Missing continuation token"))
    )
    for {
      i <- continuationTokens.indices
    } yield {
      val continuation = getAcsF(eventFormat, Some(continuationTokens(i))).futureValue
      (allContracts.take(i + 1) ++ continuation) should equal(allContracts)
    }
  }

  private def createContract(party: Ref.Party) = ExampleContractFactory.build(
    stakeholders = Set(party),
    signatories = Set(party),
    templateId = templateId,
    argument = Value.ValueRecord(
      tycon = None,
      fields = ImmArray(None -> Value.ValueText("42")),
    ),
  )

  private def mkTransaction(contracts: ContractInstance*) = {
    val txBuilder = TxBuilder()
    contracts.foreach(c => txBuilder.add(c.inst.toCreateNode))
    val txn =
      transaction(synchronizer1, recordTime())(txBuilder.buildCommitted())
    (txn, contracts.toVector)
  }

  private def getAcsF(
      eventFormat: EventFormat,
      continuationToken: Option[AcsContinuationToken],
  ): Future[Vector[GetActiveContractsResponse]] =
    for {
      ledgerEnd <- index.currentLedgerEnd()
      contracts <- index
        .getActiveContracts(
          eventFormat,
          ledgerEnd,
          continuationToken,
          AcsContinuationToken.emptyChecksum,
        )
        .runWith(Sink.collection)
    } yield contracts.toVector

  private def recordTime() = CantonTimestamp(Time.Timestamp.now())

  private def mkReassignmentWithUnassign(
      party: Ref.Party,
      updateIdS: String,
      contracIds: Seq[Value.ContractId],
  ) = {
    val updateId = TestUpdateId(updateIdS)
    Update.OnPRReassignmentAccepted(
      workflowId = None,
      updateId = updateId,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = Source(synchronizer1),
        targetSynchronizer = Target(synchronizer2),
        submitter = Option(party),
        reassignmentId = ReassignmentId.tryCreate("00"),
        isReassigningParticipant = true,
      ),
      reassignment = Reassignment.Batch(
        Reassignment.Unassign(
          contractId = contracIds.head,
          templateId = templateId,
          packageName = Ref.PackageName.fromInt(5),
          stakeholders = Set(party),
          assignmentExclusivity = None,
          reassignmentCounter = 15L,
          nodeId = 0,
        ),
        contracIds.tail.map(contractId =>
          Reassignment.Unassign(
            contractId = contractId,
            templateId = templateId,
            packageName = Ref.PackageName.fromInt(5),
            stakeholders = Set(party),
            assignmentExclusivity = None,
            reassignmentCounter = 15L,
            nodeId = 0,
          )
        )*
      ),
      repairCounter = RepairCounter.Genesis,
      recordTime = recordTime(),
      synchronizerId = synchronizer2,
      acsChangeFactory = TestAcsChangeFactory(),
    )
  }

  private def mkReassignmentAccepted(
      party: Ref.Party,
      updateIdS: String,
      withAcsChange: Boolean,
      createNodes: Seq[Node.Create],
  ): Update.ReassignmentAccepted = {
    val updateId = TestUpdateId(updateIdS)
    if (withAcsChange)
      Update.OnPRReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizer1),
          targetSynchronizer = Target(synchronizer2),
          submitter = Option(party),
          reassignmentId = ReassignmentId.tryCreate("00"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNodes.head,
            contractAuthenticationData = Bytes.Empty,
            reassignmentCounter = 15L,
            nodeId = 0,
            internalContractId =
              -1, // will be filled when contracts are stored in the participant contract store
          ),
          createNodes.tail.map(createNode =>
            Reassignment.Assign(
              ledgerEffectiveTime = Time.Timestamp.now(),
              createNode = createNode,
              contractAuthenticationData = Bytes.Empty,
              reassignmentCounter = 15L,
              nodeId = 0,
              internalContractId =
                -1, // will be filled when contracts are stored in the participant contract store

            )
          )*
        ),
        repairCounter = RepairCounter.Genesis,
        recordTime = recordTime(),
        synchronizerId = synchronizer2,
        acsChangeFactory = TestAcsChangeFactory(),
      )
    else
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizer1),
          targetSynchronizer = Target(synchronizer2),
          submitter = Option(party),
          reassignmentId = ReassignmentId.tryCreate("00"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNodes.head,
            contractAuthenticationData = Bytes.Empty,
            reassignmentCounter = 15L,
            nodeId = 0,
            internalContractId =
              -1, // will be filled when contracts are stored in the participant contract store
          ),
          createNodes.tail.map(createNode =>
            Reassignment.Assign(
              ledgerEffectiveTime = Time.Timestamp.now(),
              createNode = createNode,
              contractAuthenticationData = Bytes.Empty,
              reassignmentCounter = 15L,
              nodeId = 0,
              internalContractId =
                -1, // will be filled when contracts are stored in the participant contract store
            )
          )*
        ),
        repairCounter = RepairCounter.Genesis,
        recordTime = recordTime(),
        synchronizerId = synchronizer2,
      )
  }
}
