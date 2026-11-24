// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo}
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.canton.platform.apiserver.FatContractInstanceHelper
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.UseOriginalViewPackageId
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{
  ContractIdEventFilter,
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfSerializationVersion,
  ReassignmentId,
  TestUpdateId,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfValue}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.Versioned
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactionLogUpdatesConversionsTransactionsByContractIdSpec
    extends AnyFlatSpec
    with Matchers
    with BaseTest
    with HasExecutionContext {

  import TransactionLogUpdatesConversionsTransactionsByContractIdSpec.*

  behavior of "TransactionLogUpdatesConversions.filter (filter by contract id)"

  it should "skip the update if there are no events related to the requested contract id" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(transactionFormatFor(requestedContractId)),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(transaction(createdEvent(1, contractId1), createdEvent(2, contractId2)))

    result shouldBe None
  }

  it should "keep only the events related to the requested contract id" in {
    val matchingEvent = createdEvent(2, requestedContractId)
    val tx = transaction(createdEvent(1, contractId1), matchingEvent, createdEvent(3, contractId2))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(transactionFormatFor(requestedContractId)),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(matchingEvent))(tx.traceContext))
  }

  it should "keep the archive of the requested contract id and skip an unrelated create" in {
    val archive = archivedEvent(2, requestedContractId)
    val tx = transaction(createdEvent(1, contractId1), archive)

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(transactionFormatFor(requestedContractId)),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(archive))(tx.traceContext))
  }

  it should "keep the events matching any of the requested contract ids" in {
    val nonExistentContractId = contractId("non-existent-contract-id")
    val create = createdEvent(1, contractId1)
    val archive = archivedEvent(3, contractId2)
    val tx = transaction(create, createdEvent(2, requestedContractId), archive)

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions =
            Some(transactionFormatFor(nonExistentContractId, contractId1, contractId2)),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(create, archive))(tx.traceContext))
  }

  it should "skip the assignment if there are no reassignment events related to the requested contract id" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatFor(requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(reassignment(assignEvent(1, contractId1), assignEvent(2, contractId2)))

    result shouldBe None
  }

  it should "skip the unassignment if there are no reassignment events related to the requested contract id" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatFor(requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(reassignment(unassignEvent(1, contractId1), unassignEvent(2, contractId2)))

    result shouldBe None
  }

  it should "keep only the assign events related to the requested contract id" in {
    val matchingEvent = assignEvent(2, requestedContractId)
    val update =
      reassignment(assignEvent(1, contractId1), matchingEvent, assignEvent(3, contractId2))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatFor(requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(update.copy(reassignments = Seq(matchingEvent))(update.traceContext))
  }

  it should "keep only the unassign events related to the requested contract id" in {
    val matchingEvent = unassignEvent(2, requestedContractId)
    val update =
      reassignment(unassignEvent(1, contractId1), matchingEvent, unassignEvent(3, contractId2))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatFor(requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(update.copy(reassignments = Seq(matchingEvent))(update.traceContext))
  }

  it should "keep the reassignment events matching any of the requested contract ids" in {
    val nonExistentContractId = contractId("non-existent-contract-id")
    val assign1 = assignEvent(1, contractId1)
    val assign2 = assignEvent(3, contractId2)
    val update = reassignment(assign1, assignEvent(2, requestedContractId), assign2)

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments =
            Some(eventFormatFor(nonExistentContractId, contractId1, contractId2)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(update.copy(reassignments = Seq(assign1, assign2))(update.traceContext))
  }

  it should "filter both transactions and reassignments by the requested contract id" in {
    val filter = TransactionLogUpdatesConversions.filter(
      InternalUpdateFormat(
        includeTransactions = Some(transactionFormatFor(requestedContractId)),
        includeReassignments = Some(eventFormatFor(requestedContractId)),
        includeTopologyEvents = None,
        includeAcsCommitments = None,
      )
    )

    val create = createdEvent(2, requestedContractId)
    val tx = transaction(createdEvent(1, contractId1), create)
    val unassign = unassignEvent(1, requestedContractId)
    val unassignment = reassignment(unassign, unassignEvent(2, contractId1))
    val assign = assignEvent(2, requestedContractId)
    val assignment = reassignment(assignEvent(1, contractId2), assign)
    val archive = archivedEvent(2, requestedContractId)
    val archivingTx = transaction(archivedEvent(1, contractId2), archive)
    val unrelatedTx = transaction(createdEvent(1, contractId1), archivedEvent(2, contractId2))
    val unrelatedReassignment = reassignment(assignEvent(1, contractId1))

    Seq(tx, unrelatedTx, unassignment, unrelatedReassignment, assignment, archivingTx)
      .map(filter) shouldBe Seq(
      Some(tx.copy(events = Vector(create))(tx.traceContext)),
      None,
      Some(unassignment.copy(reassignments = Seq(unassign))(unassignment.traceContext)),
      None,
      Some(assignment.copy(reassignments = Seq(assign))(assignment.traceContext)),
      Some(archivingTx.copy(events = Vector(archive))(archivingTx.traceContext)),
    )
  }

  it should "filter transactions and reassignments by different contract ids" in {
    val filter = TransactionLogUpdatesConversions.filter(
      InternalUpdateFormat(
        includeTransactions = Some(transactionFormatFor(requestedContractId)),
        includeReassignments = Some(eventFormatFor(contractId1)),
        includeTopologyEvents = None,
        includeAcsCommitments = None,
      )
    )

    val create = createdEvent(2, requestedContractId)
    val tx = transaction(createdEvent(1, contractId1), create)
    val unassign = unassignEvent(1, requestedContractId)
    val contract1Unassign = unassignEvent(2, contractId1)
    val unassignment = reassignment(unassign, contract1Unassign)
    val assign = assignEvent(2, requestedContractId)
    val assignment = reassignment(assignEvent(1, contractId2), assign)
    val archive = archivedEvent(2, requestedContractId)
    val archivingTx = transaction(archivedEvent(1, contractId2), archive)
    val unrelatedTx = transaction(createdEvent(1, contractId1), archivedEvent(2, contractId2))
    val contractId1Assigment = reassignment(assignEvent(1, contractId1))

    Seq(tx, unrelatedTx, unassignment, contractId1Assigment, assignment, archivingTx)
      .map(filter) shouldBe Seq(
      Some(tx.copy(events = Vector(create))(tx.traceContext)),
      None,
      Some(
        unassignment
          .copy(reassignments = Seq(unassignEvent(2, contractId1)))(unassignment.traceContext)
      ),
      Some(contractId1Assigment),
      None,
      Some(archivingTx.copy(events = Vector(archive))(archivingTx.traceContext)),
    )
  }

  behavior of "TransactionLogUpdatesConversions.filter (filter by contract id and read-as parties)"

  it should "omit transaction events of the requested contract id not witnessed by the read-as party" in {
    val visibleCreate = createdEvent(1, requestedContractId)
    val tx = transaction(
      visibleCreate,
      createdEvent(
        2,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      ),
      archivedEvent(
        3,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      ),
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(
            transactionFormatForParties(Some(Set(party)), AcsDelta, requestedContractId)
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(visibleCreate))(tx.traceContext))
  }

  it should "skip the transaction if the read-as party does not witness any event of the requested contract id" in {
    val tx = transaction(
      createdEvent(
        1,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      ),
      archivedEvent(
        2,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      ),
      // witnessed by the read-as party, but not related to the requested contract id
      createdEvent(3, contractId1),
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(
            transactionFormatForParties(Some(Set(party)), AcsDelta, requestedContractId)
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe None
  }

  it should "keep transaction events witnessed by any of the read-as parties" in {
    val aliceCreate = createdEvent(1, requestedContractId)
    val bobArchive = archivedEvent(
      2,
      requestedContractId,
      flatEventWitnesses = Set(bob),
      treeEventWitnesses = Set(bob),
    )
    val tx = transaction(
      aliceCreate,
      bobArchive,
      createdEvent(
        3,
        requestedContractId,
        flatEventWitnesses = Set(charlie),
        treeEventWitnesses = Set(charlie),
      ),
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(
            transactionFormatForParties(Some(Set(party, bob)), AcsDelta, requestedContractId)
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(aliceCreate, bobArchive))(tx.traceContext))
  }

  it should "keep transaction events witnessed by any party if read as any party is given" in {
    val aliceCreate = createdEvent(1, requestedContractId)
    val bobArchive = archivedEvent(
      2,
      requestedContractId,
      flatEventWitnesses = Set(bob),
      treeEventWitnesses = Set(bob),
    )
    val tx = transaction(aliceCreate, bobArchive, createdEvent(3, contractId1))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions =
            Some(transactionFormatForParties(None, AcsDelta, requestedContractId)),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(aliceCreate, bobArchive))(tx.traceContext))
  }

  it should "use the flat event witnesses for the read-as parties with the AcsDelta shape" in {
    // Alice is only an informee of the create (tree witness), but not a stakeholder (flat witness)
    val treeOnlyCreate = createdEvent(
      1,
      requestedContractId,
      flatEventWitnesses = Set(bob),
      treeEventWitnesses = Set(party, bob),
    )
    val flatCreate = createdEvent(
      2,
      requestedContractId,
      flatEventWitnesses = Set(party),
      treeEventWitnesses = Set(bob),
    )
    val tx = transaction(treeOnlyCreate, flatCreate)

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(
            transactionFormatForParties(Some(Set(party)), AcsDelta, requestedContractId)
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(flatCreate))(tx.traceContext))
  }

  it should "use the tree event witnesses for the read-as parties with the LedgerEffects shape" in {
    val treeOnlyCreate = createdEvent(
      1,
      requestedContractId,
      flatEventWitnesses = Set(bob),
      treeEventWitnesses = Set(party, bob),
    )
    val flatOnlyCreate = createdEvent(
      2,
      requestedContractId,
      flatEventWitnesses = Set(party),
      treeEventWitnesses = Set(bob),
    )
    val aliceExercise = nonConsumingExercisedEvent(3, requestedContractId, Set(party))
    val bobExercise = nonConsumingExercisedEvent(4, requestedContractId, Set(bob))
    val tx = transaction(treeOnlyCreate, flatOnlyCreate, aliceExercise, bobExercise)

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = Some(
            transactionFormatForParties(Some(Set(party)), LedgerEffects, requestedContractId)
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(tx)

    result shouldBe Some(tx.copy(events = Vector(treeOnlyCreate, aliceExercise))(tx.traceContext))
  }

  it should "omit assign events of the requested contract id not visible to the read-as party" in {
    val visibleAssign = assignEvent(1, requestedContractId)
    val update = reassignment(
      visibleAssign,
      assignEvent(2, requestedContractId, stakeholders = Set(bob)),
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatForParties(Some(Set(party)), requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(update.copy(reassignments = Seq(visibleAssign))(update.traceContext))
  }

  it should "omit unassign events of the requested contract id not visible to the read-as party" in {
    val visibleUnassign = unassignEvent(2, requestedContractId)
    val update = reassignment(
      unassignEvent(1, requestedContractId, stakeholders = Set(bob)),
      visibleUnassign,
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatForParties(Some(Set(party)), requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(update.copy(reassignments = Seq(visibleUnassign))(update.traceContext))
  }

  it should "skip the reassignment if the read-as party is not a stakeholder of the requested contract id" in {
    val filter = TransactionLogUpdatesConversions.filter(
      InternalUpdateFormat(
        includeTransactions = None,
        includeReassignments = Some(eventFormatForParties(Some(Set(party)), requestedContractId)),
        includeTopologyEvents = None,
        includeAcsCommitments = None,
      )
    )

    val assignment = reassignment(
      assignEvent(1, requestedContractId, stakeholders = Set(bob)),
      // visible to the read-as party, but not related to the requested contract id
      assignEvent(2, contractId1),
    )
    val unassignment = reassignment(
      unassignEvent(1, requestedContractId, stakeholders = Set(bob, charlie)),
      unassignEvent(2, contractId1),
    )

    Seq(assignment, unassignment).map(filter) shouldBe Seq(None, None)
  }

  it should "keep reassignment events visible to any of the read-as parties" in {
    val aliceAssign = assignEvent(1, requestedContractId)
    val bobAssign = assignEvent(2, requestedContractId, stakeholders = Set(bob))
    val update = reassignment(
      aliceAssign,
      bobAssign,
      assignEvent(3, requestedContractId, stakeholders = Set(charlie)),
    )

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments =
            Some(eventFormatForParties(Some(Set(party, bob)), requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(
      update.copy(reassignments = Seq(aliceAssign, bobAssign))(update.traceContext)
    )
  }

  it should "keep reassignment events visible to any party if read as any party is given" in {
    val aliceAssign = assignEvent(1, requestedContractId)
    val bobUnassign = unassignEvent(2, requestedContractId, stakeholders = Set(bob))
    val update = reassignment(aliceAssign, bobUnassign, assignEvent(3, contractId1))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatForParties(None, requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe Some(
      update.copy(reassignments = Seq(aliceAssign, bobUnassign))(update.traceContext)
    )
  }

  it should "apply different read-as parties to transactions and reassignments" in {
    val filter = TransactionLogUpdatesConversions.filter(
      InternalUpdateFormat(
        includeTransactions =
          Some(transactionFormatForParties(Some(Set(party)), AcsDelta, requestedContractId)),
        includeReassignments = Some(eventFormatForParties(Some(Set(bob)), requestedContractId)),
        includeTopologyEvents = None,
        includeAcsCommitments = None,
      )
    )

    val aliceCreate = createdEvent(1, requestedContractId)
    val tx = transaction(
      aliceCreate,
      createdEvent(
        2,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      ),
    )
    val bobOnlyTx = transaction(
      archivedEvent(
        1,
        requestedContractId,
        flatEventWitnesses = Set(bob),
        treeEventWitnesses = Set(bob),
      )
    )
    val bobUnassign = unassignEvent(2, requestedContractId, stakeholders = Set(bob))
    val unassignment = reassignment(unassignEvent(1, requestedContractId), bobUnassign)
    val aliceOnlyAssignment = reassignment(assignEvent(1, requestedContractId))

    Seq(tx, bobOnlyTx, unassignment, aliceOnlyAssignment).map(filter) shouldBe Seq(
      Some(tx.copy(events = Vector(aliceCreate))(tx.traceContext)),
      None,
      Some(unassignment.copy(reassignments = Seq(bobUnassign))(unassignment.traceContext)),
      None,
    )
  }

  it should "Drop events without witnesses with read as any party" in {
    val unassign = unassignEvent(2, requestedContractId, stakeholders = Set())
    val update = reassignment(unassign, assignEvent(3, contractId1))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatForParties(None, requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe None
  }

  it should "Drop events without witnesses with read as alice" in {
    val unassign = unassignEvent(2, requestedContractId, stakeholders = Set())
    val update = reassignment(unassign, assignEvent(3, contractId1))

    val result = TransactionLogUpdatesConversions
      .filter(
        InternalUpdateFormat(
          includeTransactions = None,
          includeReassignments = Some(eventFormatForParties(Some(Set(party)), requestedContractId)),
          includeTopologyEvents = None,
          includeAcsCommitments = None,
        )
      )
      .apply(update)

    result shouldBe None
  }

}

object TransactionLogUpdatesConversionsTransactionsByContractIdSpec {

  private val party = Ref.Party.assertFromString("Alice")
  private val bob = Ref.Party.assertFromString("Bob")
  private val charlie = Ref.Party.assertFromString("Charlie")
  private val templateId = Ref.Identifier.assertFromString("pkg:Mod:Entity")
  private val packageName = Ref.PackageName.assertFromString("pkg-name")
  private val synchronizerId = SynchronizerId.tryFromString("some::synchronizer")
  private val offset = Offset.tryFromLong(10L)

  private def contractId(name: String): LfContractId =
    LfContractId.V1(Hash.assertFromBytes(Bytes.fromByteArray("%32s".format(name).getBytes)))

  private val contractId1 = contractId("contract-id-1")
  private val contractId2 = contractId("contract-id-2")
  private val requestedContractId = contractId("requested-contract-id")

  private def eventFormatFor(contractIds: LfContractId*): InternalEventFormat =
    // the party witnesses all the events by default, so only the contract ids can exclude them
    eventFormatForParties(Some(Set(party)), contractIds*)

  private def eventFormatForParties(
      readAsParty: Option[Set[Ref.Party]],
      contractIds: LfContractId*
  ): InternalEventFormat =
    InternalEventFormat(
      eventFilter = ContractIdEventFilter(
        // the internal contract ids are irrelevant for the in-memory filtering
        contractIds = contractIds.zipWithIndex.map { case (cid, idx) => cid -> idx.toLong }.toMap,
        readAsParty = readAsParty,
      ),
      eventProjectionProperties = EventProjectionProperties(verbose = true)(
        interfaceViewPackageUpgrade = UseOriginalViewPackageId
      ),
    )

  private def transactionFormatFor(contractIds: LfContractId*): InternalTransactionFormat =
    InternalTransactionFormat(
      internalEventFormat = eventFormatFor(contractIds*),
      transactionShape = AcsDelta,
    )

  private def transactionFormatForParties(
      readAsParty: Option[Set[Ref.Party]],
      transactionShape: TransactionShape,
      contractIds: LfContractId*
  ): InternalTransactionFormat =
    InternalTransactionFormat(
      internalEventFormat = eventFormatForParties(readAsParty, contractIds*),
      transactionShape = transactionShape,
    )

  private def fatContractInstance(
      contractId: LfContractId,
      stakeholders: Set[Ref.Party],
  ) =
    FatContractInstanceHelper.buildFatContractInstance(
      templateId = templateId,
      packageName = packageName,
      contractId = contractId,
      argument = LfValue.ValueUnit,
      createdAt = Timestamp.Epoch,
      authenticationData = Bytes.Empty,
      signatories = stakeholders,
      stakeholders = stakeholders,
      keyOpt = None,
      version = LfSerializationVersion.V1,
    )

  private def createdEvent(
      nodeId: Int,
      contractId: LfContractId,
      flatEventWitnesses: Set[Ref.Party] = Set(party),
      treeEventWitnesses: Set[Ref.Party] = Set(party),
  ): TransactionLogUpdate.Event =
    TransactionLogUpdate.CreatedEvent(
      eventOffset = offset,
      updateId = TestUpdateId("tx").toHexString,
      nodeId = nodeId,
      eventSequentialId = nodeId.toLong,
      contractId = contractId,
      ledgerEffectiveTime = Timestamp.Epoch,
      templateId = templateId,
      representativePackageId = PackageId.assertFromString("pkg"),
      packageName = packageName,
      packageVersion = None,
      commandId = "",
      workflowId = "",
      keyInfo = None,
      treeEventWitnesses = treeEventWitnesses,
      flatEventWitnesses = flatEventWitnesses,
      submitters = Set(party),
      authenticationData = Bytes.Empty,
      createdContractInstance = fatContractInstance(contractId, flatEventWitnesses),
    )

  private def archivedEvent(
      nodeId: Int,
      contractId: LfContractId,
      flatEventWitnesses: Set[Ref.Party] = Set(party),
      treeEventWitnesses: Set[Ref.Party] = Set(party),
  ): TransactionLogUpdate.Event =
    exercisedEvent(
      nodeId,
      contractId,
      consuming = true,
      flatEventWitnesses = flatEventWitnesses,
      treeEventWitnesses = treeEventWitnesses,
    )

  private def nonConsumingExercisedEvent(
      nodeId: Int,
      contractId: LfContractId,
      treeEventWitnesses: Set[Ref.Party],
  ): TransactionLogUpdate.Event =
    exercisedEvent(
      nodeId,
      contractId,
      consuming = false,
      flatEventWitnesses = Set.empty,
      treeEventWitnesses = treeEventWitnesses,
    )

  private def exercisedEvent(
      nodeId: Int,
      contractId: LfContractId,
      consuming: Boolean,
      flatEventWitnesses: Set[Ref.Party],
      treeEventWitnesses: Set[Ref.Party],
  ): TransactionLogUpdate.Event =
    TransactionLogUpdate.ExercisedEvent(
      eventOffset = offset,
      updateId = TestUpdateId("tx").toHexString,
      nodeId = nodeId,
      eventSequentialId = nodeId.toLong,
      contractId = contractId,
      ledgerEffectiveTime = Timestamp.Epoch,
      templateId = templateId,
      packageName = packageName,
      interfaceId = None,
      commandId = "",
      workflowId = "",
      keyInfo = None,
      treeEventWitnesses = treeEventWitnesses,
      flatEventWitnesses = flatEventWitnesses,
      submitters = Set(party),
      choice = if (consuming) "Archive" else "NonConsumingChoice",
      actingParties = Set(party),
      lastDescendantNodeId = nodeId,
      exerciseArgument = Versioned(LfSerializationVersion.V1, LfValue.ValueUnit),
      exerciseResult = None,
      consuming = consuming,
    )

  private def transaction(
      events: TransactionLogUpdate.Event*
  ): TransactionLogUpdate.TransactionAccepted =
    TransactionLogUpdate.TransactionAccepted(
      updateId = TestUpdateId("tx").toHexString,
      commandId = "",
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = offset,
      events = events.toVector,
      completionStreamResponseO = None,
      synchronizerId = synchronizerId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
      transactionHash = None,
    )(TraceContext.empty)

  private def assignEvent(
      nodeId: Int,
      contractId: LfContractId,
      stakeholders: Set[Ref.Party] = Set(party),
  ): Reassignment =
    Reassignment.Assign(
      reassignmentCounter = 1L,
      nodeId = nodeId,
      persistedContractInstance = PersistedContractInstance(
        internalContractId = nodeId.toLong,
        inst = fatContractInstance(contractId, stakeholders),
      ),
      representativePackageId = PackageId.assertFromString("pkg"),
    )

  private def unassignEvent(
      nodeId: Int,
      contractId: LfContractId,
      stakeholders: Set[Ref.Party] = Set(party),
  ): Reassignment =
    Reassignment.Unassign(
      contractId = contractId,
      templateId = templateId,
      packageName = packageName,
      stakeholders = stakeholders,
      assignmentExclusivity = None,
      reassignmentCounter = 1L,
      nodeId = nodeId,
      keyOpt = None,
    )

  private def reassignment(
      events: Reassignment*
  ): TransactionLogUpdate.ReassignmentAccepted =
    TransactionLogUpdate.ReassignmentAccepted(
      updateId = TestUpdateId("reassignment").toHexString,
      commandId = "",
      workflowId = "",
      offset = offset,
      recordTime = Timestamp.Epoch,
      completionStreamResponseO = None,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = ReassignmentTag.Source(synchronizerId),
        targetSynchronizer = ReassignmentTag.Target(synchronizerId),
        submitter = None,
        reassignmentId = ReassignmentId.tryCreate("0001"),
        isReassigningParticipant = false,
      ),
      reassignments = events,
      synchronizerId = synchronizerId.toProtoPrimitive,
    )(TraceContext.empty)
}
