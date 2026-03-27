// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, LfPackageId, ReassignmentCounter}
import com.digitalasset.daml.lf.transaction.{CreationTime, TransactionCoder}
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

trait RepairTestUtil {
  this: BaseTest =>

  protected val aliceS: String = "Alice"
  protected val bobS: String = "Bob"
  protected val carolS: String = "Carol"

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var amount = 100

  protected def createContract(
      participant: ParticipantReference,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
      synchronizerId: Option[SynchronizerId] = None,
  ): iou.Iou.ContractId = {
    amount = amount + 1
    val createCmd =
      new iou.Iou(
        payer.toProtoPrimitive,
        owner.toProtoPrimitive,
        new iou.Amount(amount.toBigDecimal, currency),
        List.empty.asJava,
      ).create.commands.asScala.toSeq

    val createTx = participant.ledger_api.javaapi.commands.submit(
      Seq(payer),
      createCmd,
      // do not wait on all participants to observe the transaction (gets confused as we assigned the same id to different participants)
      optTimeout = None,
      synchronizerId = synchronizerId,
    )
    JavaDecodeUtil.decodeAllCreated(iou.Iou.COMPANION)(createTx).loneElement.id
  }

  protected def readContractInstance(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      contractId: ContractId[?],
  ): RepairContract = {
    val create = participant.ledger_api.javaapi.event_query
      .by_contract_id(
        contractId.toLf.coid,
        includeCreatedEventBlob = true,
        requestingParties = Seq.empty,
      )
      .pipe(queryResult =>
        if (queryResult.hasCreated) queryResult.getCreated
        else sys.error(s"No create for ${contractId.toLf.coid}")
      )
    val createdEvent = create
      .pipe(created =>
        if (created.hasCreatedEvent) created.getCreatedEvent
        else sys.error(s"No created event for ${contractId.toLf.coid}")
      )
    val contractCreatedEventBlob = createdEvent.getCreatedEventBlob
    val contractInst = TransactionCoder
      .decodeFatContractInstance(contractCreatedEventBlob)
      .fold(
        err => sys.error(s"Failed to decode created event blob for ${contractId.toLf.coid}: $err"),
        identity,
      )
      .pipe { fci =>
        fci
          .traverseCreateAt {
            case absolute: CreationTime.CreatedAt => Right(absolute)
            case _ => Left(s"Unable to determine create time for ${fci.createdAt}")
          }
          .fold(sys.error, identity)
      }
    RepairContract(
      synchronizerId = synchronizerId,
      contract = contractInst,
      reassignmentCounter = ReassignmentCounter.Genesis,
      representativePackageId =
        LfPackageId.assertFromString(createdEvent.getRepresentativePackageId),
    )
  }

  protected def createContractInstance(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): RepairContract =
    readContractInstance(
      participant,
      synchronizerId,
      createContract(participant, payer, owner, currency),
    )

  protected def createArchivedContractInstance(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): RepairContract = {
    val archivedContractId = createContract(participant, payer, owner, currency)
    val archivedContract =
      readContractInstance(participant, synchronizerId, archivedContractId)
    exerciseContract(participant, owner, archivedContractId)
    archivedContract
  }

  protected def exerciseContract(
      participant: LocalParticipantReference,
      owner: PartyId,
      cid: iou.Iou.ContractId,
  ): Assertion = {
    val exerciseCmd = cid.exerciseCall().commands.asScala.toSeq
    val exerciseTx =
      participant.ledger_api.javaapi.commands
        .submit(Seq(owner), exerciseCmd, optTimeout = None)
    val archives = exerciseTx.getEvents.asScala.toSeq.collect {
      case x if x.toProtoEvent.hasArchived =>
        val contractId = x.toProtoEvent.getArchived.getContractId
        logger.info(s"Archived contract $contractId at offset ${exerciseTx.getOffset}")
    }.size

    archives shouldBe 1
  }

  protected def createArchivedContract(
      participant: LocalParticipantReference,
      payer: PartyId,
      owner: PartyId,
      currency: String = "USD",
  ): iou.Iou.ContractId = {
    val cid = createContract(participant, payer, owner, currency)
    exerciseContract(participant, owner, cid)
    cid
  }

  protected def assertAcsCounts(
      expectedCounts: (ParticipantReference, Map[PartyId, Int])*
  ): Unit =
    assertAcsCountsWithFilter(_ => true, expectedCounts*)

  protected def assertAcsCountsWithFilter(
      filter: WrappedContractEntry => Boolean,
      expectedCounts: (ParticipantReference, Map[PartyId, Int])*
  ): Unit =
    eventually() {
      expectedCounts.foreach { case (p, partyInfo) =>
        partyInfo.foreach { case (party, expectedCount) =>
          withClue(
            s"Party ${party.toProtoPrimitive} on participant ${p.id} expected to have $expectedCount active contracts, but found"
          ) {
            val contracts = p.ledger_api.state.acs.of_party(party).toList
            contracts.count(filter) shouldBe expectedCount
          }
        }
      }
    }
}
