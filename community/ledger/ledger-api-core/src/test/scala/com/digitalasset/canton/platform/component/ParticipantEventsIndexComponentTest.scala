// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter as ProtoCumulativeFilter,
  EventFormat as ProtoEventFormat,
  Filters as ProtoFilters,
  TemplateFilter as ProtoTemplateFilter,
  TransactionFormat as ProtoTransactionFormat,
  TransactionShape as ProtoTransactionShape,
  UpdateFormat as ProtoUpdateFormat,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.UpdateFormat
import com.digitalasset.canton.ledger.api.validation.{FormatValidator, LedgerApiSizeLimitsConfig}
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdatesResponse
import com.digitalasset.canton.logging.NoLogging
import com.digitalasset.canton.platform.component.IndexComponentTest.WithPostgres
import com.digitalasset.canton.platform.config.IndexServiceConfig
import com.digitalasset.canton.platform.store.backend.PersistentParticipantEventUpdateCategory
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpec

abstract class ParticipantEventsIndexComponentTest(cacheSize: Int = 0)
    extends AnyWordSpec
    with IndexComponentTest
    with WithPostgres {
  override def serviceParams =
    super.serviceParams
      .copy(
        indexServiceConfig = IndexServiceConfig(maxTransactionsInMemoryFanOutBufferSize = cacheSize)
      )

  def validated(protoFormat: ProtoUpdateFormat): UpdateFormat =
    FormatValidator.validate(protoFormat)(NoLogging, LedgerApiSizeLimitsConfig()).value

  private val nextRecordTime = new SingleStepIncreasingRecordTime

  def protoUpdateFormat(includeReassignments: Option[Int], includeTransactions: Option[Int]) = {

    def eventFormat(i: Int) =
      ProtoEventFormat(
        filtersByParty = Map(
          dsoParty.value -> ProtoFilters(
            Seq(
              ProtoCumulativeFilter(
                IdentifierFilter.TemplateFilter(
                  ProtoTemplateFilter(
                    templateId = Some(Identifier(s"#${decodedDar._2.metadata.name}", "M", s"T$i")),
                    includeCreatedEventBlob = false,
                  )
                )
              )
            )
          )
        ),
        filtersForAnyParty = None,
        verbose = false,
      )

    val transactionFormat = ProtoTransactionFormat(
      eventFormat = includeTransactions.map(eventFormat),
      transactionShape = ProtoTransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
    )
    ProtoUpdateFormat(
      includeTransactions = includeTransactions.map(_ => transactionFormat),
      includeReassignments = includeReassignments.map(eventFormat),
      includeTopologyEvents = None,
      includeTransactionsAndReassignments = None,
    )
  }

  "update stream with ParticipantEvent updates" should {

    s"should render as transactions or reassignments according to category" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      // template name indexing starts at 1,
      // templates(i) = "#${decodedDar._2.metadata.name}:M:T${i+1}"
      val usedTemplates = Array[Int](1, 2)
      val (templateIdIdx1, templateId1) = (0, usedTemplates(0))
      val (templateIdIdx2, templateId2) = (1, usedTemplates(1))
      val unusedTemplateId = 10
      val otherUnusedTemplateId = 20
      val transactionAcsImportWithTemplateId1 = participantEventCreate(
        recordTime = nextRecordTime,
        payloadLength = 10,
        category = PersistentParticipantEventUpdateCategory.AcsImportTxStyle,
        template = templates(templateIdIdx1),
      )(1)

      val reassignmentAcsImportWithTemplateId2 = participantEventCreate(
        recordTime = nextRecordTime,
        payloadLength = 10,
        reassignmentCounter = 1,
        category = PersistentParticipantEventUpdateCategory.AcsImport,
        template = templates(templateIdIdx2),
      )(1)

      val reassignmentAcsImportWithTemplateId1 = participantEventCreate(
        recordTime = nextRecordTime,
        payloadLength = 10,
        reassignmentCounter = 1,
        category = PersistentParticipantEventUpdateCategory.AcsImport,
        template = templates(templateIdIdx1),
      )(1)

      val transactionAcsImportWithTemplateId2 = participantEventCreate(
        recordTime = nextRecordTime,
        payloadLength = 10,
        category = PersistentParticipantEventUpdateCategory.AcsImportTxStyle,
        template = templates(templateIdIdx2),
      )(1)

      val rangeEnd = ingestUpdates(
        transactionAcsImportWithTemplateId1,
        reassignmentAcsImportWithTemplateId2,
        reassignmentAcsImportWithTemplateId1,
        transactionAcsImportWithTemplateId2,
      )

      def updatesFor(updateFormat: UpdateFormat) =
        index
          .updates(
            begin = rangeStart,
            endAt = Some(rangeEnd),
            updateFormat = updateFormat,
            descendingOrder = false,
            skipPruningChecks = false,
          )
          .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
          .runWith(Sink.seq)
          .futureValue

      val withTransactionsAndReassignments =
        validated(
          protoUpdateFormat(
            includeReassignments = Some(templateId2),
            includeTransactions = Some(templateId1),
          )
        ) // matching templates
      val withTransactionsOnly =
        validated(
          protoUpdateFormat(includeReassignments = None, includeTransactions = Some(templateId1))
        )
      val withReassignmentsOnly = validated(
        protoUpdateFormat(includeReassignments = Some(templateId2), includeTransactions = None)
      )

      val withTemplateFilteringTransactionsAndReassignments = validated(
        protoUpdateFormat(
          includeReassignments = Some(unusedTemplateId),
          includeTransactions = Some(otherUnusedTemplateId),
        ) // not matching templates
      )
      val withTemplateFilteringReassignments = validated(
        protoUpdateFormat(
          includeReassignments = Some(templateId2),
          includeTransactions = Some(otherUnusedTemplateId),
        ) // reassignments only, because of template filtering
      )

      val withTemplateFilteringTransactions = validated(
        protoUpdateFormat(
          includeReassignments = Some(unusedTemplateId),
          includeTransactions = Some(templateId1),
        ) // transactions only, because of template filtering
      )

      withTransactionsOnly.includeReassignments shouldBe None
      withReassignmentsOnly.includeTransactions shouldBe None

      val updatesWithTransactionsAndReassignments = updatesFor(withTransactionsAndReassignments)
      updatesWithTransactionsAndReassignments.map(
        _.update.isReassignment
      ) should contain theSameElementsInOrderAs Seq(false, true)

      val receivedReassignment = updatesWithTransactionsAndReassignments(1)

      val receivedContractId =
        receivedReassignment.update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId

      receivedContractId shouldEqual reassignmentAcsImportWithTemplateId2._2.loneElement.contractId.coid

      val updatesWithOnlyTransactions = updatesFor(withTransactionsOnly)
      updatesWithOnlyTransactions.map(
        _.update.isReassignment
      ) should contain theSameElementsInOrderAs Seq(
        false
      )
      updatesWithOnlyTransactions shouldBe updatesWithTransactionsAndReassignments.filterNot(
        _.update.isReassignment
      )

      val updatesWithOnlyReassignments = updatesFor(withReassignmentsOnly)
      updatesWithOnlyReassignments.map(
        _.update.isReassignment
      ) should contain theSameElementsInOrderAs Seq(
        true
      )
      updatesWithOnlyReassignments shouldBe updatesWithTransactionsAndReassignments.filter(
        _.update.isReassignment
      )

      updatesFor(
        withTemplateFilteringTransactionsAndReassignments
      ) shouldBe empty

      updatesFor(
        withTemplateFilteringReassignments
      ) should contain theSameElementsAs updatesWithOnlyReassignments

      updatesFor(
        withTemplateFilteringTransactions
      ) should contain theSameElementsAs updatesWithOnlyTransactions
    }
  }
}

class ParticipantEventsIndexComponentTestPostgres
    extends ParticipantEventsIndexComponentTest(cacheSize = 0)

class ParticipantEventsIndexComponentTestCache
    extends ParticipantEventsIndexComponentTest(cacheSize = 1000)
