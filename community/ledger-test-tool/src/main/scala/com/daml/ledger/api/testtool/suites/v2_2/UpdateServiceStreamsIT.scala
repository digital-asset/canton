// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.TestDars
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, TestConstraints}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{TransactionFormat, UpdateFormat}
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesPageRequest}
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.ledger.test.java.model.iou.Iou
import com.daml.ledger.test.java.model.test.{Dummy, DummyFactory}
import com.daml.ledger.test.java.ongoing_stream_package_upload.ongoingstreampackageuploadtest.OngoingStreamPackageUploadTestTemplate
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.util.MonadUtil
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class UpdateServiceStreamsIT(testDars: TestDars) extends LedgerTestSuite {
  import CompanionImplicits.*

  private[this] val testPackageResourcePath = testDars.OngoingStreamPackageUploadTestDar.path

  private def loadTestPackage()(implicit ec: ExecutionContext): Future[ByteString] = {
    val testPackage = Future {
      val in = getClass.getClassLoader.getResourceAsStream(testPackageResourcePath)
      assert(in != null, s"Unable to load test package resource at '$testPackageResourcePath'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  test(
    "TXLedgerEffectsEndToEnd",
    "An empty stream of transactions should be served when getting transactions from and to the current ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      ledgerEnd <- ledger.currentEnd()
      request <- ledger.getTransactionsRequest(
        transactionFormat =
          ledger.transactionFormat(Some(Seq(party)), transactionShape = LedgerEffects)
      )
      fromAndToBegin =
        request.update(_.beginExclusive := ledgerEnd, _.endInclusive := ledgerEnd)
      transactions <- ledger.transactionsWithVariants(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXEndToEnd",
    "An empty stream should be served when getting transactions from and to the end of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      request <- ledger.getTransactionsRequest(transactionFormat =
        ledger.transactionFormat(Some(Seq(party)))
      )
      end <- ledger.currentEnd()
      endToEnd = request.update(_.beginExclusive := end, _.endInclusive := end)
      transactions <- ledger.transactionsWithVariants(endToEnd)
    } yield {
      assert(
        transactions.isEmpty,
        s"No transactions were expected but ${transactions.size} were read",
      )
    }
  })

  test(
    "TXAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request <- ledger.getTransactionsRequest(
        transactionFormat = ledger.transactionFormat(Some(Seq(party)))
      )
      beyondEnd = request.update(
        _.beginExclusive := futureOffset,
        _.optionalEndInclusive := None,
      )
      failure <- ledger.transactions(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "TXServeUntilCancellation",
    "Items should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      transactions <- ledger.transactions(transactionsToRead, AcsDelta, party)
    } yield {
      assert(
        dummies.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTailingStreamSingleParty",
    "Items should be served if added during subscription",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 15
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      txReq <- ledger
        .getTransactionsRequest(
          transactionFormat = ledger.transactionFormat(Some(Seq(party)))
        )
        .map(_.update(_.optionalEndInclusive := None))
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      _ <- ledger.create(party, new Dummy(party))
      transactions <- flats
    } yield {
      assert(
        dummies.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTailingStreamAcsDeltaPartyWildcard",
    "Items should be served if party and transaction were added during subscription",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 15
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      txReq = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat = ledger.transactionFormat(parties = None),
          begin = endOffsetAtTestStart,
          end = None,
        )
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      party2 <- ledger.allocateParty()
      _ <- ledger.create(party2, new Dummy(party2))
      transactions <- flats
    } yield {
      assert(
        dummies.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
      assertAcsDelta(
        transactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXServeTailingStreamLedgerEffectsPartyWildcard",
    "Transaction with ledger effects should be served if party and transaction added during subscription",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 15
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      txReq = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = None, transactionShape = LedgerEffects),
          begin = endOffsetAtTestStart,
          end = None,
        )
      trees = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      party2 <- ledger.allocateParty()
      _ <- ledger.create(party2, new Dummy(party2))
      transactions <- trees
    } yield {
      assert(
        dummies.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
      assertAcsDelta(
        transactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXServeTailingStreamsTemplateWildcard",
    "Items should be served if package was added during subscription",
    allocate(SingleParty),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly(
      "PackageManagementService listKnownPackages is not available in JSON"
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToRead = 2
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      knownPackagesBefore <- ledger.listKnownPackages().map(_.map(_.name))
      _ <- ledger.create(party, new Dummy(party))
      txReq = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat = ledger.transactionFormat(parties = None),
          begin = endOffsetAtTestStart,
          end = None,
        )
      txReqLedgerEffects = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = None, transactionShape = LedgerEffects),
          begin = endOffsetAtTestStart,
          end = None,
        )
      flats = ledger.transactions(
        transactionsToRead,
        txReq,
      )
      trees = ledger.transactions(
        transactionsToRead,
        txReqLedgerEffects,
      )

      testPackage <- loadTestPackage()
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(testPackage)
      _ <- ledger.create(party, new OngoingStreamPackageUploadTestTemplate(party))(
        OngoingStreamPackageUploadTestTemplate.COMPANION
      )

      knownPackagesAfter <- ledger.listKnownPackages().map(_.map(_.name))
      flatTransactions <- flats
      transactionTrees <- trees
    } yield {
      assert(
        knownPackagesAfter.sizeIs == knownPackagesBefore.size + 1,
        s"the test package should not have been already uploaded," +
          s"already uploaded packages: $knownPackagesBefore",
      )
      assert(
        flatTransactions.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${flatTransactions.size} were instead",
      )
      assert(
        transactionTrees.sizeIs == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactionTrees.size} were instead",
      )
      assertAcsDelta(
        flatTransactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
      assertAcsDelta(
        transactionTrees.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXCompleteAcsDeltaOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactions(
      transactionShape = AcsDelta,
      parties = party,
    )
    for {
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXCompleteLedgerEffectsOnLedgerEnd",
    "A stream of ledger effects transactions should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactions(LedgerEffects, party)
    for {
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXFilterByTemplate",
    "The transaction service should correctly filter by template identifier",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val create = ledger.submitAndWaitRequest(
      party,
      (new Dummy(party).create.commands.asScala ++ new DummyFactory(
        party
      ).create.commands.asScala).asJava,
    )
    for {
      _ <- ledger.submitAndWait(create)
      transactions <- ledger.transactionsByTemplateIdWithVariants(
        Dummy.TEMPLATE_ID,
        Some(Seq(party)),
        Some("with party filter"),
      )
      transactionsPartyWildcard <- ledger
        .transactionsByTemplateIdWithVariants(Dummy.TEMPLATE_ID, None, Some("wildcard party"))
    } yield {
      val contract = assertSingleton("FilterByTemplate", transactions.flatMap(createdEvents))
      assertEquals(
        "FilterByTemplate",
        contract.getTemplateId,
        Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )
      assertEquals(
        "FilterByTemplate transactions for party-wildcard should match the specific party",
        transactions,
        transactionsPartyWildcard,
      )
    }
  })

  test(
    "TXFilterByInterface",
    "The transaction service should correctly filter by interface identifier",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import com.daml.ledger.test.java.model.iou.{Iou, IIou}

    val iou = new Iou(party, party, "USD", java.math.BigDecimal.ONE, Nil.asJava)
    val create = ledger.submitAndWaitRequest(party, iou.create.commands)

    for {
      _ <- ledger.submitAndWait(create)
      interfaceFilter <- ledger.getTransactionsRequest(
        ledger.transactionFormat(Some(Seq(party)), Seq.empty, Seq(IIou.TEMPLATE_ID -> true))
      )
      transactions <- ledger.transactionsWithVariants(interfaceFilter)
    } yield {
      import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
      import com.daml.ledger.javaapi.data.CreatedEvent.fromProto

      val created = assertSingleton("FilterByInterface", transactions.flatMap(createdEvents))
      assertEquals(
        "FilterByInterface",
        created.getTemplateId,
        Iou.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )

      val view = IIou.INTERFACE.fromCreatedEvent(fromProto(toJavaProto(created)))
      assertEquals(view.data.icurrency, "USD")
    }
  })

  test(
    "TXTransactionFormat",
    "The transactions should be served when the transaction format is set",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToRead = 1
    for {
      _ <- ledger.create(party, new Dummy(party))
      end <- ledger.currentEnd()
      format = ledger.eventFormat(verbose = false, parties = Some(Seq(party)))
      reqForTransactions = ledger
        .getTransactionsRequestWithEnd(
          transactionFormat =
            ledger.transactionFormat(parties = Some(Seq(party)), transactionShape = LedgerEffects),
          end = Some(end),
        )
      reqForReassignments = ledger
        .getUpdatesRequestWithEnd(
          transactionFormatO = None,
          reassignmentsFormatO = Some(format),
          end = Some(end),
        )
      reqForBoth = ledger
        .getUpdatesRequestWithEnd(
          transactionFormatO =
            Some(TransactionFormat(Some(format), TRANSACTION_SHAPE_LEDGER_EFFECTS)),
          reassignmentsFormatO = Some(format),
          end = Some(end),
        )
      txsFromReqForTransactions <- ledger.transactions(reqForTransactions)
      txsFromReqForReassignments <- ledger.transactions(reqForReassignments)
      txsFromReqForBoth <- ledger.transactions(reqForBoth)
    } yield {
      assertLength(
        s"""$transactionsToRead transactions should have been received from the request for transactions but
           | ${txsFromReqForTransactions.size} were instead""".stripMargin,
        transactionsToRead,
        txsFromReqForTransactions,
      )
      assertLength(
        s"""No transactions should have been received from the request for reassignments but
           | ${txsFromReqForReassignments.size} were instead""".stripMargin,
        0,
        txsFromReqForReassignments,
      )
      assertLength(
        s"""$transactionsToRead transactions should have been received from the request for both reassignments and
           | transactions but ${txsFromReqForBoth.size} were instead""".stripMargin,
        transactionsToRead,
        txsFromReqForBoth,
      )
    }
  })

  test(
    "TXLedgerEffectsTransientContract",
    "The transactions stream with LedgerEffects should return non empty events for a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      txs <- ledger.transactionsWithVariants(
        transactionShape = LedgerEffects,
        clue = None,
        parties = owner,
      )
    } yield {
      val tx = assertSingleton("One transaction should be found", txs)
      assert(tx.events.nonEmpty, "Expected non empty events in the transaction")
      assertAcsDelta(
        tx.events,
        acsDelta = false,
        "The acs_delta field in transient events should not be set",
      )
    }
  })

  test(
    "TXAcsDeltaTransientContract",
    "The transactions stream with AcsDelta should return no transaction for a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      txs <- ledger.transactionsWithVariants(
        transactionShape = AcsDelta,
        clue = None,
        parties = owner,
      )
    } yield {
      assert(txs.isEmpty, "Expected no transactions")
    }
  })

  test(
    "TXNonConsumingChoiceAcsDeltaFlag",
    "GetUpdateById returns NOT_FOUND when command contains only a non-consuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      end <- ledger.currentEnd()
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, contractId.exerciseDummyNonConsuming().commands)
      )
      txs <- ledger
        .getTransactionsRequest(
          transactionFormat =
            ledger.transactionFormat(Some(Seq(owner)), transactionShape = LedgerEffects),
          begin = end,
        )
        .flatMap(ledger.transactions)
    } yield {
      val tx = assertSingleton(
        "Expected one transaction with a non-consuming event",
        txs,
      )
      assertSingleton(
        "Expected one non-consuming event",
        tx.events,
      )
      assertAcsDelta(
        tx.events,
        acsDelta = false,
        "The acs_delta field in transient events should not be set",
      )
    }
  })

  test(
    "TXServeStreamLedgerEffectsFromMiddlePoint",
    "Transaction with ledger effects should be served  from middle of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 15
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      ledgerMiddle <- ledger.currentEnd()
      dummies2 <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      request <- ledger.getTransactionsRequest(
        transactionFormat =
          ledger.transactionFormat(Some(Seq(party)), transactionShape = LedgerEffects)
      )

      fromAndToBegin =
        request.update(
          _.beginExclusive := endOffsetAtTestStart,
          _.endInclusive := ledgerMiddle,
        )
      transactions <- ledger.transactionsWithVariants(
        fromAndToBegin
      )
    } yield {
      assert(
        dummies.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        dummies2.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies2.size} were instead",
      )
      assert(
        transactions.sizeIs == transactionsToSubmit,
        s"$transactionsToSubmit should have been received but ${transactions.size} were instead",
      )
      assertAcsDelta(
        transactions.flatMap(_.events),
        acsDelta = true,
        "The acs_delta field in created events should be set",
      )
    }
  })

  test(
    "TXStreamDummyFilterDummyIouDummy",
    "Updates stream with Dummy transaction filter should return empty stream if asking for a range containing sole Iou transaction surrounded by dummy transaction outside of the requested range",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      _ <- ledger.create(party, new Dummy(party))
      beforeIou <- ledger.currentEnd()
      _ <- ledger.create(
        party,
        new Iou(party, party, "USD", java.math.BigDecimal.ONE, Nil.asJava),
      )
      afterIou <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      request = ledger.getTransactionsRequestWithEnd(
        transactionFormat = ledger.transactionFormat(
          Some(Seq(party)),
          Seq(Dummy.TEMPLATE_ID),
          transactionShape = LedgerEffects,
        ),
        begin = beforeIou,
        end = Some(afterIou),
        descendingOrder = true,
      )
      transactions <- ledger.transactions(request)
    } yield {
      assert(
        transactions.isEmpty,
        s"Reverse stream with Dummy filter should be empty.",
      )
    }
  })

  test(
    "TXServeStreamInDescendingOrder",
    "Transaction with descendingOrder=true should be in descending order",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummies <- MonadUtil.sequentialTraverse(Range(0, 10))(_ =>
        ledger.create(party, new Dummy(party))
      )
      request <- ledger.getTransactionsRequest(
        transactionFormat =
          ledger.transactionFormat(Some(Seq(party)), transactionShape = LedgerEffects)
      )

      transactions <- ledger.transactions(
        request.update(_.descendingOrder := true)
      )
    } yield {
      assert(
        transactions.map(_.events.loneElement.getCreated.contractId) == dummies.reverse.map(
          _.contractId
        ),
        s"Expected contract ids in order ${dummies.reverse
            .map(_.contractId)}, got ${transactions.map(_.events.loneElement.getCreated.contractId)}",
      )
    }
  })

  test(
    "TXFetchFirstPageAsc",
    "Requesting a first page of should return the page containing the requested number of elements",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      contracts <- MonadUtil.sequentialTraverse(Range(0, 10))(_ =>
        ledger.create(party, new Dummy(party))
      )
      request <- ledger.getUpdatesPageRequest(
        transactionFormat =
          ledger.transactionFormat(Some(Seq(party)), transactionShape = LedgerEffects),
        pageSize = 5,
      )
      transactions <- ledger.transactionsSinglePage(request)
    } yield {
      assert(
        transactions.map(_.events.loneElement.getCreated.contractId) == contracts
          .take(5)
          .map(
            _.contractId
          ),
        s"Expected contract ids in order ${contracts
            .take(5)
            .map(_.contractId)}, got ${transactions.map(_.events.loneElement.getCreated.contractId)}",
      )
    }
  })

  test(
    "TXPagedFetchDynamicEnd",
    "Requesting paged updates with dynamic end should return a valid token after reaching an ledger end",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      rangeStart <- ledger.currentEnd()
      dummy1 <- ledger.create(party, new Dummy(party))
      dummy2 <- ledger.create(party, new Dummy(party))
      notDummy1 <- ledger.create(
        party,
        new Iou(party, party, "USD", java.math.BigDecimal.ONE, Nil.asJava),
      )
      ledgerEndAfterNotDummy <- ledger.currentEnd()
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        templateIds = Seq(Dummy.TEMPLATE_ID),
        transactionShape = LedgerEffects,
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = Some(rangeStart - 1L),
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
        pageToken = None,
      )
      page1 <- ledger.getUpdatesPageRaw(request)
      _ = assert(page1.nextPageToken.nonEmpty, s"Expected page with next page token, got $page1")
      emptyPage <- ledger.getUpdatesPageRaw(
        request.update(_.pageToken := page1.nextPageToken.value)
      )
      dummy3 <- ledger.create(party, new Dummy(party))
      page2 <- ledger.getUpdatesPageRaw(
        request.update(_.pageToken := emptyPage.nextPageToken.value)
      )
    } yield {
      assert(
        page1.updates.map(
          _.update.transaction.value.events.loneElement.getCreated.contractId
        ) == Seq(dummy1.contractId, dummy2.contractId),
        s"Expected two first updates ${Seq(dummy1.contractId, dummy2.contractId)}, but got ${page1.updates.map(
            _.update.transaction.value.events.loneElement.getCreated.contractId
          )}",
      )
      assert(
        page1.highestPageOffsetInclusive >= ledgerEndAfterNotDummy,
        s"highestPageOffsetInclusive range end does not cover not-dummy contract id",
      )
      assert(
        page1.highestPageOffsetInclusive == page2.lowestPageOffsetExclusive,
        "page2 range is not a direct continuation of page1 range",
      )
      assert(emptyPage.updates.isEmpty, s"Expected empty page, got ${emptyPage.updates}")
      assert(
        page2.nextPageToken.nonEmpty,
        s"Token at the last page should be non-empty",
      )
      assert(
        page2.updates.loneElement.update.transaction.value.events.loneElement.getCreated.contractId == dummy3.contractId,
        s"Second non-empty page should contain ${dummy3.contractId} contract, but got ${page2.updates.loneElement.update.transaction.value.events.loneElement.getCreated.contractId}",
      )
    }
  })

  test(
    "TXPagedFetchDescendingOrderBeyondEnd",
    "Requesting an updates page with a range range beyond ledger end and descending order should fail",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      rangeStart <- ledger.currentEnd()
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = Some(rangeStart - 1L),
        endOffsetInclusive = Some(rangeStart + 2L), // Beyond the end
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = true,
        pageToken = None,
      )

      failure <- ledger
        .getUpdatesPageRaw(request)
        .mustFail("Requesting descending page beyond ledger end should fail")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "TXPagedDynamicStartEndAscending",
    "Requesting update pages in ascending order with dynamic start end end should yield all transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      contracts <- MonadUtil
        .sequentialTraverse(Range(0, 10))(_ => ledger.create(party, new Dummy(party)))
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
        pageToken = None,
      )
      (transactions, token) <- fetchNPages(ledger)(request, 5)
    } yield {
      assert(
        contracts.sizeIs == transactions.length,
        s"Expected ${contracts.length} transactions, got ${transactions.length}, got ids: ${transactions
            .map(_.update.transaction.value.events.loneElement.getCreated.contractId)}, wanted: ${contracts
            .map(_.contractId)}",
      )
      assert(
        transactions.map(
          _.update.transaction.value.events.loneElement.getCreated.contractId
        ) == contracts.map(_.contractId),
        s"Expected ${contracts.reverse
            .map(_.contractId)} in this order, got ${transactions
            .map(_.update.transaction.value.events.loneElement.getCreated.contractId)}",
      )
      assert(
        token.nonEmpty,
        s"Continuation token from the last page should contain page token, but the token was missing",
      )
    }
  })

  test(
    "TXPagedDynamicStartEndDescending",
    "Requesting update pages in descending order with dynamic start end end should yield all transactions in descending order",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      contracts <- MonadUtil
        .sequentialTraverse(Range(0, 10))(_ => ledger.create(party, new Dummy(party)))
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = true,
        pageToken = None,
      )
      transactions <- ledger.transactionsAllPages(request, None)
    } yield {
      assert(
        transactions.map(
          _.update.transaction.value.events.loneElement.getCreated.contractId
        ) == contracts.reverse.map(_.contractId),
        s"Expected ${contracts.reverse
            .map(_.contractId)} in this order, got ${transactions
            .map(_.update.transaction.value.events.loneElement.getCreated.contractId)}",
      )
    }
  })

  test(
    "TXPagedDynamicPruningStartEndDescending",
    "Requesting update pages in descending order with dynamic start end end should yield all transactions until pruning is reached",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      dummy1 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy2 <- ledger.create(party, new Dummy(party))
      dummy3 <- ledger.create(party, new Dummy(party))

      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = true,
        pageToken = None,
      )

      firstPage <- ledger.getUpdatesPageRaw(request)
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      secondPage <- ledger.getUpdatesPageRaw(
        request.update(_.pageToken := firstPage.nextPageToken.value)
      )
    } yield {
      assert(
        secondPage.updates.isEmpty,
        s"The second page of dynamic bound response should updates should be empty due to pruning, but is ${secondPage.updates}",
      )
      assert(
        secondPage.lowestPageOffsetExclusive == pruningOffset,
        s"The second page should end at pruning bound $pruningOffset, but was ${secondPage.lowestPageOffsetExclusive} instead",
      )
      ()
    }
  })

  test(
    "TXPagedDynamicPruningInJustAfterThePageDescending",
    "Requesting update pages in descending order with dynamic start end end should yield all transactions until reaching the pruning offset after the last page",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      dummy1 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy2 <- ledger.create(party, new Dummy(party))
      dummy3 <- ledger.create(party, new Dummy(party))
      dummy4 <- ledger.create(party, new Dummy(party))

      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = true,
        pageToken = None,
      )

      firstPage <- ledger.getUpdatesPageRaw(request)
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      secondPage <- ledger.getUpdatesPageRaw(
        request.update(_.pageToken := firstPage.nextPageToken.value)
      )
    } yield {
      compareContractIds(
        "Second page should contain only one element due to pruning",
        secondPage.updates,
        Seq(dummy2),
      )
      assert(
        secondPage.nextPageToken.isEmpty,
        "The second page should not have a next page token as the contents after it are pruned",
      )
      assert(
        secondPage.lowestPageOffsetExclusive == pruningOffset,
        s"The second page should end at pruning bound $pruningOffset, but was ${secondPage.lowestPageOffsetExclusive} instead",
      )
      ()
    }
  })

  test(
    "TXPagedDynamicPruningStartEndInPageBoundaryDescending",
    "Requesting update pages in descending order with dynamic start end end should yield all transactions until pruning is reached in page boundary",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      dummy1 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy2 <- ledger.create(party, new Dummy(party))
      dummy3 <- ledger.create(party, new Dummy(party))
      dummy4 <- ledger.create(party, new Dummy(party))
      dummy5 <- ledger.create(party, new Dummy(party))

      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = true,
        pageToken = None,
      )

      firstPage <- ledger.getUpdatesPageRaw(request)
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      secondPage <- ledger.getUpdatesPageRaw(
        request.update(_.pageToken := firstPage.nextPageToken.value)
      )
    } yield {
      compareContractIds("First page contain two elements", firstPage.updates, Seq(dummy5, dummy4))
      compareContractIds(
        "Second page should contain two elements",
        secondPage.updates,
        Seq(dummy3, dummy2),
      )
      assert(
        secondPage.nextPageToken.isEmpty,
        "The second page should not have a next page token as the contents after it are pruned",
      )
      assert(
        secondPage.lowestPageOffsetExclusive == pruningOffset,
        s"The second page should end at pruning bound $pruningOffset, but was ${secondPage.lowestPageOffsetExclusive} instead",
      )
      ()
    }
  })

  test(
    "TXPagedDynamicStartAscendingPruning",
    "Ascending page update stream should start at pruning offset",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      dummy1 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy2 <- ledger.create(party, new Dummy(party))
      dummy3 <- ledger.create(party, new Dummy(party))
      lastOffset <- ledger.currentEnd()
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = Some(lastOffset),
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
        pageToken = None,
      )
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      firstPage <- ledger.getUpdatesPageRaw(request)
    } yield {
      compareContractIds("First page contains two elements", firstPage.updates, Seq(dummy2, dummy3))
      assert(
        firstPage.nextPageToken.isEmpty,
        "There should not be a next page",
      )
      assert(
        firstPage.lowestPageOffsetExclusive == pruningOffset,
        s"The second page should end at pruning bound $pruningOffset, but was ${firstPage.lowestPageOffsetExclusive} instead",
      )
      ()
    }
  })

  test(
    "TXPagedAscendingPruningCatchesUp",
    "Ascending page fetch should fail if pruning catches it",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      dummy1 <- ledger.create(party, new Dummy(party))
      dummy2 <- ledger.create(party, new Dummy(party))
      dummy3 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy4 <- ledger.create(party, new Dummy(party))
      lastOffset <- ledger.currentEnd()
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = None,
        endOffsetInclusive = None,
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
        pageToken = None,
      )
      firstPage <- ledger.getUpdatesPageRaw(request)
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      err <- ledger
        .getUpdatesPageRaw(request.update(_.pageToken := firstPage.getNextPageToken))
        .mustFail(
          s"Second page fetch should fail as it interferes with pruning, first page contents: ${firstPage.updates}"
        )
    } yield {
      compareContractIds("First page contains two elements", firstPage.updates, Seq(dummy1, dummy2))
      assertGrpcError(
        err,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some("precedes pruned offset"),
      )
    }
  })

  test(
    "TXPagedAscendingPruningBehindEnd",
    "Ascending page fetch should fail if pruning offset goes beyond query end",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Move the ledger end forward
      beginExclusive <- ledger.currentEnd()
      dummy1 <- ledger.create(party, new Dummy(party))
      dummy2 <- ledger.create(party, new Dummy(party))
      endInclusive <- ledger.currentEnd()
      dummy3 <- ledger.create(party, new Dummy(party))
      dummy4 <- ledger.create(party, new Dummy(party))
      pruningOffset <- ledger.currentEnd()
      dummy5 <- ledger.create(party, new Dummy(party)) // One more to make pruning possible
      transactionFormat = ledger.transactionFormat(
        Some(Seq(party)),
        transactionShape = LedgerEffects,
        templateIds = Seq(Dummy.TEMPLATE_ID),
      )
      request = GetUpdatesPageRequest(
        beginOffsetExclusive = Some(beginExclusive),
        endOffsetInclusive = Some(endInclusive),
        maxPageSize = Some(2),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(transactionFormat),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
        pageToken = None,
      )
      _ <- ledger.pruneCantonSafe(
        pruningOffset,
        party,
        p => new Dummy(p).create.commands,
      )
      err <- ledger
        .getUpdatesPageRaw(request)
        .mustFail(
          s"First strict page fetch should fail as it interferes with pruning"
        )
    } yield {
      assertGrpcError(
        err,
        RequestValidationErrors.ParticipantPrunedDataAccessed,
        Some("precedes pruned offset"),
      )
    }
  })

  private def fetchNPages(ledger: ParticipantTestContext)(
      request: GetUpdatesPageRequest,
      count: Int,
      transactions: Seq[GetUpdateResponse] = Seq(),
  )(implicit ec: ExecutionContext): Future[(Seq[GetUpdateResponse], Option[ByteString])] =
    ledger.getUpdatesPageRaw(request).flatMap { response =>
      val remainingPages = count - 1
      response.nextPageToken match {
        case token if remainingPages == 0 =>
          Future.successful((transactions ++ response.updates, token))
        case None if remainingPages > 0 =>
          Future.failed(
            new AssertionError(s"Expected $remainingPages more pages, but got no next page token")
          )
        case Some(token) =>
          fetchNPages(ledger)(
            request.update(_.pageToken := token),
            remainingPages,
            transactions ++ response.updates,
          )
        case _ => Future.failed(new IllegalArgumentException("remaining pages is negative"))
      }
    }

  private def compareContractIds(
      clue: String,
      updates: Seq[GetUpdateResponse],
      expectedIds: Seq[ContractId[?]],
  ): Unit =
    assert(
      updates.map(
        _.update.transaction.value.events.loneElement.getCreated.contractId
      ) == expectedIds.map(_.contractId),
      s"$clue, Expected: ${expectedIds.map(_.contractId)}, got ${updates
          .map(_.update.transaction.value.events.loneElement.getCreated.contractId)}",
    )
}
