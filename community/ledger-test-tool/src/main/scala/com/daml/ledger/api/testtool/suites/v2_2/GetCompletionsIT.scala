// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, WithTimeout}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class GetCompletionsIT extends LedgerTestSuite {
  private val within: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(2L)

  test(
    "GCSingleParty",
    "Read completions through GetCompletions for a single party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val createRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    for {
      beginExclusive <- ledger.currentEnd()
      _ <- ledger.submit(createRequest)
      completionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(ledger.getCompletionsRequest(beginExclusive)(alice))(
          _.commandId == createRequest.getCommands.commandId
        )
      )
    } yield {
      val completion = assertDefined(completionO, "Expected a completion for alice")
      assertEquals(
        "Wrong command identifier on completion",
        completion.commandId,
        createRequest.getCommands.commandId,
      )
      assertEquals(
        "Single-party GetCompletions should preserve the submitting party",
        completion.actAs.toSet,
        Set(alice.underlying.getValue),
      )
    }
  })

  test(
    "GCMultiParty",
    "Read completions through GetCompletions for multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val aliceRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger.submitRequest(bob, new Dummy(bob).create.commands)
    val aliceBobRequest = multiPartyRequest(ledger, alice, bob)
    val request = ledger.getCompletionsRequest(ledger.referenceOffset)(alice, bob)

    for {
      _ <- ledger.submit(aliceRequest)
      _ <- ledger.submit(bobRequest)
      _ <- ledger.submit(aliceBobRequest)
      aliceCompletionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(request)(_.commandId == aliceRequest.getCommands.commandId)
      )
      bobCompletionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(request)(_.commandId == bobRequest.getCommands.commandId)
      )
      aliceBobCompletionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(request)(_.commandId == aliceBobRequest.getCommands.commandId)
      )
    } yield {
      val aliceCompletion = assertDefined(aliceCompletionO, "Expected a completion for alice")
      val bobCompletion = assertDefined(bobCompletionO, "Expected a completion for bob")
      val aliceBobCompletion =
        assertDefined(aliceBobCompletionO, "Expected a completion for alice and bob")
      assertEquals(
        "Single-party completion for alice should keep alice as act_as",
        aliceCompletion.actAs.toSet,
        Set(alice.underlying.getValue),
      )
      assertEquals(
        "Single-party completion for bob should keep bob as act_as",
        bobCompletion.actAs.toSet,
        Set(bob.underlying.getValue),
      )
      assertEquals(
        "Multi-party completion should contain both act_as parties",
        aliceBobCompletion.actAs.toSet,
        Set(alice.underlying.getValue, bob.underlying.getValue),
      )
    }
  })

  test(
    "GCWildcardParties",
    "Read completions through GetCompletions with wildcard parties",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val aliceRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger.submitRequest(bob, new Dummy(bob).create.commands)
    val aliceBobRequest = multiPartyRequest(ledger, alice, bob)

    for {
      beginExclusive <- ledger.currentEnd()
      _ <- ledger.submit(aliceRequest)
      _ <- ledger.submit(bobRequest)
      _ <- ledger.submit(aliceBobRequest)
      explicitResponses <- ledger.completions(
        within,
        ledger.getCompletionsRequest(beginExclusive)(alice, bob),
      )
      wildcardResponses <- ledger.completions(
        within,
        ledger.getCompletionsRequest(beginExclusive)(),
      )
    } yield {
      val expectedCommandIds = Set(
        aliceRequest.getCommands.commandId,
        bobRequest.getCommands.commandId,
        aliceBobRequest.getCommands.commandId,
      )
      val explicitByCommandId =
        completionsByCommandId(explicitResponses.flatMap(_.completion), expectedCommandIds)
      val wildcardByCommandId =
        completionsByCommandId(wildcardResponses.flatMap(_.completion), expectedCommandIds)

      assertEquals(
        "Explicit multi-party and wildcard GetCompletions should return the same command identifiers",
        wildcardByCommandId.keySet,
        explicitByCommandId.keySet,
      )
      assertEquals(
        "Explicit multi-party and wildcard GetCompletions should expose the same act_as parties",
        wildcardByCommandId,
        explicitByCommandId,
      )
    }
  })

  test(
    "GCBeginExclusive",
    "Honor begin_exclusive in GetCompletions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val firstRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val secondRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)

    for {
      // submit is asynchronous, so anchor begin_exclusive to the first completion's actual offset
      // rather than to currentEnd() (which may still precede that completion).
      startOffset <- ledger.currentEnd()
      _ <- ledger.submit(firstRequest)
      firstCompletionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(ledger.getCompletionsRequest(startOffset)(alice))(
          _.commandId == firstRequest.getCommands.commandId
        )
      )
      firstCompletion =
        assertDefined(firstCompletionO, "Expected a completion for the first submission")
      _ <- ledger.submit(secondRequest)
      secondCompletionO <- WithTimeout(5.seconds)(
        ledger.findCompletion(ledger.getCompletionsRequest(firstCompletion.offset)(alice))(
          _.commandId == secondRequest.getCommands.commandId
        )
      )
      _ = assertDefined(secondCompletionO, "Expected a completion for the second submission")
      responses <- ledger.completions(
        within,
        ledger.getCompletionsRequest(firstCompletion.offset)(alice),
      )
    } yield {
      val commandIds = responses.flatMap(_.completion).map(_.commandId).toSet
      assert(
        !commandIds.contains(firstRequest.getCommands.commandId),
        "GetCompletions should not return completions at or before begin_exclusive",
      )
      assert(
        commandIds.contains(secondRequest.getCommands.commandId),
        "GetCompletions should return completions after begin_exclusive",
      )
    }
  })

  test(
    "GCTailing",
    "Completions should be served if added during the subscription",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val completionsToSubmitBefore = 14
    val completionsToRead = completionsToSubmitBefore + 1
    for {
      // Settle a batch of completions before opening the stream, then demand one more completion
      // than currently exists so the read must block at ledger end and serve the final completion
      // live, after the subscription has started.
      submittedBefore <- Future.sequence(
        Vector.fill(completionsToSubmitBefore)(
          ledger.submitAndWait(
            ledger.submitAndWaitRequest(alice, new Dummy(alice).create.commands)
          )
        )
      )
      completionsF = ledger.completions(
        completionsToRead,
        ledger.getCompletionsRequest(ledger.referenceOffset)(alice),
      )
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(alice, new Dummy(alice).create.commands)
      )
      completions <- completionsF
    } yield {
      assert(
        submittedBefore.sizeIs == completionsToSubmitBefore,
        s"$completionsToSubmitBefore completions should have been submitted before the subscription but ${submittedBefore.size} were instead",
      )
      assert(
        completions.sizeIs == completionsToRead,
        s"$completionsToRead completions should have been received but ${completions.size} were instead",
      )
    }
  })

  private def completionsByCommandId(
      completions: Seq[Completion],
      expectedCommandIds: Set[String],
  ): Map[String, Set[String]] =
    completions.iterator
      .filter(completion => expectedCommandIds.contains(completion.commandId))
      .map(completion => completion.commandId -> completion.actAs.toSet)
      .toMap

  private def multiPartyRequest(
      ledger: ParticipantTestContext,
      alice: Party,
      bob: Party,
  ) = {
    val request = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    request.withCommands(
      request.getCommands.copy(
        actAs = Seq(alice, bob)
      )
    )
  }
}
