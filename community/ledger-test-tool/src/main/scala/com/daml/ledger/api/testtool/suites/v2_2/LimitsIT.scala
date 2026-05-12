// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.semantic.limits.*

import scala.jdk.CollectionConverters.*

final class LimitsIT extends LedgerTestSuite {

  test(
    "LLargeMapInContract",
    "Create a contract with a field containing large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice.getValue)).toMap.asJava
    for {
      contract: WithMap.ContractId <- ledger.create(alice, new WithMap(alice, elements))(
        WithMap.COMPANION
      )
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Noop())
    } yield {
      ()
    }
  })

  test(
    "LLargeMapInChoice",
    "Exercise a choice with a large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice.getValue)).toMap.asJava
    for {
      contract: WithMap.ContractId <- ledger.create(
        alice,
        new WithMap(alice, Map.empty[String, String].asJava),
      )(WithMap.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Expand(elements))
    } yield {
      ()
    }
  })

  test(
    "LLargeListInContract",
    "Create a contract with a field containing large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d").asJava
    for {
      contract: WithList.ContractId <- ledger.create(alice, new WithList(alice, elements))(
        WithList.COMPANION
      )
      _ <- ledger.exercise(alice, contract.exerciseWithList_Noop())
    } yield {
      ()
    }
  })

  test(
    "LLargeListInChoice",
    "Exercise a choice with a large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d").asJava
    for {
      contract: WithList.ContractId <- ledger
        .create(alice, new WithList(alice, List.empty[String].asJava))(WithList.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseWithList_Expand(elements))
    } yield {
      ()
    }
  })

  test(
    "LDeepTransaction",
    "Create a transaction with a long chain of exercises",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    // TODO(#26565) should handle at least depth=10000
    val depth = 100
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseBuild(depth, 1))
    } yield ()
  })

  test(
    "LWideTransaction",
    "Create a transaction with many sibling exercises",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseBuild(1, 12500))
    } yield ()
  })

  test(
    "LWithLargeTransaction",
    "Create a large balanced binary transaction tree",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseBuild(12, 2))
    } yield ()
  })

  test(
    "ExternalWithDeepTransaction",
    "External submission with a long chain of exercises",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    // TODO(#26565) should handle at least depth=10000
    val depth = 100
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      prepareRequest = ledger.prepareSubmissionRequest(
        alice,
        contract.exerciseBuild(depth, 1).commands(),
      )
      _ <- ledger.prepareSubmission(prepareRequest)
    } yield {
      ()
    }
  })

  test(
    "ExternalWithWideTransaction",
    "External submission with many sibling exercises",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      prepareRequest = ledger.prepareSubmissionRequest(
        alice,
        contract.exerciseBuild(1, 12000).commands(),
      )
      _ <- ledger.prepareSubmission(prepareRequest)
    } yield ()
  })

  test(
    "ExternalWithLargeTransaction",
    "External submission of a large balanced binary transaction tree",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      contract: Tree.ContractId <- ledger.create(alice, new Tree(alice))(Tree.COMPANION)
      prepareRequest = ledger.prepareSubmissionRequest(
        alice,
        contract.exerciseBuild(8, 2).commands(),
      )
      _ <- ledger.prepareSubmission(prepareRequest)
    } yield {
      ()
    }
  })

}
