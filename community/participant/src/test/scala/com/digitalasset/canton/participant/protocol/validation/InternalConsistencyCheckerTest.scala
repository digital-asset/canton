// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.protocol.*
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.nonempty.NonEmptyUtil
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

abstract class InternalConsistencyCheckerTest extends AnyWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext
  protected val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  private val dummyViews =
    NonEmptyUtil.fromUnsafe(factory.standardHappyCases(1).rootTransactionViewTrees)

  private val dummyTx = LfTransaction(Map.empty, ImmArray.empty)

  def checkViews(
      sut: InternalConsistencyChecker,
      views: Seq[FullTransactionViewTree],
  ): Either[ErrorWithInternalConsistencyCheck, Unit] =
    sut.check(NonEmptyUtil.fromUnsafe(views), Seq(dummyTx), Set.empty)

  def checkTransaction(
      sut: InternalConsistencyChecker,
      lfTransactions: Seq[LfTransaction],
      hostedKeys: Set[LfGlobalKey],
  ): Either[ErrorWithInternalConsistencyCheck, Unit] =
    sut.check(dummyViews, lfTransactions, hostedKeys)

  def checkStandardHappyCases(sut: InternalConsistencyChecker): Unit = {
    val relevantExamples = factory.standardHappyCases.filter(_.rootTransactionViewTrees.nonEmpty)
    forEvery(relevantExamples) { example =>
      s"checking $example" must {

        "yield the correct result" in {
          checkViews(sut, example.rootTransactionViewTrees) shouldBe Either.unit
        }

        "reinterpret views individually" in {
          example.transactionViewTrees.foreach { viewTree =>
            checkViews(sut, Seq(viewTree)) shouldBe Either.unit
          }
        }
      }
    }
  }

}
