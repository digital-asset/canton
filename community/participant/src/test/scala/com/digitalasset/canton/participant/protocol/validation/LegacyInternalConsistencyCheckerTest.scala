// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.digitalasset.canton.data.PathRollbackContextFactory
import com.digitalasset.canton.protocol.PathRollbackContext
import com.digitalasset.canton.topology.ParticipantId

import scala.util.Random

class LegacyInternalConsistencyCheckerTest extends InternalConsistencyCheckerTest {

  def checkRollbackScopeOrder(): Unit =
    "checkRollbackScopeOrder should validate sequences of scopes" in {
      val ops: Seq[PathRollbackContext => PathRollbackContext] = Seq(
        _.enterRollback,
        _.enterRollback,
        _.tryExitRollback,
        _.enterRollback,
        _.tryExitRollback,
        _.tryExitRollback,
        _.enterRollback,
        _.tryExitRollback,
      )

      val (_, testScopes) =
        ops.foldLeft((PathRollbackContext.empty, Seq(PathRollbackContext.empty))) {
          case ((c, seq), op) =>
            val nc = op(c)
            (nc, seq :+ nc)
        }

      Random.shuffle(testScopes).sorted shouldBe testScopes

      PathRollbackContextFactory.checkRollbackScopeOrder(testScopes) shouldBe Either.unit
      PathRollbackContextFactory.checkRollbackScopeOrder(testScopes.reverse).isLeft shouldBe true

    }

  "Internal consistency checker" when {

    val participantId: ParticipantId = ParticipantId("test")
    val sut = new NextGenInternalConsistencyChecker(participantId, loggerFactory)

    "rollback scope order" should checkRollbackScopeOrder()

    "standard happy cases" should checkStandardHappyCases(sut)

  }
}
