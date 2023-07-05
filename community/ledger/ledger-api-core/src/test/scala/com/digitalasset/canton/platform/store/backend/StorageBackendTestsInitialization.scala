// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{LedgerId, ParticipantId}
import com.digitalasset.canton.platform.common.MismatchException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitialization extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initialization)"

  it should "correctly handle repeated initialization" in {
    val ledgerId = LedgerId("ledger")
    val participantId = ParticipantId(Ref.ParticipantId.assertFromString("participant"))
    val otherLedgerId = LedgerId("otherLedger")
    val otherParticipantId = ParticipantId(Ref.ParticipantId.assertFromString("otherParticipant"))

    loggerFactory.assertLogs(
      within = {
        executeSql(
          backend.parameter.initializeParameters(
            ParameterStorageBackend.IdentityParams(
              ledgerId = ledgerId,
              participantId = participantId,
            ),
            loggerFactory,
          )
        )
        val error1 = intercept[RuntimeException](
          executeSql(
            backend.parameter.initializeParameters(
              ParameterStorageBackend.IdentityParams(
                ledgerId = otherLedgerId,
                participantId = participantId,
              ),
              loggerFactory,
            )
          )
        )
        val error2 = intercept[RuntimeException](
          executeSql(
            backend.parameter.initializeParameters(
              ParameterStorageBackend.IdentityParams(
                ledgerId = ledgerId,
                participantId = otherParticipantId,
              ),
              loggerFactory,
            )
          )
        )
        val error3 = intercept[RuntimeException](
          executeSql(
            backend.parameter.initializeParameters(
              ParameterStorageBackend.IdentityParams(
                ledgerId = otherLedgerId,
                participantId = otherParticipantId,
              ),
              loggerFactory,
            )
          )
        )
        executeSql(
          backend.parameter.initializeParameters(
            ParameterStorageBackend.IdentityParams(
              ledgerId = ledgerId,
              participantId = participantId,
            ),
            loggerFactory,
          )
        )

        error1 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
        error2 shouldBe MismatchException.ParticipantId(participantId, otherParticipantId)
        error3 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
      },
      assertions = _.errorMessage should include(
        "Found existing database with mismatching ledgerId: existing 'ledger', provided 'otherLedger'"
      ),
      _.errorMessage should include(
        "Found existing database with mismatching participantId: existing 'participant', provided 'otherParticipant'"
      ),
      _.errorMessage should include(
        "Found existing database with mismatching ledgerId: existing 'ledger', provided 'otherLedger'"
      ),
    )
  }
}
