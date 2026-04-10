// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.client.pool.InternalSequencerConnection.{
  ConnectionAttributes,
  SequencerConnectionError,
  SequencerConnectionState,
}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class GrpcInternalSequencerConnectionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  import ConnectionPoolTestHelpers.*

  "GrpcInternalSequencerConnection" should {
    "be validated in the happy path" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
      )
      withConnection(responses) { (connection, listener) =>
        connection.start().valueOrFail("start connection")

        listener.shouldStabilizeOn(SequencerConnectionState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        responses.assertAllResponsesSent()
      }
    }

    "refuse to start if it is in a fatal state" in {
      val errorMessage = "Validation failure: Failed handshake: bad handshake"

      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")

            listener.shouldStabilizeOn(SequencerConnectionState.Fatal(Some(errorMessage)))
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(errorMessage),
                "Handshake fails",
              )
            )
          ),
        )

        // Try to restart
        inside(connection.start()) {
          case Left(SequencerConnectionError.InvalidStateError(message)) =>
            message shouldBe s"The connection is in Fatal($errorMessage) state and cannot be started"
        }

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the returned API is not for a sequencer" in {
      val errorMessage = "Validation failure: Bad API: this is not a valid API info"
      val responses = TestResponses(
        apiResponses = Seq(incorrectApiResponse)
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionState.Fatal(Some(errorMessage)))
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(errorMessage),
                "API response is invalid",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the protocol handshake fails" in {
      val errorMessage = "Validation failure: Failed handshake: bad handshake"

      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionState.Fatal(Some(errorMessage)))
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(errorMessage),
                "Protocol handshake fails",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if an expected sequencer ID is specified and it is incorrect" in {
      val errorMessage = "Validation failure: Connection is not on expected sequencer: " +
        s"expected Some(${testSequencerId(666)}), got ${testSequencerId(1)}"

      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
      )
      val expectedSequencerIdO = Some(testSequencerId(666))
      withConnection(responses, expectedSequencerIdO = expectedSequencerIdO) {
        (connection, listener) =>
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              connection.start().valueOrFail("start connection")
              listener.shouldStabilizeOn(SequencerConnectionState.Fatal(Some(errorMessage)))
              connection.attributes shouldBe None
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.warningMessage should include(errorMessage),
                  "Protocol handshake fails",
                )
              )
            ),
          )

          responses.assertAllResponsesSent()
      }
    }

    "pass validation if an expected sequencer ID is specified and it is correct" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
      )
      val expectedSequencerIdO = Some(testSequencerId(1))
      withConnection(responses, expectedSequencerIdO = expectedSequencerIdO) {
        (connection, listener) =>
          connection.start().valueOrFail("start connection")

          listener.shouldStabilizeOn(SequencerConnectionState.Validated)
          connection.attributes shouldBe Some(correctConnectionAttributes)

          responses.assertAllResponsesSent()
      }
    }

    "validate the connection attributes after restart" in {
      val attributes1 = ConnectionAttributes(
        physicalSynchronizerId = testSynchronizerId(1),
        sequencerId = testSequencerId(1),
        staticParameters = defaultStaticSynchronizerParameters,
      )
      val attributes2 = ConnectionAttributes(
        physicalSynchronizerId = testSynchronizerId(2),
        sequencerId = testSequencerId(2),
        staticParameters = defaultStaticSynchronizerParameters,
      )
      val errorMessage = "Attributes mismatch: Sequencer connection has changed attributes: " +
        s"expected $attributes1, got $attributes2"

      val responses = TestResponses(
        apiResponses = Seq.fill(2)(correctApiResponse),
        handshakeResponses = Seq.fill(2)(successfulHandshake),
        synchronizerAndSeqIdResponses =
          Seq(correctSynchronizerIdResponse1, correctSynchronizerIdResponse2),
        staticParametersResponses = Seq.fill(2)(correctStaticParametersResponse),
      )
      withConnection(responses) { (connection, listener) =>
        connection.start().valueOrFail("start connection")
        listener.shouldStabilizeOn(SequencerConnectionState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        listener.clear()
        connection.fail("test")
        listener.shouldStabilizeOn(SequencerConnectionState.Stopped(Some("test")))
        listener.clear()

        // A different identity triggers a warning and the connection never gets validated
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionState.Fatal(Some(errorMessage)))
            // Synchronizer info does not change
            connection.attributes shouldBe Some(correctConnectionAttributes)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(errorMessage),
                "Different attributes after restart",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }
  }
}
