// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamRequest as GrpcCompletionStreamRequest,
  GetCompletionsRequest,
}
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.ledger.api.validation.CompletionServiceRequestValidator.GetCompletionsStreamRequest
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class CompletionServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ErrorLoggingContext = NoLogging
  private val grpcCompletionReq = GrpcCompletionStreamRequest(
    expectedUserId,
    List(party),
    offsetLong,
  )
  private val completionReq = CompletionStreamRequest(
    Ref.UserId.assertFromString(expectedUserId),
    List(party).toSet,
    offset,
  )

  private val validator = CompletionServiceRequestValidator

  "CompletionRequestValidation" when {

    "validating gRPC completion requests" should {

      "accept plain requests" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq)
        ) { case Right(req) =>
          req shouldBe completionReq
        }
      }

      "return the correct error on missing user ID" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withUserId("")
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: user_id",
          metadata = Map.empty,
        )
      }

      "accept requests with begin exclusive offset zero" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq.withBeginExclusive(0))
        ) { case Right(req) =>
          req shouldBe completionReq.copy(offset = None)
        }
      }

      "return the correct error on negative begin exclusive offset" in {
        requestMustFailWith(
          request = validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withBeginExclusive(-100)
          ),
          code = INVALID_ARGUMENT,
          description =
            "NEGATIVE_OFFSET(8,0): Offset -100 in begin_exclusive is a negative integer: the offset in begin_exclusive field has to be a non-negative integer (>=0)",
          metadata = Map.empty,
        )
      }

      "tolerate all fields filled out" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(grpcCompletionReq)
        ) { case Right(req) =>
          req shouldBe completionReq
        }
      }

      "tolerate empty offset (participant begin)" in {
        inside(
          validator.validateGrpcCompletionStreamRequest(
            grpcCompletionReq.withBeginExclusive(0L)
          )
        ) { case Right(req) =>
          req.userId shouldEqual expectedUserId
          req.parties shouldEqual Set(party)
          req.offset shouldBe empty
        }
      }

    }

    "validate api completion requests" should {

      "accept simple requests" in {
        inside(
          validator.validateCompletionStreamRequest(completionReq, ledgerEnd)
        ) { case Right(req) =>
          req shouldBe completionReq
        }

      }

      "return the correct error on missing party" in {
        requestMustFailWith(
          request = validator.validateCompletionStreamRequest(
            completionReq.copy(parties = Set.empty),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: parties",
          metadata = Map.empty,
        )
      }

      "return the correct error when offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateCompletionStreamRequest(
            completionReq.copy(offset = ledgerEnd.map(_.increment)),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            s"OFFSET_AFTER_LEDGER_END(12,0): Begin offset (${ledgerEnd.value.unwrap + 1}) is after ledger end (${ledgerEnd.value.unwrap})",
          metadata = Map("definite_answer" -> "false", "category" -> "12"),
          retryDelay = Seq(Duration.ofSeconds(1L)),
        )
      }

      "tolerate empty offset (participant begin)" in {
        inside(
          validator.validateCompletionStreamRequest(
            completionReq.copy(offset = None),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.userId shouldEqual expectedUserId
          req.parties shouldEqual Set(party)
          req.offset shouldBe empty
        }
      }
    }

    "validate GetCompletions requests" should {

      "accept requests with parties" in {
        val request = GetCompletionsRequest(List(party), offsetLong)
        inside(validator.validateGetCompletionsRequest(request, ledgerEnd)) { case Right(req) =>
          req shouldBe GetCompletionsStreamRequest(
            Set(party),
            offset,
          )
        }
      }

      "accept requests with empty parties" in {
        val request = GetCompletionsRequest(List.empty, offsetLong)
        inside(validator.validateGetCompletionsRequest(request, ledgerEnd)) { case Right(req) =>
          req.parties shouldBe empty
          req.offset shouldBe offset
        }
      }

      "reject invalid party format" in {
        requestMustFailWith(
          request = validator.validateGetCompletionsRequest(
            GetCompletionsRequest(List("??invalid_party??"), offsetLong),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: non expected character 0x3f in Daml-LF Party \"??invalid_party??\"",
          metadata = Map.empty,
        )
      }

      "reject offset beyond ledger end" in {
        requestMustFailWith(
          request = validator.validateGetCompletionsRequest(
            GetCompletionsRequest(List(party), ledgerEnd.value.unwrap + 1),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            s"OFFSET_AFTER_LEDGER_END(12,0): Begin offset (${ledgerEnd.value.unwrap + 1}) is after ledger end (${ledgerEnd.value.unwrap})",
          metadata = Map("definite_answer" -> "false", "category" -> "12"),
          retryDelay = Seq(Duration.ofSeconds(1L)),
        )
      }

      "reject negative offset" in {
        requestMustFailWith(
          request = validator.validateGetCompletionsRequest(
            GetCompletionsRequest(List(party), -100),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "NEGATIVE_OFFSET(8,0): Offset -100 in begin_exclusive is a negative integer: the offset in begin_exclusive field has to be a non-negative integer (>=0)",
          metadata = Map.empty,
        )
      }

      "accept zero offset (participant begin)" in {
        inside(
          validator.validateGetCompletionsRequest(
            GetCompletionsRequest(List(party), 0),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.offset shouldBe None
        }
      }
    }

    "validating the transaction hash for GetCompletionByHash" should {

      "reject an empty hash" in {
        requestMustFailWith(
          request = validator.validateCompletionByHash(ByteString.EMPTY),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Missing field: transaction_hash",
          metadata = Map.empty,
        )
      }

      "accept a non-empty hash" in {
        val hash = ByteString.copyFrom(Array.fill(32)(1.toByte))
        inside(validator.validateCompletionByHash(hash)) { case Right(result) =>
          result shouldBe hash
        }
      }
    }
  }
}
