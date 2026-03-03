// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.base.error.*
import com.google.protobuf.any.Any
import com.google.rpc.status.Status
import io.circe.parser
import io.circe.syntax.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

class GrpcStatusEncoderSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues {
  import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*

  it should "encode Status with all error details" in {
    val someLoggingContext =
      new NoBaseLogging(correlationId = Some("corrId"), properties = Map.empty)
    case object SampleError
        extends ErrorCode("SOME_ERROR", ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd)(
          ErrorClass.root()
        ) {

      final case class Reject(override val cause: String)(implicit
          loggingContext: BaseErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause, extraContext = Map("ck" -> "cv")) {
        override def resources: Seq[(ErrorResource, String)] =
          Seq((ErrorResource.DevErrorType, "some/resource"))
      }
    }

    val error = SampleError.Reject("This is a sample error")(someLoggingContext)
    val jsonError = error.rpcStatus()(someLoggingContext).asJson

    jsonError shouldBe parser
      .parse("""{
               |  "code" : 11,
               |  "message" : "SOME_ERROR(12,corrId): This is a sample error",
               |  "details" : [
               |    {
               |      "typeUrl" : "type.googleapis.com/google.rpc.ErrorInfo",
               |      "value" : "CgpTT01FX0VSUk9SGggKAmNrEgJjdhoYCg9kZWZpbml0ZV9hbnN3ZXISBWZhbHNlGg4KCGNhdGVnb3J5EgIxMg==",
               |      "valueDecoded" : {
               |        "reason" : "SOME_ERROR",
               |        "domain" : "",
               |        "metadata" : {
               |          "ck" : "cv",
               |          "definite_answer" : "false",
               |          "category" : "12"
               |        },
               |        "unknownFields" : {
               |          "fields" : {
               |
               |          }
               |        }
               |      },
               |      "unknownFields" : {
               |        "fields" : {
               |
               |        }
               |      }
               |    },
               |    {
               |      "typeUrl" : "type.googleapis.com/google.rpc.RetryInfo",
               |      "value" : "CgIIAQ==",
               |      "valueDecoded" : {
               |        "retryDelay" : {
               |          "seconds" : 1,
               |          "nanos" : 0,
               |          "unknownFields" : {
               |            "fields" : {
               |
               |            }
               |          }
               |        },
               |        "unknownFields" : {
               |          "fields" : {
               |
               |          }
               |        }
               |      },
               |      "unknownFields" : {
               |        "fields" : {
               |
               |        }
               |      }
               |    },
               |    {
               |      "typeUrl" : "type.googleapis.com/google.rpc.RequestInfo",
               |      "value" : "CgZjb3JySWQ=",
               |      "valueDecoded" : {
               |        "requestId" : "corrId",
               |        "servingData" : "",
               |        "unknownFields" : {
               |          "fields" : {
               |
               |          }
               |        }
               |      },
               |      "unknownFields" : {
               |        "fields" : {
               |
               |        }
               |      }
               |    },
               |    {
               |      "typeUrl" : "type.googleapis.com/google.rpc.ResourceInfo",
               |      "value" : "Cg5ERVZfRVJST1JfVFlQRRINc29tZS9yZXNvdXJjZQ==",
               |      "valueDecoded" : {
               |        "resourceType" : "DEV_ERROR_TYPE",
               |        "resourceName" : "some/resource",
               |        "owner" : "",
               |        "description" : "",
               |        "unknownFields" : {
               |          "fields" : {
               |
               |          }
               |        }
               |      },
               |      "unknownFields" : {
               |        "fields" : {
               |
               |        }
               |      }
               |    }
               |  ]
               |}""".stripMargin)
      .value
  }

  it should "encode Status with unknown detail type" in {
    val unknownAny = Any(
      typeUrl = "type.googleapis.com/unknown.Type",
      value = com.google.protobuf.ByteString.copyFromUtf8("data"),
    )
    val status = Status(2, "unknown", Seq(unknownAny))

    val json = status.asJson
    val detailsOpt = json.hcursor.downField("details").values
    detailsOpt should not be empty
    val details = detailsOpt.value
    details.head.hcursor.get[String]("valueDecoded").isRight shouldBe true
    details.head.hcursor
      .get[String]("valueDecoded")
      .toOption
      .fold(fail("Expected valueDecoded to be present")) { valueDecoded =>
        valueDecoded should be("Unknown type for decoding")
      }
  }

  it should "encode Status with no details" in {
    val status = Status(0, "ok", Seq.empty)

    val json = status.asJson
    val detailsOpt = json.hcursor.downField("details").values
    detailsOpt should not be empty
    detailsOpt.fold(fail("Expected details to be present"))(_.isEmpty shouldBe true)
  }
}
