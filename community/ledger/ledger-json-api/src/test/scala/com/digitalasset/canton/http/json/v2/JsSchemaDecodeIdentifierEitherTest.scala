// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.value.Identifier
import io.circe.parser.decode
import io.circe.{DecodingFailure, Error}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside}

/** Tests error handling of an invalid `templateId` (the JSON representation of a
  * [[com.daml.ledger.api.v2.value.Identifier]]) as used by command submission endpoints such as
  * `submit-and-wait`. An invalid template id must surface as a decode failure rather than a thrown
  * exception, so the endpoint can report a 400/INVALID_ARGUMENT instead of failing the request.
  */
class JsSchemaDecodeIdentifierEitherTest
    extends AnyFreeSpec
    with Matchers
    with EitherValues
    with Inside {

  private val VALID_TEMPLATE_ID = "package123:Module.Name:Entity"
  private val TOO_FEW_SEGMENTS = "package123:ModuleName"
  private val TOO_MANY_SEGMENTS = "package123:ModuleName:Entity:extra"
  private val EMPTY_TEMPLATE_ID = ""

  private val expectedFormat = IdentifierConverter.expectedFormat

  private def expectedError(input: String): String =
    s"Invalid identifier format ($input) not matching the expected format ($expectedFormat)"

  private def invokeDecodeIdentifierEither(input: String): Either[Error, Identifier] = {
    import JsSchema.DirectScalaPbRwImplicits.*
    decode[Identifier](s"\"$input\"")
  }

  "decodeIdentifier" - {
    "should decode a valid <package>:<moduleName>:<entityName> template id" in {
      invokeDecodeIdentifierEither(VALID_TEMPLATE_ID).value shouldBe Identifier(
        packageId = "package123",
        moduleName = "Module.Name",
        entityName = "Entity",
      )
    }

    "should return a DecodingFailure when the template id has too few segments" in {
      val res = invokeDecodeIdentifierEither(TOO_FEW_SEGMENTS)
      inside(res) { case Left(DecodingFailure(message, _)) =>
        message shouldBe expectedError(TOO_FEW_SEGMENTS)
      }
    }

    "should return a DecodingFailure when the template id has too many segments" in {
      val res = invokeDecodeIdentifierEither(TOO_MANY_SEGMENTS)
      inside(res) { case Left(DecodingFailure(message, _)) =>
        message shouldBe expectedError(TOO_MANY_SEGMENTS)
      }
    }

    "should return a DecodingFailure when the template id is empty" in {
      val res = invokeDecodeIdentifierEither(EMPTY_TEMPLATE_ID)
      inside(res) { case Left(DecodingFailure(message, _)) =>
        message shouldBe expectedError(EMPTY_TEMPLATE_ID)
      }
    }
  }
}
