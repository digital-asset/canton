// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{ErrorCategory, ErrorClass, ErrorCode, ErrorGroup}
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.error.TestGroup.NestedGroup.MyCode
import com.digitalasset.canton.error.TestGroup.NestedGroup.MyCode.MyError
import com.digitalasset.canton.logging.ErrorLoggingContext
import org.slf4j.event.Level

object TestGroup extends ErrorGroup()(ErrorClass.root()) {
  object NestedGroup extends ErrorGroup() {
    object MyCode extends ErrorCode(id = "NESTED_CODE", ErrorCategory.ContentionOnSharedResources) {
      override def logLevel: Level = Level.ERROR
      case class MyError(arg: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "this is my error")
    }
  }
}

class CantonErrorTest extends BaseTestWordSpec {

  "canton errors" should {
    "log proper error messages and provides a nice string" in {
      loggerFactory.assertLogs(
        MyError("testArg"),
        x => {
          x.errorMessage should include(MyCode.id)
          x.errorMessage should include("this is my error")
          x.mdc should contain(("arg" -> "testArg"))
          val loc = x.mdc.get("location")
          loc should not be empty
          loc.exists(_.startsWith("CantonErrorTest")) shouldBe true
        },
      )

    }

    "ship the context as part of the status and support decoding from exceptions" in {
      val err = loggerFactory.suppressErrors(MyError("testArg"))

      val status = DecodedRpcStatus.fromStatusRuntimeException(err.asGrpcError).value

      status.retryIn should not be empty
      status.context("arg") shouldBe "testArg"
      status.id shouldBe MyCode.id
      status.category shouldBe MyCode.category

    }

  }

  "canton error codes" should {
    "allow to recover the recoverability from the string" in {
      implicit val klass = new ErrorClass(Nil)
      val code = new ErrorCode(id = "TEST_ERROR", ErrorCategory.ContentionOnSharedResources) {}
      ErrorCodeUtils.errorCategoryFromString(code.toMsg("bla bla", None)) should contain(
        ErrorCategory.ContentionOnSharedResources
      )
    }
  }

}
