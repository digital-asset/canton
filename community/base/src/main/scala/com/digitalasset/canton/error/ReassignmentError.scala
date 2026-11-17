// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.base.error.ErrorCode

trait ReassignmentError extends CantonBaseError {

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)
}

/** Reassignment errors are derived from BaseCantonError and need to be logged explicitly */
abstract class ReassignmentErrorImpl(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCode)
    extends ReassignmentError
