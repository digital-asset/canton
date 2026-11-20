// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

/** Service call tests where the notion of a successful (i.e. authorized) call is delegated to
  * ``successfulBehavior``, which each concrete service provides (e.g. a successful submission, or a
  * non-PERMISSION_DENIED rejection).
  */
trait SuccessfulServiceCallAuthTests extends ServiceCallAuthTests {

  /** Provided by [[SyncServiceCallAuthTests]] / [[ReadOnlyServiceCallAuthTests]] / ... of the
    * concrete service under test.
    */
  def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectSuccess(f)
}
