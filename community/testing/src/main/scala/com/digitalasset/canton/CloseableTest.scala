// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait CloseableTest extends BeforeAndAfterAll with FlagCloseable with HasCloseContext {
  self: Suite =>

  override protected def afterAll(): Unit = close()

}
