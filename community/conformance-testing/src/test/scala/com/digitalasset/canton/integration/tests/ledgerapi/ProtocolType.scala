// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

/** The Ledger API flavour a conformance test run exercises. Some test cases only fail against one
  * of the two, so exclusions are selected per flavour.
  */
sealed trait ProtocolType extends Product with Serializable

object ProtocolType {
  case object Grpc extends ProtocolType
  case object Json extends ProtocolType

  def fromUseJson(useJson: Boolean): ProtocolType = if (useJson) Json else Grpc
}
