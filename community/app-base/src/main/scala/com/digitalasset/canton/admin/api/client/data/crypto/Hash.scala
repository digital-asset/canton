// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}

sealed abstract class HashAlgorithm(val name: String) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[HashAlgorithm] = HashAlgorithm
}

object HashAlgorithm extends PrettyPrintingCompanion[HashAlgorithm] {
  override protected val pretty: Pretty[HashAlgorithm] = prettyOfString(_.name)

  case object Sha256 extends HashAlgorithm("SHA-256")
}
