// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.LfContractId

sealed abstract class Error extends Product with Serializable

object Error {

  final case class TimestampAfterPrehead(
      requestedTimestamp: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
  ) extends Error

  final case class TimestampBeforePruning(
      requestedTimestamp: CantonTimestamp,
      prunedTimestamp: CantonTimestamp,
  ) extends Error

  final case class InconsistentSnapshot(missingContract: LfContractId) extends Error

}
