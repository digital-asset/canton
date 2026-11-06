// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Overrides a safety check of the party ACS import. */
sealed abstract class ImportPartyAcsForceFlag(val toProtoV30: v30.ImportPartyAcsForceFlag)
    extends Product
    with Serializable

object ImportPartyAcsForceFlag {

  /** Imports even though the participant has processed events past the party's activation. */
  case object AllowImportAfterActivation
      extends ImportPartyAcsForceFlag(
        v30.ImportPartyAcsForceFlag.IMPORT_PARTY_ACS_FORCE_FLAG_ALLOW_IMPORT_AFTER_ACTIVATION
      )

  private val all: Map[v30.ImportPartyAcsForceFlag, ImportPartyAcsForceFlag] =
    Seq[ImportPartyAcsForceFlag](AllowImportAfterActivation)
      .map(flag => flag.toProtoV30 -> flag)
      .toMap

  /** Rejects unspecified and unknown values, rather than ignoring an override the client relies on.
    */
  def fromProtoV30(flagP: v30.ImportPartyAcsForceFlag): ParsingResult[ImportPartyAcsForceFlag] =
    all.get(flagP).toRight(UnrecognizedEnum("force_flags", flagP.value))
}
