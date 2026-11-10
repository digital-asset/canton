// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.participant.v30
import org.scalatest.wordspec.AnyWordSpec

class ImportPartyAcsForceFlagTest extends AnyWordSpec with BaseTest {

  "ImportPartyAcsForceFlag.fromProtoV30" should {
    "parse a known flag" in {
      val flag = ImportPartyAcsForceFlag.AllowImportAfterActivation
      ImportPartyAcsForceFlag.fromProtoV30(flag.toProtoV30) shouldBe Right(flag)
    }

    "reject the unspecified value" in {
      ImportPartyAcsForceFlag.fromProtoV30(
        v30.ImportPartyAcsForceFlag.IMPORT_PARTY_ACS_FORCE_FLAG_UNSPECIFIED
      ) shouldBe Left(UnrecognizedEnum("force_flags", 0))
    }

    "reject a value this participant does not know" in {
      ImportPartyAcsForceFlag.fromProtoV30(
        v30.ImportPartyAcsForceFlag.Unrecognized(42)
      ) shouldBe Left(UnrecognizedEnum("force_flags", 42))
    }
  }
}
