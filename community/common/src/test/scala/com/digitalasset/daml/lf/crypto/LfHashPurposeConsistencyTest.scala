// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// In the package of the Daml-LF hash to access its private purposes
package com.digitalasset.daml.lf.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.HashPurpose
import org.scalatest.wordspec.AnyWordSpec

class LfHashPurposeConsistencyTest extends AnyWordSpec with BaseTest {
  "The Daml-LF hash purposes reserved in HashPurpose" should {
    "have the same ids as the Daml-LF purposes" in {
      Seq(
        HashPurpose._LfReassignmentChangeId -> Hash.Purpose.ReassignmentChangeId,
        HashPurpose._LfLedgerApiReassignmentChangeId -> Hash.Purpose.LedgerApiReassignmentChangeId,
        HashPurpose._LfAssignmentReassignmentId -> Hash.Purpose.AssignmentReassignmentId,
      ).foreach { case (cantonPurpose, lfPurpose) =>
        cantonPurpose.id shouldBe lfPurpose.id.toInt
      }
    }
  }
}
