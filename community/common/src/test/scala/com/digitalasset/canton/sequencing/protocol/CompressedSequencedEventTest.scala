// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.BaseTestWordSpec

class CompressedSequencedEventTest extends BaseTestWordSpec {

  "CompressedSequencedEvent.versioningTable" should {
    // CompressedSequencedEvent deserializes the same wire format as SequencedEvent while keeping
    // the batch compressed, so its table must register the exact same proto versions mapped to the
    // same protocol versions. This guards against adding a proto version to one table only.
    "stay in sync with SequencedEvent.versioningTable" in {
      val compressed =
        CompressedSequencedEvent.versioningTable.table.view.mapValues(_.representative).toMap
      val decompressed =
        SequencedEvent.versioningTable.table.view.mapValues(_.representative).toMap

      compressed shouldBe decompressed
    }
  }
}
