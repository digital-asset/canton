// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex

class ViewPositionTest extends BaseTestWordSpec {

  def mkIndex(i: Int): MerkleSeqIndex = MerkleSeqIndex(List.fill(i)(MerkleSeqIndex.Direction.Left))

  "Correctly determine descendants" in {
    val p1 = ViewPosition(List(mkIndex(1)))
    val p2 = ViewPosition(List(mkIndex(2), mkIndex(1)))
    val p3 = ViewPosition(List(mkIndex(3), mkIndex(1)))
    val p4 = ViewPosition(List(mkIndex(4), mkIndex(3), mkIndex(1)))

    ViewPosition.isDescendant(p1, p1) shouldBe true
    ViewPosition.isDescendant(p2, p1) shouldBe true
    ViewPosition.isDescendant(p3, p1) shouldBe true
    ViewPosition.isDescendant(p4, p1) shouldBe true

    ViewPosition.isDescendant(p1, p2) shouldBe false
    ViewPosition.isDescendant(p3, p2) shouldBe false
    ViewPosition.isDescendant(p4, p2) shouldBe false

    ViewPosition.isDescendant(p1, p3) shouldBe false
    ViewPosition.isDescendant(p2, p3) shouldBe false
    ViewPosition.isDescendant(p4, p3) shouldBe true

    ViewPosition.isDescendant(p1, p4) shouldBe false
    ViewPosition.isDescendant(p2, p4) shouldBe false
    ViewPosition.isDescendant(p3, p4) shouldBe false
  }
}
