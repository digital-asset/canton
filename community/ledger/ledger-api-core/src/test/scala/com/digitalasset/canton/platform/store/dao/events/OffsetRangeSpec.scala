// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.OffsetGen.offset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OffsetRangeSpec extends AnyWordSpec with Matchers with BaseTest {

  private def range(startInclusive: Long, endInclusive: Long): OffsetRange =
    OffsetRange(offset(startInclusive), offset(endInclusive))

  "intersection" should {
    "return the overlapping part for partially overlapping ranges" in {
      range(2, 8).intersection(range(5, 10)) shouldBe Some(range(5, 8))
      range(5, 10).intersection(range(2, 8)) shouldBe Some(range(5, 8))
    }

    "return the inner range when one range fully contains the other" in {
      range(2, 10).intersection(range(4, 6)) shouldBe Some(range(4, 6))
      range(4, 6).intersection(range(2, 10)) shouldBe Some(range(4, 6))
    }

    "return a single-point range when ranges touch at the boundary" in {
      range(2, 5).intersection(range(5, 9)) shouldBe Some(range(5, 5))
      range(5, 9).intersection(range(2, 5)) shouldBe Some(range(5, 5))
    }

    "return None for disjoint ranges" in {
      range(2, 4).intersection(range(5, 8)) shouldBe None
      range(5, 8).intersection(range(2, 4)) shouldBe None
    }

    "return the same range if a range is intersected with itself" in {
      range(2, 8).intersection(range(2, 8)) shouldBe Some(range(2, 8))
    }
  }

  "before" should {
    "return the prefix of this range before the overlapping section" in {
      range(2, 8).before(range(5, 10)) shouldBe Some(range(2, 4))
    }

    "return None when overlap starts before range start" in {
      range(2, 8).before(range(1, 3)) shouldBe None
    }

    "return None when overlap starts at this range start" in {
      range(2, 8).before(range(2, 5)) shouldBe None
    }

    "return the prefix when the other range is fully inside this range" in {
      range(2, 8).before(range(4, 5)) shouldBe Some(range(2, 3))
    }

    "return None for disjoint ranges" in {
      range(2, 4).before(range(5, 8)) shouldBe None
    }
  }

  "after" should {
    "return the suffix of this range after the overlapping section" in {
      range(2, 8).after(range(1, 3)) shouldBe Some(range(4, 8))
    }

    "return None when overlap ends at this range end" in {
      range(2, 8).after(range(5, 8)) shouldBe None
    }

    "return None when overlap ends before this range end" in {
      range(2, 8).after(range(7, 10)) shouldBe None
    }

    "return the suffix when the other range is fully inside this range" in {
      range(2, 8).after(range(4, 5)) shouldBe Some(range(6, 8))
    }

    "return None for disjoint ranges" in {
      range(2, 4).after(range(5, 8)) shouldBe None
    }
  }
}
