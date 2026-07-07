// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.language.LanguageVersion as LV
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class LanguageVersionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "allLfVersions is sorted" in {
    LV.allLfVersions shouldBe sorted
  }

  "Minor.compare orders all possible minor variants correctly" in {
    import LV.Minor

    // Construct a list of synthetic minors in expected ascending order:
    // Stable(1) < Staging(2, 1) < Staging(2, 2) < Stable(2) < Stable(3) < Staging(4, 1) < Staging(5, 1) < Stable(5) < Dev
    val minorsInOrder: List[Minor] = List(
      Minor.Stable(1),
      Minor.Staging(2, 1),
      Minor.Staging(2, 2),
      Minor.Stable(2),
      Minor.Stable(3),
      Minor.Staging(4, 1),
      Minor.Staging(5, 1),
      Minor.Stable(5),
      Minor.Dev,
    )

    val rank = minorsInOrder.zipWithIndex.toMap

    val minors = Table("minor", minorsInOrder*)

    forEvery(minors)(m1 =>
      forEvery(minors)(m2 =>
        withClue(s"${m1.pretty} vs ${m2.pretty}: ") {
          m1.compare(m2).sign shouldBe (rank(m1) compareTo rank(m2)).sign
        }
      )
    )
  }

  "fromString" should {

    "recognize known versions" in {
      val testCases = Table("version", (LV.allLegacyLfVersions ++ LV.allLfVersions)*)

      forEvery(testCases)(v => LV.fromString(v.pretty) shouldBe Right(v))
    }

    "reject invalid versions" in {
      val testCases = Table("invalid version", "1.1", "2", "14", "version", "2.1.11")

      forEvery(testCases)(s => LV.fromString(s) shouldBe a[Left[?, ?]])
    }
  }

}
