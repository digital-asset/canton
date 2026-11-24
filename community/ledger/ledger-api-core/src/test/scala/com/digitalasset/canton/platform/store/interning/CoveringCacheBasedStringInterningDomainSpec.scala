// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoveringCacheBasedStringInterningDomainSpec extends AnyFlatSpec with Matchers {

  import CoveringCacheBasedStringInterningDomainSpec.*

  behavior of "CoveringCacheBasedStringInterningDomain"

  it should "handle a known value" in {
    val domain = newDomain()
    domain.addPrefixed(1, "b|one")
    domain.addPrefixed(2, "b|two")

    domain.tryExternalize(2) shouldBe Some(StringBox("two"))
    domain.externalize(2) shouldBe StringBox("two")
    domain.tryInternalize(StringBox("two")) shouldBe Some(2)
    domain.internalize(StringBox("two")) shouldBe 2

    domain.unsafe.tryExternalize(2) shouldBe Some("two")
    domain.unsafe.externalize(2) shouldBe "two"
    domain.unsafe.tryInternalize("two") shouldBe Some(2)
    domain.unsafe.internalize("two") shouldBe 2
  }

  it should "handle an unknown value" in {
    val domain = newDomain()
    domain.addPrefixed(1, "b|one")

    domain.tryExternalize(3) shouldBe None
    domain.tryInternalize(StringBox("three")) shouldBe None
    a[NoSuchElementException] should be thrownBy domain.externalize(3)
    a[NoSuchElementException] should be thrownBy domain.internalize(StringBox("three"))

    domain.unsafe.tryExternalize(3) shouldBe None
    domain.unsafe.tryInternalize("three") shouldBe None
    a[NoSuchElementException] should be thrownBy domain.unsafe.externalize(3)
    a[NoSuchElementException] should be thrownBy domain.unsafe.internalize("three")
  }

  it should "ignore entries with another prefix" in {
    // all domains are offered every entry, and each keeps only the ones with its own prefix
    val domain = newDomain()
    domain.addPrefixed(1, "a|one")
    domain.addPrefixed(2, "b|one")

    domain.tryExternalize(1) shouldBe None
    domain.tryExternalize(2) shouldBe Some(StringBox("one"))
    domain.tryInternalize(StringBox("one")) shouldBe Some(2)
  }

  it should "drop only the entries above the given id on reset" in {
    val domain = newDomain()
    domain.addPrefixed(1, "b|one")
    domain.addPrefixed(2, "b|two")
    domain.addPrefixed(3, "b|three")

    domain.resetTo(2)

    domain.tryExternalize(1) shouldBe Some(StringBox("one"))
    domain.tryExternalize(2) shouldBe Some(StringBox("two"))
    domain.tryExternalize(3) shouldBe None
    domain.tryInternalize(StringBox("one")) shouldBe Some(1)
    domain.tryInternalize(StringBox("two")) shouldBe Some(2)
    domain.tryInternalize(StringBox("three")) shouldBe None
  }
}

object CoveringCacheBasedStringInterningDomainSpec {

  private final case class StringBox(value: String)

  private def newDomain(): CoveringCacheBasedStringInterningDomain[StringBox] =
    new CoveringCacheBasedStringInterningDomain[StringBox](
      to = StringBox(_),
      from = _.value,
      prefix = "b|",
    )
}
