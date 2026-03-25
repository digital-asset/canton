// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import com.digitalasset.daml.lf.data.Ref.*
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class RefTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks with EitherValues {

  "Name.fromString" - {
    "reject" - {
      "digit at the start" in {
        Name.fromString("9test") shouldBe a[Left[?, ?]]
      }

      "bad symbols" in {
        val testCases =
          Table(
            "non valid Name",
            "test%",
            "test-",
            "test@",
            "test:", // used for QualifiedName string encoding
            "test.", // used for DottedName string encoding
            "test#", // used for QualifiedChoiceId string encoding
          )
        forEvery(testCases)(Name.fromString(_) shouldBe a[Left[?, ?]])
      }

      "unicode" in {
        Name.fromString("à") shouldBe a[Left[?, ?]]
        Name.fromString("ਊ") shouldBe a[Left[?, ?]]
      }

      "too long" in {
        Name.fromString("a" * 1000) shouldBe a[Right[?, ?]]
        Name.fromString("a" * 1001) shouldBe a[Left[?, ?]]
        Name.fromString("a" * 10000) shouldBe a[Left[?, ?]]
      }

      "empty" in {
        Name.fromString("") shouldBe a[Left[?, ?]]
      }
    }

    "accepts" - {
      "dollar" in {
        Name.fromString("$") shouldBe Right("$")
        Name.fromString("$blAH9") shouldBe Right("$blAH9")
        Name.fromString("foo$bar") shouldBe Right("foo$bar")
        Name.fromString("baz$") shouldBe Right("baz$")
      }

      "underscore" in {
        Name.fromString("_") shouldBe Right("_")
        Name.fromString("_blAH9") shouldBe Right("_blAH9")
        Name.fromString("foo_bar") shouldBe Right("foo_bar")
        Name.fromString("baz_") shouldBe Right("baz_")
      }
    }
  }

  "DottedName.fromString" - {
    "reject empty" in {
      DottedName.fromString("") shouldBe a[Left[?, ?]]
    }

    "rejects empty segment" in {
      DottedName.fromString(".") shouldBe a[Left[?, ?]]
      DottedName.fromString("a..a") shouldBe a[Left[?, ?]]
      DottedName.fromString("a.") shouldBe a[Left[?, ?]]
      DottedName.fromString(".a") shouldBe a[Left[?, ?]]
    }

    "rejects colon" in {
      DottedName.fromString("foo:bar") shouldBe a[Left[?, ?]]
    }

    "reject too long string" in {
      DottedName.fromString("a" * 1000) shouldBe a[Right[?, ?]]
      DottedName.fromString("a" * 1001) shouldBe a[Left[?, ?]]
      DottedName.fromString("a" * 10000) shouldBe a[Left[?, ?]]
      DottedName.fromString("a" * 500 + "." + "a" * 499) shouldBe a[Right[?, ?]]
      DottedName.fromString("a" * 500 + "." + "a" * 500) shouldBe a[Left[?, ?]]
      DottedName.fromString("a" * 5000 + "." + "a" * 5000) shouldBe a[Left[?, ?]]
      DottedName.fromString("a" * 10000) shouldBe a[Left[?, ?]]
      DottedName.fromString("a." * 499 + "aa") shouldBe a[Right[?, ?]]
      DottedName.fromString("a." * 500 + "a") shouldBe a[Left[?, ?]]
      DottedName.fromString("a." * 5000 + "a") shouldBe a[Left[?, ?]]
    }

    "rejects empty segments" in {
      DottedName.fromString(".") shouldBe a[Left[?, ?]]
      DottedName.fromString(".foo.") shouldBe a[Left[?, ?]]
      DottedName.fromString(".foo") shouldBe a[Left[?, ?]]
      DottedName.fromString("foo.") shouldBe a[Left[?, ?]]
      DottedName.fromString("foo..bar") shouldBe a[Left[?, ?]]
    }

    "accepts good segments" - {
      "dollar" in {
        DottedName.fromString("$.$blAH9.foo$bar.baz$").map(_.segments) shouldBe Right(
          ImmArray("$", "$blAH9", "foo$bar", "baz$")
        )
      }

      "underscore" in {
        DottedName.fromString("_._blAH9.foo_bar.baz_").map(_.segments) shouldBe Right(
          ImmArray("_", "_blAH9", "foo_bar", "baz_")
        )
      }
    }
  }

  "DottedName.fromSegments" - {
    "rejects" - {
      "empty" in {
        DottedName.fromSegments(List.empty) shouldBe a[Left[?, ?]]
      }
      "too long" in {
        val s1 = Name.assertFromString("a")
        val s499 = Name.assertFromString("a" * 499)
        val s500 = Name.assertFromString("a" * 500)
        val s1000 = Name.assertFromString("a" * 1000)
        DottedName.fromSegments(List.fill(500)(s1)) shouldBe a[Right[?, ?]] // length = 999
        DottedName.fromSegments(List.fill(501)(s1)) shouldBe a[Left[?, ?]] // length = 1001
        DottedName.fromSegments(List.fill(5000)(s1)) shouldBe a[Left[?, ?]] // length = 5002
        DottedName.fromSegments(List(s499, s500)) shouldBe a[Right[?, ?]] // length = 1000
        DottedName.fromSegments(List(s500, s500)) shouldBe a[Left[?, ?]] // length = 1001
        DottedName.fromSegments(List(s1000)) shouldBe a[Right[?, ?]] // length = 1000
        DottedName.fromSegments(List(s1000, s1)) shouldBe a[Left[?, ?]] // length = 1002
        DottedName.fromSegments(List(s1, s1000)) shouldBe a[Left[?, ?]] // length = 1002
        DottedName.fromSegments(List(s1000, s1000)) shouldBe a[Left[?, ?]] // length = 2001
      }
    }
  }

  private[this] val dottedNamesInOrder = List(
    DottedName.assertFromString("a"),
    DottedName.assertFromString("a.a"),
    DottedName.assertFromString("aa"),
    DottedName.assertFromString("b"),
    DottedName.assertFromString("b.a"),
  )

  testOrdered("DottedName", dottedNamesInOrder)

  "QualifiedName.fromString" - {
    "rejects no colon" in {
      QualifiedName.fromString("foo") shouldBe a[Left[?, ?]]
    }

    "rejects multiple colons" in {
      QualifiedName.fromString("foo:bar:baz") shouldBe a[Left[?, ?]]
    }

    "rejects empty dotted names" in {
      QualifiedName.fromString(":bar") shouldBe a[Left[?, ?]]
      QualifiedName.fromString("bar:") shouldBe a[Left[?, ?]]
    }

    "accepts valid qualified names" in {
      QualifiedName.fromString("foo:bar").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo"),
        name = DottedName.assertFromString("bar"),
      )
      QualifiedName.fromString("foo.bar:baz").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo.bar"),
        name = DottedName.assertFromString("baz"),
      )
      QualifiedName.fromString("foo:bar.baz").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo"),
        name = DottedName.assertFromString("bar.baz"),
      )
      QualifiedName.fromString("foo.bar:baz.quux").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo.bar"),
        name = DottedName.assertFromString("baz.quux"),
      )
    }
  }

  "PackageVersion.fromString" - {
    "parse valid package version strings" in {
      val testCases = Table(
        "Raw version string" -> "Parsed package version",
        "0" -> ImmArray(0),
        "1" -> ImmArray(1),
        "1.10" -> ImmArray(1, 10),
        Int.MaxValue.toString -> ImmArray(Int.MaxValue),
        "1.0.2" -> ImmArray(1, 0, 2),
        // Max size (255)
        "0" + ".123" * 63 + ".1" -> ImmArray.from(Seq(0) ++ Seq.fill(63)(123) ++ Seq(1)),
        // Max size - 1 (254)
        "0" + ".123" * 62 + ".1234" -> ImmArray.from(Seq(0) ++ Seq.fill(62)(123) ++ Seq(1234)),
      )

      testCases.forEvery { (input, expected) =>
        PackageVersion.fromString(input) shouldBe Right(PackageVersion(expected))
      }
    }

    "reject invalid package version strings" in {
      val testCases = Table(
        "(Invalid) Raw version string",
        "",
        ".",
        "1.",
        ".0",
        "00",
        "1.002",
        "0.-1",
        "+1.0",
        "beef.0",
        "0.1.beef",
      )

      testCases.forEvery(input =>
        PackageVersion.fromString(input) shouldBe Left(
          s"Invalid package version string: `$input`. Package versions are non-empty strings consisting of segments of digits (without leading zeros) separated by dots."
        )
      )

      val tooBigForInt = s"${Int.MaxValue.toLong + 1}"
      val versionWithNumberBiggerThanInt = s"1.$tooBigForInt"
      PackageVersion.fromString(versionWithNumberBiggerThanInt) shouldBe Left(
        s"Failed parsing $tooBigForInt as an integer"
      )

      // Correct format, but length over limit
      PackageVersion.fromString("0" + ".123" * 63 + ".12") shouldBe Left(
        "Package version string length (256) exceeds the maximum supported length (255)"
      )

      // Invalid format, but length over limit
      // This checks that first the length is checked, and then the regex conformance
      PackageVersion.fromString("abcd" * 64) shouldBe Left(
        "Package version string length (256) exceeds the maximum supported length (255)"
      )
    }
  }

  private[this] val packageVersionsInOrder = List(
    // Lowest possible package version
    PackageVersion.assertFromString("0"),
    PackageVersion.assertFromString("0.1"),
    PackageVersion.assertFromString("0.11"),
    PackageVersion.assertFromString("1.0"),
    PackageVersion.assertFromString("2"),
    PackageVersion.assertFromString("10"),
    PackageVersion.assertFromString(s"${Int.MaxValue}"),
    PackageVersion.assertFromString(s"${Int.MaxValue}.3"),
    // Highest possible package version
    PackageVersion.assertFromString(s"${Int.MaxValue}." * 23 + "99"),
  )

  testOrdered("PackageVersion", packageVersionsInOrder)

  private[this] val qualifiedNamesInOrder =
    for {
      modNane <- dottedNamesInOrder
      name <- dottedNamesInOrder
    } yield QualifiedName(modNane, name)

  testOrdered("QualifiedName", qualifiedNamesInOrder)

  "Identifier.fromString" - {

    val errorMessageBeginning =
      "Separator ':' between package identifier and qualified name not found in "

    "rejects strings without any colon" in {
      Identifier.fromString("foo").left.value should startWith(errorMessageBeginning)
    }

    "rejects strings with empty segments but the error is caught further down the stack" in {
      Identifier.fromString(":bar").left.value should not startWith errorMessageBeginning
      Identifier.fromString("bar:").left.value should not startWith errorMessageBeginning
      Identifier.fromString("::").left.value should not startWith errorMessageBeginning
      Identifier.fromString("bar:baz").left.value should not startWith errorMessageBeginning
    }

    "accepts valid identifiers" in {
      Identifier.fromString("foo:bar:baz").toOption.get shouldBe Identifier(
        pkg = PackageId.assertFromString("foo"),
        qualifiedName = QualifiedName.assertFromString("bar:baz"),
      )
    }
  }

  private[this] val pkgIdsInOrder = List(
    Ref.PackageId.assertFromString("a"),
    Ref.PackageId.assertFromString("aa"),
    Ref.PackageId.assertFromString("b"),
  )

  private[this] val identifiersInOrder =
    for {
      pkgId <- pkgIdsInOrder
      qualifiedName <- qualifiedNamesInOrder
    } yield Ref.Identifier(pkgId, qualifiedName)

  testOrdered("Indenfitiers", identifiersInOrder)

  "Party, PackageId and LedgerString" - {

    val packageIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
    val partyIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:-_ "
    val ledgerStringChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-#/ "

    def makeString(c: Char): String = s"the character $c is not US-ASCII"

    "rejects the empty string" in {
      PackageId.fromString("") shouldBe a[Left[?, ?]]
      Party.fromString("") shouldBe a[Left[?, ?]]
      LedgerString.fromString("") shouldBe a[Left[?, ?]]
    }

    "treats US-ASCII characters as expected" in {
      for (c <- '\u0001' to '\u007f') {
        val s = makeString(c)
        PackageId.fromString(s) shouldBe (if (packageIdChars.contains(c)) a[Right[?, ?]]
                                          else a[Left[?, ?]])
        Party.fromString(s) shouldBe (if (partyIdChars.contains(c)) a[Right[?, ?]]
                                      else a[Left[?, ?]])
        LedgerString.fromString(s) shouldBe (if (ledgerStringChars.contains(c)) a[Right[?, ?]]
                                             else a[Left[?, ?]])
      }
    }

    "rejects no US-ASCII characters" in {

      val negativeTestCase = makeString('a')

      PackageId.fromString(negativeTestCase) shouldBe a[Right[?, ?]]
      Party.fromString(negativeTestCase) shouldBe a[Right[?, ?]]
      LedgerString.fromString(negativeTestCase) shouldBe a[Right[?, ?]]

      for (c <- '\u0080' to '\u00ff') {
        val positiveTestCase = makeString(c)
        PackageId.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
        Party.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
        LedgerString.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
      }
      for (
        positiveTestCase <- List(
          "español",
          "東京",
          "Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))",
        )
      ) {
        Party.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
        PackageId.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
        LedgerString.fromString(positiveTestCase) shouldBe a[Left[?, ?]]
      }
    }

    "reject too long string" in {
      Party.fromString("p" * 255) shouldBe a[Right[?, ?]]
      Party.fromString("p" * 256) shouldBe a[Left[?, ?]]
      PackageId.fromString("p" * 64) shouldBe a[Right[?, ?]]
      PackageId.fromString("p" * 65) shouldBe a[Left[?, ?]]
      LedgerString.fromString("p" * 255) shouldBe a[Right[?, ?]]
      LedgerString.fromString("p" * 256) shouldBe a[Left[?, ?]]
    }
  }

  "QualifiedChoiceId.fromString" - {

    val errorMessageBeginning =
      "Separator ':' between package identifier and qualified name not found in "

    "rejects strings without any colon" in {
      Identifier.fromString("foo").left.value should startWith(errorMessageBeginning)
    }

    "rejects strings with empty segments but the error is caught further down the stack" in {
      val testCases = Table(
        "invalid qualified choice Name",
        "#",
        "##",
        "###",
        "##ChName",
        "-pkgId-:Mod:Name#ChName",
        "-pkgId-:Mod:Name#ChName#",
        "#-pkgId-:Mod:Name",
        "#-pkgId-:Mod:Name#",
        "#-pkgId-:Mod:Name#ChName#",
      )

      forEvery(testCases)(s =>
        if (QualifiedChoiceId.fromString(s).isRight)
          QualifiedChoiceId.fromString(s) shouldBe Left(s)
        else
          QualifiedChoiceId.fromString(s) shouldBe a[Left[?, ?]]
      )
    }

    "accepts valid identifiers" in {
      QualifiedChoiceId.fromString("ChName") shouldBe
        Right(QualifiedChoiceId(None, ChoiceName.assertFromString("ChName")))

      QualifiedChoiceId.fromString("#-pkgId-:Mod:Name#ChName") shouldBe
        Right(
          QualifiedChoiceId(
            Some(Identifier.assertFromString("-pkgId-:Mod:Name")),
            ChoiceName.assertFromString("ChName"),
          )
        )
    }
  }

  "UserId" - {
    val validCharacters = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "._-#!|@^$`+'~:()"
    val validUserIds =
      validCharacters.flatMap(c => Vector(c.toString, s"$c$c")) ++
        Vector(
          "a",
          "jo1hn.d200oe",
          "ALL_UPPER_CASE",
          "JOHn_doe",
          "office365|10030000838D23AF@MicrosoftOnline.com",
          validCharacters.mkString,
          // The below are examples from https://auth0.com/docs/users/user-profiles/sample-user-profiles
          // with the exception of requiring only lowercase characters to be used
          "google-oauth2|103547991597142817347",
          "windowslive|4cf0a30169d55031",
          "office365|10030000838d23ad@microsoftonline.com",
          "adfs|john@fabrikam.com",
          "mailto:john@fabrikam.com",
          "(usr!67324682736476)",
        )

    "accept valid user ids" in {
      validUserIds.foreach(userId => UserId.fromString(userId) shouldBe a[Right[?, ?]])
    }

    "reject user ids containing invalid characters" in {
      val invalidCharacters = "àá \\%&*=[]{};<>,?\""
      val invalidUserIds = invalidCharacters.map(_.toString) :+ "john/doe"
      invalidUserIds.foreach(userId => UserId.fromString(userId) shouldBe a[Left[?, ?]])
    }

    "reject the empty string" in {
      UserId.fromString("") shouldBe a[Left[?, ?]]
    }

    "reject too long strings" in {
      UserId.fromString("a" * 128) shouldBe a[Right[?, ?]]
      UserId.fromString("a" * 129) shouldBe a[Left[?, ?]]
    }
  }

  private def testOrdered[X: Ordering](name: String, elems: Iterable[X]): Unit =
    s"$name#compare" - {
      import Ordered.orderingToOrdered
      "agrees with equality" in {
        for {
          x <- elems
          y <- elems
        } ((x compare y) == 0) shouldBe (x == y)
      }

      "is reflexive" in {
        for {
          x <- elems
        } (x compare x) shouldBe 0
      }

      "is symmetric" in {
        for {
          x <- elems
          y <- elems
        } (x compare y) shouldBe -(y compare x)
      }

      "is transitive on comparable values" in {
        for {
          x <- elems
          y <- elems
          if (x compare y) <= 0
          z <- elems
          if (y compare z) <= 0
        } (x compare z) shouldBe <=(0)
      }
    }

}
