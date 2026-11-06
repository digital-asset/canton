// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.testing.parser.Implicits.*
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// The interned-type table of an LF 2.x archive lets entry i reference
// entry i-1, so a few hundred KB of flat protobuf decode to a Type nested `depth` levels deep.
// Several validation traversals recurse on type structure without a trampoline and overflow the
// stack; Validation reports that as EStackOverflow.
class DeepTypeSpec extends AnyWordSpec with Matchers {

  implicit val parserParameters: ParserParameters[this.type] = ParserParameters.default
  private val pkgId = parserParameters.defaultPackageId

  private val depth = 100000

  // List (List (... (List Int64)))
  private def deepList(n: Int): Type =
    (1 to n).foldLeft[Type](TBuiltin(BTInt64))((t, _) => TApp(TBuiltin(BTList), t))

  private val modName = Ref.DottedName.assertFromString("Mod")

  private val basePkg =
    p"""
       metadata ( 'pkg' : '1.0.0' )
       module Mod {
         record @serializable T = { f: Int64 };
       }
     """

  // A serializable record whose field has the deep type (reaches Serializability), or a value
  // whose declared type is deep but whose body has a different type (reaches the
  // expandTypeSynonyms fallback of Typing.alphaEquiv).
  private def pkgWith(defName: String, definition: Definition): Package = {
    val mod = basePkg.modules(modName)
    basePkg.copy(modules =
      Map(
        modName -> mod.copy(definitions =
          Map(Ref.DottedName.assertFromString(defName) -> definition)
        )
      )
    )
  }

  private def recordPkg(n: Int): Package =
    pkgWith(
      "T",
      DDataType(
        serializable = true,
        params = ImmArray.empty,
        cons = DataRecord(ImmArray(Ref.Name.assertFromString("f") -> deepList(n))),
      ),
    )

  private def valuePkg(n: Int): Package =
    pkgWith("v", DValue(deepList(n), ENil(TBuiltin(BTInt64))))

  private def interface(pkg: Package) = PackageInterface(Map(pkgId -> pkg))

  "Sanity check" should {
    "accept the record and reject the ill-typed value at small depth" in {
      val rec = recordPkg(2)
      Validation.checkPackage(interface(rec), pkgId, rec) shouldBe Right(())
      val value = valuePkg(2)
      Validation.checkPackage(interface(value), pkgId, value) shouldBe a[Left[?, ?]]
    }
  }

  "Validation.checkPackage" should {
    "reject a package with a deep record field type as too deeply nested" in {
      val pkg = recordPkg(depth)
      Validation.checkPackage(interface(pkg), pkgId, pkg) shouldBe Left(
        EStackOverflow(Context.None)
      )
    }

    "reject a package with a deep ill-typed value as too deeply nested" in {
      val pkg = valuePkg(depth)
      Validation.checkPackage(interface(pkg), pkgId, pkg) shouldBe Left(
        EStackOverflow(Context.None)
      )
    }
  }
}
