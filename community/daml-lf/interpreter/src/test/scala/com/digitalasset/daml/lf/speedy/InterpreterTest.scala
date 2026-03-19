// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.speedy.SExpr.LfDefRef
import com.digitalasset.daml.lf.speedy.SResult._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}

import scala.language.implicitConversions

class InterpreterTest extends AnyWordSpec with Inside with Matchers with TableDrivenPropertyChecks with SuppressingLogging {

  private implicit def id(s: String): Ref.Name = Name.assertFromString(s)

  private val compilerConfig =
    Compiler.Config.Default // should contain languageVersion defined below
  private val languageVersion = LanguageVersion.latestStableLfVersion

  private def runExpr(e: Expr): SValue = {
    val machine = Speedy.Machine.fromPureExpr(PureCompiledPackages.Empty(compilerConfig), e)
    machine.run() match {
      case SResultFinal(v) => v
      case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
    }
  }

  "evaluator behaves responsibly" should {
    // isolated rendition of the DA.Test.List.concat_test script in
    // stdlib, which failed after I introduced FrontQueue. It happened
    // to be a missing reverse in Interp.
    "concat works" in {
      val int64 = TBuiltin(BTInt64)
      val int64List = TApp(TBuiltin(BTList), int64)

      def int64Cons(nums: ImmArray[Long], tail: Expr): Expr =
        ECons(int64, nums.map(i => EBuiltinLit(BLInt64(i))), tail)

      val int64Nil = ENil(int64)
      val concat =
        EAbs(
          ("xss", TApp(TBuiltin(BTList), int64List)),
          ELet(
            Binding(
              Some("work"),
              TFun(int64List, TFun(int64List, int64List)),
              EAbs(
                ("xs", int64List),
                EAbs(
                  ("acc", int64List),
                  EApp(
                    EApp(
                      EApp(
                        EBuiltinFun(BFoldr),
                        EAbs(
                          ("x", int64),
                          EAbs(
                            ("accInner", int64List),
                            ECons(int64, ImmArray(EVar("x")), EVar("accInner")),
                          ),
                        ),
                      ),
                      EVar("acc"),
                    ),
                    EVar("xs"),
                  ),
                ),
              ),
            ),
            EApp(EApp(EApp(EBuiltinFun(BFoldl), EVar("work")), ENil(int64)), EVar("xss")),
          ),
        )
      val xss1 = ECons(
        int64List,
        ImmArray(int64Cons(ImmArray(2, 5), int64Nil), int64Cons(ImmArray[Long](7), int64Nil)),
        ENil(int64List),
      )
      val xss2 = ECons(int64List, ImmArray(int64Cons(ImmArray(2, 5, 7), int64Nil)), ENil(int64List))
      runExpr(EApp(concat, xss1)) shouldBe runExpr(EApp(concat, xss2))
    }
  }

  "large lists" should {
    val t_int64 = TBuiltin(BTInt64)
    val t_int64List = TApp(TBuiltin(BTList), t_int64)
    val list = ECons(
      t_int64List,
      (1 to 100000).map(i => EBuiltinLit(BLInt64(i.toLong))).to(ImmArray),
      ENil(t_int64List),
    )
    var machine: Speedy.PureMachine = null
    "compile" in {
      machine = Speedy.Machine.fromPureExpr(PureCompiledPackages.Empty(compilerConfig), list)
    }
    "interpret" in {
      val value = machine.run() match {
        case SResultFinal(v) => v
        case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
      }
      value match {
        case SValue.SList(lst) =>
          lst.length shouldBe 100000
          val arr = lst.toImmArray
          arr(0) shouldBe SValue.SInt64(1)
          arr(99999) shouldBe SValue.SInt64(100000)
        case v => sys.error(s"unexpected resulting value $v")

      }
    }
  }

  /** Test that the package reloading works */
  "package reloading" should {
    val dummyPkg = PackageId.assertFromString("dummy")
    val ref = Identifier(dummyPkg, QualifiedName.assertFromString("Foo:bar"))
    val modName = DottedName.assertFromString("Foo")
    val pkgs1 = PureCompiledPackages.Empty(compilerConfig)
    val pkgs2 =
      PureCompiledPackages.assertBuild(
        Map(
          dummyPkg ->
            Package.build(
              List(
                Module.build(
                  name = modName,
                  definitions = Map(
                    DottedName.assertFromString("bar") ->
                      DValue(TUpdate(TBool), EUpdate(UpdatePure(TBool, ETrue)))
                  ),
                  templates = Map.empty,
                  exceptions = Map.empty,
                  interfaces = List.empty,
                  featureFlags = FeatureFlags.default,
                )
              ),
              Set.empty[PackageId],
              languageVersion,
              Ast.PackageMetadata(
                Ref.PackageName.assertFromString("foo"),
                Ref.PackageVersion.assertFromString("0.0.0"),
                None,
              ),
              GeneratedImports(
                reason = "package made in com.digitalasset.daml.lf.speedy.InterpreterTest",
                pkgIds = Set.empty,
              ),
            )
        ),
        compilerConfig,
      )
    val pkgs3 = PureCompiledPackages.assertBuild(
      Map(
        dummyPkg ->
          Package.build(
            List(
              Module(
                name = modName,
                definitions = Map.empty,
                templates = Map.empty,
                exceptions = Map.empty,
                interfaces = Map.empty,
                featureFlags = FeatureFlags.default,
              )
            ),
            Set.empty[PackageId],
            languageVersion,
            Ast.PackageMetadata(
              Ref.PackageName.assertFromString("foo"),
              Ref.PackageVersion.assertFromString("0.0.0"),
              None,
            ),
            GeneratedImports(
              reason = "package made in com.digitalasset.daml.lf.speedy.InterpreterTest",
              pkgIds = Set.empty,
            ),
          )
      ),
      compilerConfig,
    )

    val seed = crypto.Hash.hashPrivateKey("test")
    val committers = Set(Ref.Party.assertFromString("alice"))

    "succeeds" in {
      val result = SpeedyTestLib.run(
        machine = Speedy.Machine.fromUpdateExpr(pkgs1, seed, EVal(ref), committers),
        getPkg = { case pkgId if pkgId == ref.packageId => pkgs2 },
      )
      result shouldBe Right(SValue.SBool(true))
    }

    "crashes without definition" in {
      val result = SpeedyTestLib.run(
        machine = Speedy.Machine.fromUpdateExpr(pkgs1, seed, EVal(ref), committers),
        getPkg = { case pkgId if pkgId == ref.packageId => pkgs3 },
      )
      inside(result) { case Left(SError.SErrorCrash(loc, msg)) =>
        loc shouldBe "com.digitalasset.daml.lf.speedy.Speedy.Machine.lookupVal"
        msg should include(s"definition ${LfDefRef(ref)} not found")
      }
    }
  }

}
