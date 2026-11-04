// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.daml.lf.archive.DarSchemaDecoder
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.transcode.DamlExamples
import com.digitalasset.transcode.schema.*
import zio.test.*

// Packages can carry types nested far deeper than the stack allows (e.g. through chains of
// interned types); processing them must not overflow.
object DeepTypeSpec extends ZIOSpecDefault:
  private val depth = 100_000

  private val packages =
    DarSchemaDecoder.assertReadArchiveFromFile(DamlExamples.darPath.toFile).all.toMap

  private def moduleName(name: String) = Ref.DottedName.assertFromString(name)
  private val templateModule = moduleName("Other.ChoiceAsDatatype")
  private val templateName = Ref.DottedName.assertFromString("TheTemplate")
  private def packageWith(module: Ref.DottedName) =
    packages
      .find(_._2.modules.contains(module))
      .fold(throw Exception(s"no package with module $module"))(identity)

  private val (pkgId, pkg) = packageWith(templateModule)
  private val typeVarsModule = moduleName("Com.Digitalasset.Transcode.Conformance.Data.TypeVars")
  private val diyOptional = Ref.Identifier(
    packageWith(typeVarsModule)._1,
    Ref.QualifiedName(typeVarsModule, Ref.DottedName.assertFromString("DiyOptional")),
  )

  private def nested(wrap: Ast.Type => Ast.Type): Ast.Type =
    (1 to depth).foldLeft[Ast.Type](Ast.TBuiltin(Ast.BTInt64))((t, _) => wrap(t))

  private def builtinApp(bt: Ast.BuiltinType, args: Ast.Type*): Ast.Type =
    args.foldLeft[Ast.Type](Ast.TBuiltin(bt))(Ast.TApp(_, _))

  // TheTemplate with one field of the given type
  private def packagesWithField(fieldType: Ast.Type) =
    val mod = pkg.modules(templateModule)
    val Ast.DDataType(serializable, params, _) = mod.definitions(templateName): @unchecked
    val record = Ast.DataRecord(ImmArray(Ref.Name.assertFromString("deep") -> fieldType))
    val newMod = mod.copy(definitions =
      mod.definitions.updated(templateName, Ast.DDataType(serializable, params, record))
    )
    packages.updated(pkgId, pkg.copy(modules = pkg.modules.updated(templateModule, newMod)))

  private def processes(fieldType: Ast.Type) =
    val result =
      LfSchemaProcessor.process(packagesWithField(fieldType), IdentifierFilter.AcceptAll)(
        new SchemaVisitor.Unit {
          type Result = Seq[Template[Unit]]
          def collect(entities: Seq[Template[Unit]]) = entities
        }
      )
    assertTrue(result.isRight)

  override def spec = suite("LfSchemaProcessor on deeply nested types")(
    test("List")(processes(nested(builtinApp(Ast.BTList, _)))),
    test("Optional")(processes(nested(builtinApp(Ast.BTOptional, _)))),
    test("TextMap")(processes(nested(builtinApp(Ast.BTTextMap, _)))),
    test("GenMap")(processes(nested(builtinApp(Ast.BTGenMap, Ast.TBuiltin(Ast.BTText), _)))),
    test("type application")(processes(nested(Ast.TApp(Ast.TTyCon(diyOptional), _)))),
  )
