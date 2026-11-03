// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.language.iterable.TypeIterable
import com.digitalasset.daml.lf.language.{Graphs, Util as AstUtil}

import scala.annotation.tailrec

private[validation] object Recursion {

  /* Check there are no cycles in the module references */

  @throws[ValidationError]
  def checkPackage(pkgId: PackageId, pkg: Package): Unit = {
    val g: Graphs.Graph[ModuleName] = pkg.modules.map { case (modName0, mod) =>
      modName0 ->
        AstUtil
          .collectIdentifiers(mod)
          .collect {
            case Identifier(`pkgId`, QualifiedName(modName, _)) if modName != modName0 => modName
          }
          .toSet
    }

    Graphs.topoSort(g).left.foreach(cycle => throw EImportCycle(Context.None, cycle.vertices))

    pkg.modules.foreach { case (modName, mod) => checkModule(pkgId, modName, mod) }
  }

  /* Check there are no cycles in the type synonym definitions of a module */

  private def checkModule(pkgId: PackageId, modName: ModuleName, mod: Module): Unit = {
    val g =
      mod.definitions.collect { case (dottedName, DTypeSyn(_, replacementTyp)) =>
        val name = Identifier(pkgId, QualifiedName(modName, dottedName))
        (name, synRefsOfType(replacementTyp))
      }
    Graphs.topoSort(g).left.foreach(cycle => throw ETypeSynCycle(Context.None, cycle.vertices))
  }

  private def synRefsOfType(typ: Type): Set[TypeSynId] = {
    @tailrec
    def loop(todo: List[Type], acc: Set[TypeSynId]): Set[TypeSynId] =
      todo match {
        case Nil => acc
        case typ :: rest =>
          val acc1 = typ match {
            case TSynApp(typeSynName, _) => acc + typeSynName
            case _ => acc
          }
          loop(TypeIterable(typ).toList ::: rest, acc1)
      }
    loop(List(typ), Set.empty)
  }
}
