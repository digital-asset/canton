// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package refinement

import com.daml.nameof.NameOf
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast

import scala.annotation.tailrec

abstract class SafePackageResolver private[refinement] (
    compiledPackages: CompiledPackages,
    loadPackage: (Ref.PackageId, language.Reference) => Result[Unit],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  @tailrec
  final protected def collectNewPackagesFromTypes(
      types: List[Ast.Type],
      acc: Map[Ref.PackageId, language.Reference] = Map.empty,
  )(implicit traceContext: TraceContext): Result[Map[Ref.PackageId, language.Reference]] = {
    types match {
      case typ :: rest =>
        typ match {
          case Ast.TTyCon(tycon) =>
            val pkgId = tycon.packageId
            val newAcc =
              if (compiledPackages.contains(pkgId) || acc.contains(pkgId))
                acc
              else
                acc.updated(pkgId, language.Reference.DataType(tycon))
            collectNewPackagesFromTypes(rest, newAcc)
          case Ast.TApp(tyFun, tyArg) =>
            collectNewPackagesFromTypes(tyFun :: tyArg :: rest, acc)
          case Ast.TNat(_) | Ast.TBuiltin(_) | Ast.TVar(_) =>
            collectNewPackagesFromTypes(rest, acc)
          case Ast.TSynApp(_, _) | Ast.TForall(_, _) | Ast.TStruct(_) =>
            // We assume that collectPackages is always given serializable types
            ResultError(
              Error.Preprocessing
                .Internal(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"unserializable type ${typ.pretty}",
                  None,
                )
            )
        }
      case Nil =>
        ResultDone(acc)
    }
  }

  final protected def collectNewPackagesFromTemplatesOrInterfaces(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyRefs: Iterable[Ref.TypeConRef],
  ): Map[Ref.PackageId, language.Reference] =
    tyRefs
      .foldLeft(Map.empty[Ref.PackageId, language.Reference]) { (acc, tycon) =>
        val pkgId = tycon.pkg match {
          case Ref.PackageRef.Name(name) =>
            pkgResolution.get(name)
          case Ref.PackageRef.Id(id) =>
            Some(id)
        }
        pkgId match {
          case Some(id) if !compiledPackages.contains(id) && !acc.contains(id) =>
            acc.updated(
              id,
              language.Reference.TemplateOrInterface(tycon.copy(Ref.PackageRef.Id(id))),
            )
          case _ =>
            acc
        }
      }

  final protected def collectNewPackagesFromTemplatesOrInterfaces(
      tycons: Iterable[Ref.TypeConId]
  ): Map[Ref.PackageId, language.Reference] = {
    tycons
      .foldLeft(Map.empty[Ref.PackageId, language.Reference]) { (acc, tycon) =>
        val pkgId = tycon.packageId
        if (compiledPackages.contains(pkgId) || acc.contains(pkgId))
          acc
        else
          acc.updated(pkgId, language.Reference.TemplateOrInterface(tycon.toRef))
      }
  }

  final protected def pullPackages(
      pkgIds: Map[Ref.PackageId, language.Reference]
  ): Result[Unit] = {
    def loop(
        pkgIds: List[(Ref.PackageId, language.Reference)]
    ): Result[Unit] =
      pkgIds match {
        case (pkgId, context) :: rest =>
          loadPackage(pkgId, context).flatMap(_ => loop(rest))
        case Nil =>
          Result.Unit
      }

    loop(pkgIds.toList)
  }

  final protected def pullTypePackages(typ: Ast.Type)(implicit
      traceContext: TraceContext
  ): Result[Unit] =
    collectNewPackagesFromTypes(List(typ)).flatMap(pullPackages)

  final protected def pullPackage(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyCons: Iterable[Ref.TypeConRef],
  ): Result[Unit] =
    pullPackages(collectNewPackagesFromTemplatesOrInterfaces(pkgResolution, tyCons))

  final protected def pullPackage(tyCons: Iterable[Ref.TypeConId]): Result[Unit] =
    pullPackages(collectNewPackagesFromTemplatesOrInterfaces(tyCons))

}
