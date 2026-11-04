// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.transcode.schema.*
import com.google.common.collect.MapMaker
import org.slf4j.LoggerFactory

import scala.PartialFunction.condOpt
import scala.collection.mutable
import scala.util.Try
import scala.util.control.TailCalls.{TailRec, done, tailcall}

/** [[LfSchemaProcessor]] traverses Daml Schema and produces results by calling into handler methods
  * of [[com.digitalasset.transcode.schema.SchemaVisitor]].
  */
object LfSchemaProcessor:
  /** Process Daml Packages. Provide visitor (codec/codegen).
    *
    * Example:
    *
    * `val codecs = LfSchemaProcessor.process(packageSignatures)(JsonCodec)`
    */
  def process(
      packages: Map[Ref.PackageId, Ast.PackageSignature],
      filter: IdentifierFilter,
  )(visitor: SchemaVisitor): Either[String, visitor.Result] =
    process(packages.keys, packages.apply, filter)(visitor)

  /** Process Daml Packages. Package signatures are computed lazily on demand. Provide visitor
    * (codec/codegen)
    *
    * Example:
    *
    * `val codecs = LfSchemaProcessor.process(packageIds, getPackageSignature, filter)(JsonCodec)`
    */
  def process(
      rootPackages: IterableOnce[Ref.PackageId],
      getPackageSignature: Ref.PackageId => Ast.PackageSignature,
      filter: IdentifierFilter,
  )(visitor: SchemaVisitor): Either[String, visitor.Result] =
    Try {
      new LfSchemaProcessor(rootPackages, getPackageSignature, filter, visitor).getResult
    }.toEither.left.map(_.getMessage)
end LfSchemaProcessor

private class LfSchemaProcessor[R](
    rootPackages: IterableOnce[Ref.PackageId],
    getPackageSignature: Ref.PackageId => Ast.PackageSignature,
    filter: IdentifierFilter,
    visitor: SchemaVisitor { type Result = R },
):
  // All top-level API definitions (Templates, Choices, Interfaces)
  def getResult: R =
    val entities = for
      pkgId <- rootPackages.iterator.toSeq
      pkg = getPackage(pkgId) if validatePackage(pkg)
      (moduleName, module) <- pkg.modules.toSeq

      entities =
        module.templates.map((name, t) =>
          ( // templates
            Ref.Identifier(pkgId, Ref.QualifiedName(moduleName, name)),
            t.implements.values.map(x => getIdentifier(x.interfaceId)).filter(filter).toList,
            t.key,
            t.choices,
            false,
          )
        ) ++ module.interfaces.map((name, i) =>
          ( // interfaces
            Ref.Identifier(pkgId, Ref.QualifiedName(moduleName, name)),
            List.empty,
            None,
            i.choices,
            true,
          )
        )

      (refId, implements, key, choices, isInterface) <- entities if filter(getIdentifier(refId))

      entity = Template(
        getIdentifier(refId),
        fromType(Ast.TTyCon(refId)),
        key.map(x => fromType(x.typ)),
        isInterface,
        implements,
        mapChoices(choices),
      )
    yield entity
    visitor.collect(Lazy.withForcedValues(entities.sortBy(_.templateId)))

  private def mapChoices(choices: Map[Ref.ChoiceName, GenTemplateChoice[Unit]]) =
    choices.toSeq.map((_, choice) =>
      Choice(
        ChoiceName(choice.name),
        choice.consuming,
        fromType(choice.argBinder._2),
        fromType(choice.returnType),
      )
    )

  private val tpeCache = mutable.Map.empty[Identifier, visitor.Type]
  private def fromType(tpe: Ast.Type | Ast.DataCons): visitor.Type = fromTypeRec(tpe).result

  // Trampolined so that deeply nested types do not overflow the stack
  private def fromTypeRec(tpe: Ast.Type | Ast.DataCons): TailRec[visitor.Type] =
    tpe match
      case TInterface(id, tpe) =>
        done(
          tpeCache.getOrElseUpdate(
            id, {
              val lazyBody = Lazy(fromType(tpe))
              visitor.constructor(id, Seq.empty, lazyBody())
            },
          )
        )
      case TUnknown(id, args) =>
        val typeParams = 1.to(args.size).map(i => TypeVarName(s"X$i")) // synthetic type params
        val ctor =
          tpeCache.getOrElseUpdate(id, visitor.constructor(id, typeParams, visitor.unknown))
        if args.nonEmpty then
          fromTypes(args).flatMap(ts => done(visitor.application(ctor, typeParams, ts)))
        else done(ctor)
      case TTyConApp(id, cons, params, args) =>
        val ctor = tpeCache.getOrElseUpdate(
          id, {
            val lazyBody = Lazy(fromType(cons))
            visitor.constructor(id, params.map(TypeVarName), lazyBody())
          },
        )
        if args.nonEmpty
        then
          fromTypes(args).flatMap(ts =>
            done(visitor.application(ctor, params.map(TypeVarName), ts))
          )
        else done(ctor)
      case TSynApp(tpe, params, args) =>
        tailcall(fromTypeRec(tpe)).flatMap(body =>
          fromTypes(args).flatMap(ts =>
            done(visitor.application(body, params.map(TypeVarName), ts))
          )
        )

      case Ast.DataRecord(fields) =>
        val (names, types) = fields.toSeq.unzip
        fromTypes(types).flatMap(ts => done(visitor.record(names.map(FieldName).zip(ts))))
      case Ast.DataVariant(variants) =>
        val (names, types) = variants.toSeq.unzip
        fromTypes(types).flatMap(ts => done(visitor.variant(names.map(VariantConName).zip(ts))))
      case Ast.DataEnum(constructors) =>
        done(visitor.enumeration(constructors.map(EnumConName).toSeq))

      case Ast.TVar(name) => done(visitor.variable(TypeVarName(name)))

      case Util.TList(typ) => tailcall(fromTypeRec(typ)).flatMap(t => done(visitor.list(t)))
      case Util.TOptional(typ) => tailcall(fromTypeRec(typ)).flatMap(t => done(visitor.optional(t)))
      case Util.TTextMap(typ) => tailcall(fromTypeRec(typ)).flatMap(t => done(visitor.textMap(t)))
      case Util.TGenMap(kTyp, vTyp) =>
        tailcall(fromTypeRec(kTyp)).flatMap(k =>
          tailcall(fromTypeRec(vTyp)).flatMap(v => done(visitor.genMap(k, v)))
        )

      case Util.TUnit => done(visitor.unit)
      case Util.TBool => done(visitor.bool)
      case Util.TText => done(visitor.text)
      case Util.TInt64 => done(visitor.int64)
      case Util.TNumeric(Ast.TNat(scale)) => done(visitor.numeric(scale))
      case Util.TTimestamp => done(visitor.timestamp)
      case Util.TDate => done(visitor.date)
      case Util.TParty => done(visitor.party)
      case Util.TContractId(typ) =>
        tailcall(fromTypeRec(typ)).flatMap(t => done(visitor.contractId(t)))

      case other => err(s"Type $other not supported")

  private def fromTypes(tpes: Seq[Ast.Type]): TailRec[Seq[visitor.Type]] =
    tpes.foldLeft(done(Vector.empty[visitor.Type])) { (acc, tpe) =>
      acc.flatMap(ts => tailcall(fromTypeRec(tpe)).flatMap(t => done(ts :+ t)))
    }

  private val Lazy = LazinessManager()

  private def err(msg: String) = throw RuntimeException(msg)

  private def cached[K, V](compute: K => V): K => V =
    val cache = new MapMaker().weakValues().makeMap[K, V]()
    (k: K) =>
      LfSchemaProcessor.this.synchronized {
        Option(cache.get(k)) match
          case Some(value) => value
          case None => val res = compute(k); cache.put(k, res); res
      }

  private val getPackage = cached { (pkgId: Ref.PackageId) =>
    getPackageSignature(pkgId)
  }

  private val getInterface = cached { (id: Ref.Identifier) =>
    val interface =
      getPackage(id.packageId).modules(id.qualifiedName.module).interfaces(id.qualifiedName.name)
    warnIfNot(
      interface.coImplements.isEmpty,
      s"$id contains coImplements definition, which is not supported. Please remove it.",
    )
    warnIfNot(
      filter(getIdentifier(id)),
      s"Interface $id is mentioned, but is not included in filter",
    )
    interface
  }

  private val isInterface = cached { (id: Ref.Identifier) =>
    getPackage(id.packageId)
      .modules(id.qualifiedName.module)
      .interfaces
      .contains(id.qualifiedName.name)
  }

  private val isUnknown = cached { (id: Ref.Identifier) =>
    val pkg = getPackage(id.packageId)
    !pkg.modules.contains(id.qualifiedName.module) ||
    !pkg.modules(id.qualifiedName.module).definitions.contains(id.qualifiedName.name)
  }

  private val getPackageInfo = cached { (id: Ref.PackageId) =>
    getMetadataDetails(getPackage(id), id)
  }

  private val getIdentifier = { (id: Ref.Identifier) =>
    Identifier(
      PackageId(id.packageId),
      getPackageInfo(id.packageId)._1,
      getPackageInfo(id.packageId)._2,
      ModuleName(id.qualifiedName.module.dottedName),
      EntityName(id.qualifiedName.name.dottedName),
    )
  }

  private val getDefinition = cached { (id: Ref.Identifier) =>
    getPackage(id.packageId).modules(id.qualifiedName.module).definitions(id.qualifiedName.name)
  }

  private object TTyConApp {
    def unapply(
        tpe: Ast.Type
    ): Option[(Identifier, Ast.DataCons, Seq[Ast.TypeVarName], Seq[Ast.Type])] = for
      (id, tpeDef, args) <- condOpt(tpe) { case Util.TTyConApp(id, args) =>
        (id, getDefinition(id), args)
      }
      (params, cons) <- condOpt(tpeDef) { case Ast.DDataType(_, params, cons) => (params, cons) }
    yield (getIdentifier(id), cons, params.toSeq.map(_._1), args.toSeq)
  }
  private object TSynApp {
    def unapply(tpe: Ast.Type): Option[(Ast.Type, Seq[Ast.TypeVarName], Seq[Ast.Type])] = for
      (tpeDef, args) <- condOpt(tpe) { case Ast.TSynApp(id, args) => (getDefinition(id), args) }
      (params, targetTpe) <- condOpt(tpeDef) { case Ast.DTypeSyn(params, tpe) => (params, tpe) }
    yield (targetTpe, params.toSeq.map(_._1), args.toSeq)
  }
  private object TInterface {
    def unapply(tpe: Ast.Type): Option[(Identifier, Ast.DataCons)] = for
      (id, interfaceView) <- condOpt(tpe) {
        case Ast.TTyCon(id) if isInterface(id) => (id, getInterface(id).view)
      }
      viewDef <- condOpt(interfaceView) { case Util.TTyConApp(viewId, args) =>
        getDefinition(viewId)
      }
      dataCons <- condOpt(viewDef) { case Ast.DDataType(_, _, cons) => cons }
    yield (getIdentifier(id), dataCons)
  }
  private object TUnknown {
    def unapply(tpe: Ast.Type): Option[(Identifier, Seq[Ast.Type])] = condOpt(tpe) {
      case Util.TTyConApp(id, args) if isUnknown(id) => (getIdentifier(id), args.toSeq)
      case Ast.TSynApp(id, args) if isUnknown(id) => (getIdentifier(id), args.toSeq)
    }
  }

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private def warnIfNot(cond: Boolean, msg: => String): Unit = if !cond then logger.warn(msg)

end LfSchemaProcessor
