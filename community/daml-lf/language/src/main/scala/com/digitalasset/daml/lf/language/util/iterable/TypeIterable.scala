// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language
package iterable

import com.digitalasset.daml.lf.language.Ast.*

import scala.annotation.tailrec

// The topmost types under a node: the direct subtypes of a type, or the types attached to any
// sub-expression of an expression or definition. Callers descend into the returned types themselves.
private[lf] object TypeIterable {
  that =>

  private def toType(tyCon: TypeConApp): Type =
    (tyCon.args.iterator foldLeft (TTyCon(tyCon.tycon): Type))(TApp.apply)

  private def iterator(typ: Type): Iterator[Type] =
    typ match {
      case TSynApp(_, args) =>
        args.iterator
      case TVar(_) | TTyCon(_) | TBuiltin(_) | TNat(_) =>
        Iterator.empty
      case TApp(tyfun, arg) =>
        Iterator(tyfun, arg)
      case TForall(binder @ _, body) =>
        Iterator(body)
      case TStruct(fields) =>
        fields.values
    }

  // stack-safe: descends into sub-expressions with an explicit worklist
  private def typesIn(expr0: Expr): List[Type] = {
    @tailrec
    def loop(todo: List[Expr], acc: List[Type]): List[Type] =
      todo match {
        case Nil => acc
        case expr :: rest =>
          loop(ExprIterable.iterator(expr).toList ::: rest, localTypes(expr) ::: acc)
      }
    loop(List(expr0), Nil)
  }

  // types attached to this node only, sub-expressions are visited by the caller
  private def localTypes(expr0: Expr): List[Type] =
    expr0 match {
      case ERecCon(tycon, fields @ _) =>
        List(toType(tycon))
      case ERecProj(tycon, field @ _, record @ _) =>
        List(toType(tycon))
      case ERecUpd(tycon, field @ _, record @ _, update @ _) =>
        List(toType(tycon))
      case EVariantCon(tycon, variant @ _, arg @ _) =>
        List(toType(tycon))
      case ETyApp(expr @ _, typ) =>
        List(typ)
      case EAbs((boundVarName @ _, boundVarType), body @ _) =>
        List(boundVarType)
      case ELet(binding, body @ _) =>
        List(binding.typ)
      case EEnumCon(tyConName, _) =>
        List(TTyCon(tyConName))
      case EToAny(typ, expr @ _) =>
        List(typ)
      case EFromAny(typ, expr @ _) =>
        List(typ)
      case ETypeRep(tyCon) =>
        List(tyCon)
      case ENil(typ) =>
        List(typ)
      case ECons(typ, front @ _, tail @ _) =>
        List(typ)
      case ENone(typ) =>
        List(typ)
      case ESome(typ, body @ _) =>
        List(typ)
      case EUpdate(u) =>
        localTypes(u)
      case EThrow(returnType, exceptionType, exception @ _) =>
        List(returnType, exceptionType)
      case EToAnyException(typ, value @ _) =>
        List(typ)
      case EFromAnyException(typ, value @ _) =>
        List(typ)
      case EToInterface(iface, tpl, value @ _) =>
        List(TTyCon(iface), TTyCon(tpl))
      case EFromInterface(iface, tpl, value @ _) =>
        List(TTyCon(iface), TTyCon(tpl))
      case EUnsafeFromInterface(iface, tpl, cid @ _, value @ _) =>
        List(TTyCon(iface), TTyCon(tpl))
      case ECallInterface(iface, _, value @ _) =>
        List(TTyCon(iface))
      case EToRequiredInterface(requiredIfaceId, requiringIfaceId, body @ _) =>
        List(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId))
      case EFromRequiredInterface(requiredIfaceId, requiringIfaceId, body @ _) =>
        List(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId))
      case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cid @ _, body @ _) =>
        List(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId))
      case EInterfaceTemplateTypeRep(ifaceId, body @ _) =>
        List(TTyCon(ifaceId))
      case ESignatoryInterface(ifaceId, body @ _) =>
        List(TTyCon(ifaceId))
      case EObserverInterface(ifaceId, body @ _) =>
        List(TTyCon(ifaceId))
      case EViewInterface(ifaceId, expr @ _) =>
        List(TTyCon(ifaceId))
      case EChoiceController(tpl, choiceName @ _, contract @ _, choiceArg @ _) =>
        List(TTyCon(tpl))
      case EChoiceObserver(tpl, choiceName @ _, contract @ _, choiceArg @ _) =>
        List(TTyCon(tpl))
      case EVar(_) | EVal(_) | EBuiltinFun(_) | EBuiltinCon(_) | EBuiltinLit(_) | EApp(_, _) |
          ECase(_, _) | ELocation(_, _) | EStructCon(_) | EStructProj(_, _) | EStructUpd(_, _, _) |
          ETyAbs(_, _) | EExperimental(_, _) =>
        Nil
    }

  private def localTypes(update: Update): List[Type] =
    update match {
      case UpdatePure(typ, expr @ _) =>
        List(typ)
      case UpdateBlock(bindings, body @ _) =>
        bindings.toList.map(_.typ)
      case UpdateCreate(templateId, arg @ _) =>
        List(TTyCon(templateId))
      case UpdateCreateInterface(interface, arg @ _) =>
        List(TTyCon(interface))
      case UpdateFetchTemplate(templateId, contractId @ _) =>
        List(TTyCon(templateId))
      case UpdateFetchInterface(interface, contractId @ _) =>
        List(TTyCon(interface))
      case UpdateUnpackTemplate(templateId, contractId @ _) =>
        List(TTyCon(templateId))
      case UpdateUnpackInterface(interface, contractId @ _) =>
        List(TTyCon(interface))
      case UpdateExercise(templateId, choice @ _, cid @ _, arg @ _) =>
        List(TTyCon(templateId))
      case UpdateExerciseInterface(interface, choice @ _, cid @ _, arg @ _, guard @ _) =>
        List(TTyCon(interface))
      case UpdateExerciseByKey(templateId, choice @ _, key @ _, arg @ _) =>
        List(TTyCon(templateId))
      case UpdateEmbedExpr(typ, body @ _) =>
        List(typ)
      case UpdateGetTime | UpdateLedgerTimeLT(_) | UpdateFetchByKey(_) | UpdateQueryNByKey(_) =>
        Nil
      case UpdateTryCatchV1(typ, body @ _, binder @ _, handler @ _) =>
        List(typ)
    }

  private def iterator(defn: Definition): Iterator[Type] =
    defn match {
      case DTypeSyn(params @ _, typ) =>
        Iterator(typ)
      case DDataType(serializable @ _, params @ _, DataRecord(fields)) =>
        fields.iterator.map(_._2)
      case DDataType(serializable @ _, params @ _, DataVariant(variants)) =>
        variants.iterator.map(_._2)
      case DDataType(serializable @ _, params @ _, DataEnum(values @ _)) =>
        Iterator.empty
      case DDataType(serializable @ _, params @ _, DataInterface) =>
        Iterator.empty
      case DValue(typ, body) =>
        Iterator(typ) ++ typesIn(body)

    }

  private def iterator(x: Template): Iterator[Type] =
    x match {
      case Template(
            param @ _,
            precond,
            signatories,
            choices,
            observers,
            key,
            implements,
          ) =>
        typesIn(precond).iterator ++
          typesIn(signatories) ++
          choices.values.flatMap(iterator(_)) ++
          typesIn(observers) ++
          key.iterator.flatMap(iterator(_)) ++
          implements.values.flatMap(iterator(_))
    }

  private def iterator(choice: TemplateChoice): Iterator[Type] =
    choice match {
      case TemplateChoice(
            name @ _,
            consuming @ _,
            controllers,
            observers,
            authorizers,
            selfBinder @ _,
            (boundedVarName @ _, boundedVarType),
            retType,
            update,
          ) =>
        typesIn(controllers).iterator ++
          observers.iterator.flatMap(typesIn) ++
          authorizers.iterator.flatMap(typesIn) ++
          typesIn(update) ++
          Iterator(boundedVarType) ++
          Iterator(retType)
    }

  private def iterator(key: TemplateKey): Iterator[Type] =
    key match {
      case TemplateKey(typ, body, maintainers) =>
        Iterator(typ) ++
          typesIn(body) ++
          typesIn(maintainers)
    }

  private def iterator(impl: TemplateImplements): Iterator[Type] =
    impl match {
      case TemplateImplements(interface, body) =>
        Iterator(TTyCon(interface)) ++ iterator(body)
    }

  private def iterator(iiBody: InterfaceInstanceBody): Iterator[Type] =
    iiBody match {
      case InterfaceInstanceBody(methods, view) =>
        methods.values.iterator.flatMap(iterator) ++ typesIn(view)
    }

  private def iterator(method: InterfaceInstanceMethod): Iterator[Type] =
    method match {
      case InterfaceInstanceMethod(name @ _, value) =>
        typesIn(value).iterator
    }

  private def iterator(coImpl: InterfaceCoImplements): Iterator[Type] =
    coImpl match {
      case InterfaceCoImplements(template, body) =>
        Iterator(TTyCon(template)) ++ iterator(body)
    }

  private def iterator(interface: DefInterface): Iterator[Type] =
    interface match {
      case DefInterface(requires, _, choices, methods, view, coImplements) =>
        requires.iterator.map(TTyCon.apply) ++
          choices.values.iterator.flatMap(iterator) ++
          methods.values.iterator.flatMap(iterator) ++
          iterator(view) ++
          coImplements.values.flatMap(iterator)
    }

  private def iterator(imethod: InterfaceMethod): Iterator[Type] =
    imethod match {
      case InterfaceMethod(name @ _, retType) =>
        Iterator(retType)
    }

  def apply(typ: Type): Iterable[Type] =
    new Iterable[Type] {
      override def iterator = that.iterator(typ)
    }

  def apply(expr: Expr): Iterable[Type] =
    new Iterable[Type] {
      override def iterator = that.typesIn(expr).iterator
    }

  def apply(module: Module): Iterable[Type] =
    new Iterable[Type] {
      override def iterator: Iterator[Type] =
        module.definitions.values.iterator.flatMap(that.iterator(_)) ++
          module.interfaces.values.iterator.flatMap(that.iterator(_)) ++
          module.templates.values.iterator.flatMap(that.iterator(_))
    }
}
