// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.mockito.Mockito
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.IterableOnceOps
import scala.quoted.{Expr, Type}

/** Flags code that discards a future-like value. Typically, we should not discard
  * [[scala.concurrent.Future]] because exceptions inside the future may not get logged. Use
  * `FutureUtil.doNotAwait` to log exceptions and discard the future where necessary.
  *
  * A type is future-like if it is a [[scala.concurrent.Future]], an
  * [[cats.data.EitherT]]`[`[[scala.concurrent.Future]]`, ..., ...]` or
  * [[cats.data.OptionT]]`[`[[scala.concurrent.Future]]`, ...]`, any nesting of those, or a type
  * annotated with [[DoNotDiscardLikeFuture]] (see `FutureLikeTester` for the details).
  *
  * The wart flags these patterns:
  *   - (A) A future-like statement in a block, class or object body: `{ Future.unit; 42 }`.
  *   - (B) [[scala.collection.IterableOnceOps.foreach]] and
  *     [[scala.collection.IterableOnceOps.tapEach]] with a future-like function result:
  *     `xs.foreach(x => Future(x))`.
  *   - (C) A wildcard alias in a for-comprehension: `_ = Future.unit`. It desugars into a block
  *     with `val \$1\$ = Future.unit match { ... }` that ends in a tuple of the aliases, recognized
  *     by the `\$` in the compiler-generated name. Named aliases are not flagged.
  *   - (D) A `map`, `flatMap` or `foreach` whose function discards a future-like argument through a
  *     wildcard: `_ <- Future.successful(Future.unit)` or `Option(Future.unit).map(_ => ())`.
  *     Unlike the Scala 2 version, the receiver need not be future-like. Unused named parameters
  *     are left to `-Wunused:params`.
  *
  * Mockito calls `Mockito.verify(...).method(...)` are exempt from (A) and (C).
  *
  * Known gap: (C) and (D) recognize compiler-generated names by their `\$`, so hand-written names
  * like `x\$1` will get flagged as well, even when used.
  *
  * This wart is a special case of the warts `NonUnitStatements` and [[NonUnitForEach]] and scalac's
  * `-Wnonunit-statement` flag, in that it warns only if the return type is future-like.
  * Additionally, this wart uses a different set of exceptions when no warning is issued. We keep
  * this specialized wart for two reasons:
  *   1. It is not practically feasible to use `-Wnonunit-statement` and [[NonUnitForEach]] in
  *      scalatest because it would flag many of the assertions of the form `x should be >= y` in
  *      statement positions. Yet, it is important to check for discarded futures in tests because a
  *      discarded future may hide an exception.
  *   1. In some production code, it is convenient to suppress the warnings coming from
  *      `-Wnonunit-statement` and [[NonUnitForEach]], just due to how the code is written. In such
  *      places, we still want to benefit from the explicit checks against discarded futures.
  *
  * This wart does not look at futures that are discarded at the end of a unit-type expression.
  * These cases are caught by `-Ywarn-value-discard`. We do not implement a specialized version for
  * future-like values because we do not expect to suppress the warnings coming from
  * `-Ywarn-value-discard`.
  */
object DiscardedFuture extends WartTraverser {
  val message = "Statements must not discard a Future"

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          checkForIgnoredFutureParameter(tree)
          tree match
            // Patterns (A) and (C).
            case tree: Block =>
              checkForDiscardedFutures(tree.statements)
              checkForDiscardedForAliases(tree)
            // Pattern (A) for class and object bodies.
            case tree: ClassDef =>
              checkForDiscardedFutures(tree.body)
            // Pattern (B).
            case tree if tree.isExpr =>
              tree.asExpr match
                case '{ ($_ : IterableOnceOps[a, cc, c]).foreach[o]($_) } =>
                  if isFutureLike[o] then error(tree.pos, message)
                case '{ ($_ : IterableOnceOps[a, cc, c]).tapEach[o]($_) } =>
                  if isFutureLike[o] then error(tree.pos, message)
                case _ => ()
            case _ => ()
          super.traverseTree(tree)(owner)

      // Pattern (A).
      private def checkForDiscardedFutures(statements: List[Statement]): Unit =
        statements.filter(_.isExpr).foreach { statement =>
          statement.asExpr match
            case '{ $expr: t } if isFutureLike[t] && !isMockitoVerify(statement) =>
              error(statement.pos, message)
            case _ => ()
        }

      // Patterns (C) and (D): wildcards in lambdas and for-comprehensions get compiler-generated
      // names like `$1$` or `_$1` that contain a `$`, which plain user-written identifiers do not.
      private def isCompilerGenerated(name: String): Boolean = name.contains("$")

      private val tupleXXLModuleClass = Symbol.requiredModule("scala.runtime.TupleXXL").moduleClass

      // Pattern (C): the arguments of the tuple that ends a block of aliases, built by
      // `TupleN.apply(...)` or, for more than 22 elements, `TupleXXL.apply(...)` followed by a cast.
      private object TupledArgs:
        private def isApply(fun: Term): Boolean = fun.symbol.name == "apply"

        def unapply(expr: Term): Option[List[Term]] =
          expr match
            case Apply(fun, args)
                if isApply(fun) && defn.isTupleClass(fun.symbol.owner.companionClass) =>
              Some(args)
            case TypeApply(Select(Apply(fun, List(Typed(Repeated(args, _), _))), _), _)
                if isApply(fun) && fun.symbol.owner == tupleXXLModuleClass =>
              Some(args)
            case _ => None

      // Pattern (C): flags the compiler-named vals of a block that ends in the tuple of its bindings.
      private def checkForDiscardedForAliases(block: Block): Unit =
        block.expr match
          case TupledArgs(args) =>
            val tupled = args.collect { case ident: Ident => ident.symbol }.toSet
            block.statements
              .collect {
                case vd @ ValDef(name, _, Some(rhs))
                    if isCompilerGenerated(name) && tupled.contains(vd.symbol) &&
                      futureLikeTester(rhs.tpe) && !isMockitoVerify(aliasedValue(rhs)) =>
                  rhs
              }
              .foreach(rhs => error(rhs.pos, message))
          case _ => ()

      // Pattern (C): a wildcard alias `_ = value` desugars into `val $1$ = value match { case $1$ @ _ => $1$ }`.
      private def aliasedValue(rhs: Term): Term =
        rhs match
          case Match(Typed(value, _), List(CaseDef(Bind(_, Wildcard()), None, _))) => value
          case _ => rhs

      // Pattern (D): matches `receiver.method(x => body)`, with optional type arguments.
      private object FunctorCall:
        private val methodNames = Set("map", "flatMap", "foreach")
        def unapply(tree: Tree): Option[(Term, List[ValDef], Term)] =
          tree match
            case Apply(TypeApply(Select(receiver, method), _), List(Lambda(params, body)))
                if methodNames.contains(method) =>
              Some((receiver, params, body))
            case Apply(Select(receiver, method), List(Lambda(params, body)))
                if methodNames.contains(method) =>
              Some((receiver, params, body))
            case _ => None

      // Pattern (D): flags `functor.map(_ => ...)` where the mapped value is future-like.
      private def checkForIgnoredFutureParameter(tree: Tree): Unit =
        tree match
          case FunctorCall(_, List(param), body)
              if futureLikeTester(param.tpt.tpe) && ignoresParameter(param, body) =>
            error(tree.pos, message)
          case _ => ()

      // Pattern (D): the compiler-generated scrutinee `(x: @unchecked)` of a desugared generator.
      @tailrec
      private def isParameter(param: ValDef, tree: Tree): Boolean =
        tree match
          case Typed(expr, _) => isParameter(param, expr)
          case ident: Ident => ident.symbol == param.symbol
          case _ => false

      // Pattern (D): a generator `_ <- future` desugars into
      // `future.map(x$1 => (x$1: @unchecked) match { case _ => ... })` and a lambda `_ => ...` into
      // `_$1 => ...`. User code cannot refer to such a parameter, so we need not traverse the body,
      // which in a for-comprehension contains all subsequent steps. The placeholder in `_.foo` also
      // becomes `_$1 => _$1.foo`, but only such parameters are marked synthetic in a plain lambda.
      // Unused named parameters are left to scalac's `-Wunused:params`.
      private def ignoresParameter(param: ValDef, body: Term): Boolean =
        isCompilerGenerated(param.name) && (body match
          case Match(scrutinee, cases) if isParameter(param, scrutinee) =>
            cases.forall(_.pattern match
              case Wildcard() => true
              case _ => false
            )
          case _ => !param.symbol.flags.is(Flags.Synthetic)
        )

      // Patterns (A) and (C): allow Mockito `verify` calls because they do not produce a future but merely check that a
      // mocked Future-returning method has been called.
      @tailrec
      private def isMockitoVerify(tree: Tree): Boolean =
        tree match {
          // Match on verify(...).someMethod
          case Select(tree, _) =>
            tree.asExpr match
              case '{ Mockito.verify($_ : Any) } => true
              case _ => false
          // Strip away any further argument lists on the method as in verify(...).someMethod(...)(...)(...
          // including implicit arguments and type arguments
          case Apply(tree, _) => isMockitoVerify(tree)
          case TypeApply(tree, _) => isMockitoVerify(tree)
          case _ => false
        }

      private val futureLikeTester = FutureLikeTester.tester[DoNotDiscardLikeFuture](u)
      private def isFutureLike[T: Type] = futureLikeTester(TypeRepr.of[T])
    }

}
