// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.tailrec
import scala.collection.IterableOnceOps

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
  *     with `val x\$1 = Future.unit` that ends in `TupleN(..., x\$1)`, recognized by the `\$` in
  *     the compiler-generated name. Named aliases like `f = Future.unit` are not flagged.
  *   - (D) A future-like `map`, `flatMap` or `foreach` whose function discards a future-like
  *     argument through a wildcard: `_ <- Future.successful(Future.unit)` or `nested.map(_ => ())`.
  *     Unused named parameters are left to `-Wunused:params`.
  *
  * Mockito calls `verify(...).method(...)` are exempt from (A) and (C).
  *
  * Known gaps:
  *   - (C) and (D) recognize compiler-generated names by their `\$`, so hand-written names like
  *     `x\$1` will get flagged as well, even when used.
  *   - (C) also flags `Tuple2(_2 = f, _1 = 1)`, because futures passed as named arguments to a
  *     tuple constructor desugar like aliases.
  *   - (D) requires a future-like receiver, so it does not flag `_ <- Option(Future.unit)`.
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

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val verifyMethodName: TermName = TermName("verify")
    val futureLikeType = typeOf[DoNotDiscardLikeFuture]
    val isFutureLike = FutureLikeTester.tester(u)(futureLikeType)

    val iterableOnceOpsTypeSymbol =
      typeOf[IterableOnceOps[Unit, Iterable, Unit]].typeConstructor.typeSymbol
    require(iterableOnceOpsTypeSymbol != NoSymbol)
    val foreachMethodName: TermName = TermName("foreach")
    val tapEachMethodName: TermName = TermName("tapEach")
    val applyMethodName: TermName = TermName("apply")

    // Pattern (D): matches `receiver.method(x => body)`, with optional type arguments.
    object FunctorCall {
      private val methodNames: Set[Name] =
        Set[Name](TermName("map"), TermName("flatMap"), foreachMethodName)
      def unapply(tree: Tree): Option[(Tree, Function)] =
        tree match {
          case Apply(TypeApply(Select(receiver, method), _), List(function: Function))
              if methodNames.contains(method) =>
            Some((receiver, function))
          case Apply(Select(receiver, method), List(function: Function))
              if methodNames.contains(method) =>
            Some((receiver, function))
          case _ => None
        }
    }

    // Patterns (A) and (C): allow Mockito `verify` calls because they do not produce a future but merely check that a
    // mocked Future-returning method has been called.
    //
    // We do not check whether the receiver of the `verify` call is actually something that inherits from org.mockito.MockitoSugar
    // because `BaseTest` extends `MockitoSugar` and we'd therefore have to do some virtual method resolution.
    // As a result, we ignore all statements of the above form verify(...).someMethod(...)(...)
    @tailrec
    def isMockitoVerify(statement: Tree): Boolean =
      statement match {
        // Match on verify(...).someMethod
        case Select(Apply(TypeApply(Select(_, verifyMethod), _), _), _)
            if verifyMethod == verifyMethodName =>
          true
        // Strip away any further argument lists on the method as in verify(...).someMethod(...)(...)(...
        // including implicit arguments and type arguments
        case Apply(maybeVerifyCall, _) => isMockitoVerify(maybeVerifyCall)
        case TypeApply(maybeVerifyCall, _) => isMockitoVerify(maybeVerifyCall)
        case _ => false
      }

    new u.Traverser {
      // Pattern (A).
      def checkForDiscardedFutures(statements: List[Tree]): Unit =
        statements.foreach {
          case Block((statements0, _)) =>
            checkForDiscardedFutures(statements0)
          case statement =>
            val typeIsFuture = statement.tpe != null && isFutureLike(statement.tpe.dealias)
            if (typeIsFuture && !isMockitoVerify(statement)) {
              error(u)(statement.pos, message)
            }
        }

      // Patterns (C) and (D): wildcards in lambdas and for-comprehensions get compiler-generated
      // names like `x$1` that contain a `$`, which plain user-written identifiers do not.
      def isCompilerGenerated(name: Name): Boolean = name.decodedName.toString.contains("$")

      // Pattern (C): matches `TupleN.apply`, but not other functions returning a tuple. The latter
      // matters because named and default arguments also desugar into blocks of `val x$1 = ...`.
      def isTupleApply(fun: Tree, arity: Int): Boolean = {
        val tupleClass = definitions.TupleClass(arity)
        tupleClass != NoSymbol && fun.symbol != null && fun.symbol.name == applyMethodName &&
        fun.symbol.owner.companion == tupleClass
      }

      // Pattern (C): flags the compiler-named vals of a block that ends in the tuple of its bindings.
      def checkForDiscardedForAliases(statements: List[Tree], expr: Tree): Unit =
        expr match {
          case Apply(fun, args) if isTupleApply(fun, args.size) =>
            val tupled = args.collect { case ident: Ident => ident.symbol }.toSet
            statements
              .collect {
                case vd: ValDef
                    if isCompilerGenerated(vd.name) && tupled.contains(vd.symbol) &&
                      isTypedFutureLike(vd.rhs) && !isMockitoVerify(vd.rhs) =>
                  vd.rhs
              }
              .foreach(rhs => error(u)(rhs.pos, message))
          case _ =>
        }

      def isTypedFutureLike(tree: Tree): Boolean =
        tree.tpe != null && isFutureLike(tree.tpe.dealias)

      // Pattern (D): the compiler-generated scrutinee `(x: @unchecked)` of a desugared generator.
      @tailrec
      def isParameter(param: ValDef, tree: Tree): Boolean =
        tree match {
          case Typed(expr, _) => isParameter(param, expr)
          case Annotated(_, arg) => isParameter(param, arg)
          case ident: Ident => ident.symbol == param.symbol
          case _ => false
        }

      // Pattern (D): a generator `_ <- future` desugars into
      // `future.map(x$1 => (x$1: @unchecked) match { case _ => ... })` and a lambda `_ => ...` into
      // `x$1 => ...`. User code cannot refer to such a parameter, so we need not traverse the body,
      // which in a for-comprehension contains all subsequent steps. The placeholder in `_.foo` also
      // becomes `x$1 => x$1.foo`, but only such parameters are marked synthetic in a plain lambda.
      // Unused named parameters are left to scalac's `-Wunused:params`.
      def ignoresParameter(param: ValDef, body: Tree): Boolean =
        isCompilerGenerated(param.name) && (body match {
          case Match(selector, cases) if isParameter(param, selector) =>
            cases.forall(_.pat match {
              case Ident(termNames.WILDCARD) => true
              case _ => false
            })
          case _ => !param.mods.hasFlag(Flag.SYNTHETIC)
        })

      // Pattern (B).
      def isSubtypeOfIterableOnce(typ: Type): Boolean =
        typ.typeConstructor
          .baseType(iterableOnceOpsTypeSymbol)
          .typeConstructor
          .typeSymbol == iterableOnceOpsTypeSymbol

      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          // Patterns (A) and (C).
          case Block(statements, expr) =>
            checkForDiscardedFutures(statements)
            checkForDiscardedForAliases(statements, expr)
            super.traverse(tree)
          // Pattern (D): flags `future.map(_ => ...)` where the mapped value is itself future-like.
          case FunctorCall(receiver, Function(List(param), body))
              if isTypedFutureLike(receiver) && isTypedFutureLike(param.tpt) &&
                ignoresParameter(param, body) =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          // Pattern (A) for class and object bodies.
          case ClassDef(_, _, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case ModuleDef(_, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          // Pattern (B). Does not traverse into `receiver` (see known gaps).
          case TypeApply(Select(receiver, method), tyArgs)
              if (method == foreachMethodName || method == tapEachMethodName) &&
                receiver.tpe != null && isSubtypeOfIterableOnce(receiver.tpe) &&
                tyArgs.nonEmpty && isFutureLike(tyArgs(0).tpe) =>
            error(u)(tree.pos, message)

          case _ => super.traverse(tree)
        }
    }
  }
}
