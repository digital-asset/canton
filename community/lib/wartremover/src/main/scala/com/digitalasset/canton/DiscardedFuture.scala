// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates
//
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.{StaticAnnotation, tailrec}
import scala.concurrent.Future

/** Flags statements that return a [[scala.concurrent.Future]]. Typically, we should not
  * discard [[scala.concurrent.Future]] because exceptions inside the future may not get logged.
  * Use `FutureUtil.doNotAwait` to log exceptions and discard the future where necessary.
  *
  * Does not (yet) detect discarded `FutureUnlessShutdown` nor `EitherT[Future, _, _]` nor `OptionT[Future, _]`.
  */
object DiscardedFuture extends WartTraverser {
  val message = "Statements must not discard a Future"

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val futureTypeConstructor = typeOf[Future[Unit]].typeConstructor
    val eitherTTypeConstructor = typeOf[EitherT[Future, Unit, Unit]].typeConstructor
    val optionTTypeConstructor = typeOf[OptionT[Future, Unit]].typeConstructor
    val verifyMethodName: TermName = TermName("verify")
    val futureLikeType = typeOf[DoNotDiscardLikeFuture]

    // Allow Mockito `verify` calls because they do not produce a future but merely check that a mocked Future-returning
    // method has been called.
    //
    // We do not check whether the receiver of the `verify` call is actually something that inherits org.mockito.MockitoSugar
    // because `BaseTest` extends `MockitoSugar` and we'd therefore have to do some virtual method resolution.
    // As a result, we ignore all statements of the above form verify(...).someMethod(...)(...)
    @tailrec
    def isMockitoVerify(statement: Tree): Boolean = {
      statement match {
        // Match on verify(...).someMethod
        case Select(
              Apply(TypeApply(Select(_receiver, verifyMethod), _tyargs), _verifyArgs),
              _method,
            ) if verifyMethod == verifyMethodName =>
          true
        // Strip away any further argument lists on the method as in verify(...).someMethod(...)(...)(...
        // including implicit arguments and type arguments
        case Apply(maybeVerifyCall, args) => isMockitoVerify(maybeVerifyCall)
        case TypeApply(maybeVerifyCall, tyargs) => isMockitoVerify(maybeVerifyCall)
        case _ => false
      }
    }

    new u.Traverser {
      @tailrec
      def isFutureLike(typ: Type): Boolean = {
        if (typ.typeConstructor =:= futureTypeConstructor) true
        else if (typ.typeConstructor =:= eitherTTypeConstructor) {
          val args = typ.typeArgs
          args.nonEmpty && isFutureLike(args(0))
        } else if (typ.typeConstructor =:= optionTTypeConstructor) {
          val args = typ.typeArgs
          args.nonEmpty && isFutureLike(args(0))
        } else if (typ.typeConstructor.typeSymbol.annotations.exists(_.tree.tpe =:= futureLikeType))
          true
        else {
          // Strip off type functions and just look at their body
          typ match {
            case PolyType(_binds, body) => isFutureLike(body)
            case _ => false
          }
        }
      }

      def checkForDiscardedFutures(statements: List[Tree]): Unit = {
        statements.foreach {
          case Block((statements0, _)) =>
            checkForDiscardedFutures(statements0)
          case statement =>
            val typeIsFuture = statement.tpe != null && isFutureLike(statement.tpe.dealias)
            if (typeIsFuture && !isMockitoVerify(statement)) {
              error(u)(statement.pos, message)
            }
        }
      }

      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          case Block(statements, _) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case ClassDef(_, _, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case ModuleDef(_, _, Template((_, _, statements))) =>
            checkForDiscardedFutures(statements)
            super.traverse(tree)
          case _ => super.traverse(tree)
        }
      }
    }
  }
}

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]]
  * when looking for discarded futures.
  */
final class DoNotDiscardLikeFuture extends StaticAnnotation
