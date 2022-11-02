// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates
//
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.data.{EitherT, OptionT}
import org.wartremover.WartUniverse

import scala.annotation.tailrec
import scala.concurrent.Future

object FutureLikeTester {

  /** The returned predicate tests whether the given type is future-like.
    * A type `T` is considered future-like if one of the following holds:
    * - The type constructor of `T` is [[scala.concurrent.Future]]
    * - The type constructor of `T` is [[cats.data.EitherT]] or [[cats.data.OptionT]]
    *   and its first type argument is future-like.
    * - The type constructor of `T` is annotated with the annotation `futureLikeType`
    */
  def tester(
      u: WartUniverse
  )(futureLikeType: u.universe.Type): u.universe.Type => Boolean = {
    import u.universe.*

    val futureTypeConstructor: Type = typeOf[Future[Unit]].typeConstructor
    val eitherTTypeConstructor: Type =
      typeOf[EitherT[Future, Unit, Unit]].typeConstructor
    val optionTTypeConstructor: Type = typeOf[OptionT[Future, Unit]].typeConstructor

    @tailrec
    def go(typ: Type): Boolean =
      if (typ.typeConstructor =:= futureTypeConstructor) true
      else if (typ.typeConstructor =:= eitherTTypeConstructor) {
        val args = typ.typeArgs
        args.nonEmpty && go(args(0))
      } else if (typ.typeConstructor =:= optionTTypeConstructor) {
        val args = typ.typeArgs
        args.nonEmpty && go(args(0))
      } else if (typ.typeConstructor.typeSymbol.annotations.exists(_.tree.tpe =:= futureLikeType))
        true
      else {
        // Strip off type functions and just look at their body
        typ match {
          case PolyType(_binds, body) => go(body)
          case _ => false
        }
      }

    go
  }
}
