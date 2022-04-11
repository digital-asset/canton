// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import com.digitalasset.canton.logging.pretty.Pretty

import scala.collection.immutable

/** Additional methods for [[com.daml.nonempty.NonEmpty]].
  *
  * Cats instances for [[com.daml.nonempty.NonEmpty]] must be imported explicitly as
  * `import `[[com.daml.nonempty.catsinstances]]`._` when necessary.
  */
object NonEmptyUtil {
  def fromUnsafe[A](xs: A with immutable.Iterable[_]): NonEmpty[A] =
    NonEmpty.from(xs).getOrElse(throw new NoSuchElementException)

  object instances {

    /** This instance is exposed as [[com.digitalasset.canton.logging.pretty.PrettyInstances.prettyNonempty]].
      * It lives only here because `NonEmptyColl.Instance.subst` is private to the `nonempty` package
      */
    def prettyNonEmpty[A](implicit F: Pretty[A]): Pretty[NonEmpty[A]] = {
      type K[T[_]] = Pretty[T[A]]
      NonEmptyColl.Instance.subst[K](F)
    }
  }
}
