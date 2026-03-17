// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Warns about calling `.size` on a Scala concurrent map because `TrieMap.size` iterates over the
  * whole tree. Does not warn about `.sizeIs` and `.sizeCompare` because it is more obvious that
  * they could take some time. `.sizeIs` is inlined by the compiler, so it does not even appear in
  * the syntax tree any more!
  *
  * Does not catch calls to `.size` if the concurrent map is upcast to a non-concurrent map type
  * such as [[scala.collection.mutable.Map]].
  */
object ConcurrentMapSize extends WartTraverser {

  val message =
    ".size on concurrent Maps and friends often require to iterate over the tree and can therefore cause a performance bottleneck"

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val concurrentMapTypeSymbol =
      typeOf[scala.collection.concurrent.Map[Unit, Unit]].typeConstructor.typeSymbol
    require(concurrentMapTypeSymbol != NoSymbol)
    val sizeMethodName: TermName = TermName("size")

    new u.Traverser {

      def isSubtypeOfConcurrentMap(typ: Type): Boolean =
        typ.typeConstructor
          .baseType(concurrentMapTypeSymbol)
          .typeConstructor
          .typeSymbol == concurrentMapTypeSymbol

      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>

          case Select(receiver, method)
              if (method == sizeMethodName) &&
                receiver.tpe != null && isSubtypeOfConcurrentMap(receiver.tpe) =>
            error(u)(tree.pos, message)

          case _ =>
            super.traverse(tree)
        }
    }
  }
}
