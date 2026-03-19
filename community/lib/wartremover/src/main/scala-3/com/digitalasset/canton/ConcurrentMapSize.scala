// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Detects calls to `size` on concurrent Scala maps */
object ConcurrentMapSize extends WartTraverser {

  val message =
    ".size on concurrent Maps often requires to iterate over the tree and can therefore cause a performance bottleneck"

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          if tree.isExpr then
            tree.asExpr match
              case '{ ($r: scala.collection.concurrent.Map[?, ?]).size } => error(tree.pos, message)
              case _ => ()
          super.traverseTree(tree)(owner)
    }
}
