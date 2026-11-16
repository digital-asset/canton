// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Flags references to `ProtocolVersionValidation.NoValidation`, which skips all protocol version
  * dependent checks during deserialization.
  *
  * Pass the caller's protocol version validation, or `PV(pv)` for a protocol version from trusted
  * state. Pattern matches on `NoValidation` are not flagged. The match ignores the package, so that
  * the tests can use a stand-in.
  */
object NoProtocolVersionValidation extends WartTraverser {

  val message: String =
    "Do not hard-code ProtocolVersionValidation.NoValidation. Pass the caller's protocol version validation."

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      private def isNoValidation(symbol: Symbol): Boolean =
        !symbol.isNoSymbol && symbol.flags.is(Flags.Module) && symbol.name == "NoValidation" &&
          symbol.owner.flags.is(Flags.Module) &&
          symbol.owner.name.stripSuffix("$") == "ProtocolVersionValidation"

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          // Ignore trees marked by SuppressWarnings
          case _ if hasWartAnnotation(tree) => ()
          // The case object's own synthetic members, e.g. readResolve
          case t: DefDef if t.symbol.flags.is(Flags.Synthetic) => ()
          case CaseDef(_, guard, body) =>
            guard.foreach(traverseTree(_)(owner))
            traverseTree(body)(owner)
          case _: Ident | _: Select if isNoValidation(tree.symbol) =>
            error(tree.pos, message)
          case _ =>
            super.traverseTree(tree)(owner)
    }
}
