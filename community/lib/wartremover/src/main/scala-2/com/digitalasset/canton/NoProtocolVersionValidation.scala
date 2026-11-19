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

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val noValidationName = TermName("NoValidation")
    val protocolVersionValidationName = TermName("ProtocolVersionValidation")

    def isNoValidation(symbol: Symbol): Boolean =
      symbol != null && symbol.isModule && symbol.name == noValidationName &&
        symbol.owner.isModuleClass && symbol.owner.name.toTermName == protocolVersionValidationName

    new Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          // The case object's own synthetic members, e.g. readResolve
          case t: DefDef if isSynthetic(u)(t) =>
          case CaseDef(_, guard, body) =>
            traverse(guard)
            traverse(body)
          case _: Select | _: Ident if isNoValidation(tree.symbol) =>
            error(u)(tree.pos, message)
          case _ =>
            super.traverse(tree)
        }
    }
  }
}
