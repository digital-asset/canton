// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Warns when a tapir GET endpoint uses a request body via `.in(jsonBody[...])` or similar.
  *
  * GET requests with bodies are technically allowed by HTTP but are considered bad practice:
  * intermediate proxies and caches may strip the body, and many clients do not support it.
  * Use POST or PUT for requests that carry a body.
  *
  * To suppress for an existing endpoint that cannot be changed (e.g. for backwards compatibility),
  * annotate the enclosing `val` or `def` with:
  * {{{
  * @SuppressWarnings(Array("com.digitalasset.canton.GetEndpointWithBody"))
  * }}}
  */
object GetEndpointWithBody extends WartTraverser {

  val message =
    "GET endpoints should not have a request body. Use POST/PUT instead. " +
      "To suppress, add @SuppressWarnings(Array(\"com.digitalasset.canton.GetEndpointWithBody\")) to the enclosing definition."

  private val bodyInputMethods =
    Set("jsonBody", "stringBody", "rawBody", "binaryBody", "formBody", "multipartBody")

  override def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    def isEndpointType(tpe: Type): Boolean =
      tpe != null && tpe != NoType &&
        tpe.typeSymbol != null && tpe.typeSymbol != NoSymbol &&
        tpe.typeSymbol.fullName == "sttp.tapir.Endpoint"

    def extractMethodName(tree: Tree): String = tree match {
      case TypeApply(f, _) => extractMethodName(f)
      case Apply(f, _) => extractMethodName(f)
      case Select(_, name) => name.toString
      case Ident(name) => name.toString
      case _ => ""
    }

    def isBodyInput(tree: Tree): Boolean =
      bodyInputMethods.contains(extractMethodName(tree))

    def hasGetMethodInChain(tree: Tree): Boolean = tree match {
      case Select(recv, TermName("get")) if recv.tpe != null && isEndpointType(recv.tpe) => true
      case Apply(Select(recv, TermName("get")), Nil) if recv.tpe != null && isEndpointType(recv.tpe) =>
        true
      case Apply(Select(recv, _), _) => hasGetMethodInChain(recv)
      case Select(recv, _) => hasGetMethodInChain(recv)
      case TypeApply(recv, _) => hasGetMethodInChain(recv)
      case _ => false
    }

    new u.Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          case t if hasWartAnnotation(u)(t) =>

          case Apply(Select(recv, TermName("in")), List(arg))
              if isBodyInput(arg) &&
                recv.tpe != null && isEndpointType(recv.tpe) &&
                hasGetMethodInChain(recv) =>
            error(u)(tree.pos, message)

          case _ =>
            super.traverse(tree)
        }
    }
  }
}
