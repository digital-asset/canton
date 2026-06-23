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

  override def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      private def isEndpointType(tpe: TypeRepr): Boolean =
        tpe.typeSymbol.fullName == "sttp.tapir.Endpoint"

      private def extractMethodName(tree: Tree): String = tree match {
        case TypeApply(f, _) => extractMethodName(f)
        case Apply(f, _) => extractMethodName(f)
        case Select(_, name) => name
        case Ident(name) => name
        case _ => ""
      }

      private def isBodyInput(tree: Tree): Boolean =
        bodyInputMethods.contains(extractMethodName(tree))

      private def hasGetMethodInChain(tree: Tree): Boolean = tree match {
        case Select(recv, "get") if isEndpointType(recv.tpe) => true
        case Apply(Select(recv, "get"), Nil) if isEndpointType(recv.tpe) => true
        case Apply(Select(recv, _), _) => hasGetMethodInChain(recv)
        case Select(recv, _) => hasGetMethodInChain(recv)
        case TypeApply(recv, _) => hasGetMethodInChain(recv)
        case _ => false
      }

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        tree match
          case _ if hasWartAnnotation(tree) => ()

          // Detect .in(bodyInput) where the receiver chain includes .get on a tapir Endpoint
          case Apply(Select(recv, "in"), List(arg))
              if isBodyInput(arg) &&
                isEndpointType(recv.tpe) &&
                hasGetMethodInChain(recv) =>
            error(tree.pos, message)

          case _ =>
            super.traverseTree(tree)(owner)
    }
}
