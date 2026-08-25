// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Warns when a tapir GET endpoint uses a request body via `.in(jsonBody[...])` or similar.
  *
  * GET requests with bodies are technically allowed by HTTP but are considered bad practice:
  * intermediate proxies and caches may strip the body, and many clients do not support it. Use POST
  * or PUT for requests that carry a body.
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

    // Returns the `.get` selection in the chain, so the error can be reported on it rather than
    def getMethodInChain(tree: Tree): Option[Tree] = tree match {
      case t @ Select(recv, TermName("get")) if recv.tpe != null && isEndpointType(recv.tpe) =>
        Some(t)
      case Apply(fun, _) => getMethodInChain(fun)
      case TypeApply(fun, _) => getMethodInChain(fun)
      case Select(recv, _) => getMethodInChain(recv)
      case _ => None
    }

    def asInCall(tree: Tree): Option[(Tree, List[Tree])] = {
      def loop(t: Tree, args: List[Tree]): Option[(Tree, List[Tree])] = t match {
        case Apply(fun, as) => loop(fun, as)
        case TypeApply(fun, _) => loop(fun, args)
        case Select(recv, TermName("in")) => Some((recv, args))
        case _ => None
      }
      loop(tree, Nil)
    }

    // True if anywhere in the receiver chain there is an `.in(bodyInput)` call.
    // `.out(jsonBody[...])` is deliberately not matched, only inputs count.
    def hasInBodyInChain(tree: Tree): Boolean =
      asInCall(tree) match {
        case Some((_, args)) if args.exists(isBodyInput) => true
        case Some((recv, _)) => hasInBodyInChain(recv)
        case None =>
          tree match {
            case Apply(fun, _) => hasInBodyInChain(fun)
            case TypeApply(fun, _) => hasInBodyInChain(fun)
            case Select(recv, _) => hasInBodyInChain(recv)
            case _ => false
          }
      }

    def isEndpointExpression(tree: Tree): Boolean =
      tree.tpe != null && isEndpointType(tree.tpe)

    new u.Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          case t if hasWartAnnotation(u)(t) =>

          case _ =>
            getMethodInChain(tree) match {
              case Some(getCall) if isEndpointExpression(tree) && hasInBodyInChain(tree) =>
                val pos = Option(getCall.pos).filter(_ != NoPosition).getOrElse(tree.pos)
                error(u)(pos, message)
              case _ =>
                super.traverse(tree)
            }
        }
    }
  }
}
