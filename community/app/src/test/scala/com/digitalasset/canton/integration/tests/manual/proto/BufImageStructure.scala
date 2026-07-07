// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.proto

import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.{
  MessageCategory,
  RequestType,
  ResponseType,
}
import com.digitalasset.canton.util.OptionUtil

/** Structure of the messages. The relation parent-kid is via inclusion.
  * @param forest
  *   Map from a node to its children
  * @param messageCategories
  *   For each message, the categories it belongs to
  * @param roots
  *   All the nodes without parents
  * @param endpoints
  *   List of endpoints, with their request and response
  */
final case class BufImageStructure(
    forest: Map[String, Set[String]],
    messageCategories: Map[String, Set[MessageCategory]],
    roots: Set[String],
    endpoints: Map[String, (RequestType, ResponseType)],
)

object BufImageStructure {
  import org.scalatest.OptionValues.*

  sealed trait MessageCategory extends Product with Serializable
  object MessageCategory {
    case object Storage extends MessageCategory
    case object Request extends MessageCategory
    case object Response extends MessageCategory

    case object Alpha extends MessageCategory
    case object Stable extends MessageCategory
  }

  final case class RequestType(str: String)
  final case class ResponseType(str: String)

  private[proto] def getNonEmptyString(str: Option[String]) =
    str.flatMap(OptionUtil.emptyStringAsNone).value
}
