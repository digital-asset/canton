// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import org.json4s.JValue
import org.json4s.JsonAST.{JInt, JObject}

/** Container for storing a versioned json value, equivalent to
  * [[com.digitalasset.canton.version.v1.UntypedVersionedMessage]], but with manual serialization.
  * @param version
  *   the proto version of the json contained in `data`
  * @param data
  *   the json serialization of a type according to the specified proto `version`
  */
final case class UntypedVersionedJsonMessage(version: Int, data: JValue) {
  def toJson: JValue = JObject("version" -> JInt(version), "data" -> data)
}

object UntypedVersionedJsonMessage {
  def fromJson(value: JValue): Either[String, UntypedVersionedJsonMessage] = {
    import org.json4s.*
    value match {
      case obj: JObject =>
        for {
          version <- obj \ "version" match {
            case l: JLong => Right(l.num.toInt)
            case i: JInt => Right(i.num.toInt)
            case otherwise => Left(s"Invalid type for field 'version': $otherwise")
          }
          data <- obj \ "data" match {
            case o: JObject => Right(o)
            case otherwise => Left(s"Invalid type for field 'data': $otherwise")
          }
        } yield UntypedVersionedJsonMessage(version, data)
      case _ => Left(s"Invalid format for ${classOf[UntypedVersionedJsonMessage]}: $value")
    }
  }
}

/** The JSON equivalent of [[VersionedMessage]].
  */
object VersionedJsonMessage {
  def apply[M](json: JValue, version: Int): VersionedJsonMessage[M] =
    VersionedJsonMessage(UntypedVersionedJsonMessage(version, json))

  def apply[M](message: UntypedVersionedJsonMessage): VersionedJsonMessage[M] =
    VersionedJsonMessageImpl.Instance.subst(message)
}

/** The JSON equivalent of [[VersionedMessageImpl]].
  */
sealed abstract class VersionedJsonMessageImpl {
  type VersionedJsonMessage[+A] <: UntypedVersionedJsonMessage
  private[version] def subst[M](message: UntypedVersionedJsonMessage): VersionedJsonMessage[M]
}

object VersionedJsonMessageImpl {
  val Instance: VersionedJsonMessageImpl = new VersionedJsonMessageImpl {
    override type VersionedJsonMessage[+A] = UntypedVersionedJsonMessage

    override private[version] def subst[M](
        message: UntypedVersionedJsonMessage
    ): VersionedJsonMessage[M] = message
  }

}
