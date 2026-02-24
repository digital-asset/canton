// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.{
  StringConversionError,
  ValueConversionError,
}
import com.digitalasset.canton.config.CantonRequireTypes.{String3, String300}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{Member, UniqueIdentifier}

sealed trait Recipient extends Product with Serializable with PrettyPrinting {

  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300
}

/** Represents the state after resolving recipients, where we resolve:
  *   - [[MediatorGroupRecipient]]
  *   - [[SequencersOfSynchronizer]]
  *
  * But not [[AllMembersOfSynchronizer]], since it's too costly to resolve it.
  */
sealed trait MemberRecipientOrBroadcast extends Recipient

object Recipient {
  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[Recipient] = {
    val dlen = UniqueIdentifier.delimiter.length
    val (typ, rest) = {
      val (code, str) = recipient.splitAt(3)
      (code, str.drop(dlen))
    }
    lazy val codeE = GroupRecipientCode.fromProtoPrimitive(typ, fieldName)

    if (codeE.isLeft)
      Member.fromProtoPrimitive(recipient, fieldName).map(MemberRecipient.apply)
    else
      for {
        _ <- Either.cond(
          recipient.length >= 3 + dlen,
          (),
          ValueConversionError(
            fieldName,
            s"Invalid group recipient `$recipient`, expecting <three-letter-code>::id::info.",
          ),
        )
        _ <- Either.cond(
          recipient.substring(3, 3 + dlen) == UniqueIdentifier.delimiter,
          (),
          ValueConversionError(
            fieldName,
            s"Expected delimiter ${UniqueIdentifier.delimiter} after three letter code of `$recipient`",
          ),
        )
        code <- codeE
        groupRecipient <- code match {
          case SequencersOfSynchronizer.Code =>
            Right(SequencersOfSynchronizer)
          case MediatorGroupRecipient.Code =>
            for {
              groupInt <-
                Either
                  .catchOnly[NumberFormatException](rest.toInt)
                  .leftMap(e =>
                    StringConversionError(
                      s"Cannot parse group number $rest, error ${e.getMessage}"
                    )
                  )
              group <- ProtoConverter.parseNonNegativeInt(s"group in $recipient", groupInt)
            } yield MediatorGroupRecipient(group)
          case AllMembersOfSynchronizer.Code =>
            Right(AllMembersOfSynchronizer)
        }
      } yield groupRecipient
  }

}

sealed trait GroupRecipientCode {
  def threeLetterId: String3

  def toProtoPrimitive: String = threeLetterId.unwrap
}

object GroupRecipientCode {
  def fromProtoPrimitive_(code: String): Either[String, GroupRecipientCode] =
    String3.create(code).flatMap {
      case SequencersOfSynchronizer.Code.threeLetterId => Right(SequencersOfSynchronizer.Code)
      case MediatorGroupRecipient.Code.threeLetterId => Right(MediatorGroupRecipient.Code)
      case AllMembersOfSynchronizer.Code.threeLetterId => Right(AllMembersOfSynchronizer.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[GroupRecipientCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))
}

sealed trait GroupRecipient extends Recipient {
  def code: GroupRecipientCode
  def suffix: String

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${UniqueIdentifier.delimiter}$suffix"
    )
}

object TopologyBroadcastAddress {
  val recipient: Recipient = AllMembersOfSynchronizer
}

final case class MemberRecipient(member: Member) extends MemberRecipientOrBroadcast {

  override protected def pretty: Pretty[MemberRecipient] =
    prettyOfClass(
      unnamedParam(_.member)
    )

  override def toLengthLimitedString: String300 = member.toLengthLimitedString
}

final case object SequencersOfSynchronizer extends GroupRecipient {

  override protected def pretty: Pretty[SequencersOfSynchronizer.type] =
    prettyOfObject[SequencersOfSynchronizer.type]

  override def code: GroupRecipientCode = SequencersOfSynchronizer.Code

  override def suffix: String = ""

  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("SOD")
  }
}

final case class MediatorGroupRecipient(group: MediatorGroupIndex) extends GroupRecipient {

  override protected def pretty: Pretty[MediatorGroupRecipient] =
    prettyOfClass(
      param("group", _.group)
    )

  override def code: GroupRecipientCode = MediatorGroupRecipient.Code

  override def suffix: String = group.toString
}

object MediatorGroupRecipient {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("MGR")
  }

  def fromProtoPrimitive(
      mediatorGroupRecipientP: String,
      fieldName: String,
  ): ParsingResult[MediatorGroupRecipient] =
    Recipient.fromProtoPrimitive(mediatorGroupRecipientP, fieldName).flatMap {
      case mod: MediatorGroupRecipient => Right(mod)
      case other =>
        Left(ValueConversionError(fieldName, s"Expected MediatorGroupRecipient, got $other"))
    }
}

/** All known members of the synchronizer, i.e., the return value of
  * [[com.digitalasset.canton.topology.client.MembersTopologySnapshotClient#allMembers]].
  */
final case object AllMembersOfSynchronizer extends GroupRecipient with MemberRecipientOrBroadcast {

  override protected def pretty: Pretty[AllMembersOfSynchronizer.type] =
    prettyOfString(_ => suffix)

  override def code: GroupRecipientCode = Code

  override def suffix: String = "All"
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("ALL")
  }
}
