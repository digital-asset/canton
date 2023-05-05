// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.{
  StringConversionError,
  ValueConversionError,
}
import com.digitalasset.canton.config.CantonRequireTypes.{String3, String300}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{
  DomainId,
  Member,
  PartyId,
  SafeSimpleString,
  UniqueIdentifier,
}

import scala.util.matching.Regex

sealed trait Recipient extends Product with Serializable with PrettyPrinting {
  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300
}

object Recipient {
  private val mediatorsOfDomainPattern: Regex = """^(\d+)::(.*)$""".r

  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[Recipient] = {
    val dlen = SafeSimpleString.delimiter.length
    val (typ, rest) = {
      val (code, str) = recipient.splitAt(3)
      (code, str.drop(dlen))
    }
    lazy val codeE = GroupRecipientCode.fromProtoPrimitive(typ, fieldName)

    if (codeE.isLeft)
      Member.fromProtoPrimitive(recipient, fieldName).map(MemberRecipient)
    else
      for {
        _ <- Either.cond(
          recipient.length > 3 + (2 * dlen),
          (),
          ValueConversionError(
            fieldName,
            s"Invalid group recipient `$recipient`, expecting <three-letter-code>::id::info.",
          ),
        )
        _ <- Either.cond(
          recipient.substring(3, 3 + dlen) == SafeSimpleString.delimiter,
          (),
          ValueConversionError(
            fieldName,
            s"Expected delimiter ${SafeSimpleString.delimiter} after three letter code of `$recipient`",
          ),
        )
        code <- codeE
        groupRecipient <- code match {
          case ParticipantsOfParty.Code =>
            UniqueIdentifier
              .fromProtoPrimitive(rest, fieldName)
              .map(PartyId(_))
              .map(ParticipantsOfParty(_))
          case SequencersOfDomain.Code =>
            UniqueIdentifier
              .fromProtoPrimitive(rest, fieldName)
              .map(DomainId(_))
              .map(SequencersOfDomain(_))
          case MediatorsOfDomain.Code =>
            rest match {
              case mediatorsOfDomainPattern(number, uid) =>
                for {
                  group <-
                    Either
                      .catchOnly[NumberFormatException](number.toInt)
                      .leftMap(e =>
                        StringConversionError(
                          s"Cannot parse group number $number, error ${e.getMessage}"
                        )
                      )
                  domain <- UniqueIdentifier
                    .fromProtoPrimitive(uid, fieldName)
                    .map(DomainId(_))
                } yield MediatorsOfDomain(domain, group)
              case _ => Left(StringConversionError(s"Mediators of domain string $rest is invalid"))
            }
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
      case ParticipantsOfParty.Code.threeLetterId => Right(ParticipantsOfParty.Code)
      case SequencersOfDomain.Code.threeLetterId => Right(SequencersOfDomain.Code)
      case MediatorsOfDomain.Code.threeLetterId => Right(MediatorsOfDomain.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[GroupRecipientCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))
}

trait GroupRecipient extends Recipient {
  def code: GroupRecipientCode
  def suffix: String

  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${SafeSimpleString.delimiter}$suffix"
    )
}

final case class MemberRecipient(member: Member) extends Recipient {
  override def pretty: Pretty[MemberRecipient] =
    prettyOfClass(
      unnamedParam(_.member)
    )

  override def toLengthLimitedString: String300 = member.toLengthLimitedString
}

final case class ParticipantsOfParty(party: PartyId) extends GroupRecipient {
  override def pretty: Pretty[ParticipantsOfParty] =
    prettyOfClass(
      unnamedParam(_.party)
    )

  override def code: GroupRecipientCode = ParticipantsOfParty.Code

  override def suffix: String = party.toProtoPrimitive
}

object ParticipantsOfParty {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("POP")
  }
}

final case class SequencersOfDomain(domain: DomainId) extends GroupRecipient {
  override def pretty: Pretty[SequencersOfDomain] =
    prettyOfClass(
      unnamedParam(_.domain)
    )

  override def code: GroupRecipientCode = SequencersOfDomain.Code

  override def suffix: String = domain.toProtoPrimitive
}

object SequencersOfDomain {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("SOD")
  }
}

final case class MediatorsOfDomain(domain: DomainId, group: Int) extends GroupRecipient {
  override def pretty: Pretty[MediatorsOfDomain] =
    prettyOfClass(
      param("domain", _.domain),
      param("group", _.group),
    )

  override def code: GroupRecipientCode = MediatorsOfDomain.Code

  override def suffix: String = s"$group${SafeSimpleString.delimiter}${domain.toProtoPrimitive}"
}

object MediatorsOfDomain {
  object Code extends GroupRecipientCode {
    val threeLetterId: String3 = String3.tryCreate("MOD")
  }
}
