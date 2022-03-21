// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.either._
import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.store.db.DbDeserializationException
import slick.jdbc.{GetResult, SetParameter}

import java.time.Instant

/** Participant local identifier used to refer to a Domain without the need to fetch identifying information from a domain.
  * This does not need to be globally unique. Only unique for the participant using it.
  * @param str String with given alias
  */
case class DomainAlias(protected val str: String255)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  override def pretty: Pretty[DomainAlias] =
    prettyOfString(inst => show"Domain ${inst.unwrap.singleQuoted}")
}
object DomainAlias extends LengthLimitedStringWrapperCompanion[String255, DomainAlias] {
  override protected def companion: String255.type = String255
  override def instanceName: String = "DomainAlias"
  override protected def factoryMethodWrapper(str: String255): DomainAlias = DomainAlias(str)
}

case class TimedValue[A](timestamp: Instant, value: A)

/** Command identifier for tracking ledger commands
  * @param id ledger string representing command
  */
case class CommandId(private val id: LfLedgerString) extends PrettyPrinting {
  def unwrap: LfLedgerString = id
  def toProtoPrimitive: String = unwrap
  def toLengthLimitedString: String255 =
    checked(String255.tryCreate(id)) // LfLedgerString is limited to 255 chars
  override def pretty: Pretty[CommandId] = prettyOfParam(_.unwrap)
}

object CommandId {
  def assertFromString(str: String) = CommandId(LfLedgerString.assertFromString(str))
  def fromProtoPrimitive(str: String): Either[String, CommandId] =
    LfLedgerString.fromString(str).map(CommandId(_))

  implicit val getResultCommandId: GetResult[CommandId] = GetResult(r => r.nextString()).andThen {
    fromProtoPrimitive(_).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize command id: $err")
    )
  }

  implicit val setParameterCommandId: SetParameter[CommandId] = (v, pp) =>
    pp >> v.toLengthLimitedString
}

/** Application identifier for identifying customer applications in the ledger api
  * @param id ledger string representing application
  */
case class ApplicationId(private val id: LedgerApplicationId) extends PrettyPrinting {
  def unwrap: LedgerApplicationId = id
  def toProtoPrimitive: String = unwrap
  def toLengthLimitedString: String255 =
    checked(String255.tryCreate(id)) // LedgerApplicationId is limited to 255 chars
  override def pretty: Pretty[ApplicationId] = prettyOfParam(_.unwrap)
}

object ApplicationId {
  def assertFromString(str: String) = ApplicationId(LedgerApplicationId.assertFromString(str))
  def fromProtoPrimitive(str: String): Either[String, ApplicationId] =
    LedgerApplicationId.fromString(str).map(ApplicationId(_))

  implicit val getResultApplicationId: GetResult[ApplicationId] =
    GetResult(r => r.nextString()).andThen {
      fromProtoPrimitive(_).valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize application id: $err")
      )
    }

  implicit val setParameterApplicationId: SetParameter[ApplicationId] = (v, pp) =>
    pp >> v.toLengthLimitedString
}

/** Workflow identifier for identifying customer workflows, i.e. individual requests, in the ledger api
  * @param id ledger string representing workflow
  */
case class WorkflowId(private val id: LfLedgerString) extends PrettyPrinting {
  def unwrap: LfLedgerString = id
  def toProtoPrimitive: String = unwrap
  override def pretty: Pretty[WorkflowId] = prettyOfParam(_.unwrap)
}

object WorkflowId {
  def assertFromString(str: String) = WorkflowId(LfLedgerString.assertFromString(str))
  def fromProtoPrimitive(str: String): Either[String, WorkflowId] =
    LfLedgerString.fromString(str).map(WorkflowId(_))
}
