// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0
import slick.jdbc.GetResult

final case class ServiceAgreementId(override protected val str: String255)
    extends LengthLimitedStringWrapper

object ServiceAgreementId
    extends LengthLimitedStringWrapperCompanion[String255, ServiceAgreementId] {
  override def instanceName: String = "ServiceAgreementId"

  override protected def companion: String255.type = String255

  override protected def factoryMethodWrapper(str: String255): ServiceAgreementId =
    ServiceAgreementId(str)
}

final case class ServiceAgreement(id: ServiceAgreementId, text: String)
    extends HasProtoV0[v0.ServiceAgreement] {
  override def toProtoV0: v0.ServiceAgreement = v0.ServiceAgreement(id.unwrap, text)
}

object ServiceAgreement {
  implicit val serviceAgreementGetResult: GetResult[ServiceAgreement] =
    GetResult(r => ServiceAgreement(ServiceAgreementId.tryCreate(r.<<), r.<<))

  def fromProtoV0(
      agreement: v0.ServiceAgreement
  ): ParsingResult[ServiceAgreement] =
    ServiceAgreementId
      .fromProtoPrimitive(agreement.id)
      .map(ServiceAgreement(_, agreement.legalText))
}
