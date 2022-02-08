// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.grpc

import com.digitalasset.canton.domain.api.{v0 => adminProto}
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.v0

import scala.concurrent.Future

/** Domain service on Grpc
  * @param sequencerConnection Tells remote participants how to connect to the domain sequencer(s).
  */
class GrpcDomainService(
    agreementManager: Option[ServiceAgreementManager],
    protected val loggerFactory: NamedLoggerFactory,
) extends adminProto.DomainServiceGrpc.DomainService
    with NamedLogging {

  override def getServiceAgreement(
      request: adminProto.GetServiceAgreementRequest
  ): Future[adminProto.GetServiceAgreementResponse] = {
    val agreement =
      agreementManager.map(manager =>
        v0.ServiceAgreement(manager.agreement.id.toProtoPrimitive, manager.agreement.text)
      )
    Future.successful(adminProto.GetServiceAgreementResponse(agreement))
  }
}
