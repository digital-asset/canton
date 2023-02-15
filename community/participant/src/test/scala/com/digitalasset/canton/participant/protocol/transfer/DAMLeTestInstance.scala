// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.implicits.*
import com.daml.lf.engine.Error
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.{PackageInspectionOpsForTesting, PackageService}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import org.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, Future}

object DAMLeTestInstance {

  def apply(participant: ParticipantId, signatories: Set[LfPartyId], stakeholders: Set[LfPartyId])(
      loggerFactory: SuppressingLogger
  )(implicit ec: ExecutionContext): DAMLe = {
    val pureCrypto = new SymbolicPureCrypto
    val engine = DAMLe.newEngine(uniqueContractKeys = false, enableLfDev = false)
    val mockPackageService =
      new PackageService(
        engine,
        new InMemoryDamlPackageStore(loggerFactory),
        mock[ParticipantEventPublisher],
        pureCrypto,
        _ => EitherT.rightT(()),
        new PackageInspectionOpsForTesting(participant, loggerFactory),
        ProcessingTimeout(),
        loggerFactory,
      )
    val packageResolver = DAMLe.packageResolver(mockPackageService)
    new DAMLe(
      packageResolver,
      engine,
      loggerFactory,
    ) {
      override def contractMetadata(
          contractInstance: LfContractInst,
          supersetOfSignatories: Set[LfPartyId],
      )(implicit traceContext: TraceContext): EitherT[Future, Error, ContractMetadata] = {
        EitherT.pure[Future, Error](ContractMetadata.tryCreate(signatories, stakeholders, None))
      }
    }

  }
}
