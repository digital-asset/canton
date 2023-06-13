// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1 as ledgerProto
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success}

class GrpcValidationTest extends AnyWordSpec with BaseTest {
  private val template = ledgerProto.value.Identifier(
    packageId = LfPackageId.assertFromString("pkg"),
    moduleName = "module",
    entityName = "other",
  )

  private val alice = LfPartyId.assertFromString("Alice")

  "GrpcValidation.validateTransactionFilter" should {
    "reject filtering by interface" in {
      def getFilter(withInterface: Boolean) = {
        val interfaceFilter =
          if (withInterface)
            Seq(
              ledgerProto.transaction_filter.InterfaceFilter(
                interfaceId = Some(Identifier("pId", "mId", "entity")),
                includeInterfaceView = false,
                includeCreateArgumentsBlob = false,
              )
            )
          else Nil

        val filter = ledgerProto.transaction_filter.Filters(
          Some(ledgerProto.transaction_filter.InclusiveFilters(Seq(template), interfaceFilter))
        )

        ledgerProto.transaction_filter.TransactionFilter(Map(alice -> filter))
      }

      inside(GrpcValidation.validateTransactionFilter(getFilter(true))) { case Failure(err) =>
        err.toString should contain
        "Filtering by interface is not supported"
      }

      GrpcValidation.validateTransactionFilter(getFilter(false)) shouldBe a[Success[*]]
    }
  }
}
