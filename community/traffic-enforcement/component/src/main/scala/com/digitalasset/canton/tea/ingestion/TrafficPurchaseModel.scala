// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

object TrafficPurchaseModel {
  object Template {
    val PackageName: String = "traffic-purchase-models"
    val ModuleName: String = "Tea.TrafficPurchase"
    val TemplateName: String = "TrafficPurchaser"
    val ChoiceName: String = "TrafficPurchaser_PurchaseCredits"
  }
  object ResultFields {
    val PaymasterField = "paymaster"
    val TargetAccountField = "targetAccount"
    val AccountIdField = "accountId"
    val TrafficAmountField = "trafficAmount"
  }
}
