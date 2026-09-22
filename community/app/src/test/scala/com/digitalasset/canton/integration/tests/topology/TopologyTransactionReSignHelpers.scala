// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.FutureHelpers
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

/** Provides helper methods to re-sign a participant's topology transactions to match the
  * `testedProtocolVersion`.
  *
  * During auto-initialization, a node generates its identity transactions using a default protocol
  * version because it is isolated and does not yet know which synchronizer it will connect to.
  * Tests often extract these raw, local transactions to use as explicit onboarding payloads.
  *
  * Because synchronizers strictly require incoming onboarding transactions to be serialized against
  * their exact protocol version, this trait provides an easy way to bump and re-sign these baseline
  * transactions to match the test's dynamic environment.
  */
trait TopologyTransactionReSignHelpers extends FutureHelpers {

  import org.scalatest.OptionValues.*
  import org.scalatest.EitherValues.*

  protected def reSignForTestedProtocolVersion(
      participant: LocalParticipantReference,
      signedTxs: Seq[GenericSignedTopologyTransaction],
      testedProtocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Seq[GenericSignedTopologyTransaction] = {
    val crypto = participant.underlying.value.sync.syncCrypto.crypto.privateCrypto
    signedTxs.map { oldSignedTx =>
      val oldTx = oldSignedTx.transaction
      val newTx = TopologyTransaction.tryCreate(
        oldTx.operation,
        oldTx.serial,
        oldTx.mapping,
        testedProtocolVersion,
      )
      SignedTopologyTransaction
        .signAndCreate(
          newTx,
          signingKeys = NonEmpty
            .from(oldSignedTx.signatures.map(_.authorizingLongTermKey).toSet)
            .getOrElse(sys.error("No signatures found")),
          isProposal = oldSignedTx.isProposal,
          crypto = crypto,
          protocolVersion = testedProtocolVersion,
        )
        .futureValueUS
        .value
    }
  }

  protected def reSignTopologyStoredTransactionsOf(
      participant: LocalParticipantReference,
      filterMappings: Seq[TopologyMapping.Code],
      testedProtocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Seq[GenericSignedTopologyTransaction] = {
    val txs = participant.topology.transactions
      .list(filterMappings = filterMappings)
      .result
      .map(_.transaction)
    reSignForTestedProtocolVersion(participant, txs, testedProtocolVersion)
  }

  protected def onboardingTransactionsOf(
      participant: LocalParticipantReference,
      testedProtocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Seq[GenericSignedTopologyTransaction] =
    reSignTopologyStoredTransactionsOf(
      participant,
      TopologyStore.initialParticipantDispatchingSet.forgetNE.toSeq,
      testedProtocolVersion,
    )
}
