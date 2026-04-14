// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.shortcircuit

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBinding.TxConsumer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBinding,
  BftBindingFactory,
}

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ShortCircuitBindingFactory(override val loggerFactory: NamedLoggerFactory)
    extends BftBindingFactory {

  override type T = ShortCircuitBinding

  override def create(
      transactionSizesAndWeights: NonEmpty[Seq[BftBenchmarkConfig.TransactionSizeAndWeight]]
  ): ShortCircuitBinding =
    new ShortCircuitBinding
}

final class ShortCircuitBinding extends BftBinding {

  private val subscriptions =
    new ConcurrentHashMap[
      BftBenchmarkConfig.Node,
      TxConsumer,
    ]()

  override def write(
      node: BftBenchmarkConfig.WriteNode[?],
      txId: String,
  ): CompletableFuture[Unit] =
    CompletableFuture.completedFuture {
      subscriptions.asScala.get(node) match {
        case Some(txConsumer) => txConsumer(CompletableFuture.completedFuture(txId))
        case _ => ()
      }
    }

  override def subscribeOnce(
      node: BftBenchmarkConfig.ReadNode[?],
      txConsumer: TxConsumer,
  ): Unit =
    subscriptions
      .computeIfAbsent(
        node,
        _ => txConsumer,
      )
  ()

  override def close(): Unit = ()
}
