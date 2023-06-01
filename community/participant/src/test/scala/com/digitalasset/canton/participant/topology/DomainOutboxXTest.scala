// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits.*
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandleCommon
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TopologyStoreId,
  TopologyStoreX,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.{Remove, Replace}
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.GenericTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, DomainAlias, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.chaining.scalaUtilChainingOps

class DomainOutboxXTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  import DefaultTestIdentities.*

  private val clock = new WallClock(timeouts, loggerFactory)
  private val crypto = TestingIdentityFactory(loggerFactory).newCrypto(participant1)
  private val publicKey =
    FutureUtil
      .noisyAwaitResult(crypto.cryptoPublicStore.signingKeys.value, "get public key", 10.seconds)
      .valueOrFail("signing keys")
      .headOption
      .value
  private val namespace = Namespace(publicKey.id)
  private val domain = DomainAlias.tryCreate("target")
  private val transactions =
    Seq[TopologyMappingX](
      valueOrFail(NamespaceDelegationX.create(namespace, publicKey, isRootDelegation = true))("NS"),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("alpha"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("beta"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("gamma"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("delta"), namespace), publicKey),
    ).map(txAddFromMapping)
  val slice1 = transactions.slice(0, 2)
  val slice2 = transactions.slice(slice1.length, transactions.length)

  private def mk(expect: Int) = {
    val source = new InMemoryTopologyStoreX(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
    )
    val target = new InMemoryTopologyStoreX(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
    )
    val manager = new TopologyManagerX(
      clock,
      crypto,
      source,
      timeouts,
      testedProtocolVersion,
      futureSupervisor,
      loggerFactory,
    )
    val handle = new MockHandle(expect, store = target)
    val client = mock[DomainTopologyClientWithInit]
    (source, target, manager, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      response: State = State.Accepted,
      store: TopologyStoreX[TopologyStoreId],
  ) extends RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransactionX] {
    val buffer = ListBuffer[GenericSignedTopologyTransactionX]()
    val promise = new AtomicReference[Promise[Unit]](Promise[Unit]())
    val expect = new AtomicInteger(expectI)
    override def submit(
        transactions: Seq[GenericSignedTopologyTransactionX]
    ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult.State]] =
      FutureUnlessShutdown.outcomeF {
        logger.debug(s"Observed ${transactions.length} transactions")
        buffer ++= transactions
        for {
          _ <- MonadUtil.sequentialTraverse(transactions)(x => {
            logger.debug(s"Processing $x")
            val ts = CantonTimestamp.now()
            store.update(
              SequencedTime(ts),
              EffectiveTime(ts),
              additions = List(ValidatedTopologyTransactionX(x, None)),
              // dumbed down version of how to "append" ValidatedTopologyTransactionXs:
              removeMapping = Option
                .when(x.transaction.op == TopologyChangeOpX.Remove)(x.transaction.mapping.uniqueKey)
                .toList
                .toSet,
              removeTxs = Set.empty,
            )
          })
          _ = if (buffer.length >= expect.get()) {
            promise.get().success(())
          }
        } yield {
          logger.debug(s"Done with observed ${transactions.length} transactions")
          transactions.map(_ => response)
        }
      }

    def clear(expectI: Int): Seq[GenericSignedTopologyTransactionX] = {
      val ret = buffer.toList
      buffer.clear()
      expect.set(expectI)
      promise.set(Promise())
      ret
    }

    def allObserved(): Future[Unit] = promise.get().future

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = DomainOutboxXTest.this.logger
  }

  private def push(
      manager: TopologyManagerX,
      transactions: Seq[GenericTopologyTransactionX],
  ): Future[
    Either[TopologyManagerError, Seq[GenericSignedTopologyTransactionX]]
  ] =
    MonadUtil
      .sequentialTraverse(transactions)(tx =>
        manager.proposeAndAuthorize(
          tx.op,
          tx.mapping,
          tx.serial.some,
          signingKeys = Seq(publicKey.fingerprint),
          testedProtocolVersion,
          expectFullAuthorization = false,
        )
      )
      .value
      .failOnShutdown

  private def outboxConnected(
      manager: TopologyManagerX,
      handle: RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransactionX],
      client: DomainTopologyClientWithInit,
      source: TopologyStoreX[TopologyStoreId.AuthorizedStore],
      target: TopologyStoreX[TopologyStoreId.DomainStore],
  ): Future[DomainOutboxX] = {
    val domainOutbox = new DomainOutboxX(
      domain,
      domainId,
      participant1,
      testedProtocolVersion,
      handle,
      client,
      source,
      target,
      timeouts,
      loggerFactory,
      crypto,
    )
    domainOutbox
      .startup()
      .fold[DomainOutboxX](
        s => fail(s"Failed to start domain outbox ${s}"),
        _ =>
          domainOutbox.tap(outbox =>
            // add the outbox as an observer since these unit tests avoid instantiating the ParticipantTopologyDispatcher
            manager.addObserver(new TopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[GenericSignedTopologyTransactionX],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
                val num = transactions.size
                outbox.newTransactionsAddedToAuthorizedStore(timestamp, num)
              }
            })
          ),
      )
      .onShutdown(domainOutbox)
  }

  private def outboxDisconnected(manager: TopologyManagerX): Unit =
    manager.clearObservers()

  private def txAddFromMapping(mapping: TopologyMappingX) =
    TopologyTransactionX(
      TopologyChangeOpX.Replace,
      serial = PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

  private def headTransactions(store: TopologyStoreX[_]) = store
    .findPositiveTransactions(
      asOf = CantonTimestamp.MaxValue,
      asOfInclusive = false,
      isProposal = false,
      types = TopologyMappingX.Code.all,
      filterUid = None,
      filterNamespace = None,
    )
    .map(x => StoredTopologyTransactionsX(x.result.filter(_.validUntil.isEmpty)))

  "dispatcher" should {

    // TODO(#12373) Adapt version when releasing BFT here and below
    "dispatch transaction on new connect" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val (source, target, manager, handle, client) =
        mk(transactions.length)
      for {
        res <- push(manager, transactions)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer should have length (transactions.length.toLong)
      }
    }

    "dispatch transaction on existing connections" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val (source, target, manager, handle, client) =
        mk(transactions.length)
      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer should have length (transactions.length.toLong)
      }
    }

    "dispatch transactions continuously" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val (source, target, manager, handle, client) = mk(slice1.length)
      for {
        _res <- push(manager, slice1)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
        observed1 = handle.clear(slice2.length)
        _ <- push(manager, slice2)
        _ <- handle.allObserved()
      } yield {
        observed1.map(_.transaction) shouldBe slice1
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "not dispatch old data when reconnected" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val (source, target, manager, handle, client) = mk(slice1.length)
      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- push(manager, slice1)
        _ <- handle.allObserved()
        _ = handle.clear(slice2.length)
        _ = outboxDisconnected(manager)
        res2 <- push(manager, slice2)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res2.value shouldBe a[Seq[_]]
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "correctly find a remove in source store" onlyRunWithOrGreaterThan ProtocolVersion.dev in {

      val (source, target, manager, handle, client) =
        mk(transactions.length)

      val midRevert = transactions(2).reverse
      val another =
        txAddFromMapping(
          IdentifierDelegationX(
            UniqueIdentifier(Identifier.tryCreate("eta"), namespace),
            publicKey,
          )
        )

      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- push(manager, transactions)
        _ <- handle.allObserved()
        _ = outboxDisconnected(manager)
        // add a remove and another add
        _ <- push(manager, Seq(midRevert, another))
        // ensure that topology manager properly processed this state
        ais <- headTransactions(source).map(_.toTopologyState)
        _ = ais should not contain (midRevert.mapping)
        _ = ais should contain(another.mapping)
        // and ensure both are not in the new store
        tis <- headTransactions(target).map(_.toTopologyState)
        _ = tis should contain(midRevert.mapping)
        _ = tis should not contain (another.mapping)
        // re-connect
        _ = handle.clear(2)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
        tis <- headTransactions(target).map(_.toTopologyState)
      } yield {
        tis should not contain (midRevert.mapping)
        tis should contain(another.mapping)
      }
    }

    "not push deprecated transactions" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val (source, target, manager, handle, client) =
        mk(transactions.length - 1)
      val midRevertSerialBumped = transactions(2).reverse
      for {
        res <- push(manager, transactions :+ midRevertSerialBumped)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer.map(x =>
          (
            x.transaction.op,
            x.transaction.mapping.maybeUid.map(_.id),
          )
        ) shouldBe Seq(
          (Replace, None),
          (Replace, Some("alpha")),
          (Replace, Some("gamma")),
          (Replace, Some("delta")),
          (Remove, Some("beta")),
        )
        handle.buffer should have length (5)
      }
    }

  }
}
