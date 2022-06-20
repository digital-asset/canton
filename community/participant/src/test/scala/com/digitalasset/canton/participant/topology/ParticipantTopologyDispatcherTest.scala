// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Remove}
import com.digitalasset.canton.topology.transaction.{
  IdentifierDelegation,
  NamespaceDelegation,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyStateUpdate,
  TopologyStateUpdateMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, DomainAlias}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class ParticipantTopologyDispatcherTest extends AsyncWordSpec with BaseTest {

  import DefaultTestIdentities._

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
    Seq[TopologyStateUpdateMapping](
      NamespaceDelegation(namespace, publicKey, isRootDelegation = true),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("alpha"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("beta"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("gamma"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("delta"), namespace), publicKey),
    ).map(TopologyStateUpdate.createAdd(_, defaultProtocolVersion))
  val slice1 = transactions.slice(0, 2)
  val slice2 = transactions.slice(slice1.length, transactions.length)

  private def mk(expect: Int) = {
    val source = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
    val target = new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
    )
    val manager = new ParticipantTopologyManager(clock, source, crypto, timeouts, loggerFactory)
    val dispatcher =
      new ParticipantTopologyDispatcher(manager, timeouts, loggerFactory)
    val handle = new MockHandle(expect, store = target)
    val client = mock[DomainTopologyClientWithInit]
    (source, target, manager, dispatcher, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      response: State = State.Accepted,
      store: TopologyStore[TopologyStoreId],
  ) extends RegisterTopologyTransactionHandle {
    val buffer = ListBuffer[SignedTopologyTransaction[TopologyChangeOp]]()
    val promise = new AtomicReference[Promise[Unit]](Promise[Unit]())
    val expect = new AtomicInteger(expectI)
    override def submit(
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
    ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponse.Result]] =
      FutureUnlessShutdown.outcomeF {
        logger.debug(s"Observed ${transactions.length} transactions")
        buffer ++= transactions
        for {
          _ <- MonadUtil.sequentialTraverse(transactions)(x => {
            logger.debug(s"Adding $x")
            val ts = CantonTimestamp.now()
            store.append(
              SequencedTime(ts),
              EffectiveTime(ts),
              List(ValidatedTopologyTransaction(x, None)),
            )
          })
          _ = if (buffer.length >= expect.get()) {
            promise.get().success(())
          }
        } yield {
          logger.debug(s"Done with observed ${transactions.length} transactions")
          transactions.map(transaction =>
            RegisterTopologyTransactionResponse.Result(
              state = response,
              uniquePathProtoPrimitive = transaction.uniquePath.toProtoPrimitive,
            )
          )
        }
      }

    def clear(expectI: Int): Seq[SignedTopologyTransaction[TopologyChangeOp]] = {
      val ret = buffer.toList
      buffer.clear()
      expect.set(expectI)
      promise.set(Promise())
      ret
    }

    def allObserved(): Future[Unit] = promise.get().future

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = ParticipantTopologyDispatcherTest.this.logger
  }

  private def push(
      manager: ParticipantTopologyManager,
      transactions: Seq[TopologyTransaction[TopologyChangeOp]],
  ): Future[
    Either[ParticipantTopologyManagerError, Seq[SignedTopologyTransaction[TopologyChangeOp]]]
  ] =
    MonadUtil
      .sequentialTraverse(transactions)(x =>
        manager.authorize(x, Some(publicKey.fingerprint), ProtocolVersion.latestForTest)
      )
      .value

  private def dispatcherConnected(
      dispatcher: ParticipantTopologyDispatcher,
      handle: RegisterTopologyTransactionHandle,
      client: DomainTopologyClientWithInit,
      target: TopologyStore[TopologyStoreId.DomainStore],
  ): Future[Unit] = dispatcher
    .domainConnected(domain, domainId, defaultProtocolVersion, handle, client, target)
    .value
    .map(_ => ())
    .onShutdown(())

  "dispatcher" should {

    "dispatch transaction on new connect" in {
      val (_source, target, manager, dispatcher, handle, client) = mk(transactions.length)
      for {
        res <- push(manager, transactions)
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer should have length (transactions.length.toLong)
      }
    }

    "dispatch transaction on existing connections" in {
      val (_, target, manager, dispatcher, handle, client) = mk(transactions.length)
      for {
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer should have length (transactions.length.toLong)
      }
    }

    "dispatch transactions continuously" in {
      val (_, target, manager, dispatcher, handle, client) = mk(slice1.length)
      for {
        _res <- push(manager, slice1)
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- handle.allObserved()
        observed1 = handle.clear(slice2.length)
        _ <- push(manager, slice2)
        _ <- handle.allObserved()
      } yield {
        observed1.map(_.transaction) shouldBe slice1
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "not dispatch old data when reconnected" in {
      val (_, target, manager, dispatcher, handle, client) = mk(slice1.length)
      for {
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- push(manager, slice1)
        _ <- handle.allObserved()
        _ = handle.clear(slice2.length)
        _ = dispatcher.domainDisconnected(domain)
        res2 <- push(manager, slice2)
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- handle.allObserved()
      } yield {
        res2.value shouldBe a[Seq[_]]
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "correctly find a remove in source store" in {

      val (source, target, manager, dispatcher, handle, client) = mk(transactions.length)

      val midRevert = transactions(2).reverse
      val another =
        TopologyStateUpdate.createAdd(
          IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("eta"), namespace), publicKey),
          defaultProtocolVersion,
        )

      for {
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- push(manager, transactions)
        _ <- handle.allObserved()
        _ = dispatcher.domainDisconnected(domain)
        // add a remove and another add
        _ <- push(manager, Seq(midRevert, another))
        // ensure that topology manager properly processed this state
        ais <- source.headTransactions.map(_.toTopologyState)
        _ = ais should not contain (midRevert.element)
        _ = ais should contain(another.element)
        // and ensure both are not in the new store
        tis <- target.headTransactions.map(_.toTopologyState)
        _ = tis should contain(midRevert.element)
        _ = tis should not contain (another.element)
        // re-connect
        _ = handle.clear(2)
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- handle.allObserved()
        tis <- target.headTransactions.map(_.toTopologyState)
      } yield {
        tis should not contain (midRevert.element)
        tis should contain(another.element)
      }
    }

    "not push deprecated transactions" in {
      val (_, target, manager, dispatcher, handle, client) = mk(transactions.length - 1)
      val midRevert = transactions(2).reverse
      for {
        res <- push(manager, transactions :+ midRevert)
        _ <- dispatcherConnected(dispatcher, handle, client, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[_]]
        handle.buffer.map(x =>
          (
            x.transaction.op,
            x.transaction.element.uniquePath.maybeUid.map(_.id),
          )
        ) shouldBe Seq(
          (Add, None),
          (Add, Some("alpha")),
          (Add, Some("gamma")),
          (Add, Some("delta")),
          (Remove, Some("beta")),
        )
        handle.buffer should have length (5)
      }
    }

  }

}
