// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

class ParticipantTopologyDispatcherTest extends AsyncWordSpec with BaseTest {

  // TODO(#11255) re-enable this test. as we have refactored the api, we need to adapt this test.
  //    as the functionality itself is tested quite well in integration tests and as we
  //    won't change the dispatcher (and there is little chance of someone else changing it)
  //    we disabled to unblock the next x-node refactorings
  /*
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
    Seq[TopologyStateUpdateMapping](
      NamespaceDelegation(namespace, publicKey, isRootDelegation = true),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("alpha"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("beta"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("gamma"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("delta"), namespace), publicKey),
    ).map(TopologyStateUpdate.createAdd(_, testedProtocolVersion))
  val slice1 = transactions.slice(0, 2)
  val slice2 = transactions.slice(slice1.length, transactions.length)

  private def mk(expect: Int) = {
    val source = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val target = new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val manager = new ParticipantTopologyManager(
      clock,
      source,
      crypto,
      timeouts,
      testedProtocolVersion,
      loggerFactory,
      futureSupervisor,
    )
    val mockState = mock[SyncDomainPersistentStateManagerImpl[SyncDomainPersistentStateOld]]
    val dispatcher =
      new ParticipantTopologyDispatcher(
        manager,
        participant1,
        mockState,
        crypto,
        clock,
        timeouts,
        loggerFactory,
      )
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
    ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult]] =
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
            RegisterTopologyTransactionResponseResult.create(
              state = response,
              uniquePathProtoPrimitive = transaction.uniquePath.toProtoPrimitive,
              protocolVersion = testedProtocolVersion,
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
        manager.authorize(x, Some(publicKey.fingerprint), testedProtocolVersion)
      )
      .value
      .failOnShutdown

  private def dispatcherConnected(
      dispatcher: ParticipantTopologyDispatcher,
      handle: RegisterTopologyTransactionHandle,
      client: DomainTopologyClientWithInit,
      target: TopologyStore[TopologyStoreId.DomainStore],
  ): Future[Unit] = dispatcher.createHandler(domain, domainId, testedProtocolVersion, client)
    .domainConnected(domain, domainId, testedProtocolVersion, handle, client, target, crypto)
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
          testedProtocolVersion,
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
   */
}
