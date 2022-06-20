// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

trait TopologyManagerTest
    extends AnyWordSpec
    with BeforeAndAfterAll
    with BaseTest
    with HasExecutionContext {

  def topologyManager[E <: CantonError](
      mk: (
          Clock,
          TopologyStore[TopologyStoreId.AuthorizedStore],
          Crypto,
          NamedLoggerFactory,
      ) => Future[TopologyManager[E]]
  ): Unit = {

    val loggerFactory = SuppressingLogger(getClass)
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)

    class MySetup(
        val crypto: Crypto,
        val namespaceKey: SigningPublicKey,
        val alphaKey: SigningPublicKey,
        val betaKey: SigningPublicKey,
    ) {
      val namespace = Namespace(namespaceKey.id)
      val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)

      def createRootCert() = {
        TopologyStateUpdate.createAdd(
          NamespaceDelegation(namespace, namespaceKey, isRootDelegation = true),
          defaultProtocolVersion,
        )
      }
    }

    object MySetup {

      def create(str: String): Future[MySetup] = {
        val crypto = SymbolicCrypto.create(timeouts, loggerFactory)

        def generateSigningKey(name: String): Future[SigningPublicKey] = {
          crypto
            .generateSigningKey(name = Some(KeyName.tryCreate(name)))
            .valueOr(err => fail(s"Failed to generate signing key: $err"))
        }

        for {
          namespaceKey <- generateSigningKey(s"namespace-$str")
          alphaKey <- generateSigningKey(s"alpha-$str")
          betaKey <- generateSigningKey(s"beta-$str")
        } yield new MySetup(crypto, namespaceKey, alphaKey, betaKey)

      }
    }

    def gen(str: String): Future[(TopologyManager[E], MySetup)] =
      for {
        mysetup <- MySetup.create(str)
        idManager <- mk(clock, mysetup.store, mysetup.crypto, loggerFactory)
      } yield (idManager, mysetup)

    def genAndAddRootCert() =
      for {
        genr <- gen("success")
        (mgr, setup) = genr
        rootCert = setup.createRootCert()
        auth <- mgr
          .authorize(
            rootCert,
            Some(setup.namespace.fingerprint),
            defaultProtocolVersion,
            force = false,
          )
          .value
        transaction = auth.getOrElse(fail("root cert addition failed"))
      } yield (mgr, setup, rootCert, transaction)

    def checkStore(
        store: TopologyStore[TopologyStoreId],
        numTransactions: Int,
        numActive: Int,
    ): Future[Unit] =
      for {
        tr <- store.allTransactions
        ac <- store.headTransactions
      } yield {
        tr.result should have length (numTransactions.toLong)
        ac.result should have length (numActive.toLong)
      }

    "authorize" should {
      "successfully add one" in {
        (for {
          (_, setup, _, _) <- genAndAddRootCert()
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
      "automatically find a signing key" in {
        def add(mgr: TopologyManager[E], mapping: TopologyStateUpdateMapping) =
          mgr.authorize(
            TopologyStateUpdate.createAdd(mapping, defaultProtocolVersion),
            None,
            defaultProtocolVersion,
            force = true,
          )

        val res = for {
          generated <- EitherT.right(genAndAddRootCert())
          (mgr, setup, rootCert, _) = generated
          uid = UniqueIdentifier(Identifier.tryCreate("Apple"), setup.namespace)
          uid2 = UniqueIdentifier(Identifier.tryCreate("Banana"), setup.namespace)
          extUid = UniqueIdentifier(
            Identifier.tryCreate("Ext"),
            Namespace(setup.betaKey.fingerprint),
          )
          _ <- add(mgr, NamespaceDelegation(setup.namespace, setup.alphaKey, false))
          _ <- add(mgr, IdentifierDelegation(uid, setup.alphaKey))
          _ <- add(mgr, OwnerToKeyMapping(ParticipantId(uid), setup.betaKey))
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.Both,
              PartyId(uid),
              ParticipantId(uid2),
              ParticipantPermission.Submission,
            ),
          )
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.From,
              PartyId(uid),
              ParticipantId(extUid),
              ParticipantPermission.Submission,
            ),
          )
          _ <- add(
            mgr,
            PartyToParticipant(
              RequestSide.To,
              PartyId(extUid),
              ParticipantId(uid),
              ParticipantPermission.Submission,
            ),
          )
        } yield succeed

        res.futureValue
      }
      "allow to add and remove one" in {
        (for {
          (mgr, setup, rootCert, _) <- genAndAddRootCert()
          authRemove <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              defaultProtocolVersion,
              false,
            )
            .value
          _ = authRemove.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
        } yield succeed).futureValue
      }
      "allow to add, remove and re-add" in {
        (for {
          (mgr, setup, rootCert, _) <- genAndAddRootCert()
          removeCert = rootCert.reverse
          authRemove <- mgr
            .authorize(
              removeCert,
              Some(setup.namespace.fingerprint),
              defaultProtocolVersion,
              false,
            )
            .value
          _ = authRemove.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
          authAdd2 <- mgr
            .authorize(
              removeCert.reverse,
              Some(setup.namespace.fingerprint),
              defaultProtocolVersion,
            )
            .value
          _ = authAdd2.value shouldBe a[SignedTopologyTransaction[_]]
          _ <- checkStore(setup.store, numTransactions = 3, numActive = 1)
        } yield succeed).futureValue
      }
      "fail duplicate addition" in {
        (for {
          (mgr, setup, rootCert, _) <- genAndAddRootCert()
          authAgain <- mgr
            .authorize(rootCert, Some(setup.namespace.fingerprint), defaultProtocolVersion)
            .value
          _ = authAgain.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
      "fail on duplicate removal" in {
        (for {
          (mgr, setup, rootCert, _) <- genAndAddRootCert()
          _ <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              defaultProtocolVersion,
            )
            .value
          authFail <- mgr
            .authorize(
              rootCert.reverse,
              Some(setup.namespace.fingerprint),
              defaultProtocolVersion,
            )
            .value
          _ = authFail.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 2, numActive = 0)
        } yield succeed).futureValue
      }
      "fail on invalid removal" in {
        (for {
          (mgr, setup, rootCert, _) <- genAndAddRootCert()
          removeRootCert = TopologyStateUpdate(Remove, rootCert.element)(defaultProtocolVersion)
          invalidRev = removeRootCert.reverse.reverse
          _ = assert(
            invalidRev.element.id != rootCert.element.id
          ) // ensure transaction ids are different so we are sure to fail the test
          authFail <- mgr
            .authorize(invalidRev, Some(setup.namespace.fingerprint), defaultProtocolVersion)
            .value
          _ = authFail.left.value shouldBe a[CantonError]
          _ <- checkStore(setup.store, numTransactions = 1, numActive = 1)
        } yield succeed).futureValue
      }
    }
    "add" should {
      "support importing generated topology transactions" in {
        (for {
          (mgr, setup, rootCert, transaction) <- genAndAddRootCert()
          (otherMgr, otherSetup) <- gen("other")
          // check that we can import
          importCert <- otherMgr.add(transaction).value
          _ = importCert shouldBe Right(())
          _ = checkStore(otherSetup.store, 1, 1)
          // and we shouldn't be able to re-import
          reImportFail <- mgr.add(transaction).value
        } yield reImportFail.left.value shouldBe a[CantonError]).futureValue
      }
      "fail on re-adding removed topology transactions" in {
        (for {
          (mgr, setup, rootCert, transaction) <- genAndAddRootCert()
          (otherMgr, otherSetup) <- gen("other")
          reverted = rootCert.reverse
          authRev <- mgr
            .authorize(
              reverted,
              Some(setup.namespaceKey.fingerprint),
              defaultProtocolVersion,
            )
            .value
          _ = authRev.value shouldBe a[SignedTopologyTransaction[_]]
          // import cert
          _ <- otherMgr.add(transaction).value
          // import reversion
          _ <- otherMgr.add(authRev.getOrElse(fail("reversion failed"))).value
          _ = checkStore(otherSetup.store, 2, 0)
          // now, re-importing previous cert should fail
          reimport <- otherMgr.add(transaction).value
          _ = reimport.left.value shouldBe a[CantonError]
        } yield succeed).futureValue
      }
    }
  }
}
