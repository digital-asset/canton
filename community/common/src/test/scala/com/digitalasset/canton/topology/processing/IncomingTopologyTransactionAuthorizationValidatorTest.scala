// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersChange,
  IdentifierDelegation,
  NamespaceDelegation,
  OwnerToKeyMapping,
  ParticipantPermission,
  ParticipantState,
  PartyToParticipant,
  RequestSide,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TrustLevel,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  Identifier,
  Namespace,
  ParticipantId,
  PartyId,
  SequencerId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TopologyTransactionTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeys(domainManager, loggerFactory, initEc) {

  import SigningKeys.*

  val ns1 = Namespace(key1.fingerprint)
  val ns6 = Namespace(key6.fingerprint)
  val domainId = DomainId(UniqueIdentifier(Identifier.tryCreate("domain"), ns1))
  val uid1a = UniqueIdentifier(Identifier.tryCreate("one"), ns1)
  val uid1b = UniqueIdentifier(Identifier.tryCreate("two"), ns1)
  val uid6 = UniqueIdentifier(Identifier.tryCreate("other"), ns6)
  val party1b = PartyId(uid1b)
  val party2 = PartyId(uid6)
  val participant1 = ParticipantId(uid1a)
  val participant6 = ParticipantId(uid6)
  val ns1k1_k1 = mkAdd(NamespaceDelegation(ns1, key1, isRootDelegation = true), key1)
  val ns1k2_k1 = mkAdd(NamespaceDelegation(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(NamespaceDelegation(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(NamespaceDelegation(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail = mkAdd(NamespaceDelegation(ns1, key8, isRootDelegation = false), key3)
  val ns6k3_k6 = mkAdd(NamespaceDelegation(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(NamespaceDelegation(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k2 = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k2p = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k1 = mkAdd(IdentifierDelegation(uid1a, key4), key1)

  val id6k4_k1 = mkAdd(IdentifierDelegation(uid6, key4), key1)

  val okm1ak5_k3 = mkAdd(OwnerToKeyMapping(participant1, key5), key3)
  val okm1ak1E_k3 = mkAdd(OwnerToKeyMapping(participant1, EncryptionKeys.key1), key3)
  val okm1ak5_k2 = mkAdd(OwnerToKeyMapping(participant1, key5), key2)
  val okm1bk5_k1 = mkAdd(OwnerToKeyMapping(participant1, key5), key1)
  val okm1bk5_k4 = mkAdd(OwnerToKeyMapping(participant1, key5), key4)

  val sequencer1 = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer1"), ns1))
  val okmS1k7_k1 = mkAdd(OwnerToKeyMapping(sequencer1, key7), key1)
  val okmS1k9_k1 = mkAdd(OwnerToKeyMapping(sequencer1, key9), key1)
  val okmS1k7_k1_remove = mkTrans(okmS1k7_k1.transaction.reverse)

  val ps1d1T_k3 = mkAdd(
    ParticipantState(
      RequestSide.To,
      domainManager.domainId,
      participant1,
      ParticipantPermission.Submission,
      TrustLevel.Ordinary,
    ),
    key3,
  )
  val ps1d1F_k1 = mkAdd(
    ParticipantState(
      RequestSide.From,
      domainManager.domainId,
      participant1,
      ParticipantPermission.Submission,
      TrustLevel.Ordinary,
    ),
    key1,
  )

  val defaultDomainParameters = TestDomainParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipant(RequestSide.Both, party1b, participant1, ParticipantPermission.Submission),
      key2,
    )
  val p1p2F_k2 =
    mkAdd(
      PartyToParticipant(RequestSide.From, party1b, participant6, ParticipantPermission.Submission),
      key2,
    )
  val p1p2T_k6 =
    mkAdd(
      PartyToParticipant(RequestSide.To, party1b, participant6, ParticipantPermission.Submission),
      key6,
    )
  val p1p2B_k3 =
    mkAdd(
      PartyToParticipant(RequestSide.Both, party1b, participant6, ParticipantPermission.Submission),
      key3,
    )

  val dmp1_k2 = mkDmGov(
    DomainParametersChange(DomainId(uid1a), defaultDomainParameters),
    key2,
  )

  val dmp1_k1 = mkDmGov(
    DomainParametersChange(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkDmGov(
    DomainParametersChange(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

}

class IncomingTopologyTransactionAuthorizationValidatorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  "topology transaction authorization" when {

    object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk(
        store: InMemoryTopologyStore[TopologyStoreId] =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
    ) = {
      val validator =
        new IncomingTopologyTransactionAuthorizationValidator(
          Factory.cryptoApi.pureCrypto,
          store,
          Some(DefaultTestIdentities.domainId),
          loggerFactory,
        )
      validator
    }

    def check(
        validated: Seq[ValidatedTopologyTransaction],
        outcome: Seq[Option[TopologyTransactionRejection => Boolean]],
    ) = {
      validated should have length (outcome.size.toLong)
      validated.zipWithIndex.zip(outcome).foreach {
        case ((ValidatedTopologyTransaction(transaction, Some(err)), idx), Some(expected)) =>
          assert(expected(err), (err, expected))
        case ((ValidatedTopologyTransaction(transaction, rej), idx), expected) =>
          assertResult(expected, s"idx=$idx $transaction")(rej)
      }
      assert(true)
    }

    val unauthorized =
      Some((err: TopologyTransactionRejection) => err == TopologyTransactionRejection.NotAuthorized)

    implicit val toValidated
        : SignedTopologyTransaction[TopologyChangeOp] => ValidatedTopologyTransaction =
      x => ValidatedTopologyTransaction(x, None)

    "receiving transactions with signatures" should {
      "succeed to add if the signature is valid" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, ns1k2_k1))
        } yield {
          check(res._2, Seq(None, None))
        }
      }
      "fail to add if the signature is invalid" in {
        val validator = mk()
        import Factory.*
        val invalid = ns1k2_k1.copy(signature = ns1k1_k1.signature)(
          signedTransactionProtocolVersionRepresentative,
          None,
        )
        for {
          (_, validatedTopologyTransactions) <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, invalid),
          )
        } yield {
          check(
            validatedTopologyTransactions,
            Seq(
              None,
              Some({
                case TopologyTransactionRejection.SignatureCheckFailed(_) => true
                case _ => false
              }),
            ),
          )
        }
      }
      "reject if the transaction is for the wrong domain" in {
        val validator = mk()
        import Factory.*
        val wrongDomain = DomainId(UniqueIdentifier.tryCreate("wrong", ns1.fingerprint.unwrap))
        val pid = ParticipantId(UniqueIdentifier.tryCreate("correct", ns1.fingerprint.unwrap))
        val wrong = mkAdd(
          ParticipantState(
            RequestSide.Both,
            wrongDomain,
            pid,
            ParticipantPermission.Submission,
            TrustLevel.Ordinary,
          ),
          Factory.SigningKeys.key1,
        )
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, wrong))
        } yield {
          check(
            res._2,
            Seq(
              None,
              Some({
                case TopologyTransactionRejection.WrongDomain(_) => true
                case _ => false
              }),
            ),
          )
        }
      }
    }

    "observing namespace delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(ts(0), List(ns1k1_k1, ns1k2_k1, ns1k3_k2))
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns6k3_k6, ns1k3_k2, ns1k2_k1, ns1k3_k2),
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized, None, None))
        }
      }
      "succeed and use load existing delegations" in {
        val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(ns1k2_k1, ns1k3_k2))
        } yield {
          check(res._2, Seq(None, None))
        }
      }

      "fail on incremental non-authorized transactions" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k1_k1, ns1k3_k2, id1ak4_k2, ns1k2_k1, ns6k3_k6, id1ak4_k1),
          )

        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized, None, unauthorized, None))
        }
      }

    }

    "observing identifier delegations" should {
      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, id1ak4_k1, ns1k2_k1, id1ak4_k2),
          )
        } yield {
          check(res._2, Seq(None, None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(id1ak4_k1, ns1k1_k1, id1ak4_k1, id6k4_k1),
          )
        } yield {
          check(res._2, Seq(unauthorized, None, None, unauthorized))
        }
      }
    }

    "observing normal delegations" should {

      "succeed if transaction is properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, okm1ak5_k2, p1p1B_k2, id1ak4_k1, ns6k6_k6, p1p2F_k2, p1p2T_k6),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, None, None, None))
        }
      }
      "fail if transaction is not properly authorized" in {
        val validator = mk()
        import Factory.*
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, okm1ak5_k2, p1p1B_k2),
          )
        } yield {
          check(res._2, Seq(None, unauthorized, unauthorized))
        }
      }
      "succeed with loading existing identifier delegations" in {
        val store: InMemoryTopologyStore[TopologyStoreId.AuthorizedStore] =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1, id1ak4_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(ns1k2_k1, p1p2F_k2, p1p1B_k2))
        } yield {
          check(res._2, Seq(None, None, None))
        }
      }
    }

    "observing removals" should {
      "accept authorized removals" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = revert(ns1k2_k1)
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None))
        }
      }

      "reject un-authorized after removal" in {
        val validator = mk()
        import Factory.*
        val Rns1k2_k1 = revert(ns1k2_k1)
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          res <- validator.validateAndUpdateHeadAuthState(
            ts(0),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k1, Rns1k2_k1, Rid1ak4_k1, okm1ak5_k2, p1p2F_k2),
          )
        } yield {
          check(res._2, Seq(None, None, None, None, None, unauthorized, unauthorized))
        }
      }

    }

    "correctly determine cascading update for" should {
      "namespace additions" in {
        val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns6k6_k6))
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(ns1k1_k1, okm1bk5_k1, p1p2T_k6),
          )
        } yield {
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "namespace removals" in {
        val store: InMemoryTopologyStore[TopologyStoreId] =
          new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        val Rns1k1_k1 = revert(ns1k1_k1)
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(Rns1k1_k1, okm1bk5_k1))
        } yield {
          check(res._2, Seq(None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
        }
      }

      "identifier additions and removals" in {
        val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        val Rid1ak4_k1 = revert(id1ak4_k1)
        for {
          _ <- store.append(SequencedTime(ts(0)), EffectiveTime(ts(0)), List(ns1k1_k1))
          res <- validator.validateAndUpdateHeadAuthState(ts(1), List(id1ak4_k1))
          res2 <- validator.validateAndUpdateHeadAuthState(ts(2), List(Rid1ak4_k1))
        } yield {
          res._1.cascadingNamespaces shouldBe Set()
          res._1.cascadingUids shouldBe Set(uid1a)
          res2._1.cascadingUids shouldBe Set(uid1a)
        }
      }

      "cascading invalidation pre-existing identifier uids" in {
        val store = new InMemoryTopologyStore(TopologyStoreId.AuthorizedStore, loggerFactory)
        val validator = mk(store)
        import Factory.*
        import Factory.SigningKeys.*
        // scenario: we have id1ak4_k2 previously loaded. now we get a removal on k2. we need to ensure that
        // nothing can be added by k4
        val Rns1k2_k1 = revert(ns1k2_k1)
        val id6ak7_k6 = mkAdd(IdentifierDelegation(uid6, key7), key6)
        for {
          _ <- store.append(
            SequencedTime(ts(0)),
            EffectiveTime(ts(0)),
            List(ns1k1_k1, ns1k2_k1, id1ak4_k2, ns6k6_k6),
          )
          res <- validator.validateAndUpdateHeadAuthState(
            ts(1),
            List(p1p2F_k2, Rns1k2_k1, id6ak7_k6, p1p2F_k2),
          )
        } yield {
          check(res._2, Seq(None, None, None, unauthorized))
          res._1.cascadingNamespaces shouldBe Set(ns1)
          res._1.filteredCascadingUids shouldBe Set(uid6)
        }
      }
    }
  }

}
