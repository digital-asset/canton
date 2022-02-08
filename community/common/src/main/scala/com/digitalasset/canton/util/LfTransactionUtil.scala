// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Monad, Order}
import cats.syntax.either._
import com.daml.lf.data._
import com.daml.lf.transaction.{Node, TransactionVersion}
import com.daml.lf.value.Value
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol._

import scala.annotation.nowarn

/** Helper functions to work with `com.digitalasset.daml.lf.transaction.GenTransaction`.
  * Using these helper functions is useful to provide a buffer from upstream changes.
  */
object LfTransactionUtil {

  implicit val orderTransactionVersion: Order[TransactionVersion] =
    Order.by[TransactionVersion, String](_.protoValue)(Order.fromOrdering)

  def nodeTemplate(node: LfActionNode): Ref.Identifier = node match {
    case n: LfNodeCreate => n.coinst.template
    case n: LfNodeFetch => n.templateId
    case n: LfNodeExercises => n.templateId
    case n: LfNodeLookupByKey => n.templateId
  }

  def consumedContractId(node: LfActionNode): Option[LfContractId] = node match {
    case _: LfNodeCreate => None
    case _: LfNodeFetch => None
    case nx: LfNodeExercises if nx.consuming => Some(nx.targetCoid)
    case _: LfNodeExercises => None
    case _: LfNodeLookupByKey => None
  }

  /** Check that `keyWithMaintainers` has a key that doesn't contain a contract ID (so can be used as a contact key).
    * If so, convert `keyWithMaintainers` to a [[com.digitalasset.canton.protocol.package.LfGlobalKeyWithMaintainers]].
    */
  def globalKeyWithMaintainers(
      templateId: LfTemplateId,
      keyWithMaintainers: LfKeyWithMaintainers,
  ): Either[LfContractId, LfGlobalKeyWithMaintainers] =
    checkNoContractIdInKey(keyWithMaintainers.key).map(value =>
      LfGlobalKeyWithMaintainers(LfGlobalKey(templateId, value), keyWithMaintainers.maintainers)
    )

  /** Convert `keyWithMaintainers` to a [[com.digitalasset.canton.protocol.package.LfGlobalKeyWithMaintainers]], throwing an
    * [[java.lang.IllegalArgumentException]] if `keyWithMaintainers` has a key
    * that contains a contract ID.
    */
  def tryGlobalKeyWithMaintainers(
      templateId: LfTemplateId,
      keyWithMaintainers: LfKeyWithMaintainers,
  ): LfGlobalKeyWithMaintainers =
    LfGlobalKeyWithMaintainers(
      LfGlobalKey(templateId, assertNoContractIdInKey(keyWithMaintainers.key)),
      keyWithMaintainers.maintainers,
    )

  def fromGlobalKeyWithMaintainers(global: LfGlobalKeyWithMaintainers): LfKeyWithMaintainers =
    LfKeyWithMaintainers(global.globalKey.key, global.maintainers)

  /** Get the IDs and metadata of contracts used within a single
    * [[com.digitalasset.canton.protocol.package.LfActionNode]]
    */
  def usedContractIdWithMetadata(node: LfActionNode): Option[WithContractMetadata[LfContractId]] = {
    node match {
      case _: LfNodeCreate => None
      case nf: LfNodeFetch =>
        Some(
          WithContractMetadata(
            nf.coid,
            ContractMetadata.tryCreate(
              nf.signatories,
              nf.stakeholders,
              nf.key.map(k => tryGlobalKeyWithMaintainers(nf.templateId, k)),
            ),
          )
        )
      // TODO(M40): ensure that a malicious submitter can't trigger the exception here if a key contains a contract ID
      case nx: LfNodeExercises =>
        Some(
          WithContractMetadata(
            nx.targetCoid,
            ContractMetadata.tryCreate(
              nx.signatories,
              nx.stakeholders,
              nx.key.map(tryGlobalKeyWithMaintainers(nx.templateId, _)),
            ),
          )
        )
      // TODO(#3013): Use stakeholders instead of maintainers of the node
      case nl: LfNodeLookupByKey =>
        nl.result.map(cid =>
          WithContractMetadata(
            cid,
            ContractMetadata.tryCreate(
              nl.keyMaintainers,
              nl.keyMaintainers,
              Some(tryGlobalKeyWithMaintainers(nl.templateId, nl.key)),
            ),
          )
        )
    }
  }

  /** Get the IDs and metadata of all the used contracts of a [[com.digitalasset.canton.protocol.package.LfVersionedTransaction]] */
  def usedContractIdsWithMetadata(
      tx: LfVersionedTransaction
  ): Set[WithContractMetadata[LfContractId]] = {
    val nodes = tx.nodes.values.collect { case an: LfActionNode => an }.toSet
    nodes.flatMap(n => usedContractIdWithMetadata(n).toList)
  }

  /** Get the IDs and metadata of all input contracts of a [[com.digitalasset.canton.protocol.package.LfVersionedTransaction]].
    * Input contracts are those that are used, but are not transient.
    */
  def inputContractIdsWithMetadata(
      tx: LfVersionedTransaction
  ): Set[WithContractMetadata[LfContractId]] = {
    val createdContracts = tx.localContracts
    // Input contracts are used contracts, but not transient contracts (transient contracts are created and then
    // archived by the transaction)
    usedContractIdsWithMetadata(tx).filterNot(x => createdContracts.contains(x.unwrap))
  }

  /** Get the IDs and metadata of all the created contracts of a single [[com.digitalasset.canton.protocol.package.LfNode]]. */
  def createdContractIdWithMetadata(node: LfNode): Option[WithContractMetadata[LfContractId]] =
    node match {
      case nc: LfNodeCreate =>
        Some(
          WithContractMetadata(
            nc.coid,
            ContractMetadata.tryCreate(
              nc.signatories,
              nc.stakeholders,
              nc.key.map(tryGlobalKeyWithMaintainers(nc.coinst.template, _)),
            ),
          )
        )
      case _ => None
    }

  /** All contract IDs referenced with a Daml `com.daml.lf.value.Value` */
  def referencedContractIds(value: Value): Set[LfContractId] = value.cids

  /** Whether or not a node has a random seed */
  def nodeHasSeed(node: LfNode): Boolean = node match {
    case _: LfNodeCreate => true
    case _: LfNodeExercises => true
    case _: LfNodeFetch => false
    case _: LfNodeLookupByKey => false
    case _: LfNodeRollback => false
  }

  private[this] def suffixForDiscriminator(
      unicumOfDiscriminator: LfHash => Option[Unicum]
  )(discriminator: LfHash): Bytes = {
    /* If we can't find the discriminator we leave it unchanged,
     * because this could refer to an input contract of the transaction.
     * The well-formedness checks ensure that unsuffixed discriminators of created contracts are fresh,
     * i.e., we suffix a discriminator either everywhere in the transaction or nowhere
     * even though the map from discriminators to unicum is built up in post-order of the nodes.
     */
    unicumOfDiscriminator(discriminator).fold(Bytes.Empty)(unicum => unicum.toContractIdSuffix)
  }

  def suffixContractInst(
      unicumOfDiscriminator: LfHash => Option[Unicum]
  )(contractInst: LfContractInst): Either[String, LfContractInst] = {
    contractInst.unversioned
      .suffixCid(suffixForDiscriminator(unicumOfDiscriminator))
      .map(unversionedContractInst => // traverse being added in daml-lf
        contractInst.map(_ => unversionedContractInst)
      )
  }

  def suffixNode(
      unicumOfDiscriminator: LfHash => Option[Unicum]
  )(node: LfActionNode): Either[String, LfActionNode] = {
    node.suffixCid(suffixForDiscriminator(unicumOfDiscriminator))
  }

  /** Monadic visit to all nodes of the transaction in execution order.
    * Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
    * Crashes on malformed transactions (see `com.daml.lf.transaction.GenTransaction.isWellFormed`)
    */
  @nowarn("msg=match may not be exhaustive")
  def foldExecutionOrderM[F[_], A](tx: LfTransaction, initial: A)(
      exerciseBegin: (LfNodeId, LfNodeExercises, A) => F[A]
  )(
      leaf: (LfNodeId, LfLeafOnlyActionNode, A) => F[A]
  )(exerciseEnd: (LfNodeId, LfNodeExercises, A) => F[A])(
      rollbackBegin: (LfNodeId, LfNodeRollback, A) => F[A]
  )(rollbackEnd: (LfNodeId, LfNodeRollback, A) => F[A])(implicit F: Monad[F]): F[A] = {

    F.tailRecM(FrontStack.from(tx.roots.map(_ -> false)) -> initial) {
      case (FrontStack(), x) => F.pure(Right(x))
      case (FrontStackCons((nodeId, upwards), toVisit), x) =>
        tx.nodes(nodeId) match {
          case ne: LfNodeExercises =>
            if (upwards) F.map(exerciseEnd(nodeId, ne, x))(y => Left(toVisit -> y))
            else
              F.map(exerciseBegin(nodeId, ne, x))(y =>
                Left((ne.children.map(_ -> false) ++: (nodeId -> true) +: toVisit) -> y)
              )
          case nl: LfLeafOnlyActionNode => F.map(leaf(nodeId, nl, x))(y => Left(toVisit -> y))
          case nr: LfNodeRollback =>
            if (upwards) F.map(rollbackEnd(nodeId, nr, x))(y => Left(toVisit -> y))
            else
              F.map(rollbackBegin(nodeId, nr, x))(y =>
                Left((nr.children.map(_ -> false) ++: (nodeId -> true) +: toVisit) -> y)
              )
        }
    }
  }

  /** @throws java.lang.IllegalArgumentException if transaction's keys contain contract Ids.
    */
  def duplicatedContractKeys(tx: LfVersionedTransaction): Set[LfGlobalKey] = {
    // The original non-rollback-aware function is from LfTransaction.duplicatedContractKeys
    // TODO(#6750): Define and/or fix up the semantics and also move to the daml-repo

    case class State(
        active: Set[(LfGlobalKey, RollbackScope)],
        duplicates: Set[LfGlobalKey],
        rbContext: RollbackContext,
    ) {
      def created(key: LfGlobalKey): State =
        if (
          active.exists { case (keyActive, rbScopeActive) =>
            keyActive == key && rbContext.embeddedInScope(rbScopeActive)
          }
        ) {
          copy(duplicates = duplicates + key, active = active + ((key, rbContext.rollbackScope)))
        } else copy(active = active + ((key, rbContext.rollbackScope)))
      def consumed(key: LfGlobalKey): State =
        // Subtract active key using exact match of the rollback scope. This works because:
        // 1. If the create node is in a sub-scope (in the sense of being strictly embedded) or an unrelated scope, then
        //    the exercise cannot see the contract. So if we assume that the given transaction is internally consistent
        //    (for contracts, not keys), then this case cannot happen.
        // 2. If the create node is in a super-scope (in the sense of the exercise being strictly embedded), then the
        //    key remains active.
        copy(active = active - ((key, rbContext.rollbackScope)))
      def referenced(key: LfGlobalKey): State =
        // TODO(#6754): Resolve whether a plain exercise should bring the contract key of the input contract into scope
        copy(active = active + ((key, rbContext.rollbackScope)))
      def pushRollbackNode: State =
        copy(rbContext = rbContext.enterRollback)
      def popRollbackNode: State =
        copy(rbContext = rbContext.exitRollback)
    }

    foldExecutionOrderM[cats.Id, State](
      tx.transaction,
      State(Set.empty, Set.empty, RollbackContext.empty),
    ) {
      case (
            _,
            LfNodeExercises(_, templateId, _, true, _, _, _, _, _, _, _, Some(key), _, _, _),
            state,
          ) =>
        state.consumed(LfGlobalKey.assertBuild(templateId, key.key))
      case (
            _,
            LfNodeExercises(_, templateId, _, false, _, _, _, _, _, _, _, Some(key), _, _, _),
            state,
          ) =>
        state.referenced(LfGlobalKey.assertBuild(templateId, key.key))
      case (_, _, state) => state // non-key exercise
    } {
      case (_, LfNodeCreate(_, templateId, _, _, _, _, Some(key), _, _), state) =>
        state.created(LfGlobalKey.assertBuild(templateId, key.key))
      case (_, LfNodeFetch(_, templateId, _, _, _, Some(key), _, _, _), state) =>
        state.referenced(LfGlobalKey.assertBuild(templateId, key.key))
      case (_, Node.NodeLookupByKey(templateId, key, Some(_), _), state) =>
        state.referenced(LfGlobalKey.assertBuild(templateId, key.key))
      case (_, _, state) => state // non key create/lookup/fetch
    } { case (_, _: LfNodeExercises, state) =>
      state
    } { case (_, _: LfNodeRollback, state) =>
      state.pushRollbackNode
    } { case (_, _: LfNodeRollback, state) =>
      state.popRollbackNode
    }.duplicates
  }

  def keyWithMaintainers(node: LfActionNode): Option[LfKeyWithMaintainers] = node match {
    case c: LfNodeCreate => c.key
    case f: LfNodeFetch => f.key
    case e: LfNodeExercises => e.key
    case l: LfNodeLookupByKey => Some(l.key)
  }

  /** Check that the `com.daml.lf.value.Value` key does not contain a contract ID.
    * If the key does contain a contract ID then it will be returned in a Left.
    * Valid contract keys cannot contain contract IDs.
    */
  def checkNoContractIdInKey(key: Value): Either[LfContractId, Value] =
    key.cids.headOption.toLeft(key)

  /** @throws java.lang.IllegalArgumentException
    *            if `key` contains a contact ID.
    */
  def assertNoContractIdInKey(key: Value): Value =
    checkNoContractIdInKey(key).valueOr(cid =>
      throw new IllegalArgumentException(s"Key contains contract Id $cid")
    )

  /** Given internally consistent transactions, compute their consumed contract ids. */
  def consumedContractIds(transactions: Iterable[LfVersionedTransaction]): Set[LfContractId] =
    transactions.foldLeft(Set.empty[LfContractId]) { case (consumed, tx) =>
      consumed | tx.consumedContracts
    }

  /** Yields the signatories of the node's contract, or key maintainers for nodes without signatories.
    */
  val signatoriesOrMaintainers: LfActionNode => Set[LfPartyId] = {
    case n: LfNodeCreate => n.signatories
    case n: LfNodeFetch => n.signatories
    case n: LfNodeExercises => n.signatories
    case n: LfNodeLookupByKey => n.keyMaintainers
  }

  def stateKnownTo(node: LfActionNode): Set[LfPartyId] = node match {
    case n: LfNodeCreate => n.key.fold(n.stakeholders)(_.maintainers)
    case n: LfNodeFetch => n.stakeholders
    case n: LfNodeExercises => n.stakeholders
    case n: LfNodeLookupByKey =>
      n.result match {
        case None => n.keyMaintainers
        // TODO(#3013) use signatories or stakeholders
        case Some(_) => n.keyMaintainers
      }
  }

  /** Yields the the acting parties of the node, if applicable
    *
    * @throws java.lang.IllegalArgumentException if a Fetch node does not contain the acting parties.
    */
  val actingParties: LfActionNode => Set[LfPartyId] = {
    case _: LfNodeCreate => Set.empty

    case node @ LfNodeFetch(_, _, noActors, _, _, _, _, _, _) if noActors.isEmpty =>
      throw new IllegalArgumentException(s"Fetch node $node without acting parties.")
    case LfNodeFetch(_, _, actors, _, _, _, _, _, _) => actors

    case n: LfNodeExercises => n.actingParties

    case nl: LfNodeLookupByKey => nl.keyMaintainers
  }

  /** Compute the informees of a node based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    *
    * @throws java.lang.IllegalStateException if a Fetch node does not contain the acting parties.
    */
  def informees(node: LfActionNode): Set[LfPartyId] = node.informeesOfNode

  /** Compute the informees of a transaction based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    */
  def informees(transaction: LfVersionedTransaction): Set[LfPartyId] = {
    val nodes: Set[LfActionNode] = transaction.nodes.values.collect { case an: LfActionNode =>
      an
    }.toSet
    nodes.flatMap(informees(_))
  }

  val children: LfNode => Seq[LfNodeId] = {
    case ex: LfNodeExercises => ex.children.toSeq
    case _ => Seq.empty
  }

  /** Yields the light-weight version (i.e. without exercise children and result) of this node.
    *
    * @throws java.lang.UnsupportedOperationException if `node` is a rollback.
    */
  def lightWeight(node: LfActionNode): LfActionNode = {
    node match {
      case n: LfNodeCreate => n
      case n: LfNodeFetch => n
      case n: LfNodeExercises => n.copy(children = ImmArray.empty)
      case n: LfNodeLookupByKey => n
    }
  }
}
