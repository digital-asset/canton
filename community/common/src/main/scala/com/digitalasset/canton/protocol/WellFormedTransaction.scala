// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.data.NonEmptyChainImpl.*
import cats.data.{NonEmptyChain, Validated}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.RollbackContextFactory
import com.digitalasset.canton.protocol.WellFormedTransaction.Stage
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, LfTransactionUtil}
import com.digitalasset.daml.lf.transaction.SerializationVersion
import com.digitalasset.daml.lf.value.{Value, ValueCoder}
import com.google.common.annotations.VisibleForTesting
import monocle.macros.syntax.lens.*

import scala.collection.mutable

/** Used to mark a transaction to be well-formed. That means:
  *   - `tx` is well-formed according to the Daml-LF definition, i.e., a root node is not child of
  *     another node and every non-root node has exactly one parent and is reachable from a root
  *     node. (No cycles, no sharing, no orphaned node).
  *   - All node Ids are non-negative.
  *   - The type parameter `S` determines whether all create nodes have suffixed IDs or none.
  *   - Create nodes have unique contract ids of shape
  *     `com.digitalasset.daml.lf.value.Value.ContractId`.
  *   - The contract id of a create node is not referenced before the node.
  *   - The contract id of a rolled back create node is not referenced outside its rollback scope in
  *     an active-contracts position.
  *   - Every action node has at least one signatory.
  *   - Every signatory is also a stakeholder.
  *   - Fetch actors are defined.
  *   - All created contract instances and choice arguments in the transaction can be serialized.
  *   - [[metadata]] contains seeds exactly for those node IDs from `tx` that should have a seed
  *     (creates and exercises).
  *   - Keys of transaction nodes don't contain contract IDs.
  *   - The maintainers of keys are non-empty.
  *   - ByKey nodes have a key.
  *   - All parties referenced by the transaction can be converted to
  *     [[com.digitalasset.canton.topology.PartyId]]
  *   - Every rollback node has at least one child and no rollback node appears at the root unless
  *     the transaction has been merged by Canton.
  */
final case class WellFormedTransaction[+S <: Stage] private[protocol] (
    private[protocol] val tx: LfVersionedTransaction,
    metadata: TransactionMetadata,
) {
  def unwrap: LfVersionedTransaction = tx

  def withoutVersion: LfTransaction = tx.transaction

  def seedFor(nodeId: LfNodeId): Option[LfHash] = metadata.seeds.get(nodeId)

  /** Adjust the node IDs in an LF transaction by the given (positive or negative) offset
    *
    * For example, an offset of 1 increases the NodeId of every node by 1. Ensures that the
    * transaction stays wellformed.
    *
    * @throws java.lang.ArithmeticException
    *   if `offset` is negative or a node ID overflow would occur
    */
  def tryAdjustNodeIds(offset: Int): (LfVersionedTransaction, TransactionMetadata) = {
    require(offset >= 0, s"Offset must be non-negative: $offset")

    def adjustNodeId(nid: LfNodeId): LfNodeId = LfNodeId(Math.addExact(nid.index, offset))

    if (offset == 0) (tx, metadata)
    else {
      val adjustedTx = tx.mapNodeId(adjustNodeId)
      val adjustedMetadata = metadata.copy(
        seeds = metadata.seeds.map { case (nodeId, seed) => adjustNodeId(nodeId) -> seed }
      )
      (adjustedTx, adjustedMetadata)
    }
  }
}

object WellFormedTransaction {

  /** Determines whether the IDs of created contracts in a transaction are suffixed and whether the
    * suffix must be absolute, and whether the transactions have been merged.
    */
  sealed trait Stage extends Product with Serializable {
    def withSuffixes: Boolean
    def onlyAbsoluteSuffixes: Boolean
    def merged: Boolean
  }

  /** All IDs of created contracts must have empty suffixes. */
  case object WithoutSuffixes extends Stage {
    override def withSuffixes: Boolean = false
    override def onlyAbsoluteSuffixes: Boolean = false
    override def merged: Boolean = false
  }
  type WithoutSuffixes = WithoutSuffixes.type

  /** All IDs of created contracts must have non-empty suffixes, possibly relative. The transaction
    * not yet merged.
    */
  case object WithSuffixes extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = false
    override def merged: Boolean = false
  }
  type WithSuffixes = WithSuffixes.type

  /** All IDs of created contracts must have non-empty absolute suffixes. The transaction not yet
    * merged.
    */
  case object WithAbsoluteSuffixes extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = true
    override def merged: Boolean = false
  }
  type WithAbsoluteSuffixes = WithAbsoluteSuffixes.type

  /** All IDs of created contracts must have non-empty absolute suffixes and transaction has been
    * "merged".
    */
  case object WithSuffixesAndMerged extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = true
    override def merged: Boolean = true
  }
  type WithSuffixesAndMerged = WithSuffixesAndMerged.type

  /** Creates a [[WellFormedTransaction]] if possible or an error message otherwise.
    */
  def check[S <: Stage](
      tx: LfVersionedTransaction,
      metadata: TransactionMetadata,
      stage: S,
      contextFactory: RollbackContextFactory,
  ): Either[String, WellFormedTransaction[S]] = {

    val result = for {
      _ <- checkForest(tx)
      (normalizedTx, normalizedMetadata) = normalizeNodeIds(tx, metadata)
      _ <- checkNonNegativeNodeIds(normalizedTx)
      _ <- checkSeeds(normalizedTx, normalizedMetadata.seeds)
      _ <- checkByKeyNodes(normalizedTx)
      _ <- contextFactory.checkCreatedContracts(normalizedTx)
      _ <- checkSuffixes(normalizedTx, stage)
      _ <- checkFetchActors(normalizedTx)
      _ <- checkSignatoriesAndStakeholders(normalizedTx)
      _ <- checkNonemptyMaintainers(normalizedTx)
      _ <- checkPartyNames(normalizedTx)
      _ <- checkSerialization(normalizedTx)
      _ <- checkRollbackNodes(normalizedTx, stage)
    } yield (normalizedTx, normalizedMetadata)

    result.toEitherMergeNonaborts.bimap(
      _.toList.sorted.mkString(", "),
      (WellFormedTransaction.apply: (
          LfVersionedTransaction,
          TransactionMetadata,
      ) => WellFormedTransaction[S]).tupled,
    )
  }

  private[protocol] def checkForest(
      tx: LfVersionedTransaction
  ): Checked[NonEmptyChain[String], String, Unit] = {
    val noForest = tx.transaction.isWellFormed
    val errors = noForest.toList.map(err => s"${err.reason}: ${err.nid.index}")
    Checked.fromEither(NonEmptyChain.fromSeq(errors).toLeft(()))
  }

  private def checkNonNegativeNodeIds(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] = {
    val negativeNodeIds = tx.nodes.keys.filter(_.index < 0)
    Checked.fromEitherNonabort(())(
      Either.cond(
        negativeNodeIds.isEmpty,
        (),
        s"Negative node IDs: ${negativeNodeIds.map(_.index).mkString(", ")}",
      )
    )
  }

  private[protocol] def checkSeeds(
      tx: LfVersionedTransaction,
      seeds: Map[LfNodeId, LfHash],
  ): Checked[Nothing, String, Unit] = {
    val missingSeedsB = mutable.ListBuffer.newBuilder[LfNodeId]
    val superfluousSeedsB = mutable.ListBuffer.newBuilder[LfNodeId]

    tx.foreach { (nodeId, node) =>
      if (LfTransactionUtil.nodeHasSeed(node)) {
        if (!seeds.contains(nodeId)) missingSeedsB += nodeId
      } else if (seeds.contains(nodeId)) superfluousSeedsB += nodeId
    }

    val missingSeeds = missingSeedsB.result()
    val superfluousSeeds = superfluousSeedsB.result()

    val missing =
      Validated.condNec(
        missingSeeds.isEmpty,
        (),
        s"Nodes without seeds: ${missingSeeds.map(_.index).mkString(", ")}",
      )
    val superfluous = Validated.condNec(
      superfluousSeeds.isEmpty,
      (),
      s"Nodes with superfluous seeds: ${superfluousSeeds.map(_.index).mkString(", ")}",
    )

    Checked.fromEitherNonaborts(())(missing.product(superfluous).void.toEither)
  }

  private def checkByKeyNodes(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    val byKeyNodesWithoutKey =
      tx.nodes.collect {
        case (nodeId, node: LfActionNode) if node.byKey && node.keyOpt.isEmpty => nodeId
      }.toList
    Checked.fromEitherNonabort(())(
      Either.cond(
        byKeyNodesWithoutKey.isEmpty,
        (),
        show"byKey nodes without a key: $byKeyNodesWithoutKey",
      )
    )
  }

  private def checkSuffixes(
      tx: LfVersionedTransaction,
      stage: Stage,
  ): Checked[Nothing, String, Unit] = {

    val absoluteSuffixChecked = if (stage.onlyAbsoluteSuffixes) {
      val absoluteSuffixProblems = tx.nodes.values.flatMap(_.cids.filter(!_.isAbsolute))
      if (absoluteSuffixProblems.isEmpty) Checked.unit
      else
        Checked.continue(
          s"Contract IDs without absolute suffixes: ${absoluteSuffixProblems.mkString(", ")}"
        )
    } else Checked.unit

    val suffixProblems = tx.nodes.collect {
      case (nid, nc: LfNodeCreate) if nc.coid.isLocal == stage.withSuffixes => nid
    }

    val suffixChecked =
      if (suffixProblems.isEmpty) Checked.unit
      else
        Checked.continue(
          if (stage.withSuffixes)
            s"Created contracts of nodes lack suffixes: ${suffixProblems.map(_.index).mkString(", ")}"
          else
            s"Created contracts have suffixes in nodes ${suffixProblems.map(_.index).mkString(", ")}"
        )

    absoluteSuffixChecked.product(suffixChecked).void
  }

  private def checkNonemptyMaintainers(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] =
    Checked.fromEitherNonaborts(())(
      tx.nodes
        .to(LazyList)
        .traverse_ {
          case (nodeId, node: LfActionNode) =>
            node.keyOpt match {
              case Some(k) =>
                Validated.condNec(
                  k.maintainers.nonEmpty,
                  (),
                  s"Key of node ${nodeId.index} has no maintainer",
                )
              case None => Validated.Valid(())
            }
          case (_nodeId, _rn: LfNodeRollback) => Validated.Valid(())
        }
        .toEither
    )

  private def checkFetchActors(
      tx: LfVersionedTransaction
  ): Checked[NonEmptyChain[String], String, Unit] = {
    val missingFetchActors = tx.nodes.collect {
      case (nodeId, node: LfNodeFetch) if node.actingParties.isEmpty => nodeId.index
    }
    Checked.cond(
      missingFetchActors.isEmpty,
      (),
      NonEmptyChain.one(
        s"fetch nodes with unspecified acting parties at nodes ${missingFetchActors.mkString(", ")}"
      ),
    )
  }

  private def checkSignatoriesAndStakeholders(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] = {
    val noSignatoriesOrMaintainers = tx.nodes.collect {
      case (nodeId, node: LfActionNode)
          if LfTransactionUtil.signatoriesOrMaintainers(node).isEmpty =>
        nodeId.index
    }

    for {
      _ <-
        if (noSignatoriesOrMaintainers.isEmpty) Checked.unit
        else
          Checked.continue(
            s"neither signatories nor maintainers present at nodes ${noSignatoriesOrMaintainers.mkString(", ")}"
          )
      _ <- tx.nodes.to(LazyList).traverse_ {
        case (nodeId, an: LfActionNode) =>
          // Since we check for the fetch actors before, informees does not throw.
          val missingInformees =
            LfTransactionUtil.signatoriesOrMaintainers(an) -- an.informeesOfNode
          if (missingInformees.isEmpty) Checked.unit
          else
            Checked.continue(s"signatory or maintainer not declared as informee: ${missingInformees
                .mkString(", ")} at node ${nodeId.index}")
        case (_nodeId, _rn: LfNodeRollback) => Checked.unit
      }
    } yield ()
  }

  private def checkKeyEncoding(
      nodeId: LfNodeId,
      version: SerializationVersion,
      key: Option[LfGlobalKeyWithMaintainers],
  ): Either[String, Unit] =
    key match {
      case None => Either.unit
      case Some(LfGlobalKeyWithMaintainers(gk, _)) =>
        checkValueEncoding(nodeId, version, gk.key, "key")
    }

  private def checkValueEncoding(
      nodeId: LfNodeId,
      version: SerializationVersion,
      value: Value,
      valueType: String = "value",
  ): Either[String, Unit] =
    ValueCoder
      .encodeValue(version, value)
      .void
      .leftMap(err => s"unable to encode $valueType for $nodeId: ${err.errorMessage}")

  private def checkSerialization(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] =
    Checked.fromEitherNonabort(())(tx.nodes.to(LazyList).traverse_ {
      case (nodeId, create: LfNodeCreate) =>
        for {
          _ <- checkValueEncoding(nodeId, create.version, create.arg)
          _ <- checkKeyEncoding(nodeId, create.version, create.keyOpt)
        } yield ()

      case (nodeId, exercise: LfNodeExercises) =>
        for {
          _ <- checkValueEncoding(nodeId, exercise.version, exercise.chosenValue)
          _ <- checkKeyEncoding(nodeId, exercise.version, exercise.keyOpt)
        } yield ()

      case (nodeId, fetch: LfNodeFetch) =>
        checkKeyEncoding(nodeId, fetch.version, fetch.keyOpt)

      case (nodeId, query: LfNodeQueryByKey) =>
        checkKeyEncoding(nodeId, query.version, query.keyOpt)

      case (_, _: LfNodeRollback) =>
        Either.unit
    })

  private def checkPartyNames(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    val lfPartyIds = mutable.HashSet.empty[LfPartyId]
    tx.nodes.values
      .foreach {
        case node: LfActionNode => lfPartyIds.addAll(node.informeesOfNode)
        case _ =>
      }
    lfPartyIds.to(LazyList).traverse_ { lfPartyId =>
      Checked.fromEitherNonabort(())(
        PartyId
          .fromLfParty(lfPartyId)
          .bimap(err => s"Unable to parse party: $err", _ => ())
      )
    }
  }

  private[protocol] def checkRollbackNodes(
      tx: LfVersionedTransaction,
      stage: Stage,
  ): Checked[Nothing, String, Unit] =
    for {
      // check that root nodes of "unmerged transactions" never include rollback node (should have been peeled off by DAMLe.reinterpret)
      // Ensuring that no rollback nodes appear at the top in pre-merged transactions avoids the need to reconcile
      // rollback nodes described by the ViewParticipantData.rollbackContext and "duplicate" rollback nodes reintroduced
      // by DAMLe.reinterpret.
      _ <-
        if (stage.merged) Checked.unit
        else
          Checked.fromEitherNonabort(())(
            Either.cond(
              tx.roots.map(tx.nodes).toSeq.collectFirst { case nr: LfNodeRollback => nr }.isEmpty,
              (),
              "rollback node(s) not expected at the root of unmerged transaction",
            )
          )
      // ensure all rollback nodes always have at least one child
      _ <- tx.nodes
        .collect { case n @ (_, LfNodeRollback(children)) if children.length == 0 => n }
        .to(LazyList)
        .traverse_ { case (nodeId, _) =>
          Checked.continue(s"Rollback node $nodeId does not have children")
        }
    } yield ()

  private[protocol] def normalizeNodeIds(
      tx: LfVersionedTransaction,
      metadata: TransactionMetadata,
  ): (LfVersionedTransaction, TransactionMetadata) = {
    val normalization = tx.nodeIdNormalization
    val normalizedTransaction = tx.mapNodeId(normalization)
    val normalizedMetadata =
      metadata.focus(_.seeds).modify(_.map { case (nid, v) => normalization(nid) -> v })

    (normalizedTransaction, normalizedMetadata)
  }

  sealed trait InvalidInput
  object InvalidInput extends {
    final case class InvalidParty(cause: String) extends InvalidInput
  }

  /** Sanity check the transaction before submission for any invalid input values
    *
    * Generally, the well-formedness check is used to detect faulty or malicious behaviour. This
    * method here can be used as a pre-check during submission to filter out any user input errors.
    */
  def sanityCheckInputs(
      tx: LfVersionedTransaction
  ): Either[InvalidInput, Unit] =
    for {
      _ <- checkPartyNames(tx).toEitherMergeNonaborts.leftMap(err =>
        InvalidInput.InvalidParty(err.head)
      )
    } yield ()

  /** Creates a [[WellFormedTransaction]]
    *
    * @throws java.lang.IllegalArgumentException
    *   if the given transaction is malformed
    */
  def checkOrThrow[S <: Stage](
      lfTransaction: LfVersionedTransaction,
      metadata: TransactionMetadata,
      state: S,
      rollbackContextFactory: RollbackContextFactory,
  ): WellFormedTransaction[S] =
    check(lfTransaction, metadata, state, rollbackContextFactory)
      .valueOr(err => throw new IllegalArgumentException(err))

  /** For security testing only. DO NOT USE IN PRODUCTION!
    */
  @VisibleForTesting
  def createUnsafe[S <: Stage](
      tx: LfVersionedTransaction,
      metadata: TransactionMetadata,
  ): WellFormedTransaction[S] = WellFormedTransaction(tx, metadata)

}
