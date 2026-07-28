// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.common.annotations.VisibleForTesting

import scala.collection.{View, immutable}

case class NeedContract[+X](resume: Option[GlobalKey] => ErrOr[X])

sealed abstract class NeedKeyProgression

object NeedKeyProgression {
  trait Token

  // States: Unstarted -> InProgress -> Finished
  //
  // CanContinue = { Unstarted, InProgress }  — progression not yet exhausted
  // HasStarted  = { InProgress, Finished }   — valid resume argument

  /** The progression can yield more results. */
  sealed trait CanContinue extends NeedKeyProgression

  /** The progression has been started; valid as a resume argument. */
  sealed trait HasStarted extends NeedKeyProgression

  case object Unstarted extends CanContinue
  final case class InProgress(token: Token) extends CanContinue with HasStarted
  case object Finished extends HasStarted

  private final case class VectorToken[T](values: Vector[T]) extends Token

  /** Convenience for consumers that have all results available upfront. */
  private[lf] def takeN[T](
      canContinue: CanContinue,
      n: Int,
      initial: => Vector[T],
  ): (Vector[T], HasStarted) = {
    val all = canContinue match {
      case Unstarted => initial
      case InProgress(VectorToken(xs)) => xs.asInstanceOf[Vector[T]]
      case InProgress(other) =>
        throw new IllegalStateException(
          s"unexpected continuation token of type ${other.getClass}"
        )
    }
    val (result, rest) = all.splitAt(n)
    val next = if (result.length == n) InProgress(VectorToken(rest)) else Finished
    (result, next)
  }
}

/*
  NeedKey asks the caller for more contracts that match a key.
  The caller answers with candidates. It resumes the query until it has seen every matching contract.
  When the caller stops, the state machine checks the candidates against the earlier query results.
  The caller must respect two invariants:
  1. it returns at most `n` contracts;
  2. it may set the progression to `Finished` only when it returned fewer than `n`.
     A full batch always means there may be more.
 */
case class NeedKey[+X](
    n: Int,
    progression: NeedKeyProgression.CanContinue,
    resume: (
        View[ContractId],
        NeedKeyProgression.HasStarted,
    ) => Either[NeedKey[X], ErrOr[(KeyMapping, X)]],
)
final case class KeyMapping(queue: Vector[ContractId], exhaustive: Boolean)

object KeyMapping {
  val Unknown: KeyMapping = KeyMapping(Vector.empty, exhaustive = false)
  val Empty: KeyMapping = KeyMapping(Vector.empty, exhaustive = true)
}

object NextGenContractStateMachine {

  type KeyResolver = Map[GlobalKey, Vector[ContractId]]

  private[transaction] final case class KeyInput(
      queriedByKey: Vector[ContractId],
      overflowPreviousQuery: List[ContractId],
      onlyQueriedById: Set[ContractId],
      progression: NeedKeyProgression,
  ) {
    def exhaustive: Boolean = progression match {
      case NeedKeyProgression.Finished => true
      case _ => false
    }
    def toKeyMapping: KeyMapping = KeyMapping(queriedByKey.to(Vector), exhaustive)
  }

  private[transaction] object KeyInput {
    val Unknown: KeyInput = KeyInput(
      queriedByKey = Vector.empty,
      overflowPreviousQuery = List.empty,
      onlyQueriedById = Set.empty,
      progression = NeedKeyProgression.Unstarted,
    )
  }

  private final case class KeyMappingBuilder(
      queue: Vector[ContractId],
      missing: Int,
  ) {
    def :+(contractId: ContractId): KeyMappingBuilder =
      copy(queue = queue :+ contractId, missing = missing - 1)

    def addWhileHasMissing(more: View[ContractId]): KeyMappingBuilder =
      more.take(missing).foldLeft(this)(_ :+ _)

    def result: KeyMapping = KeyMapping(queue, missing > 0)
  }

  private object KeyMappingBuilder {
    def empty(n: Int): KeyMappingBuilder = KeyMappingBuilder(Vector.empty, n)
  }

  final case class StateMachineResult(
      inputContractIds: Set[ContractId],
      globalKeyInputs: Map[GlobalKey, KeyMapping],
      localKeys: Map[GlobalKey, Vector[ContractId]],
      consumed: Set[ContractId],
  )

  object StateMachineResult {
    def empty = StateMachineResult(
      inputContractIds = Set.empty[ContractId],
      globalKeyInputs = Map.empty[GlobalKey, KeyMapping],
      localKeys = Map.empty[GlobalKey, Vector[ContractId]],
      consumed = Set.empty[ContractId],
    )

    def emptyWith(
        inputContractIds: Set[ContractId] = Set.empty[ContractId],
        globalKeyInputs: Map[GlobalKey, KeyMapping] = Map.empty[GlobalKey, KeyMapping],
        localKeys: Map[GlobalKey, Vector[ContractId]] = Map.empty[GlobalKey, Vector[ContractId]],
        consumed: Set[ContractId] = Set.empty[ContractId],
    ): StateMachineResult = StateMachineResult(
      inputContractIds = inputContractIds,
      globalKeyInputs = globalKeyInputs,
      localKeys = localKeys,
      consumed = consumed,
    )
  }

  /** The contract journal. Stateful and transaction-agnostic.
    *
    * It records the contract IDs and keys a transaction touches, creates, and spends, and keeps
    * that record coherent. It answers the interpreter's contract and key queries and returns a new
    * journal on every mutation. It never reaches into the ledger: it asks the caller for contracts,
    * and the caller answers.
    */
  sealed abstract class Journal {
    private[lf] def create(
        nid: NodeId,
        cid: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[Journal]

    private[lf] def queryById(
        tmplId: Ref.TypeConId,
        cid: ContractId,
    ): Either[NeedContract[Journal], ErrOr[Journal]]

    private[lf] def queryNByKey(
        key: GlobalKey,
        n: Int,
    ): Either[NeedKey[Journal], ErrOr[(KeyMapping, Journal)]]

    // return None if cid is unknown
    private[lf] def archive(
        tmplId: Ref.TypeConId,
        cid: ContractId,
        nid: NodeId,
    ): Option[ErrOr[Journal]]

    private[lf] def beginTry: Journal

    private[lf] def abortTry: Either[Set[NodeId], Journal]

    private[lf] def endTry: Journal

    private[lf] def onlyQueriedById(gkey: GlobalKey): Set[ContractId]

    private[lf] def knownContract(cid: ContractId): Boolean

    // TODO(#31848) review the interface of Journal
    //  - check if we really need those methods
    //  - check if it will be better to have lazy val for the Sets/Maps
    def knownContracts: Set[ContractId]

    def knownKeys: Set[GlobalKey]

    def inputContracts: Set[ContractId]

    def keyInputs: Map[GlobalKey, KeyMapping]

    def activeContract(cid: ContractId): Boolean

    def activeKeyMapping(key: GlobalKey): KeyMapping

    @VisibleForTesting
    private[transaction] def toStateMachineResult: StateMachineResult

    //  TODO(#31848): remove this method
    def consumedByOrInactive(cid: ContractId): Option[Either[NodeId, Unit]]

    //  TODO(#31848): remove this method
    def localContracts: immutable.VectorMap[ContractId, ?]

    /** A deterministic total order over all contracts known to the state machine. It gives each key
      * a stable precedence. The only guarantee: if two contracts appear in the same `queryByKey`
      * result, their relative order in `contractOrder` matches their order in that result. So every
      * `queryByKey` result is a subsequence of `contractOrder`.
      */
    def contractOrder: List[ContractId]
  }

  object Journal {

    private[transaction] case class Impl(
        authorizeRollback: Boolean,
        localContracts: immutable.VectorMap[ContractId, Option[GlobalKey]],
        override val inputContracts: Set[ContractId],
        internalKeyInputs: Map[GlobalKey, KeyInput],
        activeLedgerState: ActiveLedgerState,
        rollbackStack: List[ActiveLedgerState],
    ) extends Journal {

      private def createdInThisTimeline: Map[ContractId, NodeId] =
        activeLedgerState.createdInThisTimeline

      def localKeys: Map[GlobalKey, Vector[ContractId]] =
        activeLedgerState.localKeys

      override def keyInputs: Map[GlobalKey, KeyMapping] =
        internalKeyInputs.flatMap { case (key, keyInput) =>
          keyInput.toKeyMapping match {
            case KeyMapping.Unknown => List.empty
            case mapping => List(key -> mapping)
          }
        }

      def consumedBy: Map[ContractId, NodeId] =
        activeLedgerState.consumedBy

      def onlyQueriedById(gkey: GlobalKey): Set[ContractId] =
        internalKeyInputs.getOrElse(gkey, KeyInput.Unknown).onlyQueriedById

      def knownContract(cid: ContractId): Boolean =
        localContracts.contains(cid) || inputContracts.contains(cid)

      def knownContracts: Set[ContractId] =
        createdInThisTimeline.keySet ++ inputContracts

      def knownKeys: Set[GlobalKey] = localKeys.keySet & internalKeyInputs.keySet

      def toStateMachineResult: StateMachineResult = StateMachineResult(
        inputContractIds = inputContracts,
        globalKeyInputs = keyInputs,
        localKeys = localKeys,
        consumed = consumedBy.keySet,
      )

      def create(nid: NodeId, cid: ContractId, mbKey: Option[GlobalKey]): ErrOr[Impl] =
        Either.cond(
          !knownContract(cid),
          this.copy(
            localContracts = this.localContracts.updated(cid, mbKey),
            activeLedgerState = activeLedgerState.copy(
              createdInThisTimeline = this.createdInThisTimeline + (cid -> nid),
              localKeys = mbKey match {
                case None => this.localKeys
                case Some(key) =>
                  this.localKeys.updated(key, cid +: this.localKeys.getOrElse(key, Vector.empty))
              },
            ),
          ),
          TransactionError.DuplicateContractId(cid),
        )

      private def continueQueryById(
          cid: Value.ContractId
      ): Option[GlobalKey] => ErrOr[Impl] = {
        case None =>
          Right(copy(inputContracts = inputContracts + cid))
        case Some(key) =>
          val keyInput = internalKeyInputs.getOrElse(key, KeyInput.Unknown)
          keyInput.progression match {
            case _: NeedKeyProgression.CanContinue =>
              val updatedKeyInput = keyInput.copy(onlyQueriedById = keyInput.onlyQueriedById + cid)
              Right(
                copy(
                  inputContracts = inputContracts + cid,
                  internalKeyInputs = internalKeyInputs.updated(
                    key,
                    updatedKeyInput,
                  ),
                )
              )
            case NeedKeyProgression.Finished =>
              Left(TransactionError.InconsistentContractKey(key))
          }
      }

      def queryById(
          tmplId: Ref.TypeConId,
          cid: ContractId,
      ): Either[NeedContract[Impl], ErrOr[Impl]] =
        Either.cond(
          knownContract(cid),
          consumedBy.get(cid).map(TransactionError.AlreadyConsumed(cid, tmplId, _)).toLeft(this),
          NeedContract(continueQueryById(cid)),
        )

      @scala.annotation.tailrec
      private def continueQueryNByKeyWithCandidates(
          key: GlobalKey,
          candidates: List[ContractId],
          inputContract: Set[ContractId],
          queryByKey: Vector[ContractId],
          onlyQueryById: Set[ContractId],
          progression: NeedKeyProgression,
          acc: KeyMappingBuilder,
      ): Either[NeedKey[Impl], ErrOr[(KeyMapping, Impl)]] =
        if (acc.missing == 0) {
          Right(
            Right(
              acc.result -> copy(
                inputContracts = inputContract,
                internalKeyInputs = internalKeyInputs.updated(
                  key,
                  KeyInput(queryByKey, candidates, onlyQueryById, progression),
                ),
              )
            )
          )
        } else {
          candidates match {
            case head :: tail =>
              if (localContracts.contains(head))
                // Defensive: because local contract IDs are structurally distinct from input contract IDs,
                // this branch should be unreachable.
                Right(Left(TransactionError.DuplicateContractId(head)))
              else if (inputContract.contains(head) && !onlyQueryById.contains(head))
                continueQueryNByKeyWithCandidates(
                  key,
                  tail,
                  inputContract,
                  queryByKey,
                  onlyQueryById,
                  progression,
                  acc,
                )
              else
                continueQueryNByKeyWithCandidates(
                  key,
                  tail,
                  inputContract + head,
                  queryByKey :+ head,
                  onlyQueryById - head,
                  progression,
                  if (consumedBy.contains(head)) acc else acc :+ head,
                )
            case Nil =>
              progression match {
                case progression: NeedKeyProgression.CanContinue =>
                  Left(
                    NeedKey(
                      acc.missing,
                      progression,
                      continueQueryNByKey(key, inputContract, queryByKey, onlyQueryById, acc),
                    )
                  )
                case NeedKeyProgression.Finished =>
                  Right(
                    Either.cond(
                      onlyQueryById.isEmpty,
                      acc.result -> copy(
                        inputContracts = inputContract,
                        internalKeyInputs = internalKeyInputs.updated(
                          key,
                          KeyInput(queryByKey, candidates, onlyQueryById, progression),
                        ),
                      ),
                      TransactionError.InconsistentContractKey(key),
                    )
                  )
              }
          }
        }

      private def continueQueryNByKey(
          key: GlobalKey,
          inputContracts: Set[ContractId],
          queryByKey: Vector[ContractId],
          onlyQueryById: Set[ContractId],
          acc: KeyMappingBuilder,
      ): (
          View[ContractId],
          NeedKeyProgression.HasStarted,
      ) => Either[NeedKey[Impl], ErrOr[(KeyMapping, Impl)]] =
        (contracts: View[ContractId], progression: NeedKeyProgression.HasStarted) =>
          continueQueryNByKeyWithCandidates(
            key,
            contracts.toList,
            inputContracts,
            queryByKey,
            onlyQueryById,
            progression,
            acc,
          )

      def activeContract(cid: ContractId): Boolean =
        knownContract(cid) && !consumedBy.contains(cid)

      private def activeKeyMapping(key: GlobalKey, n: Int): KeyMappingBuilder = {
        val localKey = localKeys.getOrElse(key, Nil).view
        val keyInput = internalKeyInputs.getOrElse(key, KeyInput.Unknown)
        val cids = (localKey ++ keyInput.queriedByKey).filterNot(consumedBy.contains)
        KeyMappingBuilder.empty(n).addWhileHasMissing(cids)
      }

      def activeKeyMapping(key: GlobalKey): KeyMapping = activeKeyMapping(key, Int.MaxValue).result

      def queryNByKey(
          key: GlobalKey,
          n: Int,
      ): Either[NeedKey[Impl], ErrOr[(KeyMapping, Journal)]] = {
        if (n <= 0)
          throw new IllegalArgumentException(s"queryNByKey: n must be strictly positive, got $n")
        val mapping = activeKeyMapping(key, n)
        val keyInput = internalKeyInputs.getOrElse(key, KeyInput.Unknown)
        continueQueryNByKeyWithCandidates(
          key,
          keyInput.overflowPreviousQuery,
          inputContracts,
          keyInput.queriedByKey,
          keyInput.onlyQueriedById,
          keyInput.progression,
          mapping,
        )
      }

      def archive(tmplId: Ref.TypeConId, cid: ContractId, nid: NodeId): Option[ErrOr[Impl]] =
        queryById(tmplId, cid).toOption.map(
          _.map(state =>
            state.copy(
              activeLedgerState = state.activeLedgerState.copy(
                consumedBy = state.activeLedgerState.consumedBy.updated(cid, nid)
              )
            )
          )
        )

      def beginTry: Impl =
        this.copy(rollbackStack = activeLedgerState :: rollbackStack)

      def abortTry: Either[Set[NodeId], Impl] =
        rollbackStack match {
          case Nil =>
            throw new IllegalStateException("Not inside a rollback scope")
          case headState :: tailStack if authorizeRollback =>
            Right(
              this.copy(
                activeLedgerState = headState,
                rollbackStack = tailStack,
              )
            )
          case headState :: tailStack =>
            // locallyCreated and consumedBy increase monotonically, so can quickly check their size did not change since the last try block.
            if (
              activeLedgerState.createdInThisTimeline.size != headState.createdInThisTimeline.size || activeLedgerState.consumedBy.size != headState.consumedBy.size
            ) {
              val consumedByChanges =
                activeLedgerState.consumedBy.view
                  .filterKeys(consumedBy.keySet -- headState.consumedBy.keySet)
                  .values
                  .toSet

              val locallyCreatedChanges =
                activeLedgerState.createdInThisTimeline.view
                  .filterKeys(
                    activeLedgerState.createdInThisTimeline.keySet -- headState.createdInThisTimeline.keySet
                  )
                  .values
                  .toSet

              Left(consumedByChanges ++ locallyCreatedChanges)
            } else {
              Right(this.copy(rollbackStack = tailStack))
            }
        }

      private[lf] def endTry: Impl = rollbackStack match {
        case Nil => throw new IllegalStateException("Not inside a rollback scope")
        case _ :: tailStack => this.copy(rollbackStack = tailStack)
      }

      override def consumedByOrInactive(cid: Value.ContractId): Option[Either[NodeId, Unit]] =
        activeLedgerState.consumedBy.get(cid) match {
          case Some(nid) => Some(Left(nid)) // consumed
          case None =>
            if (
              localContracts.contains(cid) && !activeLedgerState.createdInThisTimeline.contains(cid)
            ) {
              Some(Right(())) // inactive
            } else {
              None // neither
            }
        }

      // Implementation detail (not guaranteed to be stable across versions, documented
      // for maintenance purposes only): the current ordering is
      //  1. Local contracts, ordered by recency (most recent first).
      //  2. Input contracts without key, ordered by ID.
      //  3. Input contracts with key, grouped by key (keys ordered by hash); within
      //     each key group, contracts queried by key come first (in query order),
      //     followed by contracts only queried by ID (ordered by ID).
      override def contractOrder: List[ContractId] = {

        // 1. Local contracts, most recent first
        val locals = localContracts.keys.reverseIterator

        // 3. Input contracts with key, grouped by key in sorted order
        //    Computed before step 2 so we can derive the "without key" set.
        val inputsWithKey =
          internalKeyInputs.toArray.sortBy(_._1).toList.flatMap { case (_, keyInput) =>
            keyInput.queriedByKey ++ keyInput.onlyQueriedById.toArray.sorted
          }

        // 2. Input contracts without key, ordered by ID
        val inputsWithoutKey = (inputContracts -- inputsWithKey).toArray.sorted

        // 3. Append input contracts with key
        (locals ++ inputsWithoutKey ++ inputsWithKey).toList
      }
    }

    private[transaction] case class ActiveLedgerState(
        createdInThisTimeline: Map[ContractId, NodeId],
        consumedBy: Map[ContractId, NodeId],
        localKeys: Map[GlobalKey, Vector[ContractId]],
    )

  }

  object Visitor {
    private case object ContinuationToken extends NeedKeyProgression.Token
    private val InProgress = NeedKeyProgression.InProgress(ContinuationToken)
  }

  /** The node visitor. Transaction-aware and stateless.
    *
    * It wraps a journal and replays one transaction node at a time. For each node it recomputes the
    * journal's answer and demands an exact match, so submission and validation cannot disagree. It
    * needs only contract IDs and keys, so it also runs on projected views.
    */
  implicit class Visitor(val journal: Journal) extends AnyVal {

    import Visitor.*

    def visitCreate(
        nid: NodeId,
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[Journal] =
      journal.create(nid, contractId, mbKey)

    def visitFetchById(
        tmplId: Ref.TypeConId,
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[Journal] =
      journal.queryById(tmplId, contractId) match {
        case Left(needContract) =>
          needContract.resume(mbKey)
        case Right(result) =>
          result
      }

    def visitQueryByKey(
        key: GlobalKey,
        result: Vector[ContractId],
        exhaustive: Boolean,
    ): ErrOr[Journal] = {
      if (result.isEmpty && !exhaustive)
        throw new IllegalArgumentException(
          s"visitQueryByKey: result cannot be empty when exhaustive is false"
        )
      for {
        entry <-
          journal.queryNByKey(key, if (exhaustive) result.length + 1 else result.length) match {
            case Left(NeedKey(_, _, resume)) =>
              val inputContracts = result.view.filterNot(journal.localContracts.contains)
              val either =
                if (exhaustive)
                  resume(
                    inputContracts ++ journal.onlyQueriedById(key),
                    NeedKeyProgression.Finished,
                  )
                else
                  resume(inputContracts, InProgress)
              either match {
                case Right(value) =>
                  value
                case Left(_) =>
                  Left(TransactionError.InconsistentContractKey(key))
              }
            case Right(errOrTuple) =>
              errOrTuple
          }
        (mapping, newState) = entry
        _ <- Either.cond(
          mapping.queue == result && mapping.exhaustive == exhaustive,
          (),
          TransactionError.InconsistentContractKey(key),
        )
      } yield newState
    }

    def visitFetch(
        tmplId: Ref.TypeConId,
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): ErrOr[Journal] =
      if (byKey)
        visitQueryByKey(mbKey.get, Vector(contractId), exhaustive = false)
      else
        visitFetchById(tmplId, contractId, mbKey)

    def visitExercise(
        nodeId: NodeId,
        tmplId: Ref.TypeConId,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): ErrOr[Journal] =
      for {
        state <- this.visitFetch(tmplId, targetId, mbKey, byKey)
        state <-
          if (consuming) {
            state
              .archive(tmplId, targetId, nodeId)
              .getOrElse(
                // This should never happen since visitFetch should have already verified that the contract is known and active.
                throw new IllegalStateException(
                  s"visitExercise: archive failed for $targetId at node $nodeId"
                )
              )
          } else
            Right(state)
      } yield state

    def handleCreate(nid: NodeId, node: Node.Create): ErrOr[Journal] =
      visitCreate(nid, node.coid, node.gkeyOpt)

    def handleFetch(node: Node.Fetch): ErrOr[Journal] =
      visitFetch(node.templateId, node.coid, node.gkeyOpt, node.byKey)

    def handleQueryByKey(node: Node.QueryByKey): ErrOr[Journal] =
      visitQueryByKey(node.gkey, node.result, node.exhaustive)

    def handleExercise(nid: NodeId, exe: Node.Exercise): ErrOr[Journal] =
      visitExercise(
        nid,
        exe.templateId,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      )

    def handleNode(
        id: NodeId,
        node: Node.Action,
    ): ErrOr[Journal] =
      node match {
        case create: Node.Create =>
          handleCreate(id, create)
        case fetch: Node.Fetch =>
          handleFetch(fetch)
        case queryByKey: Node.QueryByKey =>
          handleQueryByKey(queryByKey)
        case exercise: Node.Exercise =>
          handleExercise(id, exercise)
      }

    def beginRollback: Journal =
      journal.beginTry

    def endRollback: Either[Set[NodeId], Journal] =
      journal.abortTry
  }

  sealed abstract class Mode extends Product with Serializable

  object Mode {

    /** Default mode used to configure contract state machines when PV!=dev.
      */
    val default: Mode = NoKey

    /** Default mode used to configure contract state machines when in PV=dev.
      */
    val devDefault: Mode = Key

    // Ledger-effect rollback is rejected: a write inside a rolled-back scope fails the whole
    // transaction. New behaviour.
    case object Key extends Mode

    // Ledger-effect rollback is allowed: writes inside a rolled-back scope are undone locally.
    // Backward-compatible.
    case object NoKey extends Mode
  }

  def empty(mode: Mode): Journal =
    empty(mode == Mode.NoKey)

  def empty(authorizeRollBack: Boolean = true): Journal =
    Journal.Impl(
      authorizeRollback = authorizeRollBack,
      localContracts = immutable.VectorMap.empty,
      inputContracts = Set.empty,
      internalKeyInputs = Map.empty,
      activeLedgerState = Journal.ActiveLedgerState(
        createdInThisTimeline = Map.empty,
        consumedBy = Map.empty,
        localKeys = Map.empty,
      ),
      rollbackStack = Nil,
    )

}
