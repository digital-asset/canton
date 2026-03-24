// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.collection.View

case class NeedContract[+X](resume: Option[GlobalKey] => ErrOr[X])

trait NeedKeyContinuationToken

sealed trait NeedKeyProgression

object NeedKeyProgression {
  // States: Unstarted -> InProgress -> Finished
  //
  // CanContinue = { Unstarted, InProgress }  — progression not yet exhausted
  // HasStarted  = { InProgress, Finished }   — valid resume argument

  /** The progression can yield more results. */
  sealed trait CanContinue extends NeedKeyProgression

  /** The progression has been started; valid as a resume argument. */
  sealed trait HasStarted extends NeedKeyProgression

  case object Unstarted extends CanContinue
  final case class InProgress(token: NeedKeyContinuationToken) extends CanContinue with HasStarted
  case object Finished extends HasStarted
}

case class NeedKey[+X](
    n: Int,
    progression: NeedKeyProgression.CanContinue,
    resume: (
        View[ContractId],
        NeedKeyProgression.HasStarted,
    ) => Either[NeedKey[X], ErrOr[(KeyMapping, X)]],
)
final case class KeyMapping(queue: Vector[ContractId], exhaustive: Boolean)

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
    def Unknown: KeyInput = KeyInput(
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

  final case class StateMachineResult (
                                        inputContractIds: Set[ContractId],
                                        globalKeyInputs: Map[GlobalKey, KeyMapping],
                                        localKeys: Map[GlobalKey, Vector[ContractId]],
                                        consumed: Set[ContractId]
                                      )

  object StateMachineResult {
    def empty = StateMachineResult(
      inputContractIds = Set.empty[ContractId],
      globalKeyInputs = Map.empty[GlobalKey, KeyMapping],
      localKeys = Map.empty[GlobalKey, Vector[ContractId]],
      consumed = Set.empty[ContractId]
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

  trait LLState[Nid] {
    private[lf] def create(nid: Nid, cid: ContractId, mbKey: Option[GlobalKey]): ErrOr[LLState[Nid]]
    private[lf] def queryById(
        cid: ContractId
    ): Either[NeedContract[LLState[Nid]], ErrOr[LLState[Nid]]]
    private[lf] def queryNByKey(
        key: GlobalKey,
        n: Int,
    ): Either[NeedKey[LLState[Nid]], ErrOr[(KeyMapping, LLState[Nid])]]
    // return None if cid is unknown
    private[lf] def archive(cid: ContractId, nid: Nid): Option[ErrOr[State[Nid]]]
    private[lf] def beginTry: LLState[Nid]

    // TODO(#31454)
    // This must return a Nid for now until we make EffectfulRollback
    // polymorphic in Nid, which would require all use sites of TransactionError
    // to become polymorphic in Nid also.
    private[lf] def rollbackTry: Either[Set[Nid], LLState[Nid]]
    private[lf] def endTry: LLState[Nid]

    private[lf] def onlyQueriedById(gkey: GlobalKey): Set[ContractId]

    private[lf] def knownContract(cid: ContractId): Boolean

    // TODO(#30398) review the interface of LLState
    //  - check if we really need those methods
    //  - check if it will be better to have lazz val for the Sets/Maps
    def knownContracts: Set[ContractId]
    def knownKeys: Set[GlobalKey]

    def inputContracts: Set[ContractId]
    def keyInputs: Map[GlobalKey, KeyMapping]

    def activeContract(cid: ContractId): Boolean
    def activeKeyMapping(key: GlobalKey): KeyMapping

    def toStateMachineResult: StateMachineResult

    //  TODO(#30913): remove this method
    def consumedByOrInactive(cid: ContractId): Option[Either[Nid, Unit]]

    //  TODO(#30913): remove this method
    def localContracts: Map[ContractId, _]
  }

  object HHState {
    private case object ContinuationToken extends NeedKeyContinuationToken
    private val InProgress = NeedKeyProgression.InProgress(ContinuationToken)
  }

  implicit class HHState[Nid](val llState: LLState[Nid]) extends AnyVal {

    import HHState._

    def visitCreate(
        nid: Nid,
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[LLState[Nid]] =
      llState.create(nid, contractId, mbKey)

    def visitFetchById(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[LLState[Nid]] =
      llState.queryById(contractId) match {
        case Left(needContract) =>
          needContract.resume(mbKey)
        case Right(result) =>
          result.map(_ => llState)
      }

    def visitQueryByKey(
        key: GlobalKey,
        result: Vector[ContractId],
        exhaustive: Boolean,
    ): ErrOr[LLState[Nid]] = {
      if (result.isEmpty && !exhaustive)
        throw new IllegalArgumentException(
          s"visitQueryByKey: result cannot be empty when exhaustive is false"
        )
      for {
        entry <-
          llState.queryNByKey(key, if (exhaustive) result.length + 1 else result.length) match {
            case Left(NeedKey(_, _, resume)) =>
              val inputContracts = result.view.filterNot(llState.localContracts.contains)
              val either =
                if (exhaustive)
                  resume(
                    inputContracts ++ llState.onlyQueriedById(key),
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
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): ErrOr[LLState[Nid]] =
      if (byKey)
        visitQueryByKey(mbKey.get, Vector(contractId), exhaustive = false)
      else
        visitFetchById(contractId, mbKey)

    def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): ErrOr[LLState[Nid]] =
      for {
        state <- this.visitFetch(targetId, mbKey, byKey)
        state <-
          if (consuming) {
            state
              .archive(targetId, nodeId)
              .getOrElse(
                // This should never happen since visitFetch should have already verified that the contract is known and active.
                throw new IllegalStateException(
                  s"visitExercise: archive failed for $targetId at node $nodeId"
                )
              )
          } else
            Right(state)
      } yield state

    def handleCreate(nid: Nid, node: Node.Create): ErrOr[LLState[Nid]] =
      visitCreate(nid, node.coid, node.gkeyOpt)

    def handleFetch(node: Node.Fetch): ErrOr[LLState[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey)

    def handleQueryByKey(node: Node.QueryByKey): ErrOr[LLState[Nid]] =
      visitQueryByKey(node.gkey, node.result, node.exhaustive)

    def handleExercise(nid: Nid, exe: Node.Exercise): ErrOr[LLState[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      )

    def handleNode(
        id: Nid,
        node: Node.Action,
    ): ErrOr[LLState[Nid]] =
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

    def beginRollback: LLState[Nid] =
      llState.beginTry

    def endRollback: Either[Set[Nid], LLState[Nid]] =
      llState.rollbackTry
  }

  private[transaction] case class State[Nid](
      authorizeRollback: Boolean,
      localContracts: Map[ContractId, Option[GlobalKey]],
      override val inputContracts: Set[ContractId],
      val internalKeyInputs: Map[GlobalKey, KeyInput],
      activeLedgerState: ActiveLedgerState[Nid],
      rollbackStack: List[ActiveLedgerState[Nid]],
  ) extends LLState[Nid] {

    private def createdInThisTimeline: Map[ContractId, Nid] =
      activeLedgerState.createdInThisTimeline

    def localKeys: Map[GlobalKey, Vector[ContractId]] =
      activeLedgerState.localKeys

    override def keyInputs: Map[GlobalKey, KeyMapping] =
      internalKeyInputs.transform((_, keyInput) => keyInput.toKeyMapping)

    def consumedBy: Map[ContractId, Nid] =
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

    def create(nid: Nid, cid: ContractId, mbKey: Option[GlobalKey]): ErrOr[State[Nid]] =
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
    ): Option[GlobalKey] => ErrOr[State[Nid]] = {
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
        cid: ContractId
    ): Either[NeedContract[State[Nid]], ErrOr[State[Nid]]] =
      Either.cond(
        knownContract(cid),
        consumedBy.get(cid).map(TransactionError.AlreadyConsumed(cid, _)).toLeft(this),
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
    ): Either[NeedKey[State[Nid]], ErrOr[(KeyMapping, State[Nid])]] =
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
    ) => Either[NeedKey[State[Nid]], ErrOr[(KeyMapping, State[Nid])]] =
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
    ): Either[NeedKey[State[Nid]], ErrOr[(KeyMapping, LLState[Nid])]] = {
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

    def archive(cid: ContractId, nid: Nid): Option[ErrOr[State[Nid]]] =
      queryById(cid).toOption.map(
        _.map(state =>
          state.copy(
            activeLedgerState = state.activeLedgerState.copy(
              consumedBy = state.activeLedgerState.consumedBy.updated(cid, nid)
            )
          )
        )
      )

    def beginTry: State[Nid] =
      this.copy(rollbackStack = activeLedgerState :: rollbackStack)

    def rollbackTry: Either[Set[Nid], State[Nid]] =
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
          if (activeLedgerState.createdInThisTimeline.size != headState.createdInThisTimeline.size || activeLedgerState.consumedBy.size != headState.consumedBy.size) {
            val consumedByChanges =
              activeLedgerState.consumedBy.view.filterKeys(consumedBy.keySet -- headState.consumedBy.keySet).values.toSet

            val locallyCreatedChanges =
              activeLedgerState.createdInThisTimeline.view.filterKeys(activeLedgerState.createdInThisTimeline.keySet -- headState.createdInThisTimeline.keySet).values.toSet

            Left(consumedByChanges ++ locallyCreatedChanges)
          } else {
            Right(this.copy(rollbackStack = tailStack))
          }
      }

    private[lf] def endTry: State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    override def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] =
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
  }

  case class ActiveLedgerState[Nid](
      createdInThisTimeline: Map[ContractId, Nid],
      consumedBy: Map[ContractId, Nid],
      localKeys: Map[GlobalKey, Vector[ContractId]],
  )

  sealed abstract class Mode extends Product with Serializable

  object Mode {

    /** Default mode used to configure contract state machines when PV!=dev.
      */
    val default: Mode = NoKey

    /** Default mode used to configure contract state machines when in PV=dev.
      */
    val devDefault: Mode = NUCK

    // Disallow ledger effects rollback
    case object NUCK extends Mode {
      override val toString: String = "NUCK"
    }

    // Allow ledger effects rollback
    case object NoKey extends Mode {
      override val toString: String = "NoKey"
    }

    def fromString: String => Option[Mode] = {
      case "nuck" => Some(NUCK)
      case "nokey" => Some(NoKey)
      case _ => None
    }
  }

  def empty[Nid](mode: Mode): State[Nid] =
    empty[Nid](mode == Mode.NoKey)

  def empty[Nid](authorizeRollBack: Boolean = true): State[Nid] =
    State(
      authorizeRollback = authorizeRollBack,
      localContracts = Map.empty,
      inputContracts = Set.empty,
      internalKeyInputs = Map.empty,
      activeLedgerState = ActiveLedgerState(
        createdInThisTimeline = Map.empty,
        consumedBy = Map.empty,
        localKeys = Map.empty,
      ),
      rollbackStack = Nil,
    )

}
