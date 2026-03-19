// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import cats.implicits._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.collection.View

case class NeedContract[X](resume: Option[GlobalKey] => Either[TransactionError, X])

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

case class NeedKeys[X](
    n: Int,
    progression: NeedKeyProgression.CanContinue,
    resume: (
        View[ContractId],
        NeedKeyProgression.HasStarted,
    ) => Either[NeedKeys[X], ErrOr[(KeyMapping, X)]],
)
final case class KeyMapping(queue: Vector[ContractId], exhaustive: Boolean)

object NextGenContractStateMachine {

  private def crash(str: String): Nothing = throw new IllegalStateException(str)

  type KeyResolver = Map[GlobalKey, Vector[ContractId]]

  private[transaction] final case class KeyInput(
      queriedByKey: Vector[ContractId],
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
    def Unknown: KeyInput = KeyInput(Vector.empty, Set.empty, NeedKeyProgression.Unstarted)
  }

  private final case class KeyMappingBuilder(
      queue: Vector[ContractId],
      missing: Int,
  ) {
    def add(contractId: ContractId): KeyMappingBuilder =
      copy(queue = queue :+ contractId, missing = missing - 1)

    def addWhileHasMissing(more: View[ContractId]): KeyMappingBuilder =
      more.take(missing).foldLeft(this)(_.add(_))

    def result: KeyMapping = KeyMapping(queue, missing > 0)
  }

  private object KeyMappingBuilder {
    def empty(n: Int): KeyMappingBuilder = KeyMappingBuilder(Vector.empty, n)
  }

  trait LLState[Nid] {
    private[lf] def create(cid: ContractId, mbKey: Option[GlobalKey]): ErrOr[LLState[Nid]]
    private[lf] def queryById(
        cid: ContractId
    ): Either[NeedContract[State[Nid]], ErrOr[LLState[Nid]]]
    private[lf] def queryNByKey(
        key: GlobalKey,
        n: Int,
    ): Either[NeedKeys[State[Nid]], ErrOr[(KeyMapping, LLState[Nid])]]
    // return None if cid is unknown
    private[lf] def archive(cid: ContractId, nid: Nid): Option[ErrOr[State[Nid]]]
    private[lf] def beginTry: LLState[Nid]
    private[lf] def rollbackTry: ErrOr[LLState[Nid]]
    private[lf] def endTry: LLState[Nid]

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
  }

  implicit class HHState[Nid](val llState: LLState[Nid]) extends AnyVal {

    def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): ErrOr[LLState[Nid]] =
      llState.create(contractId, mbKey)

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
        gk: GlobalKey,
        result: Vector[ContractId],
        exhaustive: Boolean,
    ): ErrOr[LLState[Nid]] =
      for {
        entry <-
          llState.queryNByKey(gk, if (exhaustive) result.length + 1 else result.length) match {
            case Left(NeedKeys(_, _, resume)) =>
              resume(result.view.filterNot(llState.knownContract), NeedKeyProgression.Finished)
                .getOrElse(
                  crash("unexpected NeedKeys with finished progression")
                )
            case Right(errOrTuple) =>
              errOrTuple
          }
        (mapping, newState) = entry
        _ <- Either.cond(
          mapping.queue == result && mapping.exhaustive == exhaustive,
          (),
          TransactionError.InconsistentContractKey(gk),
        )
      } yield newState

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

    def handleCreate(node: Node.Create): ErrOr[LLState[Nid]] =
      visitCreate(node.coid, node.gkeyOpt)

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
          handleCreate(create)
        case fetch: Node.Fetch =>
          handleFetch(fetch)
        case queryByKey: Node.QueryByKey =>
          handleQueryByKey(queryByKey)
        case exercise: Node.Exercise =>
          handleExercise(id, exercise)
      }

    def beginRollback: LLState[Nid] =
      llState.beginTry

    def endRollback: ErrOr[LLState[Nid]] =
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

    private def createdInThisTimeline: Set[ContractId] =
      activeLedgerState.createdInThisTimeline

    def localKeys: Map[GlobalKey, Vector[ContractId]] =
      activeLedgerState.localKeys

    override def keyInputs: Map[GlobalKey, KeyMapping] =
      internalKeyInputs.transform((_, keyInput) => keyInput.toKeyMapping)

    def consumedBy: Map[ContractId, Nid] =
      activeLedgerState.consumedBy

    def knownContract(cid: ContractId): Boolean =
      localContracts.contains(cid) || inputContracts.contains(cid)

    def knownContracts: Set[ContractId] =
      createdInThisTimeline ++ inputContracts

    def knownKeys: Set[GlobalKey] = localKeys.keySet & internalKeyInputs.keySet

    def create(cid: ContractId, mbKey: Option[GlobalKey]): ErrOr[State[Nid]] =
      Either.cond(
        !knownContract(cid),
        this.copy(
          localContracts = this.localContracts.updated(cid, mbKey),
          activeLedgerState = activeLedgerState.copy(
            createdInThisTimeline = this.createdInThisTimeline + cid,
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
    ): Option[GlobalKey] => ErrOr[State[Nid]] = (
        (_: Option[GlobalKey]) => Right(copy(inputContracts = inputContracts + cid))
    )

    def queryById(
        cid: ContractId
    ): Either[NeedContract[State[Nid]], ErrOr[State[Nid]]] =
      Either.cond(
        knownContract(cid),
        consumedBy.get(cid).map(TransactionError.AlreadyConsumed(cid, _)).toLeft(this),
        NeedContract(continueQueryById(cid)),
      )

    private case class ContinueQueryNByKeyAcc(
        inputContracts: Set[ContractId],
        queriedByKey: Vector[ContractId],
        onlyQueriedById: Set[ContractId],
        mapping: KeyMappingBuilder,
    ) {
      def add(cid: ContractId): ErrOr[ContinueQueryNByKeyAcc] =
        // Defensive: because local contract IDs are structurally distinct from input contract IDs,
        // this branch should be unreachable.
        if (localContracts.contains(cid))
          Left(TransactionError.DuplicateContractId(cid))
        else if (inputContracts.contains(cid) && !onlyQueriedById.contains(cid))
          Right(this)
        else
          Right(
            copy(
              inputContracts = inputContracts + cid,
              queriedByKey = queriedByKey :+ cid,
              onlyQueriedById = onlyQueriedById - cid,
              mapping = mapping.add(cid),
            )
          )
    }

    private def continueQueryNByKey(
        key: GlobalKey,
        n: Int,
        acc0: ContinueQueryNByKeyAcc,
    ): (
        View[ContractId],
        NeedKeyProgression.HasStarted,
    ) => Either[NeedKeys[State[Nid]], ErrOr[(KeyMapping, State[Nid])]] = {
      (contracts: View[ContractId], progression: NeedKeyProgression.HasStarted) =>
        contracts.toList.foldM[ErrOr, ContinueQueryNByKeyAcc](acc0)(_.add(_)) match {
          case Right(acc) =>
            progression match {
              case progression: NeedKeyProgression.InProgress if acc.mapping.missing > 0 =>
                Left(
                  NeedKeys(acc.mapping.missing, progression, continueQueryNByKey(key, n, acc))
                )
              case NeedKeyProgression.Finished if acc.onlyQueriedById.nonEmpty =>
                Right(Left(TransactionError.InconsistentContractKey(key)))
              case _ =>
                Right(
                  Right(
                    acc.mapping.result ->
                      copy(
                        inputContracts = acc.inputContracts,
                        internalKeyInputs = internalKeyInputs.updated(
                          key,
                          KeyInput(acc.queriedByKey, acc.onlyQueriedById, progression),
                        ),
                      )
                  )
                )
            }
          case Left(err) =>
            Right(Left(err))
        }
    }

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
    ): Either[NeedKeys[State[Nid]], ErrOr[(KeyMapping, LLState[Nid])]] = {
      if (n <= 0)
        throw new IllegalArgumentException(s"queryNByKey: n must be strictly positive, got $n")
      val mapping = activeKeyMapping(key, n)
      val keyInput = internalKeyInputs.getOrElse(key, KeyInput.Unknown)
      keyInput.progression match {
        case progression: NeedKeyProgression.CanContinue if mapping.missing > 0 =>
          Left(
            NeedKeys(
              mapping.missing,
              progression,
              continueQueryNByKey(
                key,
                n,
                ContinueQueryNByKeyAcc(
                  inputContracts,
                  keyInput.queriedByKey,
                  keyInput.onlyQueriedById,
                  mapping,
                ),
              ),
            )
          )
        case _ =>
          Right(Right(mapping.result -> this))
      }
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

    def rollbackTry: ErrOr[State[Nid]] =
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
          Either.cond(
            activeLedgerState == headState,
            this.copy(rollbackStack = tailStack),
            TransactionError.EffectfulRollbackNotSupported,
          )
      }

    private[lf] def endTry: State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }
  }

  case class ActiveLedgerState[Nid](
      createdInThisTimeline: Set[ContractId],
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
        createdInThisTimeline = Set.empty,
        consumedBy = Map.empty,
        localKeys = Map.empty,
      ),
      rollbackStack = Nil,
    )

}
