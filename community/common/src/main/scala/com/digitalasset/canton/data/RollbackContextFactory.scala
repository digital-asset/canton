// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.implicits.toFoldableOps
import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.TransactionViewDecompositionFactory.RollbackState
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.PathRollbackContext.{
  RollbackSibling,
  tryToPathRollbackContext,
}
import com.digitalasset.canton.util.{Checked, LfTransactionUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.mutable

trait RollbackContextFactory {
  def empty: RollbackContext
  def checkRollbackScopeOrder(presented: Seq[RollbackContext]): Either[String, Unit]
  def checkCreatedContracts(tx: LfVersionedTransaction): Checked[Nothing, String, Unit]
  def fromRollbackState(state: RollbackState): RollbackContext
  def toRollbackState(context: RollbackContext): RollbackState
}

object RollbackContextFactory {
  def apply(protocolVersion: ProtocolVersion): RollbackContextFactory =
    if (protocolVersion >= ProtocolVersion.v36) NoPathRollbackContextFactory
    else PathRollbackContextFactory
}

object NoPathRollbackContextFactory extends RollbackContextFactory {

  // NoPathRollbackContext does not track the path of nested rollbacks
  override def checkRollbackScopeOrder(presented: Seq[RollbackContext]): Either[String, Unit] =
    Either.unit

  // From PV35 contract creation is not allowed inside rollbacks
  override def checkCreatedContracts(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    val referenced = mutable.Map.empty[LfContractId, LfNodeId]
    val created = mutable.Map.empty[LfContractId, LfNodeId]

    def addReference(nodeId: LfNodeId)(
        refId: LfContractId
    ): Unit =
      referenced += (refId -> nodeId)

    def addReferencesByLfValue(nodeId: LfNodeId, refIds: LazyList[LfContractId]): Unit =
      refIds.foreach(addReference(nodeId))

    LfTransactionUtil
      .foldExecutionOrderM(tx.transaction, ())(exerciseBegin = { (nodeId, nodeExercise, _) =>
        val argRefs = nodeExercise.chosenValue.cids
        addReference(nodeId)(nodeExercise.targetCoid)
        addReferencesByLfValue(nodeId, argRefs.to(LazyList))
        Checked.unit[Nothing, String]
      })(leaf = {
        case (nodeId, nf: LfNodeFetch, _) =>
          addReference(nodeId)(nf.coid)
          Checked.unit

        case (nodeId, lookup: LfNodeQueryByKey, _) =>
          lookup.result.traverse_ { cid =>
            addReference(nodeId)(cid)
            Checked.unit
          }
        case (nodeId, nc: LfNodeCreate, _) =>
          val argRefs = nc.arg.cids
          addReferencesByLfValue(nodeId, argRefs.to(LazyList))
          for {
            _ <- referenced.get(nc.coid).traverse_ { otherNodeId =>
              Checked.continue(
                s"Contract id ${nc.coid.coid} created in node $nodeId is referenced before in $otherNodeId"
              )
            }
            _ <- created.put(nc.coid, nodeId).traverse_ { otherNodeId =>
              Checked.continue(
                s"Contract id ${nc.coid.coid} is created in nodes $otherNodeId and $nodeId"
              )
            }
          } yield ()
      })(exerciseEnd = { (nodeId, ne, _) =>
        val resultRefs = ne.exerciseResult.map(_.cids).getOrElse(Set.empty)
        addReferencesByLfValue(nodeId, resultRefs.to(LazyList))
        Checked.unit
      })(
        rollbackBegin = { (_, _, _) => Checked.unit }
      )(
        rollbackEnd = { (_, _, _) => Checked.unit }
      )
  }

  // As the path is not used with NoPath any non-empty path can serve for a rolled back state
  override def toRollbackState(context: RollbackContext): RollbackState =
    if (context.inRollback) RollbackState.empty.enterRollback else RollbackState.empty

  // The path is not taken forward
  override def fromRollbackState(state: RollbackState): RollbackContext = NoPathRollbackContext(
    state.inRollback
  )

  override def empty: RollbackContext = NoPathRollbackContext.empty
}

object PathRollbackContextFactory extends RollbackContextFactory {

  override def empty: RollbackContext = PathRollbackContext.empty

  override def checkRollbackScopeOrder(presented: Seq[RollbackContext]): Either[String, Unit] = {
    val presentedPath = presented.map(checked(PathRollbackContext.tryToPathRollbackContext))
    Either.cond(
      presentedPath == presentedPath.sorted,
      (),
      s"Detected out of order rollback scopes in: $presented",
    )
  }

  override def checkCreatedContracts(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var rbContext = PathRollbackContext.empty
    val referenced = mutable.Map.empty[LfContractId, LfNodeId]
    val created = mutable.Map.empty[LfContractId, (LfNodeId, PathRollbackScope)]

    // Check that references to a locally created contract are within the rollback scope of the creation.
    // Must only be checked for inputs to a node because reference inside an LF value can happen,
    // e.g., if the contract ID escapes its rollback scope via exceptions.
    def checkRollbackVisibility(nodeId: LfNodeId)(
        refId: LfContractId
    ): Checked[Nothing, String, Unit] =
      created.get(refId).traverse_ { case (createNodeId, createdScope) =>
        val referenceRbScope = rbContext.rollbackScope
        Checked.fromEitherNonabort(())(
          Either.cond(
            referenceRbScope.path.startsWith(createdScope.path),
            (),
            s"Contract id ${refId.coid} created node with $createNodeId in rollback scope ${createdScope.path
                .mkString(".")} referenced outside in rollback scope ${referenceRbScope.path
                .mkString(".")} of node $nodeId",
          )
        )
      }

    def addReference(nodeId: LfNodeId)(
        refId: LfContractId
    ): Unit =
      referenced += (refId -> nodeId)

    def addReferencesByLfValue(nodeId: LfNodeId, refIds: LazyList[LfContractId]): Unit =
      refIds.foreach(addReference(nodeId))

    LfTransactionUtil
      .foldExecutionOrderM(tx.transaction, ())(exerciseBegin = { (nodeId, nodeExercise, _) =>
        val argRefs = nodeExercise.chosenValue.cids
        addReference(nodeId)(nodeExercise.targetCoid)
        addReferencesByLfValue(nodeId, argRefs.to(LazyList))
        checkRollbackVisibility(nodeId)(nodeExercise.targetCoid)
      })(leaf = {
        case (nodeId, nf: LfNodeFetch, _) =>
          addReference(nodeId)(nf.coid)
          checkRollbackVisibility(nodeId)(nf.coid)
        case (nodeId, lookup: LfNodeQueryByKey, _) =>
          lookup.result.traverse_ { cid =>
            addReference(nodeId)(cid)
            checkRollbackVisibility(nodeId)(cid)
          }
        case (nodeId, nc: LfNodeCreate, _) =>
          val argRefs = nc.arg.cids
          addReferencesByLfValue(nodeId, argRefs.to(LazyList))
          for {
            _ <- referenced.get(nc.coid).traverse_ { otherNodeId =>
              Checked.continue(
                s"Contract id ${nc.coid.coid} created in node $nodeId is referenced before in $otherNodeId"
              )
            }
            _ <- created.put(nc.coid, (nodeId, rbContext.rollbackScope)).traverse_ {
              case (otherNodeId, _) =>
                Checked.continue(
                  s"Contract id ${nc.coid.coid} is created in nodes $otherNodeId and $nodeId"
                )
            }
          } yield ()
      })(exerciseEnd = { (nodeId, ne, _) =>
        val resultRefs = ne.exerciseResult.map(_.cids).getOrElse(Set.empty)
        addReferencesByLfValue(nodeId, resultRefs.to(LazyList))
        Checked.unit
      })(rollbackBegin = { (_, _, _) =>
        rbContext = rbContext.enterRollback
        Checked.unit
      })(rollbackEnd = { (_, _, _) =>
        rbContext = checked(rbContext.tryExitRollback)
        Checked.unit
      })
  }

  override def toRollbackState(context: RollbackContext): RollbackState = {
    val pathContext = checked(tryToPathRollbackContext(context))
    RollbackState(pathContext.path: Vector[PositiveInt], pathContext.nextChild: PositiveInt)
  }

  override def fromRollbackState(state: RollbackState): RollbackContext =
    PathRollbackContext(state.path: Vector[RollbackSibling], state.nextChild: RollbackSibling)

}
