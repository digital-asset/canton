// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Monad
import cats.data.StateT
import com.daml.lf.data.Ref.{Location, QualifiedName}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.Value
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.{LfChoiceName, LfPackageId, LfPartyId}

import scala.collection.immutable.HashMap

object LfTransactionBuilder {

  type NodeIdState = Int

  type LfAction = (LfNodeId, Map[LfNodeId, LfActionNode])

  // Helper methods for Daml-LF types
  val defaultPackageId: LfPackageId = LfPackageId.assertFromString("pkg")
  val defaultTemplateId: Ref.Identifier =
    Ref.Identifier(defaultPackageId, QualifiedName.assertFromString("module:template"))

  val defaultLocation =
    Location(
      LfPackageId.assertFromString("package-id"),
      Ref.ModuleName.assertFromString("module"),
      "definition",
      (0, 0),
      (0, 0),
    )

  val alice: LfPartyId = Ref.IdString.Party.assertFromString("alice::identity")
  val bob: LfPartyId = Ref.IdString.Party.assertFromString("bob::identity")
  val charlie: LfPartyId = Ref.IdString.Party.assertFromString("charlie::identity")

  val defaultGlobalKey: LfGlobalKey =
    LfGlobalKey(defaultTemplateId, LfTransactionUtil.assertNoContractIdInKey(Value.ValueUnit))

  def allocateNodeId[M[_]](implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfNodeId] =
    for {
      nodeId <- StateT.get[M, NodeIdState]
      _ <- StateT.set[M, NodeIdState](nodeId + 1)
    } yield LfNodeId(nodeId)

  def exerciseFromLf[M[_]](lfExercise: LfNodeExercises, children: List[LfAction])(implicit
      monadInstance: Monad[M]
  ): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
      childrenIds = children.map(_._1)
      childrenMap = children.map(_._2).fold(Map.empty[LfNodeId, LfActionNode])(_ ++ _)
      nodeWithChildren = lfExercise.copy(children = childrenIds.to(ImmArray))
    } yield (nodeId, childrenMap ++ Map(nodeId -> nodeWithChildren))

  def createFromLf[M[_]](
      lfCreate: LfNodeCreate
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
    } yield (nodeId, Map(nodeId -> lfCreate))

  def createWithId[M[_]](
      contractInstance: LfContractInst,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfKeyWithMaintainers] = None,
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, (LfAction, LfContractId.V1)] =
    for {
      nodeId <- allocateNodeId
      cid = ExampleTransactionFactory.unsuffixedId(nodeId.index)
      unversionedContractInstance = contractInstance.unversioned
      node = LfNodeCreate(
        cid,
        unversionedContractInstance.template,
        unversionedContractInstance.arg,
        unversionedContractInstance.agreementText,
        signatories = signatories,
        stakeholders = signatories ++ observers,
        key,
        ExampleTransactionFactory.transactionVersion,
      )
    } yield ((nodeId, Map(nodeId -> node)), cid)

  def fetchFromLf[M[_]](
      lfFetch: LfNodeFetch
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
    } yield (nodeId, Map(nodeId -> lfFetch))

  def lookupByKeyFromLf[M[_]](
      lfLookupByKey: LfNodeLookupByKey
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
    } yield (nodeId, Map(nodeId -> lfLookupByKey))

  def exercise[M[_]](
      children: List[LfAction],
      targetCoid: LfContractId,
      templateId: Ref.Identifier = defaultTemplateId,
      consuming: Boolean = true,
      args: Value = Value.ValueUnit,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      choiceObservers: Set[LfPartyId] = Set.empty,
      actingParties: Set[LfPartyId] = Set.empty,
      exerciseResult: Option[Value] = Some(Value.ValueNone),
      key: Option[LfKeyWithMaintainers] = None,
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    exerciseFromLf[M](
      LfNodeExercises(
        targetCoid = targetCoid,
        templateId = templateId,
        interfaceId = None,
        choiceId = LfChoiceName.assertFromString("choice"),
        consuming = consuming,
        actingParties = actingParties,
        chosenValue = args,
        stakeholders = signatories ++ observers,
        signatories = signatories,
        choiceObservers = choiceObservers,
        children = ImmArray.empty[LfNodeId],
        exerciseResult = exerciseResult,
        key = key,
        byKey = key.nonEmpty, // Not true in general, but okay for tests
        ExampleTransactionFactory.transactionVersion,
      ),
      children,
    )

  def create[M[_]](
      cid: LfContractId,
      contractInstance: LfContractInst,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfKeyWithMaintainers] = None,
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] = {
    val unversionedContractInstance = contractInstance.unversioned
    createFromLf(
      LfNodeCreate(
        cid,
        unversionedContractInstance.template,
        unversionedContractInstance.arg,
        unversionedContractInstance.agreementText,
        signatories = signatories,
        stakeholders = signatories ++ observers,
        key,
        ExampleTransactionFactory.transactionVersion,
      )
    )
  }

  def fetch[M[_]](
      cid: LfContractId,
      templateId: Ref.Identifier,
      actingParties: Set[LfPartyId] = Set.empty,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfKeyWithMaintainers] = None,
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    fetchFromLf(
      LfNodeFetch(
        cid,
        templateId,
        actingParties = actingParties,
        signatories = signatories,
        stakeholders = signatories ++ observers,
        key = key,
        byKey = key.nonEmpty, // Not true in general, but okay for tests
        ExampleTransactionFactory.transactionVersion,
      )
    )

  def lookupByKey[M[_]](
      templateId: Ref.Identifier,
      keyWithMaintainers: LfKeyWithMaintainers,
      result: Option[LfContractId],
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    lookupByKeyFromLf(
      LfNodeLookupByKey(
        templateId,
        keyWithMaintainers,
        result,
        ExampleTransactionFactory.transactionVersion,
      )
    )

  def initialState: NodeIdState = 0

  def usedPackages(action: LfAction): Set[LfPackageId] = action match {
    case (_, nodeMap) =>
      val nodeSet = nodeMap.values
      nodeSet.map {
        case c: LfNodeCreate => c.coinst.template.packageId
        case e: LfNodeExercises => e.templateId.packageId
        case f: LfNodeFetch => f.templateId.packageId
        case l: LfNodeLookupByKey => l.templateId.packageId
      }.toSet
  }

  /** Turn a state containing a list of actions into a transaction.
    *
    * @param state The (monadic) list of actions
    */
  def toTransaction[M[_]](
      state: StateT[M, NodeIdState, List[LfAction]]
  )(implicit monadInstance: Monad[M]): M[LfTransaction] =
    state
      .map(
        _.foldRight((List.empty[LfNodeId], Map.empty[LfNodeId, LfNode], Set.empty[LfPackageId])) {
          case (act @ (actionRoot, actionMap), (roots, nodeMap, pkgs)) =>
            (actionRoot +: roots, nodeMap ++ actionMap, pkgs ++ usedPackages(act))
        }
      )
      .map { case (rootNodes, nodeMap, actuallyUsedPkgs) =>
        LfTransaction(nodes = HashMap(nodeMap.toSeq: _*), roots = rootNodes.to(ImmArray))
      }
      .runA(initialState)

}
